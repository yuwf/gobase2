package mrcache

// https://github.com/yuwf/gobase2

import (
	"context"
	"fmt"
	"gobase/goredis"
	"gobase/mysql"
	"gobase/utils"
	"reflect"

	"github.com/rs/zerolog"
)

// T 为数据库结构类型
// 使用场景：查询条件对应多个结果 redis使用多个key缓存这个结果，一条数据对应一个key，即dataKey，有一个索引key记录所有的dataKey
// 索引key：存储所有的dataKey，hash结构，dataKeyValue:dataKey
// dataKey：存储一条mysql数据，存储方式和CacheRow一样，key命名：索引key名_dataKeyField对应的值
// dataKey对应mysql的一个字段，且数据类型只能为基本的数据类型，查询条件和datakey对应的数据要唯一，最好有唯一索引
// 索引key一直缓存所有的数据key，但数据key可能在redis中不存在时，此时就重新加载
type CacheRows[T any] struct {
	*Cache
}

// condFields：查询字段，不可为空， 查询的数据有多条
func NewCacheRows[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, tableCount, tableIndex int, condFields []string, dataKeyField string) (*CacheRows[T], error) {
	cache, err := NewCache[T](redis, mysql, tableName, tableCount, tableIndex, condFields)
	if err != nil {
		return nil, err
	}
	cache.keyPrefix = "mrrs"

	// 验证dataKeyField 只能是基本的数据int 和 string 类型
	dataKeyFieldIndex := cache.FindIndexByTag(dataKeyField)
	if dataKeyFieldIndex == -1 {
		err := fmt.Errorf("tag:%s not find in %s", dataKeyField, cache.T.String())
		return nil, err
	}
	if !cache.IsBaseType(dataKeyFieldIndex) {
		err := fmt.Errorf("tag:%s(%s) as dataKeyField type error", dataKeyField, cache.T.String())
		return nil, err
	}

	cache.dataKeyField = dataKeyField
	cache.dataKeyFieldIndex = dataKeyFieldIndex

	c := &CacheRows[T]{
		Cache: cache,
	}
	return c, nil
}

func (c *CacheRows[T]) genDataKey(key string, dataKeyValue interface{}) string {
	return key + "_" + c.fmtBaseType(dataKeyValue)
}

// 读取符合condValues的全部数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAll(ctx context.Context, condValues []interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Msg("CacheRows GetAll")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 从Redis中读取
	dest, err := c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData && GetPass(key) {
		return make([]*T, 0), nil
	}

	// 其他情况不处理执行下面的预加载
	preData, err := c.preLoadAll(ctx, key, condValues)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest, err = c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return make([]*T, 0), nil
	} else {
		return nil, err
	}
}

// 读取符合condValues的部分数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValues 填充dataKeyField类型的值，要查询的值
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Gets(ctx context.Context, condValues []interface{}, dataKeyValues []interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValues", dataKeyValues).Err(_err_).Interface("rst", _rst_).Msg("CacheRows Gets")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	return c.gets(ctx, key, condValues, dataKeyValues)
}

func (c *CacheRows[T]) gets(ctx context.Context, key string, condValues []interface{}, dataKeyValues []interface{}) (_rst_ []*T, _err_ error) {
	if len(dataKeyValues) == 0 {
		return make([]*T, 0), nil
	}

	keys := make([]string, 0, 1+len(dataKeyValues))
	keys = append(keys, key)
	// 检测 dataKeyValue的类型和值
	for _, dataKeyValue := range dataKeyValues {
		err := c.checkValue(c.dataKeyFieldIndex, dataKeyValue)
		if err != nil {
			return nil, err
		}
		keys = append(keys, c.fmtBaseType(dataKeyValue))
	}

	// 从Redis中读取
	dest, err := c.redisGets(ctx, keys)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData && GetPass(key) {
		return make([]*T, 0), nil
	}

	// 其他情况不处理执行下面的预加载
	preData, err := c.preLoads(ctx, key, condValues, dataKeyValues)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里必须递归调用，因为不知道别的preLoads有没有加载自己的dataKeyValues
	return c.gets(ctx, key, condValues, dataKeyValues)
}

// 读取一条数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValue：数据key的值
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Get(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_rst_ *T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Interface("rst", _rst_).Msg("CacheRows Get")
	}, ErrNullData)()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	err = c.checkValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return nil, err
	}

	return c.get(ctx, key, condValues, dataKeyValue, false)
}

// 第一步是否直接跳过Redis加载
func (c *CacheRows[T]) get(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, ingnoreRedis bool) (_rst_ *T, _err_ error) {
	// 从Redis中读取
	if !ingnoreRedis {
		redisParams := make([]interface{}, 0, 2+len(c.Tags))
		redisParams = append(redisParams, c.expire)
		redisParams = append(redisParams, c.fmtBaseType(dataKeyValue))
		redisParams = append(redisParams, c.RedisTagsInterface()...)

		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := c.redis.DoScript2(ctx, rowsGetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
		if err == nil {
			return dest, nil
		} else if goredis.IsNilError(err) && (GetPass(key) || GetPass(c.genDataKey(key, dataKeyValue))) {
			return nil, ErrNullData
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, dataKeyValue, nil)
	if err != nil {
		return nil, err
	}
	// 预加载的数据就是新增的
	if preData != nil {
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下，正常来说这种情况很少
	redisParams := make([]interface{}, 0, 2+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.fmtBaseType(dataKeyValue))
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
	err = c.redis.DoScript2(ctx, rowsGetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		return nil, ErrNullData
	} else {
		return nil, err
	}
}

// 读取数据
// cond：查询条件变量
// dataKeyValue：数据key的值
// 返回值：是否存在
func (c *CacheRows[T]) Exist(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_rst_ bool, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Interface("rst", _rst_).Msg("CacheRows Exist")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return false, err
	}

	// 数据key的类型和值
	err = c.checkValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return false, err
	}

	// 从Redis中读取
	rstV, err := c.redis.HExists(ctx, key, c.fmtBaseType(dataKeyValue)).Result()
	if err == nil {
		if rstV {
			return true, nil
		} else if GetPass(key) || GetPass(c.genDataKey(key, dataKeyValue)) {
			return false, nil
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, dataKeyValue, nil)
	if err != nil {
		if err == ErrNullData { // 空数据直接返回false
			return false, nil
		}
		return false, err
	}
	// 有预加载数据，说明存在
	if preData != nil {
		return true, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	rstV, err = c.redis.HExists(ctx, key, c.fmtBaseType(dataKeyValue)).Result()
	if err == nil {
		if rstV {
			return true, nil
		}
		return false, nil
	} else {
		return false, err
	}
}

// 添加数据，需要外部已经确保没有数据了调用此函数，直接添加数据 ctx: CtxKey_NR
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// data：修改内容
// -     【内部必须要有设置的dataKeyField字段】
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，含有函【CtxKey_NR】时不返回值
// _incr_： 自增ID，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Add(ctx context.Context, condValues []interface{}, data interface{}) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msg("CacheRows Add")
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, dataM)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, dataInfo.TagElemsMap())
	}
}

func (c *CacheRows[T]) add(ctx context.Context, condValues []interface{}, data map[string]interface{}) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyValue, ok := data[c.dataKeyField]
	if !ok {
		err := fmt.Errorf("not find %s field", c.dataKeyField)
		return nil, nil, err
	}

	incrValue, err := c.addToMySQL(ctx, condValues, data)
	if err != nil {
		return nil, nil, err
	}
	DelPass(key)
	DelPass(c.genDataKey(key, dataKeyValue))

	nr := ctx.Value(CtxKey_NR) != nil
	if nr {
		// 不需要返回值
		return nil, incrValue, nil
	}

	rst, err := c.get(ctx, key, condValues, dataKeyValue, true)
	if err != nil {
		return nil, nil, err
	}
	return rst, incrValue, nil
}

// 删除全部数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValue：数据key的值
// 返回值：error
func (c *CacheRows[T]) DelAll(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msg("CacheRows DelAll")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelAllScript, []string{key})
	if cmd.Err() != nil {
		return cmd.Err()
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues)) // 删mysql
	if err != nil {
		return err
	}

	return nil
}

// 删除数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValue：数据key的值
// 返回值：error
func (c *CacheRows[T]) Del(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Msg("CacheRows Del")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	err = c.checkValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelScript, []string{key, c.genDataKey(key, dataKeyValue)}, dataKeyValue)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue)) // 删mysql
	if err != nil {
		return err
	}

	return nil
}

// 只Cache删除数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：error
func (c *CacheRows[T]) DelAllCache(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msg("CacheRows DelAllCache")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelAllScript, []string{key})
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 只Cache删除数据
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValue：数据key的值
// 返回值：error
func (c *CacheRows[T]) DelCache(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Msg("CacheRows DelCache")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	err = c.checkValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelScript, []string{key, c.genDataKey(key, dataKeyValue)}, dataKeyValue)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 写数据 ctx: CtxKey_NR, CtxKey_NEC
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有设置的dataKeyField字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略set
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，含有函【CtxKey_NR】时不返回值
// _incr_： 自增ID，ctx含有【CtxKey_NEC】时如果新增数据，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Set(ctx context.Context, condValues []interface{}, data interface{}) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msg("CacheRows Set")
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, dataM)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, dataInfo.TagElemsMap())
	}
}

func (c *CacheRows[T]) set(ctx context.Context, condValues []interface{}, data map[string]interface{}) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	// 判断data中是否含有数据key字段
	dataKeyValue, ok := data[c.dataKeyField]
	if !ok {
		err := fmt.Errorf("not find %s field", c.dataKeyField)
		return nil, nil, err
	}
	nr := ctx.Value(CtxKey_NR) != nil

	var dest *T
	if nr {
		err = c.setSave(ctx, key, condValues, dataKeyValue, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, dataKeyValue, data)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}
	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, dataKeyValue, utils.If(ctx.Value(CtxKey_NEC) != nil, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	if nr {
		err = c.setSave(ctx, key, condValues, dataKeyValue, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, dataKeyValue, data)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 没有返回值
func (c *CacheRows[T]) setSave(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, data map[string]interface{}) error {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// 获取Redis参数
	redisParams := c.redisSetParam(data)
	cmd := c.redis.DoScript2(ctx, rowsSetScript, []string{key, c.genDataKey(key, dataKeyValue)}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql，添加上数据key字段
		mysqlUnlock = true
		err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue), data, key, func(err error) {
			defer unlock()
			if err != nil {
				c.redis.Del(ctx, c.genDataKey(key, dataKeyValue)) // mysql错了 删除数据键
			}
		})
		if err != nil {
			return err
		}
		return nil
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 返回值*T
func (c *CacheRows[T]) setGetTSave(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, data map[string]interface{}) (*T, error) {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return nil, err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// 获取Redis参数
	redisParams := c.redisSetGetParam(c.Tags, data, false)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, c.genDataKey(key, dataKeyValue)}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据key字段
			mysqlUnlock = true
			err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue), data, key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, c.genDataKey(key, dataKeyValue)) // mysql错了 删除数据键
				}
			})
			if err != nil {
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, c.genDataKey(key, dataKeyValue)) // 删除数据键
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
	}
}

// 写数据 ctx: CtxKey_NR, CtxKey_NEC
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有设置的dataKeyField字段
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉modify
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，含有函【CtxKey_NR】时不返回值
// _incr_： 自增ID，ctx含有【CtxKey_NEC】时如果新增数据，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Modify(ctx context.Context, condValues []interface{}, data interface{}) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msg("CacheRows Modify")
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, dataM)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, dataInfo.TagElemsMap())
	}
}

func (c *CacheRows[T]) modify(ctx context.Context, condValues []interface{}, data map[string]interface{}) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	// 判断data中是否含有数据key字段
	dataKeyValue, ok := data[c.dataKeyField]
	if !ok {
		err := fmt.Errorf("not find %s field", c.dataKeyField)
		return nil, nil, err
	}
	nr := ctx.Value(CtxKey_NR) != nil

	// 按c.Tags的顺序构造 有顺序
	modifydata := &ModifyData{data: data}
	for i, tag := range c.Tags {
		if v, ok := data[tag]; ok {
			modifydata.tags = append(modifydata.tags, tag)
			modifydata.values = append(modifydata.values, v)
			// 填充c.Fields的类型
			modifydata.rsts = append(modifydata.rsts, reflect.New(c.Fields[i].Type).Elem())
		}
	}

	var dest *T
	if nr {
		err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, dataKeyValue, modifydata)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, dataKeyValue, utils.If(ctx.Value(CtxKey_NEC) != nil, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		if nr {
			return nil, incrValue, nil
		}
		return preData, incrValue, nil
	}

	// 再次写数据，这种情况很少
	if nr {
		err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, dataKeyValue, modifydata)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 写数据 ctx: CtxKey_NEC
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有设置的dataKeyField字段
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉modify
// 返回值
// _rst_ ： 是data结构类型的指针，修改后的值
// _incr_： 自增ID，ctx含有【CtxKey_NEC】时如果新增数据，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Modify2(ctx context.Context, condValues []interface{}, data interface{}) (_rst_ interface{}, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msg("CacheRow Modify2")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkStructData(data)
	if err != nil {
		return nil, nil, err
	}
	dataM := dataInfo.TagElemsMap()

	// 判断data中是否含有数据key字段
	dataKeyValue, ok := dataM[c.dataKeyField]
	if !ok {
		err := fmt.Errorf("not find %s field", c.dataKeyField)
		return nil, nil, err
	}

	// 修改后的值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 按c.Tags的顺序构造 有顺序
	modifydata := &ModifyData{data: dataM}
	for _, tag := range c.Tags {
		if v, ok := dataM[tag]; ok {
			modifydata.tags = append(modifydata.tags, tag)
			modifydata.values = append(modifydata.values, v)
			// 填充res对应字段
			modifydata.rsts = append(modifydata.rsts, resInfo.Elemts[resInfo.FindIndexByTag(tag)])
		}
	}

	// 写数据
	err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	if err == nil {
		return res, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, dataKeyValue, utils.If(ctx.Value(CtxKey_NEC) != nil, dataM, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByStructType(preData, c.StructType)
		CopyTo(dInfo, resInfo)
		return res, incrValue, nil
	}

	// 再次写数据
	err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	if err == nil {
		return res, nil, nil
	} else {
		return nil, nil, err
	}
}

// 写数据 ctx: CtxKey_NEC
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有设置的dataKeyField字段
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉modify
// 返回值
// _rst_ ： 是data结构类型map，修改后的值
// _incr_： 自增ID，ctx含有【CtxKey_NEC】时如果新增数据，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) ModifyM2(ctx context.Context, condValues []interface{}, data map[string]interface{}) (_rst_ map[string]interface{}, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msg("CacheRow ModifyM2")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	// 检查data数据
	err = c.checkMapData(data)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyValue, ok := data[c.dataKeyField]
	if !ok {
		err := fmt.Errorf("not find %s field", c.dataKeyField)
		return nil, nil, err
	}

	// 按c.Tags的顺序构造 有顺序
	modifydata := &ModifyData{data: data}
	for i, tag := range c.Tags {
		if v, ok := data[tag]; ok {
			modifydata.tags = append(modifydata.tags, tag)
			modifydata.values = append(modifydata.values, v)
			// 填充map的值类型，如v=nil 在map中是没有类型的，用c.Fields的
			if v != nil {
				modifydata.rsts = append(modifydata.rsts, reflect.New(reflect.TypeOf(v)).Elem())
			} else {
				modifydata.rsts = append(modifydata.rsts, reflect.New(c.Fields[i].Type).Elem())
			}
		}
	}

	// 写数据
	err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	if err == nil {
		return modifydata.TagsRstsMap(), nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, dataKeyValue, utils.If(ctx.Value(CtxKey_NEC) != nil, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByStructType(preData, c.StructType)
		modifydata.RstsFrom(dInfo)
		return modifydata.TagsRstsMap(), incrValue, nil
	}

	// 再次写数据
	err = c.modifyGetSave(ctx, key, condValues, dataKeyValue, modifydata)
	if err == nil {
		return modifydata.TagsRstsMap(), nil, nil
	} else {
		return nil, nil, err
	}
}

// 不要返回值
// 会填充modifydata.rsts
func (c *CacheRows[T]) modifyGetSave(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, modifydata *ModifyData) error {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// 获取Redis参数
	redisParams := c.redisSetGetParam(modifydata.tags, modifydata.data, true)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, c.genDataKey(key, dataKeyValue)}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(modifydata.rsts)
		if err == nil {
			// 同步mysql
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue), modifydata.TagsRstsMap(), key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, key) // mysql错了 要删缓存
				}
			})
			if err != nil {
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除dataKey返回错误
			c.redis.Del(ctx, c.genDataKey(key, dataKeyValue))
			return err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 返回值*T
// 会填充modifydata.rsts
func (c *CacheRows[T]) modifyGetTSave(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, modifydata *ModifyData) (*T, error) {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return nil, err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// redis参数
	redisParams := c.redisSetGetParam(c.Tags, modifydata.data, true)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, c.genDataKey(key, dataKeyValue)}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的resInfo中再保存，否则会保存整个T结构
			modifydata.RstsFrom(destInfo)
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue), modifydata.TagsRstsMap(), key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, c.genDataKey(key, dataKeyValue)) // mysql错了 要删缓存
				}
			})
			if err != nil {
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, key)
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
	}
}

// 预加载所有数据，确保写到Redis中
// key：主key
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值
// []*T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoadAll(ctx context.Context, key string, condValues []interface{}) ([]*T, error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	// 加载
	t, err := c.getsFromMySQL(ctx, c.T, c.Tags, NewConds().eqs(c.condFields, condValues))
	if err != nil {
		return nil, err
	}
	allData := t.([]*T)
	if len(allData) == 0 {
		SetPass(key)
		return allData, nil
	}

	// 主key和多个数据key
	dataKeys := make([]string, 0, 1+len(allData))
	redisParams := make([]interface{}, 0, 1+(1+2*len(allData))+len(allData)*(1+2*len(c.Tags)))
	redisParams = append(redisParams, c.expire)
	// 第一个key是数据索引
	dataKeys = append(dataKeys, key)
	redisParams = append(redisParams, 2*len(allData))
	for _, data := range allData {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		thisKeyValue := dataInfo.Elemts[c.dataKeyFieldIndex].Interface() // 不需要判断，读出来的就是基础数据类型
		redisParams = append(redisParams, thisKeyValue)
		redisParams = append(redisParams, c.genDataKey(key, thisKeyValue))
	}
	// 其他的数据
	for _, data := range allData {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		sli := make([]interface{}, 0, 2*len(dataInfo.Tags))
		for i, v := range dataInfo.Elemts {
			vfmt := goredis.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, c.RedisTags[i])
			sli = append(sli, vfmt)
		}
		thisKeyValue := dataInfo.Elemts[c.dataKeyFieldIndex].Interface() // 不需要判断，读出来的就是基础数据类型
		dataKeys = append(dataKeys, c.genDataKey(key, thisKeyValue))
		redisParams = append(redisParams, len(sli))
		redisParams = append(redisParams, sli...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return allData, nil
}

// 预加载多条数据，确保写到Redis中
// key：主key
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValues 要加在的数据key
// 返回值
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoads(ctx context.Context, key string, condValues []interface{}, dataKeyValues []interface{}) ([]*T, error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	cond := NewConds().eqs(c.condFields, condValues)
	// 查询到要读取的数据
	t, err := c.getsFromMySQL(ctx, c.T, c.Tags, cond.In(c.dataKeyField, dataKeyValues...))
	if err != nil {
		return nil, err
	}
	datas := t.([]*T)
	if len(datas) == 0 {
		SetPass(key)
		return datas, nil
	}

	// 保存到redis中
	// 查询所有的dataValue，只读取dataKeyField对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	alldataKeyValues, err := c.getsFromMySQL(ctx, c.Fields[c.dataKeyFieldIndex].Type, []string{c.dataKeyField}, cond)
	if err != nil {
		return nil, err
	}
	alldataKeyValuesVO := reflect.ValueOf(alldataKeyValues)

	// 主key和多个数据key
	dataKeys := make([]string, 0, 1+len(datas))
	redisParams := make([]interface{}, 0, 1+(1+2*alldataKeyValuesVO.Len())+len(datas)*(1+2*len(c.Tags)))
	redisParams = append(redisParams, c.expire)
	// 第一个key是数据索引
	dataKeys = append(dataKeys, key)
	redisParams = append(redisParams, 2*alldataKeyValuesVO.Len())
	for i := 0; i < alldataKeyValuesVO.Len(); i++ {
		thisKeyValue := alldataKeyValuesVO.Index(i).Elem().Interface() // 不需要判断，读出来的就是基础数据类型
		redisParams = append(redisParams, thisKeyValue)
		redisParams = append(redisParams, c.genDataKey(key, thisKeyValue))
	}
	// 其他的数据
	for _, data := range datas {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		sli := make([]interface{}, 0, 2*len(dataInfo.Tags))
		for i, v := range dataInfo.Elemts {
			vfmt := goredis.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, c.RedisTags[i])
			sli = append(sli, vfmt)
		}
		thisKeyValue := dataInfo.Elemts[c.dataKeyFieldIndex].Interface() // 不需要判断，读出来的就是基础数据类型
		dataKeys = append(dataKeys, c.genDataKey(key, thisKeyValue))
		redisParams = append(redisParams, len(sli))
		redisParams = append(redisParams, sli...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return datas, nil
}

// 预加载一条数据，确保写到Redis中
// key：主key
// condValues：查询条件变量，要和condFields顺序和对应的类型一致
// dataKeyValue：数据key的值
// ncInfo：!= nil 表示不存在是否创建
// 返回值
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheRows[T]) preLoad(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, ncData map[string]interface{}) (*T, interface{}, error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, c.genDataKey(key, dataKeyValue))
	if err != nil || unlock == nil {
		return nil, nil, err
	}
	defer unlock()

	var incrValue interface{}
	cond := NewConds().eqs(c.condFields, condValues)

	// 查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.T, c.Tags, cond.Eq(c.dataKeyField, dataKeyValue))
	if ncData != nil {
		// 不存在要创建数据
		if err != nil {
			if err == ErrNullData {
				// 创建数据
				incrValue, err = c.addToMySQL(ctx, condValues, ncData)
				if err != nil {
					return nil, nil, err
				}
				DelPass(key)
				DelPass(c.genDataKey(key, dataKeyValue))
				// 重新加载下
				t, err = c.getFromMySQL(ctx, c.T, c.Tags, cond.Eq(c.dataKeyField, dataKeyValue))
				if err != nil {
					return nil, nil, err
				}
			} else {
				return nil, nil, err
			}
		}
	} else {
		// 不用创建
		if err != nil {
			if err == ErrNullData {
				SetPass(c.genDataKey(key, dataKeyValue))
			}
			return nil, nil, err
		}
	}

	data := t.(*T)

	// 保存到redis中
	// 查询所有的dataValue，只读取dataKeyField对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	alldataKeyValues, err := c.getsFromMySQL(ctx, c.Fields[c.dataKeyFieldIndex].Type, []string{c.dataKeyField}, cond)
	if err != nil {
		return nil, nil, err
	}
	alldataKeyValuesVO := reflect.ValueOf(alldataKeyValues)

	// 两个key
	dataKeys := make([]string, 0, 2)
	redisParams := make([]interface{}, 0, 1+(1+2*alldataKeyValuesVO.Len())+(1+2*len(c.Tags)))
	redisParams = append(redisParams, c.expire)
	// 第一个key是数据索引
	dataKeys = append(dataKeys, key)
	redisParams = append(redisParams, 2*alldataKeyValuesVO.Len())
	for i := 0; i < alldataKeyValuesVO.Len(); i++ {
		thisKeyValue := alldataKeyValuesVO.Index(i).Elem().Interface() // 不需要判断，读出来的就是基础数据类型
		redisParams = append(redisParams, thisKeyValue)
		redisParams = append(redisParams, c.genDataKey(key, thisKeyValue))
	}
	// 第二个key读取的数据
	dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
	sli := make([]interface{}, 0, 2*len(dataInfo.Tags))
	for i, v := range dataInfo.Elemts {
		vfmt := goredis.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		sli = append(sli, c.RedisTags[i])
		sli = append(sli, vfmt)
	}
	dataKeys = append(dataKeys, c.genDataKey(key, dataKeyValue))
	redisParams = append(redisParams, len(sli))
	redisParams = append(redisParams, sli...)

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除所有键
		return nil, nil, cmd.Err()
	}
	return data, incrValue, nil
}

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGetAll(ctx context.Context, key string) ([]*T, error) {
	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(ctx, rowsGetAllScript, []string{key}, redisParams).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := goredis.InterfaceToValue(v, destInfo.Elemts[i])
				if err != nil {
					return nil, err
				}
			}
			res = append(res, dest)
		}
		return res, nil
	} else {
		if goredis.IsNilError(err) {
			return nil, ErrNullData
		}
		return nil, err
	}
}

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGets(ctx context.Context, keys []string) ([]*T, error) {
	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(ctx, rowsGetsScript, keys, redisParams).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := goredis.InterfaceToValue(v, destInfo.Elemts[i])
				if err != nil {
					return nil, err
				}
			}
			res = append(res, dest)
		}
		return res, nil
	} else {
		if goredis.IsNilError(err) {
			return nil, ErrNullData
		}
		return nil, err
	}
}
