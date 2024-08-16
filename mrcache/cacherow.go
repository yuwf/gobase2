package mrcache

// https://github.com/yuwf/gobase2

import (
	"context"
	"errors"
	"fmt"
	"gobase/goredis"
	"gobase/mysql"
	"gobase/utils"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// T 为数据库结构类型，可重复多协程使用
// 使用场景：查询条件只对应一个结果 redis使用hash结构缓存这个结果
// redis的hash的field和mysql的field对应，空值不会写入到redis中
// redis的key 见CacheRow.checkCond说明
type CacheRow[T any] struct {
	*Cache
}

func NewCacheRow[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheRow[T] {
	dest := new(T)
	sInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 不需要判断err，后续的查询中都会通过checkCond判断
	elemtsType := make([]reflect.Type, 0, len(sInfo.Elemts))
	for _, e := range sInfo.Elemts {
		elemtsType = append(elemtsType, e.Type())
	}
	c := &CacheRow[T]{
		Cache: &Cache{
			redis:     redis,
			mysql:     mysql,
			tableName: tableName,
			expire:    Expire,
			tableInfo: &TableStruct{
				T:          sInfo.T,
				TS:         reflect.ValueOf([]*T{}).Type(),
				Tags:       sInfo.Tags,
				ElemtsType: elemtsType,
			},
		},
	}
	// 默认配置第一个Key为自增
	if len(sInfo.Tags) > 0 {
		c.ConfigIncrement(redis, sInfo.Tags[0].(string), tableName)
	}
	return c
}

// 读取数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：是T结构类型的指针
func (c *CacheRow[T]) Get(ctx context.Context, cond TableConds) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.Tags...)

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
	err = c.redis.DoScript2(ctx, hmgetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, false)
	if err == ErrNullData {
		return nil, err // 空数据直接返回
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest = new(T) // 防止上面的数据有干扰
	destInfo, _ = utils.GetStructInfoByTag(dest, DBTag)
	err = c.redis.DoScript2(ctx, hmgetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	return dest, nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) GetOC(ctx context.Context, condValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	return c.Get(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 写数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRow[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// redis参数
	redisParams := c.redisSetParam(cond, dataInfo)

	err = c.redisSetToMysql(ctx, cond, key, redisParams, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, key, nc)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 再次写数据
	err = c.redisSetToMysql(ctx, cond, key, redisParams, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheRow[T]) SetOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
	return c.Set(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 增量修改数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是data结构类型的数据，修改后的值
func (c *CacheRow[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// redis参数
	redisParams := c.redisModifyParam(cond, dataInfo)

	// 返回值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModifyToMysql(ctx, cond, key, redisParams, resInfo)
	if err == nil {
		return res, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, _, err = c.preLoad(ctx, cond, key, nc)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 再次写数据
	err = c.redisModifyToMysql(ctx, cond, key, redisParams, resInfo)
	if err == nil {
		return res, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify说明
func (c *CacheRow[T]) ModifyOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
	return c.Modify(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 检查条件，返回redis缓存key
// key可以：表名_{condValue1}_condValue2  如果CacheRow.hashTagField == condValue1 condValue1会添加上{}
func (c *CacheRow[T]) checkCond(cond TableConds) (string, error) {
	if len(cond) == 0 {
		return "", errors.New("cond is nil")
	}

	// 条件中的字段必须都存在，且类型还要一致
	for _, v := range cond {
		at := c.tableInfo.FindIndexByTag(v.field)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", v.field, c.tableInfo.T.String())
			return "", err
		}
		vo := reflect.ValueOf(v.value)
		if !(c.tableInfo.ElemtsType[at] == vo.Type() ||
			(c.tableInfo.ElemtsType[at].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[at].Elem() == vo.Type())) {
			err := fmt.Errorf("tag:%s(%s) type err, should be %s", v.field, vo.Type().String(), c.tableInfo.ElemtsType[at].String())
			return "", err
		}
	}

	// 优化效率
	if len(cond) == 1 {
		for _, v := range cond {
			if c.hashTagField == v.field {
				return fmt.Sprintf("%s_{%v}", c.tableName, v.value), nil
			} else {
				return fmt.Sprintf("%s_%v", c.tableName, v.value), nil
			}
		}
	}

	// 根据field排序
	temp := make(TableConds, len(cond))
	copy(temp, cond)
	sort.Slice(temp, func(i, j int) bool { return temp[i].field < temp[j].field })

	var key strings.Builder
	key.WriteString(c.tableName)
	for _, v := range temp {
		if v.field == c.hashTagField {
			key.WriteString(fmt.Sprintf("_{%v}", v.value))
		} else {
			key.WriteString(fmt.Sprintf("_%v", v.value))
		}
	}
	return key.String(), nil
}

// 预加载，确保写到Redis中
// key：用来加锁的key
// nc：加载不存在是否创建
// *T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRow[T]) preLoad(ctx context.Context, cond TableConds, key string, nc bool) (*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		var incrValue interface{}
		t, err := c.getFromMySQL(ctx, cond)
		if err == ErrNullData {
			// 创建数据
			incrValue, err = c.addToMySQL(ctx, cond)
			if err != nil {
				return nil, nil, err
			}
			// 重新加载下
			t, err = c.getFromMySQL(ctx, cond)
			if err != nil {
				return nil, nil, err
			}
		} else if err != nil {
			return nil, nil, err
		}
		data := t.(*T)
		// 保存到redis中
		err = c.mysqlToRedis(ctx, key, data)
		if err != nil {
			return nil, nil, err
		}
		return data, incrValue, nil
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			t, err := c.getFromMySQL(ctx, cond)
			if err != nil {
				return nil, nil, err
			}
			data := t.(*T)
			// 保存到redis中
			err = c.mysqlToRedis(ctx, key, data)
			if err != nil {
				return nil, nil, err
			}
			return data, nil, nil
		}
		return nil, nil, nil
	}
}

func (c *CacheRow[T]) mysqlToRedis(ctx context.Context, key string, data *T) error {
	// 保存到redis中
	tInfo, _ := utils.GetStructInfoByTag(data, DBTag)
	redisParams := make([]interface{}, 0, 1+len(tInfo.Tags))
	redisParams = append(redisParams, c.expire)
	for i, v := range tInfo.Elemts {
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, tInfo.Tags[i])
		redisParams = append(redisParams, vfmt)
	}
	cmd := c.redis.DoScript(ctx, hmaddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

func (c *CacheRow[T]) redisSetToMysql(ctx context.Context, cond TableConds, key string, redisParams []interface{}, dataInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, hmsetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql
		err := c.saveToMySQL(ctx, cond, dataInfo)
		if err != nil {
			c.redis.Del(ctx, key) // mysql错了 要删缓存
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

// 组织redis数据，数据格式符合hmsetScript脚本解析
func (c *CacheRow[T]) redisSetParam(cond TableConds, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 1+len(dataInfo.Elemts)*2)
	redisParams = append(redisParams, c.expire)
	for i, v := range dataInfo.Elemts {
		if dataInfo.Tags[i] == c.incrementField {
			continue // 忽略自增增段
		}
		if ok := cond.Find(dataInfo.Tags[i].(string)); ok != nil {
			continue // 忽略条件字段
		}
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, dataInfo.Tags[i])
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}

func (c *CacheRow[T]) redisModifyToMysql(ctx context.Context, cond TableConds, key string, redisParams []interface{}, resInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, hmodifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(resInfo.Elemts)
		if err == nil {
			// 同步mysql
			err = c.saveToMySQL(ctx, cond, resInfo)
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, key)
			return err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 组织redis数据，数据格式符合hmodifyScript脚本解析
func (c *CacheRow[T]) redisModifyParam(cond TableConds, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 1+len(dataInfo.Elemts)*3)
	redisParams = append(redisParams, c.expire)
	for i, v := range dataInfo.Elemts {
		redisParams = append(redisParams, dataInfo.Tags[i])
		if dataInfo.Tags[i] == c.incrementField {
			redisParams = append(redisParams, "get") // 忽略自增增段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		if ok := cond.Find(dataInfo.Tags[i].(string)); ok != nil {
			redisParams = append(redisParams, "get") // 忽略条件字段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			redisParams = append(redisParams, "get") // 空的不填充 读取下
			redisParams = append(redisParams, nil)
			continue
		}
		switch reflect.ValueOf(vfmt).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			redisParams = append(redisParams, "incr") // 数值都是增量
		case reflect.Float32, reflect.Float64:
			redisParams = append(redisParams, "fincr") // 数值都是增量
		default:
			redisParams = append(redisParams, "set") // 其他都是直接设置
		}
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}
