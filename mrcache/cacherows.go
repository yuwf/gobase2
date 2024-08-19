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
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// T 为数据库结构类型，可重复多协程使用
// 使用场景：查询条件只对应多个结果 redis使用多个key缓存这个结果
// 有一个总key索引所有的数据key，索引key，为hash结果 datakeyvalue:datakey datakey命名：索引key名_dataKeyField对应的值
// 为了效率，索引key的倒计时可能和其他数据不一致，所以如果索引中存在，数据key不存在，就重新加载
// 最好查询条件和dataKeyField做一个唯一索引
type CacheRows[T any] struct {
	*Cache

	dataKeyField      string // 查询结果中唯一的字段tag，用来做key，区分大小写 默认为结构表的第一个字段
	dataKeyFieldIndex int    // key在tableInfo中的索引
}

func NewCacheRows[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheRows[T] {
	dest := new(T)
	sInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 不需要判断err，后续的查询中都会通过checkCond判断
	elemtsType := make([]reflect.Type, 0, len(sInfo.Elemts))
	for _, e := range sInfo.Elemts {
		elemtsType = append(elemtsType, e.Type())
	}
	c := &CacheRows[T]{
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
	// 默认配置第一个Key为key字段
	if len(sInfo.Tags) > 0 {
		c.ConfigDataKeyField(sInfo.Tags[0].(string))
	}
	return c
}

// 配置数据key字段
func (c *CacheRows[T]) ConfigDataKeyField(keyField string) error {
	// 自增字段必须存在
	idx := c.tableInfo.FindIndexByTag(keyField)
	if idx == -1 {
		return fmt.Errorf("tag:%s not find in %s", keyField, c.tableInfo.T.String())
	}
	// 数据字段只能是基本的数据类型
	typeOk := false
	switch c.tableInfo.ElemtsType[idx].Kind() {
	case reflect.Bool:
		typeOk = true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		typeOk = true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		typeOk = true
	case reflect.Slice:
		if c.tableInfo.ElemtsType[idx].Elem().Kind() == reflect.Uint8 {
			typeOk = true
		}
	case reflect.String:
		typeOk = true
	}
	if !typeOk {
		return fmt.Errorf("tag:%s(%s) as dataKeyField type error", keyField, c.tableInfo.T.String())
	}

	c.dataKeyField = keyField
	c.dataKeyFieldIndex = idx
	return nil
}

func (c *CacheRows[T]) dataKeyValue(dataKeyValue reflect.Value) string {
	switch dataKeyValue.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(dataKeyValue.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(dataKeyValue.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(dataKeyValue.Uint(), 10)
	case reflect.Slice:
		if dataKeyValue.Elem().Kind() == reflect.Uint8 {
			return utils.BytesToString(dataKeyValue.Bytes())
		}
	case reflect.String:
		return dataKeyValue.String()
	}
	return ""
}

// 读取数据
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAll(ctx context.Context, cond TableConds) ([]*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("GetAll error")
		return nil, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.Tags...)

	dest, err := c.redisGetAll(ctx, key, redisParams)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("GetAll error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, err := c.preLoadAll(ctx, cond, key)
	if err == ErrNullData {
		return nil, err // 空数据直接返回
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("GetAll error")
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest, err = c.redisGetAll(ctx, key, redisParams)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return nil, ErrNullData
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("GetAll error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) GetAllOC(ctx context.Context, condValue interface{}) ([]*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("GetAll error")
		return nil, err
	}
	return c.GetAll(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 读取一条数据
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Get(ctx context.Context, cond TableConds, dataKeyValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	// 数据key的类型和值
	dataKeyVO := reflect.ValueOf(dataKeyValue)
	if !(c.tableInfo.ElemtsType[c.dataKeyFieldIndex] == dataKeyVO.Type() ||
		(c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Elem() == dataKeyVO.Type())) {
		err := fmt.Errorf("dataKeyValue:%s type err, should be %s", dataKeyVO.Type().String(), c.tableInfo.ElemtsType[c.dataKeyFieldIndex].String())
		return nil, err
	}
	dataKey := key + "_" + c.dataKeyValue(dataKeyVO)

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.Tags...)

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
	err = c.redis.DoScript2(ctx, rowsGetScript, []string{key, dataKey}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, dataKeyValue, false, nil)
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
	err = c.redis.DoScript2(ctx, rowGetScript, []string{key, dataKey}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		return nil, ErrNullData
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) GetOC(ctx context.Context, condValue interface{}, dataKeyValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	return c.Get(ctx, NewConds().Eq(c.oneCondField, condValue), dataKeyValue)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略set
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRows[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
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

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
	// 数据key的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 获取Redis参数
	redisParams := c.redisSetParam(cond, dataInfo)

	// 写数据
	err = c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 返回了自增，数据添加已经完成了
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheRows[T]) SetOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
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

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉modify
// nc：表示不存在是否创建
// 返回值：是data结构类型的数据，修改后的值
func (c *CacheRows[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
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

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
	// 自增字段的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 获取Redis参数
	redisParams := c.redisModifyParam(cond, dataInfo)

	// 返回值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModifyToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, resInfo)
	if err == nil {
		return res, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dataKeyValueStr := c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])
		dInfo, _ := utils.GetStructInfoByTag(preData, DBTag)
		if c.dataKeyValue(dInfo.Elemts[c.dataKeyFieldIndex]) == dataKeyValueStr {
			dInfo.CopyTo(resInfo)
			return res, nil
		}
		return nil, ErrNullData
	}

	// 再次写数据
	err = c.redisModifyToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, resInfo)
	if err == nil {
		return res, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify说明
func (c *CacheRows[T]) ModifyOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
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

// 检查条件，返回redis缓存key列表，key列表的大小和c.tableInfo.Tags一一对应
// 第一个key为主key，用来判断是否有缓存的key
// key格式：mrrs_表名_字段名_{condValue1}_condValue2  如果CacheRow.hashTagField == condValue1 condValue1会添加上{}
func (c *CacheRows[T]) checkCond(cond TableConds) (string, error) {
	if len(cond) == 0 {
		return "", errors.New("cond is nil")
	}

	// 必须要设置数据key，
	if len(c.dataKeyField) == 0 {
		return "", errors.New("dataKeyField is nil")
	}

	// 条件中的字段必须都存在，且类型还要一致
	for _, v := range cond {
		if v.op != "=" {
			err := fmt.Errorf("cond:%s not Eq", v.field) // 条件变量的是等号
			return "", err
		}
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
				return fmt.Sprintf("mrrs_%s_{%v}", c.tableName, v.value), nil
			} else {
				return fmt.Sprintf("mrrs_%s_%v", c.tableName, v.value), nil
			}
		}
	}

	// 根据field排序
	temp := make(TableConds, len(cond))
	copy(temp, cond)
	sort.Slice(temp, func(i, j int) bool { return temp[i].field < temp[j].field })

	var key strings.Builder
	key.WriteString("mrrs_" + c.tableName)
	for _, v := range temp {
		if v.field == c.hashTagField {
			key.WriteString(fmt.Sprintf("_{%v}", v.value))
		} else {
			key.WriteString(fmt.Sprintf("_%v", v.value))
		}
	}
	return key.String(), nil
}

// 预加载所有数据，确保写到Redis中
// key：主key
// 返回值
// []*T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoadAll(ctx context.Context, cond TableConds, key string) ([]*T, error) {
	unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
	if err == nil && unlock != nil {
		defer unlock()
		// 加载
		t, err := c.mysqlAllToRedis(ctx, cond, key)
		return t, err
	}
	return nil, nil
}

func (c *CacheRows[T]) mysqlAllToRedis(ctx context.Context, cond TableConds, key string) ([]*T, error) {
	t, err := c.getsFromMySQL(ctx, c.tableInfo.TS, c.tableInfo.Tags, cond)
	if err != nil {
		return nil, err
	}
	data := t.([]*T)

	// 保存到redis中
	dataKeys := []string{key}
	redisKV := make([][]interface{}, 0, 1+len(data))
	redisKV = append(redisKV, make([]interface{}, 0, 1+len(data))) // 第0个位置是总key数据
	redisKVNum := 0
	for _, d := range data {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		dataKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		if dataKeyValue == nil {
			continue // 理论上不应该 忽略这条数据
		}
		sli := make([]interface{}, 0, len(tInfo.Tags))
		for i, v := range tInfo.Elemts {
			vfmt := utils.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, tInfo.Tags[i])
			sli = append(sli, vfmt)
		}
		k := key + "_" + fmt.Sprint(dataKeyValue)
		dataKeys = append(dataKeys, k)
		redisKV[0] = append(redisKV[0], dataKeyValue)
		redisKV[0] = append(redisKV[0], k)
		redisKV = append(redisKV, sli)
		redisKVNum += (2 + len(sli))
	}

	redisParams := make([]interface{}, 0, 1+len(dataKeys)+redisKVNum) // 过期 num数 kv数
	redisParams = append(redisParams, c.expire)
	for _, params := range redisKV {
		redisParams = append(redisParams, len(params))
		redisParams = append(redisParams, params...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除主键
		return nil, cmd.Err()
	}
	return data, nil
}

// 预加载一条数据，确保写到Redis中
// key：主key
// dataKeyValue 这条数据的值
// nc 不存在就创建
// 返回值
// *T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheRows[T]) preLoad(ctx context.Context, cond TableConds, key string, dataKeyValue interface{}, nc bool, dataInfo *utils.StructInfo) (*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		// 查询mysql是否存在这条数据
		_, err = c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.Tags, cond.Eq(c.dataKeyField, dataKeyValue)) // 此处用get函数
		if err != nil && err != ErrNullData {
			return nil, nil, err
		}
		var incrValue interface{}
		if err == ErrNullData {
			// 如果是空数据 添加一条
			incrValue, err = c.addToMySQL(ctx, cond, dataInfo) // 自增加进去
			if err != nil {
				return nil, nil, err
			}
		}

		// 加载
		t, err := c.mysqlToRedis(ctx, cond, key, dataKeyValue)
		return t, incrValue, err
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			// 加载
			t, err := c.mysqlToRedis(ctx, cond, key, dataKeyValue)
			return t, nil, err
		}
		return nil, nil, nil
	}
}

func (c *CacheRows[T]) mysqlToRedis(ctx context.Context, cond TableConds, key string, dataKeyValue interface{}) (*T, error) {
	// 先查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.Tags, cond.Eq(c.dataKeyField, dataKeyValue))
	if err != nil {
		return nil, err
	}
	// 查询所有的dataValue，只读取dataKeyField对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	allKeyT, err := c.getsFromMySQL(ctx, c.tableInfo.TS, []interface{}{c.dataKeyField}, cond)
	if err != nil {
		return nil, err
	}

	dataKeys := []string{key, key + "_" + fmt.Sprint(dataKeyValue)}
	data := t.(*T)
	allKeyData := allKeyT.([]*T)

	// 保存到redis中
	redisParams := make([]interface{}, 0, 1+1+len(allKeyData)*2+1+len(c.tableInfo.Tags)*2)
	redisParams = append(redisParams, c.expire)
	// 主key
	redisParams = append(redisParams, len(allKeyData)*2)
	for _, d := range allKeyData {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		dataKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		redisParams = append(redisParams, dataKeyValue)
		redisParams = append(redisParams, key+"_"+fmt.Sprint(dataKeyValue))
	}
	// 数据key
	tInfo, _ := utils.GetStructInfoByTag(data, DBTag)
	paramNumAt := len(redisParams) // 记录后面要写入数据个数的位置
	paramNum := 0
	redisParams = append(redisParams, &paramNum)
	for i, v := range tInfo.Elemts {
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, tInfo.Tags[i])
		redisParams = append(redisParams, vfmt)
		paramNum += 2
	}
	redisParams[paramNumAt] = paramNum

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除主键
		return nil, cmd.Err()
	}
	return data, nil
}

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGetAll(ctx context.Context, key string, redisParams []interface{}) ([]*T, error) {
	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(ctx, rowsGetAllScript, []string{key}, redisParams).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := utils.InterfaceToValue(v, destInfo.Elemts[i])
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

func (c *CacheRows[T]) redisSetToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, dataInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, rowsSetScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql，添加上数据key字段
		err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), dataInfo)
		if err != nil {
			c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
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

// 组织redis数据，数据格式符合rowsSetScript脚本解析
func (c *CacheRows[T]) redisSetParam(cond TableConds, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 1+len(dataInfo.Elemts)*2)
	redisParams = append(redisParams, c.expire)
	for i, v := range dataInfo.Elemts {
		if dataInfo.Tags[i] == c.incrementField {
			continue // 忽略自增增段
		}
		if ok := cond.Find(dataInfo.Tags[i].(string)); ok != nil {
			continue // 忽略条件字段
		}
		if dataInfo.Tags[i] == c.dataKeyField {
			continue // 忽略数据字段
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

func (c *CacheRows[T]) redisModifyToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, resInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(resInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据字段
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), resInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey)
			return err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 组织redis数据，数据格式符合rowsModifyScript脚本解析
func (c *CacheRows[T]) redisModifyParam(cond TableConds, dataInfo *utils.StructInfo) []interface{} {
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
		if dataInfo.Tags[i] == c.dataKeyField {
			redisParams = append(redisParams, "get") // 忽略数据字段写入 只读取
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
