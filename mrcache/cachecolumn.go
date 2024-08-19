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

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// T 为数据库结构类型，可重复多协程使用
// 使用场景：查询条件只对应多个结果 redis使用多个key缓存这个结果 每个key以mysql的字段为维度 用hash存储，等于是变成列式存储了
// redis的hash的field和mysql中的自增字段匹配
type CacheColumn[T any] struct {
	*Cache
}

func NewCacheColumn[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheColumn[T] {
	dest := new(T)
	sInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 不需要判断err，后续的查询中都会通过checkCond判断
	elemtsType := make([]reflect.Type, 0, len(sInfo.Elemts))
	for _, e := range sInfo.Elemts {
		elemtsType = append(elemtsType, e.Type())
	}
	c := &CacheColumn[T]{
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
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheColumn[T]) Get(ctx context.Context, cond TableConds) ([]*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	keys, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, (c.incrementFieldIndex + 1)) // lua坐标

	reply := make([][]interface{}, 0)
	err = c.redis.DoScript2(ctx, columnGetScript, keys, redisParams).BindSlice(&reply)
	if err == nil {
		dest, err := c.parseRedis(reply)
		if err == nil {
			return dest, nil
		}
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, keys, false, 0, nil)
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
	reply = make([][]interface{}, 0) // 重新make，防止上面的数据有干扰
	err = c.redis.DoScript2(ctx, columnGetScript, keys, redisParams).BindSlice(&reply)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	dest, err := c.parseRedis(reply)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	return dest, nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheColumn[T]) GetOC(ctx context.Context, condValue interface{}) ([]*T, error) {
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

// 读取一条数据数据
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheColumn[T]) GetOne(ctx context.Context, cond TableConds, incrementId int64) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	keys, err := c.checkCond(cond)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, (c.incrementFieldIndex + 1)) // lua坐标
	redisParams = append(redisParams, incrementId)

	reply := make([][]interface{}, 0)
	err = c.redis.DoScript2(ctx, columnGetOneScript, keys, redisParams).BindSlice(&reply)
	if err == nil {
		dest, err := c.parseRedisOne(reply)
		if err == nil {
			return dest, nil
		} else if err == ErrNullData {
			// 不处理执行下面的预加载
		} else {
			utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
			return nil, err
		}
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, keys, false, 0, nil)
	if err == ErrNullData {
		return nil, err // 空数据直接返回
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	if preData != nil { // 执行了预加载, 从预加载数据查找
		for _, d := range preData {
			dInfo, _ := utils.GetStructInfoByTag(d, DBTag)
			if dInfo.Elemts[c.incrementFieldIndex].Int() == incrementId {
				return d, nil
			}
		}
		return nil, ErrNullData
	}

	// 如果不是自己执行的预加载，这里重新读取下
	reply = make([][]interface{}, 0) // 重新make，防止上面的数据有干扰
	err = c.redis.DoScript2(ctx, columnGetOneScript, keys, redisParams).BindSlice(&reply)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}

	dest, err := c.parseRedisOne(reply)
	if err == nil {
		return dest, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheColumn[T]) GetOneOC(ctx context.Context, condValue interface{}, incrementId int64) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Get error")
		return nil, err
	}
	return c.GetOne(ctx, NewConds().Eq(c.oneCondField, condValue), incrementId)
}

// 写数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段，该字段为0时，执行nc逻辑】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheColumn[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	keys, err := c.checkCond(cond)
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

	// 判断data中是否含有自增字段
	incrIndex := dataInfo.FindIndexByTag(c.incrementField)
	if incrIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.incrementField)
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
	// 自增字段的值
	incrementId := dataInfo.Elemts[incrIndex].Int()

	// 获取Redis参数
	redisParams := c.redisSetParam(cond, incrementId, dataInfo)

	// 写数据
	err = c.redisSetToMysql(ctx, cond, nc, keys, redisParams, incrementId, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, keys, nc, incrementId, dataInfo)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 返回了自增，数据添加已经完成了
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.redisSetToMysql(ctx, cond, nc, keys, redisParams, incrementId, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheColumn[T]) SetOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
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
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段，该字段为0时，执行nc逻辑】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是data结构类型的数据，修改后的值
func (c *CacheColumn[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	keys, err := c.checkCond(cond)
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

	// 判断data中是否含有自增字段
	incrIndex := dataInfo.FindIndexByTag(c.incrementField)
	if incrIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.incrementField)
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
	// 自增字段的值
	incrementId := dataInfo.Elemts[incrIndex].Int()

	// 获取Redis参数
	redisParams := c.redisModifyOneParam(cond, incrementId, dataInfo)

	// 返回值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModifyToMysql(ctx, cond, nc, keys, redisParams, incrementId, resInfo)
	if err == nil {
		return res, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, keys, nc, incrementId, dataInfo)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Set error")
		return nil, err
	}

	// 返回了自增，数据添加已经完成了，从预加载数据中查找
	if incrValue != nil {
		for _, d := range preData {
			dInfo, _ := utils.GetStructInfoByTag(d, DBTag)
			if dInfo.Elemts[c.incrementFieldIndex].Int() == incrValue.(int64) {
				dInfo.CopyTo(resInfo)
				return res, nil
			}
		}
		return nil, ErrNullData
	}

	// 再次写数据
	err = c.redisModifyToMysql(ctx, cond, nc, keys, redisParams, incrementId, resInfo)
	if err == nil {
		return res, nil
	} else {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Modify error")
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify说明
func (c *CacheColumn[T]) ModifyOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
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
// key格式：mrc_表名_字段名_{condValue1}_condValue2  如果CacheRow.hashTagField == condValue1 condValue1会添加上{}
func (c *CacheColumn[T]) checkCond(cond TableConds) ([]string, error) {
	if len(cond) == 0 {
		return nil, errors.New("cond is nil")
	}

	// 必须要设置自增key，用来hash结构中的field
	if len(c.incrementField) == 0 {
		return nil, errors.New("incrementField is nil")
	}

	// 条件中的字段必须都存在，且类型还要一致
	for _, v := range cond {
		if v.op != "=" {
			err := fmt.Errorf("cond:%s not Eq", v.field) // 条件变量的是等号
			return nil, err
		}
		at := c.tableInfo.FindIndexByTag(v.field)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", v.field, c.tableInfo.T.String())
			return nil, err
		}
		vo := reflect.ValueOf(v.value)
		if !(c.tableInfo.ElemtsType[at] == vo.Type() ||
			(c.tableInfo.ElemtsType[at].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[at].Elem() == vo.Type())) {
			err := fmt.Errorf("tag:%s(%s) type err, should be %s", v.field, vo.Type().String(), c.tableInfo.ElemtsType[at].String())
			return nil, err
		}
	}

	// 根据field排序
	temp := make(TableConds, len(cond))
	copy(temp, cond)
	sort.Slice(temp, func(i, j int) bool { return temp[i].field < temp[j].field })

	var keys = make([]string, len(c.tableInfo.Tags))
	for i, tag := range c.tableInfo.Tags {
		var key strings.Builder
		key.WriteString("mrc_" + c.tableName)
		key.WriteString("_")
		key.WriteString(tag.(string))
		for _, t := range temp {
			if t.field == c.hashTagField {
				key.WriteString(fmt.Sprintf("_{%v}", t.value))
			} else {
				key.WriteString(fmt.Sprintf("_%v", t.value))
			}
		}
		keys[i] = key.String()
	}
	return keys, nil
}

// 解析Redis数据，reply的第一个维度是c.tableInfo所有key，也就是c.tableInfo.Tags的长度
// 按T来解析
func (c *CacheColumn[T]) parseRedis(reply [][]interface{}) ([]*T, error) {
	if len(reply) != len(c.tableInfo.Tags) { // 理论他他们必须相等
		return nil, errors.New("what?")
	}
	// 转化成map，便于读取
	reply2 := make([]map[interface{}]interface{}, len(reply))
	for i, r := range reply {
		err := utils.InterfaceToValue(r, reflect.ValueOf(&reply2[i]))
		if err != nil {
			return nil, err
		}
	}
	dest := make([]*T, 0)
	// 按主键位置的数据读取
	for incrValue, _ := range reply[c.incrementFieldIndex] {
		d := new(T)
		dInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		for i, _ := range dInfo.Tags {
			v, ok := reply2[i][incrValue]
			if ok && v != nil {
				err := utils.InterfaceToValue(v, dInfo.Elemts[i])
				if err != nil {
					return nil, err
				}
			}
		}
		dest = append(dest, d)
	}
	return dest, nil
}

// 解析Redis数据，reply的第一个维度是c.tableInfo所有key，也就是c.tableInfo.Tags的长度
// dest是slice地址的value， 如 []*T，外面保证下，里面不验证了
func (c *CacheColumn[T]) parseRedis2(reply [][]interface{}, dest reflect.Value) error {
	if len(reply) != len(c.tableInfo.Tags) { // 理论他他们必须相等
		return errors.New("what?")
	}
	// 转化成map，便于读取
	reply2 := make([]map[interface{}]interface{}, len(reply))
	for i, r := range reply {
		err := utils.InterfaceToValue(r, reflect.ValueOf(&reply2[i]))
		if err != nil {
			return err
		}
	}
	// 按主键位置的数据读取
	for incrValue, _ := range reply[c.incrementFieldIndex] {
		fmt.Println(dest.Type(), dest.Type().Elem(), dest.Type().Elem().Elem())
		d := reflect.New(dest.Type().Elem().Elem().Elem())
		dInfo, _ := utils.GetStructInfoByTag(d.Interface(), DBTag)
		for i, tag := range c.tableInfo.Tags {
			idx := dInfo.FindIndexByTag(tag)
			if idx == -1 {
				continue
			}
			v, ok := reply2[i][incrValue]
			if ok && v != nil {
				err := utils.InterfaceToValue(v, dInfo.Elemts[idx])
				if err != nil {
					return err
				}
			}
		}
		reflect.AppendSlice(dest, d)
	}
	return nil
}

// 解析Redis数据，reply[1]对应所有的key，也就是c.tableInfo.Tags的长度
func (c *CacheColumn[T]) parseRedisOne(reply [][]interface{}) (*T, error) {
	if len(reply) != 2 || len(reply[0]) < 1 { // 理论他他们必须相等
		return nil, errors.New("what?")
	}

	if reply[0][0].(string) == "OK" {
		if len(reply[1]) != len(c.tableInfo.Tags) { // 理论他他们必须相等
			return nil, errors.New("what?")
		}

		d := new(T)
		dInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		for i, _ := range c.tableInfo.Tags {
			if reply[1][i] != nil {
				err := utils.InterfaceToValue(reply[1][i], dInfo.Elemts[i])
				if err != nil {
					return nil, err
				}
			}
		}
		return d, nil
	} else {
		return nil, ErrNullData // 目前只有NULL没有其他值 暂时写这个
	}
}

// 解析Redis数据，reply对应所有的key，也就是c.tableInfo.Tags的长度
// dest一个结构数据
func (c *CacheColumn[T]) parseRedisOnePart(reply []interface{}, destInfo *utils.StructInfo) error {
	if len(reply) != len(c.tableInfo.Tags) { // 理论他他们必须相等
		return errors.New("what?")
	}

	for i, tag := range c.tableInfo.Tags {
		idx := destInfo.FindIndexByTag(tag)
		if idx == -1 {
			continue
		}
		if reply[i] != nil {
			err := utils.InterfaceToValue(reply[i], destInfo.Elemts[idx])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 预加载，确保写到Redis中
// keys：
// nc 要检查不存在就创建
// ncId：检查存在的自增id
// 返回值
// []*T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheColumn[T]) preLoad(ctx context.Context, cond TableConds, keys []string, nc bool, ncId int64, dataInfo *utils.StructInfo) ([]*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+keys[0], time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		// 查询mysql是否存在这条数据
		_, err = c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.Tags, cond.Eq(c.incrementField, ncId)) // 此处用get函数
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
		t, err := c.mysqlToRedis(ctx, cond, keys)
		return t, incrValue, err
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+keys[0], time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			// 加载
			t, err := c.mysqlToRedis(ctx, cond, keys)
			return t, nil, err
		}
		return nil, nil, nil
	}
}

func (c *CacheColumn[T]) mysqlToRedis(ctx context.Context, cond TableConds, keys []string) ([]*T, error) {
	t, err := c.getsFromMySQL(ctx, c.tableInfo.TS, c.tableInfo.Tags, cond)
	if err != nil {
		return nil, err
	}
	data := t.([]*T)

	// 保存到redis中
	redisKV := make([][]interface{}, len(c.tableInfo.Tags))
	for i, _ := range c.tableInfo.Tags {
		redisKV[i] = make([]interface{}, 0, len(data)*2)
	}
	redisKVNum := 0
	for _, d := range data {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		incrfmt := utils.ValueFmt(tInfo.Elemts[c.incrementFieldIndex])
		if incrfmt == nil {
			continue // 这啥情况
		}
		for i, v := range tInfo.Elemts {
			vfmt := utils.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			redisKV[i] = append(redisKV[i], incrfmt)
			redisKV[i] = append(redisKV[i], vfmt)
			redisKVNum += 2
		}
	}

	redisParams := make([]interface{}, 0, 1+len(redisKV)+redisKVNum)
	redisParams = append(redisParams, c.expire)
	for _, params := range redisKV {
		redisParams = append(redisParams, len(params))
		redisParams = append(redisParams, params...)
	}

	cmd := c.redis.DoScript(ctx, columnAddScript, keys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, keys...) // 失败了，删除主键
		return nil, cmd.Err()
	}
	return data, nil
}

func (c *CacheColumn[T]) redisSetToMysql(ctx context.Context, cond TableConds, nc bool, keys []string, redisParams []interface{}, incrementId int64, dataInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, columnSetScript, keys, redisParams...)
	if cmd.Cmd.Err() == nil {
		t, _ := cmd.Cmd.(*redis.Cmd)
		ok, _ := t.Text()
		if ok == "OK" {
			// 同步mysql 加上等于自增id的条件
			err := c.saveToMySQL(ctx, cond.Eq(c.incrementField, incrementId), dataInfo)
			if err != nil {
				c.redis.Del(ctx, keys...) // mysql错了 要删缓存
				return err
			}
			return nil
		} else if ok == "NULL" {
			// 缓存存在 但操作的对象不存在
			if !nc {
				return fmt.Errorf("%s=%v not exist", c.incrementField, incrementId)
			} else {
				return ErrNullData // 走空数据创建流程
			}
		} else {
			return ErrNullData // 目前没有其他值 暂时写这个
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 组织redis数据，数据格式符合mhmsetScript脚本解析
// 按照c.tableInfo.Tags的顺序填充dataInfo中有的
func (c *CacheColumn[T]) redisSetParam(cond TableConds, incrementId int64, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(c.tableInfo.Tags)*3)
	redisParams = append(redisParams, c.expire)    // 第一个时间
	redisParams = append(redisParams, incrementId) // 第二个自增key

	for i, tag := range c.tableInfo.Tags { // 这里需要遍历c.tableInfo的tag 需要和key对应
		idx := dataInfo.FindIndexByTag(tag)
		if idx == -1 {
			redisParams = append(redisParams, "")
			redisParams = append(redisParams, "")
			continue // data没有修改
		}
		if i == c.incrementFieldIndex {
			redisParams = append(redisParams, "")
			redisParams = append(redisParams, "")
			continue // 忽略自增增段
		}
		if ok := cond.Find(tag.(string)); ok != nil {
			redisParams = append(redisParams, "")
			redisParams = append(redisParams, "")
			continue // 忽略条件字段
		}
		vfmt := utils.ValueFmt(dataInfo.Elemts[idx])
		if vfmt == nil {
			redisParams = append(redisParams, "")
			redisParams = append(redisParams, "")
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}

		redisParams = append(redisParams, "set")
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}

func (c *CacheColumn[T]) redisModifyToMysql(ctx context.Context, cond TableConds, nc bool, keys []string, redisParams []interface{}, incrementId int64, resInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, columnModifyOneScript, keys, redisParams...)
	if cmd.Cmd.Err() == nil {
		reply := make([][]interface{}, 0)
		err := cmd.BindSlice(&reply)
		if err == nil {
			if reply[0][0].(string) == "OK" {
				err := c.parseRedisOnePart(reply[1], resInfo)
				if err != nil {
					return err
				}
				// 同步mysql 加上等于自增id的条件
				err = c.saveToMySQL(ctx, cond.Eq(c.incrementField, incrementId), resInfo)
				if err != nil {
					c.redis.Del(ctx, keys...) // mysql错了 要删缓存
					return err
				}
				return nil
			} else if reply[0][0] == "NULL" {
				// 缓存存在 但操作的对象不存在
				if !nc {
					return fmt.Errorf("%s=%v not exist", c.incrementField, incrementId)
				} else {
					return ErrNullData // 走空数据创建流程
				}
			} else {
				return ErrNullData // 目前没有其他值 暂时写这个
			}
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, keys...)
			return err
		}

	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 组织redis数据，数据格式符合mhmodifyScript脚本解析
// 按照c.tableInfo.Tags的顺序填充dataInfo中有的
func (c *CacheColumn[T]) redisModifyParam(cond TableConds, incrementId int64, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(c.tableInfo.Tags)*4)
	redisParams = append(redisParams, c.expire)    // 第一个时间
	redisParams = append(redisParams, incrementId) // 第二个自增key

	for i, tag := range c.tableInfo.Tags { // 这里需要遍历c.tableInfo的tag 需要和key对应
		idx := dataInfo.FindIndexByTag(tag)
		if idx == -1 {
			redisParams = append(redisParams, 0)
			continue // data没有修改
		}
		redisParams = append(redisParams, 3)
		redisParams = append(redisParams, incrementId)
		if i == c.incrementFieldIndex {
			redisParams = append(redisParams, "get") // 忽略自增增段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		if ok := cond.Find(tag.(string)); ok != nil {
			redisParams = append(redisParams, "get") // 忽略条件字段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		vfmt := utils.ValueFmt(dataInfo.Elemts[idx])
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

// 组织redis数据，数据格式符合columnModifyOneScript脚本解析
// 按照c.tableInfo.Tags的顺序填充dataInfo中有的
func (c *CacheColumn[T]) redisModifyOneParam(cond TableConds, incrementId int64, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(c.tableInfo.Tags)*2)
	redisParams = append(redisParams, c.expire)    // 第一个时间
	redisParams = append(redisParams, incrementId) // 第二个自增key

	for i, tag := range c.tableInfo.Tags { // 这里需要遍历c.tableInfo的tag 需要和key对应
		if i == c.incrementFieldIndex {
			redisParams = append(redisParams, "get") // 忽略自增增段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		if ok := cond.Find(tag.(string)); ok != nil {
			redisParams = append(redisParams, "get") // 忽略条件字段写入 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		idx := dataInfo.FindIndexByTag(tag)
		if idx == -1 {
			redisParams = append(redisParams, "get")
			redisParams = append(redisParams, nil)
			continue // data没有修改
		}
		vfmt := utils.ValueFmt(dataInfo.Elemts[idx])
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
