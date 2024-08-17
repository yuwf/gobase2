package mrcache

// https://github.com/yuwf/gobase2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gobase/goredis"
	"gobase/mysql"
	"gobase/utils"
	"reflect"
	"strings"
)

var ErrNullData = errors.New("null") // 空数据，即没有数据

const DBTag = "db"

var Expire = 36 * 3600 // 支持修改

var IncrementKey = "_mrcache_increment_" // 自增key，hash结构，field使用table名

// 基础类
type Cache struct {
	redis     *goredis.Redis
	mysql     *mysql.MySQL
	tableName string // 表名

	// 其他配置参数
	// 生成key时的hasgtag
	hashTagField    string // 如果表结构条件中有字段名等于该值，就用查询你条件中这个字段的值设置redis中hashtag
	hashTagFieldIdx int    // hashTagField在tableInfo中的索引

	// 缓存过期
	expire int // 过期时间 单位秒 不设置默认为36h

	// 新增数据的自增ID，自增时通过redis来做的，redis根据incrementKey通过HINCRBY命令获取增长ID，其中hash的field就是incrementTable
	incrementReids      *goredis.Redis // 存储自增的Reids对象 默认值和redis为同一个对象
	incrementField      string         // mysql中自增字段tag名 区分大小写 默认为结构表的第一个字段
	incrementFieldIndex int            // 自增key在tableInfo中的索引
	incrementTable      string         // 插入数据时 获取自增id的table名 拆表时 不同的表要用同一个名, 默认值为tableName

	// 只有一个查询条件时，配置这个查询字段名，然后调用OC结尾的系列函数
	oneCondField string // 该字段名必须在结果表中存在  区分大小写

	queryCond TableConds // 查找数据总过滤条件

	// 运行时数据，结构表数据
	tableInfo *TableStruct // 不可修改 不可接受数据 只是用来记录结构类型
}

// 配置redishashtag
func (c *Cache) ConfigHashTag(hashTagField string) error {
	idx := c.tableInfo.FindIndexByTag(hashTagField)
	if idx == -1 {
		return fmt.Errorf("tag:%s not find in %s", hashTagField, c.tableInfo.T.String())
	}
	c.hashTagField = hashTagField
	c.hashTagFieldIdx = idx
	return nil
}

// 配置过期
func (c *Cache) ConfigQueryCond(cond TableConds) error {
	c.queryCond = cond
	return nil
}

// 配置自增参数
func (c *Cache) ConfigIncrement(incrementReids *goredis.Redis, incrementField, incrementTable string) error {
	if incrementReids == nil {
		return errors.New("incrementReids is nil")
	}
	// 自增字段必须存在 且类型是int或者uint
	idx := c.tableInfo.FindIndexByTag(incrementField)
	if idx == -1 {
		return fmt.Errorf("tag:%s not find in %s", incrementField, c.tableInfo.T.String())
	}
	switch c.tableInfo.ElemtsType[idx].Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		break
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		break
	default:
		return fmt.Errorf("tag:%s not int or uint", incrementField)
	}

	c.incrementReids = incrementReids
	c.incrementTable = incrementTable
	c.incrementField = incrementField
	c.incrementFieldIndex = idx
	if incrementTable == "" {
		c.incrementTable = c.tableName
	}
	return nil
}

// 只有一个字段作为查询条件时，字段名字可以提前设置好了，然后调用OC结尾的系列函数
func (c *Cache) ConfigOneCondField(oneCondField string) error {
	// 字段必须存在
	if c.tableInfo.FindIndexByTag(oneCondField) == -1 {
		return fmt.Errorf("tag:%s not find in %s", oneCondField, c.tableInfo.T.String())
	}
	return nil
}

func (c *Cache) ConfigExpire(expire int) error {
	c.expire = expire
	return nil
}

// 检查结构数据
// 可以是结构或者结构指针 data.tags名称需要和T一致，可以是T的一部分
// 如果合理 返回data的结构信息
func (c *Cache) checkData(data interface{}) (*utils.StructInfo, error) {
	dataInfo, err := utils.GetStructInfoByTag(data, DBTag)
	if err != nil {
		return nil, err
	}
	// 结构中的字段必须都存在，且类型还要一致
	for i, tag := range dataInfo.Tags {
		at := c.tableInfo.FindIndexByTag(tag)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", tag.(string), c.tableInfo.T.String())
			return nil, err
		}
		if !(c.tableInfo.ElemtsType[at] == dataInfo.Elemts[i].Type() ||
			(c.tableInfo.ElemtsType[at].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[at].Elem() == dataInfo.Elemts[i].Type())) {
			err := fmt.Errorf("tag:%s(%s) type err, should be %s", tag.(string), dataInfo.Elemts[i].Type().String(), c.tableInfo.ElemtsType[at].String())
			return nil, err
		}
	}
	return dataInfo, nil
}

// 往MySQL中添加一条数据，返回自增值，如果条件是=的，会设置为默认值
func (c *Cache) addToMySQL(ctx context.Context, cond TableConds, dataInfo *utils.StructInfo) (int64, error) {
	var incrementId int64
	if at := dataInfo.FindIndexByTag(c.incrementField); at != -1 {
		// 如果结构中有自增字段，优先使用
		incrementId = dataInfo.Elemts[at].Int()
	}
	if incrementId == 0 && len(c.incrementTable) != 0 {
		// 如果配置了自增 先获取自增id
		err := c.incrementReids.Do2(ctx, "HINCRBY", IncrementKey, c.incrementTable, 1).Bind(&incrementId)
		if err != nil {
			return 0, err
		}
	}

	var sqlStr strings.Builder
	sqlStr.WriteString("INSERT INTO ")
	sqlStr.WriteString(c.tableName)

	sqlStr.WriteString(" (")
	for i, tag := range c.tableInfo.Tags {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag.(string))
	}
	sqlStr.WriteString(") VALUES(")

	args := make([]interface{}, 0, len(c.tableInfo.Tags))
	for i, tag := range c.tableInfo.Tags {
		if len(args) > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString("?")

		if tag == c.incrementField {
			args = append(args, &incrementId) // 这里填充地址，下面如果自增主键冲突了，会再次修改，mysql内部支持*int的转化操作，Redis不会
			continue
		}
		// 从条件变量中查找
		if v := cond.Find(tag.(string)); v != nil {
			args = append(args, v.value)
			continue
		}
		// 从结构数据中查找
		if dataInfo != nil {
			if at := dataInfo.FindIndexByTag(tag); at != -1 && dataInfo.Elemts[at].CanInterface() {
				args = append(args, dataInfo.Elemts[at].Interface())
				continue
			}
		}
		// 都没有就创建一个空的
		args = append(args, reflect.New(c.tableInfo.ElemtsType[i]).Interface())
	}
	sqlStr.WriteString(")")

	_, err := c.mysql.Exec(ctx, sqlStr.String(), args...)

	if err != nil {
		// 自增ID冲突了 尝试获取最大的ID， 重新写入下
		if len(c.incrementField) != 0 && utils.IsMatch("*Error 1062**Duplicate*PRIMARY*", err.Error()) {
			var maxIncrement int64
			err2 := c.mysql.Get(ctx, &maxIncrement, "SELECT MAX("+c.incrementField+") FROM "+c.tableName)
			if err2 == nil {
				incrementId = maxIncrement + 1000
				_, err := c.mysql.Exec(ctx, sqlStr.String(), args...)
				if err == nil {
					c.incrementReids.Do(ctx, "HSET", IncrementKey, c.incrementTable, maxIncrement+1) // 保存下最大的key
					return incrementId, nil
				}
			}
		}
		return 0, err
	}
	return incrementId, nil
}

// 读取mysql数据 返回的是 *T
func (c *Cache) getFromMySQL(ctx context.Context, cond TableConds) (interface{}, error) {
	var sqlStr strings.Builder
	sqlStr.WriteString("SELECT ")

	for i, tag := range c.tableInfo.Tags {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag.(string))
	}
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" WHERE ")

	args := make([]interface{}, 0, len(cond)+len(c.queryCond))
	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}
	for _, v := range c.queryCond {
		sqlStr.WriteString(" AND ")
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}

	t := reflect.New(c.tableInfo.T)
	err := c.mysql.Get(ctx, t.Interface(), sqlStr.String(), args...)

	if err == sql.ErrNoRows {
		return nil, ErrNullData
	}
	if err != nil {
		return nil, err
	}
	return t.Interface(), nil
}

// 读取mysql数据 返回的是 []*T
func (c *Cache) getsFromMySQL(ctx context.Context, cond TableConds) (interface{}, error) {
	var sqlStr strings.Builder
	sqlStr.WriteString("SELECT ")

	for i, tag := range c.tableInfo.Tags {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag.(string))
	}
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" WHERE ")

	args := make([]interface{}, 0, len(cond)+len(c.queryCond))
	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}
	for _, v := range c.queryCond {
		sqlStr.WriteString(" AND ")
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}

	t := reflect.New(c.tableInfo.TS)
	err := c.mysql.Select(ctx, t.Interface(), sqlStr.String(), args...)

	if err == sql.ErrNoRows {
		return nil, ErrNullData
	}
	if err != nil {
		return nil, err
	}
	return t.Elem().Interface(), nil
}

// 保存到mysql
func (c *Cache) saveToMySQL(ctx context.Context, cond TableConds, destInfo *utils.StructInfo) error {
	var sqlStr strings.Builder
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" SET ")

	args := make([]interface{}, 0, len(cond)+len(destInfo.Tags))
	num := 0
	for i, tag := range destInfo.Tags {
		if tag.(string) == c.incrementField {
			continue // 忽略自增增段
		}
		if ok := cond.Find(tag.(string)); ok != nil {
			continue // 忽略条件字段
		}

		if num > 0 {
			sqlStr.WriteString(",")
		}
		num++
		sqlStr.WriteString(tag.(string))
		sqlStr.WriteString("=?")
		args = append(args, destInfo.Elemts[i].Interface())
	}
	sqlStr.WriteString(" WHERE ")

	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}

	_, err := c.mysql.Update(ctx, sqlStr.String(), args...)

	if err == sql.ErrNoRows {
		return ErrNullData
	}
	return err
}
