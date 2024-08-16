package mrcache

import (
	"context"
	"fmt"
	"gobase/goredis"
	"gobase/mysql"
	"testing"
	"time"

	_ "gobase/log"
)

var mysqlCfg = &mysql.Config{
	Source: "root:1235@tcp(localhost:3306)/test?charset=utf8",
}

var redisCfg = &goredis.Config{
	Addrs:  []string{"127.0.0.1:6379"},
	Passwd: "1235",
}

// db中
type Test struct {
	Id   int     `db:"Id"json:"Id,omitempty"`     //自增住建  不可为空
	UID  int     `db:"UID"json:"UID,omitempty"`   //用户ID  不可为空
	Name string  `db:"Name"json:"Name,omitempty"` //名字  不可为空
	Age  *int    `db:"Age"json:"Age,omitempty"`   //年龄
	T    *string `db:"T"json:"T,omitempty"`       //测试时间
}

func BenchmarkGet(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheRow[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}

	user, err := cache.Get(context.TODO(), NewConds().Eq("UID", 123))
	fmt.Println(user, err)
}

func BenchmarkSet(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheRow[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}
	err = cache.ConfigIncrement(redis, "Id", "test")
	if err != nil {
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	//a := 10
	s := &SetTest{
		Name: "好17uu7u",
		Age:  20,
	}

	incrValue, err := cache.Set(context.TODO(), NewConds().Eq("UID", 126), s, true)

	fmt.Println(incrValue, err)
}

func BenchmarkModify(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheRow[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  *int   `db:"Age"json:"Age,omitempty"`   //年龄
	}

	a := 10
	ss := time.Now().Format(time.TimeOnly)
	s := &Test{
		Id:   7015,
		Name: "好12ppp32",
		Age:  &a,
		T:    &ss,
	}

	user, err := cache.Modify(context.TODO(), NewConds().Eq("UID", 123).Eq("Id", 7015), s, true)

	fmt.Println(user, err)
}

func BenchmarkGetColumn(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheColumn[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}

	user, err := cache.Get(context.TODO(), NewConds().Eq("UID", 123))
	fmt.Println(user, err)
}

func BenchmarkSetColumn(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheColumn[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}

	type SetTest struct {
		Id   int    `db:"Id"json:"Id,omitempty"`     //自增住建  不可为空
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  string `db:"Age"json:"Age,omitempty"`   //年龄
	}

	s := &SetTest{
		Id:   7010,
		Name: "dddddddddddddd",
		Age:  "10000",
	}

	incrValue, err := cache.Set(context.TODO(), NewConds().Eq("UID", 123), s, true)
	fmt.Println(incrValue, err)
}

func BenchmarkModifyColumn(b *testing.B) {
	mysql, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	redis, err := goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cache := NewCacheColumn[Test](redis, mysql, "test")
	err = cache.ConfigHashTag("UID")
	if err != nil {
		return
	}

	type SetTest struct {
		Age  *int   `db:"Age"json:"Age,omitempty"`   //年龄
		Id   int    `db:"Id"json:"Id,omitempty"`     //自增住建  不可为空
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
	}

	a := 10
	s := &SetTest{
		Id:   7015,
		Name: "dddddddddddddddd",
		Age:  &a,
	}

	t, err := cache.Modify(context.TODO(), NewConds().Eq("UID", 123), s, true)
	fmt.Println(t, err)
}
