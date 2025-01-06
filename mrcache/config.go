package mrcache

// https://github.com/yuwf/gobase2

import (
	"context"
	"errors"
	"gobase/utils"
	"sync"
	"time"
)

// 是否屏蔽DB层的日志，上层没有CtxKey_nolog前提下才生效
var DBNolog = true

// 读写单条数据时，不存在返回空错误，读取多条数据时不会返回空错误
var ErrNullData = errors.New("null")

// 结构中存储的tag名
const DBTag = "db"

// 存储Redis使用的tag，只有在初始化的NewCacheRow 或者 NewCacheRows是解析出来，为了减少Redis中field可能太长的问题
// 如果不设置默认是用的DBTag
const RedisTag = "redis"

var Expire = 36 * 3600 // 支持修改

var IncrementKey = "_mrcache_increment_" // 自增key，hash结构，field使用table名

// Add、Set Modify
const CtxKey_NR = utils.CtxKey("_cache_no_resp_")          // 不需要返回值，有时为了优化性能不需要返回值
const CtxKey_NEC = utils.CtxKey("_cache_no_exist_create_") // 不存在就创建

// 不需要返回值
func NoResp(parent context.Context) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if parent.Value(CtxKey_NR) != nil {
			return parent
		}
	}
	return context.WithValue(parent, CtxKey_NR, 1)
}

// 不存在时创建
func NoExistCreate(parent context.Context) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if parent.Value(CtxKey_NEC) != nil {
			return parent
		}
	}
	return context.WithValue(parent, CtxKey_NEC, 1)
}

// 查询数据不存在的缓存，防止缓存穿透
var passCache sync.Map

func GetPass(key string) bool {
	passTime, ok := passCache.Load(key)
	if !ok {
		return false
	}
	if time.Now().Unix()-passTime.(int64) >= 4 {
		passCache.Delete(key)
		return false
	}
	return true
}

func SetPass(key string) {
	passCache.Store(key, time.Now().Unix())
}

func DelPass(key string) {
	passCache.Delete(key)
}
