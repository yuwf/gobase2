package goredis

// https://github.com/yuwf/gobase2

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gobase/utils"

	"github.com/rs/zerolog/log"
)

var loopCheckServicesOnce sync.Once

// 使用Redis做服务器发现使用

// WatchService 监控服务器变化 RegistryConfig值填充Registry前缀的变量 回调外部不要修改infos参数
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func (r *Redis) WatchServices(key string, serverNames []string, fun func(infos []*RegistryInfo)) {
	log.Info().Str("key", key).Msg("Redis WatchService")
	ctx := utils.CtxNolog(context.TODO())
	// 开启读
	go func() {
		// 先创建订阅对象
		subscriber, _ := r.CreateSubscribe(ctx, key)
		var last []*RegistryInfo
		for {
			// 先读一次
			rst, err := r.ReadServices(ctx, key, serverNames)
			if err == nil {
				if !isSame(last, rst) {
					last = rst
					fun(rst)
				}
			}
			// 等待逻辑
			if subscriber != nil {
				message, err := subscriber.Receive(ctx)
				if err == nil {
					log.Debug().Str("reason", message).Msg("Redis will ReadServices")
					continue
				}
			}
			// 其他情况都走Sleep
			time.Sleep(time.Second * time.Duration(RegExprieTime))
		}
	}()
	// 开启检查
	loopCheckServicesOnce.Do(func() {
		go r.loopCheckServicesChange(ctx, key)
	})
}

// WatchService 监控服务器变化，通知变化，增加或者删除
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func (r *Redis) WatchServices2(key string, serverNames []string, fun func(addInfos, delInfos []*RegistryInfo)) {
	log.Info().Str("key", key).Msg("Redis WatchService")
	ctx := utils.CtxNolog(context.TODO())
	go func() {
		// 先创建订阅对象
		subscriber, _ := r.CreateSubscribe(ctx, key)
		var last []*RegistryInfo
		for {
			// 先读一次
			rst, err := r.ReadServices(ctx, key, serverNames)
			if err == nil {
				addInfos, delInfos := diff(last, rst)
				if len(addInfos) != 0 || len(delInfos) != 0 {
					fun(addInfos, delInfos)
					last = rst
				}
			}
			// 等待逻辑
			if subscriber != nil {
				message, err := subscriber.Receive(ctx)
				if err == nil {
					log.Debug().Str("reason", message).Msg("Redis will ReadServices")
					continue
				}
			}
			// 其他情况都走Sleep
			time.Sleep(time.Second * time.Duration(RegExprieTime))
		}
	}()
	// 开启检查
	loopCheckServicesOnce.Do(func() {
		go r.loopCheckServicesChange(ctx, key)
	})
}

// 读取一次服务器列表
func (r *Redis) ReadServices(ctx context.Context, key string, serverNames []string) ([]*RegistryInfo, error) {
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "WatchServices")
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	result, err := r.DoScript(ctx, readRegisterScirpt, []string{key}, RegExprieTime).StringSlice()
	if err != nil {
		// 错误了 在来一次
		result, err = r.DoScript(ctx, readRegisterScirpt, []string{key}, RegExprieTime).StringSlice()
		if err != nil {
			return nil, err
		}
	}

	rst := []*RegistryInfo{}
	for _, s := range result {
		ss := strings.Split(s, RegSep)
		if len(ss) < 5 {
			continue
		}
		port, err := strconv.Atoi(ss[3])
		if err != nil {
			continue
		}
		if len(serverNames) > 0 {
			if !utils.Contains(serverNames, ss[0]) {
				continue
			}
		}
		rst = append(rst, &RegistryInfo{
			RegistryName:   ss[0],
			RegistryID:     ss[1],
			RegistryAddr:   ss[2],
			RegistryPort:   port,
			RegistryScheme: ss[4],
		})
	}

	// 排序
	sort.SliceStable(rst, func(i, j int) bool {
		if rst[i].RegistryID < rst[j].RegistryID {
			return true
		}
		return rst[i].RegistryName < rst[j].RegistryName
	})
	return rst, nil
}

func (r *Redis) loopCheckServicesChange(ctx context.Context, key string) {
	uuid := utils.LocalIPString() + ":" + strconv.Itoa(os.Getpid()) + ":" + utils.RandString(8)
	for {
		if r.checkServicesChange(ctx, key, uuid) {
			time.Sleep(time.Second)
		} else {
			time.Sleep(time.Second * time.Duration(RegExprieTime))
		}
	}
}

// 检查是否有服务器是否有变化，有变化就发送订阅
// 检查也是抢占时，谁抢到了，谁来负责检查和删除过期的服务器
// 返回值表示是否抢占到了
func (r *Redis) checkServicesChange(ctx context.Context, key string, uuid string) bool {
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "CheckServices")
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	ok, err := r.DoScript(ctx, checkServicesScirpt, []string{key}, RegExprieTime, uuid).Int()
	if err != nil || ok == 0 {
		return false
	}
	return true
}

// 判断两个列表是否一样
func isSame(last, new []*RegistryInfo) bool {
	if len(last) != len(new) {
		return false
	}
	for i := 0; i < len(new); i++ {
		if last[i].RegistryName != new[i].RegistryName {
			return false
		}
		if last[i].RegistryID != new[i].RegistryID {
			return false
		}
		if last[i].RegistryAddr != new[i].RegistryAddr {
			return false
		}
		if last[i].RegistryPort != new[i].RegistryPort {
			return false
		}
	}
	return true
}

// 比较新旧列表，返回：new相比last，增加列表，删除的列表
func diff(last, new []*RegistryInfo) ([]*RegistryInfo, []*RegistryInfo) {
	addInfos, delInfos := make([]*RegistryInfo, 0), make([]*RegistryInfo, 0)
	for i := range new {
		addFlag := true
		for j := range last {
			if equal(new[i], last[j]) {
				addFlag = false
				break
			}
		}
		if addFlag {
			addInfos = append(addInfos, new[i])
		}
	}
	for i := range last {
		delFlag := true
		for j := range new {
			if equal(new[j], last[i]) {
				delFlag = false
				break
			}
		}
		if delFlag {
			delInfos = append(delInfos, last[i])
		}
	}
	return addInfos, delInfos
}

func equal(last, new *RegistryInfo) bool {
	if last.RegistryName != new.RegistryName {
		return false
	}
	if last.RegistryID != new.RegistryID {
		return false
	}
	if last.RegistryAddr != new.RegistryAddr {
		return false
	}
	if last.RegistryPort != new.RegistryPort {
		return false
	}
	return true
}
