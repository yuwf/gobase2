package goredis

// https://github.com/yuwf/gobase2

import (
	"context"
	"crypto/tls"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const CtxKey_nonilerr = utils.CtxKey("nonilerr")   // 命令移除空错误 值：不受限制 一般写1
const CtxKey_rediscmd = utils.CtxKey("rediscmd")   // 值：RedisCommand对象 一般情况内部使用
const CtxKey_addcaller = utils.CtxKey("addcaller") // 值：1 标记redis封装层有没有添加caller

const CtxKey_cmddesc = utils.CtxKey("cmddesc")       // 值：字符串 命令描述 一般情况内部使用
const CtxKey_noscript = utils.CtxKey("noscript")     // 屏蔽NOSCRIPT的错误提示(在使用Reids.Run命令时建议使用)， 值：不受限制 一般情况内部使用
const CtxKey_scriptname = utils.CtxKey("scriptname") // 值：字符串 优化日志输出 一般情况内部使用

type Config struct {
	Master string   `json:"master,omitempty"` // 不为空就创建哨兵模式的连接
	Addrs  []string `json:"addrs,omitempty"`  // host:port 地址数<=1 创建单节点连接 否则创建多节点
	Passwd string   `json:"passwd,omitempty"` // 秘钥
	DB     int      `json:"db,omitempty"`     // 只有单节点模式使用
	TSL    bool     `json:"tsl,omitempty"`    // 是否使用TSL连接
}

// Redis对象
type Redis struct {
	redis.UniversalClient

	// 执行命令时的回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, cmd *RedisCommond)
}

var defaultRedis *Redis

func DefaultRedis() *Redis {
	return defaultRedis
}

func InitDefaultRedis(cfg *Config) (*Redis, error) {
	var err error
	defaultRedis, err = NewRedis(cfg)
	return defaultRedis, err
}

func NewRedis(cfg *Config) (*Redis, error) {
	//参考 redis.Options 说明
	options := &redis.UniversalOptions{
		MasterName: cfg.Master,
		Addrs:      cfg.Addrs,

		//钩子函数
		//仅当客户端执行命令需要从连接池获取连接时，如果连接池需要新建连接则会调用此钩子函数
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			return nil
		},

		Password: cfg.Passwd, //密码
		DB:       cfg.DB,     // redis数据库

		//命令执行失败时的重试策略
		MaxRetries:      -1,                     // 命令执行失败时，最多重试多少次, -1表示不重试 0表示重试3次
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

		//超时
		DialTimeout:  4 * time.Second, //连接建立超时时间
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时

		//连接池设置
		PoolFIFO:     true,            // 连接池使用FIFO管理
		PoolSize:     0,               // 连接池最大socket连接数，默认为10倍CPU数， 10 * runtime.NumCPU
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。
		MinIdleConns: 10,              //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量
		//MaxIdleConns: 256, // 最大的空闲连接数
		//ConnMaxIdleTime: time.Minute, // 空闲的最大时间
		//ConnMaxLifetime
	}
	if cfg.TSL {
		options.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	client := redis.NewUniversalClient(options)
	r := &Redis{UniversalClient: client}

	// 测试连接
	cmd := client.Ping(context.TODO())
	if cmd.Err() != nil {
		client.Close()
		log.Error().Err(cmd.Err()).Str("addr", strings.Join(cfg.Addrs, ",")).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Bool("tsl", cfg.TSL).Msg("Redis Conn Fail")
		return nil, cmd.Err()
	}
	client.AddHook(&hook{redis: r})

	log.Info().Str("addr", strings.Join(cfg.Addrs, ",")).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Bool("tsl", cfg.TSL).Msg("Redis Conn Success")
	return r, nil
}

func (r *Redis) RegHook(f func(ctx context.Context, cmd *RedisCommond)) {
	r.hook = append(r.hook, f)
}

// 支持返回值绑定的函数
func (r *Redis) Do2(ctx context.Context, args ...interface{}) *RedisCommond {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	r.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return redisCmd
}

// 针对HMGET命令 调用Cmd时，参数不需要包括field
// 结构成员首字母需要大写，tag中必须是包含 `redis:"hello"`  其中hello就表示在redis中存储的field名称
// 结构成员类型 : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
// 结构成员其他类型 : 通过Json转化
// 传入的参数为结构的地址
func (r *Redis) HMGetObj(ctx context.Context, key string, v interface{}) error {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	// 获取结构数据
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis HMSetObj Param error")
		return err
	}
	if len(sInfo.Tags) == 0 {
		return nil // 没有值要读取，直接返回
	}

	args := []interface{}{"hmget", key}
	args = append(args, sInfo.TagsSlice()...)
	rst := r.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	if rst.Err() != nil {
		return rst.Err()
	}
	// 回调
	err = redisCmd.BindValues(sInfo.Elemts)
	return err
}

// 参数v 参考Redis.HMGetObj的说明
func (r *Redis) HMSetObj(ctx context.Context, key string, v interface{}) error {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis HMSetObj Param error")
		return err
	}
	fargs := TagElemtNoNilFmt(sInfo)
	if len(fargs) == 0 {
		return nil // 没有值写入，直接返回
	}
	args := []interface{}{"hmset", key}
	args = append(args, fargs...)
	rst := r.Do(ctx, args...)
	return rst.Err()
}

func getCmd(query, def string) string {
	for i, r := range query {
		if unicode.IsSpace(r) { // 检查是否为空格或其他空白字符
			return strings.ToUpper(query[:i])
		}
	}
	return def
}

func (r *Redis) cmdCallback(ctx context.Context, cmd redis.Cmder, entry time.Time) {
	if ctx.Value(CtxKey_addcaller) == nil {
		ctx = utils.CtxCaller(ctx, 5) // // 有些命令不准确
	}
	// 构造或查找RedisCommond
	var redisCmd *RedisCommond
	rediscmd, ok := ctx.Value(CtxKey_rediscmd).(*RedisCommond)
	if ok {
		redisCmd = rediscmd
	}
	if redisCmd == nil {
		redisCmd = &RedisCommond{ctx: ctx}
	}

	// 命令赋值
	redisCmd.Cmd = cmd
	redisCmd.Elapsed = time.Since(entry)
	// 填充cmddesc 优先使用ctx中的
	cmddesc, ok := ctx.Value(CtxKey_cmddesc).(string)
	if ok {
		redisCmd.CmdDesc = cmddesc
	}

	logOut := !utils.CtxHasNolog(ctx)
	// 先把命令内容格式化，防止修改Nil
	var cmdStr string
	var replyStr string
	if logOut || cmd.Err() != nil { // 预测要输出日志
		cmdStr = redisCmd.CmdString()
		replyStr = redisCmd.ReplyString()
	}

	// 屏蔽redis.Nil错误
	if cmd.Err() == redis.Nil {
		if ctx.Value(CtxKey_nonilerr) != nil {
			cmd.SetErr(nil)
		}
	}

	if cmd.Err() != nil && redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
		noscript := ctx.Value(CtxKey_noscript)
		if noscript == nil {
			utils.LogCtx(log.Error(), ctx).Err(cmd.Err()).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", cmdStr).
				Msg("Redis NOSCRIPT")
		}
	} else if cmd.Err() != nil && cmd.Err() != redis.Nil {
		utils.LogCtx(log.Error(), ctx).Err(cmd.Err()).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Msg("Redis " + getCmd(cmdStr, "cmd") + " Fail")
	} else if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
		utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("Redis " + getCmd(cmdStr, "cmd") + " Success")
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range r.hook {
			f(ctx, redisCmd)
		}
	}()
}

func (r *Redis) pipelineCallback(ctx context.Context, cmds []redis.Cmder, err error, entry time.Time) error {
	if ctx.Value(CtxKey_addcaller) == nil {
		ctx = utils.CtxCaller(ctx, 4) // // 有些命令不准确
	}
	// 构造或查找RedisCommond
	var redisCmd *RedisCommond
	rediscmd, ok := ctx.Value(CtxKey_rediscmd).(*RedisCommond)
	if ok {
		redisCmd = rediscmd
	}
	if redisCmd == nil {
		redisCmd = &RedisCommond{ctx: ctx}
	}
	// 命令赋值
	redisCmd.Cmds = cmds
	redisCmd.Elapsed = time.Since(entry)
	// 填充cmddesc 优先使用ctx中的
	cmddesc, ok := ctx.Value(CtxKey_cmddesc).(string)
	if ok {
		redisCmd.CmdDesc = cmddesc
	}

	// 回调绑定的
	for _, cmd := range cmds {
		c, _ := cmd.(*redis.Cmd) // 能绑定的都是redis.Cmd类型的命令
		if c == nil {
			continue
		}
		// 取出Cmd中ctx
		cvo := reflect.ValueOf(c).Elem()
		cctx := cvo.FieldByName("ctx")
		tx, _ := reflect.NewAt(cctx.Type(), unsafe.Pointer(cctx.UnsafeAddr())).Elem().Interface().(context.Context)
		if tx != nil {
			rcmd, _ := tx.Value(CtxKey_rediscmd).(*RedisCommond)
			if rcmd == nil {
				continue
			}
			rcmd.Cmd = cmd
			if rcmd.nscallback != nil && redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
				r := rcmd.nscallback()
				c.SetVal(r.Val()) // 修改命令结果
				c.SetErr(r.Err())
			}
			if cmd.Err() != nil {
				utils.LogCtx(log.Error(), ctx).Err(cmd.Err()).Str("cmd", rcmd.CmdString()).Msg("RedisCommond Error")
				continue
			}
			if rcmd.callback != nil {
				err := rcmd.callback(c.Val())
				if err != nil {
					c.SetErr(err)
					if err != redis.Nil { // 有些绑定函数也会返回空
						utils.LogCtx(log.Error(), ctx).Err(err).Str("cmd", rcmd.CmdString()).Msg("RedisCommond Bind Error")
					}
				}
			}
		}
	}

	// 因为上面的回调绑定可能会重新刷新结果 给重新找一个错误，go-redis也是这样找的
	err = nil
	for _, cmd := range cmds {
		if r := cmd.Err(); r != nil {
			err = r
			break
		}
	}

	logOut := !utils.CtxHasNolog(ctx)
	// 先把命令内容格式化，防止修改Nil
	var cmdStr string
	var replyStr string
	if logOut || err != nil { // 预测要输出日志
		cmdStr = redisCmd.CmdString()
		replyStr = redisCmd.ReplyString()
	}

	// 屏蔽redis.Nil错误
	if ctx.Value(CtxKey_nonilerr) != nil {
		for _, cmd := range cmds {
			if cmd.Err() == redis.Nil {
				cmd.SetErr(nil)
			}
		}
	} else {
		// 每个命令是否屏蔽redis.Nil错误
		for _, cmd := range cmds {
			if cmd.Err() == redis.Nil {
				cvo := reflect.ValueOf(cmd).Elem()
				cctx := cvo.FieldByName("ctx")
				tx, _ := reflect.NewAt(cctx.Type(), unsafe.Pointer(cctx.UnsafeAddr())).Elem().Interface().(context.Context)
				if tx != nil {
					if tx.Value(CtxKey_nonilerr) != nil {
						cmd.SetErr(nil)
					}
				}
			}
		}
	}
	// 给重新找一个错误，go-redis也是这样找的
	if err == redis.Nil {
		err = nil
		for _, cmd := range cmds {
			if r := cmd.Err(); r != nil {
				err = r
				break
			}
		}
	}

	if err != nil && err != redis.Nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("RedisPipeline Fail")
	} else if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
		utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("RedisPipeline Success")
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range r.hook {
			f(ctx, redisCmd)
		}
	}()

	return err
}

type hook struct {
	redis *Redis
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	callback := func(ctx context.Context, cmd redis.Cmder) error {
		entry := time.Now()
		err := next(ctx, cmd)
		cmd.SetErr(err) // 会在下一层设置，这里需要提前设置下，cmdCallback中就可以使用了
		h.redis.cmdCallback(ctx, cmd, entry)
		return cmd.Err()
	}
	return callback
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	callback := func(ctx context.Context, cmds []redis.Cmder) error {
		entry := time.Now()
		err := next(ctx, cmds)
		err = h.redis.pipelineCallback(ctx, cmds, err, entry)
		return err
	}
	return callback
}
