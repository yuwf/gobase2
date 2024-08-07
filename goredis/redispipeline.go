package goredis

// https://github.com/yuwf/gobase2

import (
	"context"
	"errors"

	"gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// 支持绑定的管道
type RedisPipeline struct {
	redis.Pipeliner
}

func (r *Redis) NewPipeline() *RedisPipeline {
	pipeline := &RedisPipeline{
		Pipeliner: r.Pipeline(),
	}
	return pipeline
}

// 统一的命令
func (p *RedisPipeline) Cmd(ctx context.Context, args ...interface{}) RedisResultBind {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return redisCmd
}

// 此函数提交的管道命令，不会产生nil的错误
func (p *RedisPipeline) ExecNoNil(ctx context.Context) ([]redis.Cmder, error) {
	return p.Pipeliner.Exec(context.WithValue(ctx, CtxKey_nonilerr, 1))
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMGetObj(ctx context.Context, key string, v interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	// 获取结构数据
	tags, elemts, err := utils.StructTagsAndValueOs(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	if len(tags) == 0 {
		err := errors.New("structmem invalid")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}

	redisCmd.bindobj = v
	redisCmd.BindValues(elemts) // 管道里这个不会返回错误

	args := []interface{}{"hmget", key}
	args = append(args, tags...)
	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return cmd.Err()
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMSetObj(ctx context.Context, key string, v interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	fargs, err := utils.StructTagValues(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	if len(fargs) == 0 {
		err := errors.New("structmem invalid")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	// 组织参数
	args := []interface{}{"hmset", key}
	args = append(args, fargs...)
	cmd := p.Pipeliner.Do(ctx, args...)
	return cmd.Err()
}
