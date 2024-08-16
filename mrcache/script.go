package mrcache

// https://github.com/yuwf/gobase2

import "gobase/goredis"

// 注意
// lua层返回return nil 或者直接return， goredis都会识别为空值，即redis.Nil
// local rst = redis.call 如果命令出错会直接返回error，不会再给rst了
// hmset的返回值有点坑，在lua中返回的table n['ok']='OK'

// 读取数据，key不存在返回值为空
// 第一个参数是有效期 其他：field field ..
// 返回值 err=nil时 分两种情况 1:空 数据为空   2:value value .. 和上面 field 对应，不存在对应的Value填充nil
var hmgetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	return redis.call('HMGET', KEYS[1], select(2,unpack(ARGV)))
`)

// 读取多个key数据，第一个key为主key，不存在说明数据为空
// 第一个参数是有效期
// 返回值 err=nil时 分两种情况 1:空 数据为空   2:{field value field value} {field value field value} .. {}的个数和上面key对应，field不存在对应的Value填充nil
var mhgetallScript = goredis.NewScript(`
	-- 先判断主key是否存在
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local resp = {}
	resp[1] = redis.call('HGETALL', KEYS[1])
	-- 读取其他key
	for i = 2, #KEYS do
		resp[i] = {} -- 先赋值
		local rst = redis.call('EXPIRE', KEYS[i], ARGV[1])
		if rst ~= 0 then
			resp[i] = redis.call('HGETALL', KEYS[i])
		end
	end
	return resp
`)

// 新增数据，直接保存
// 第一个参数是有效期 其他: field value field value ..
// 返回值 err=nil时 OK
var hmaddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// 新增数据，直接保存
// 第一个参数是有效期 其他：num(后面field value的个数) field value field value ..  num field value field value ..
// 返回值 err=nil时 OK
var mhmaddScript = goredis.NewScript(`
	local kvpos = 2
	for i = 1, #KEYS do
		local kvnum = tonumber(ARGV[kvpos])
		kvpos = kvpos + 1
		if kvnum > 0 then
			local kv = {}
			for i = 1, kvnum do
				kv[#kv+1] = ARGV[kvpos]
				kvpos = kvpos + 1
			end
			redis.call('HMSET', KEYS[i], unpack(kv))
			redis.call('EXPIRE', KEYS[i], ARGV[1])
		end
	end
	return 'OK'
`)

// 设置数据，key不存在返回值为空
// 第一个参数是有效期 其他: field value field value ..go 读取mysql 时间lua
// 返回值 err=nil时 空：没加载数据 OK:ok
var hmsetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	return 'OK'
`)

// 设置多个key数据，第一个key不存在返回值为空，其他可以不检查
// 第一个参数是有效期，第一个参数是检查的field是否存在 其他：num(后面field value的个数，必须是0或者2的倍数) field value field value ..  num field value field value ..
// 返回值 err=nil时 空：没加载数据 NULL：没有当前自增字段值 OK:ok
var mhmsetScript = goredis.NewScript(`
	for i = 1, #KEYS do
		local rst = redis.call('EXPIRE', KEYS[i], ARGV[1])
		if i ==1 and rst == 0 then
			return -- 第一个不存在 就空数据，另外过期要一块设置
		end
	end
	-- 判断自增key是否存在
	local rst = redis.call('HEXISTS', KEYS[1], ARGV[2])
	if tonumber(rst) == 0 then
		return 'NULL'
	end
	local kvpos = 3
	for i = 1, #KEYS do
		local kvnum = tonumber(ARGV[kvpos])
		kvpos = kvpos + 1
		if kvnum > 0 then
			local kv = {}
			for fi = 1, kvnum, 2 do
				kv[#kv+1] = ARGV[kvpos]
				kv[#kv+1] = ARGV[kvpos+1]
			end
			kvpos = kvpos + 2
			redis.call('HMSET', KEYS[i], unpack(kv))
		end
	end
	return 'OK'
`)

// 修改数据，key不存在返回值为空
// 第一个参数是有效期 其他: field op value field op value ..
// 返回值 err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var hmodifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local fields = {}
	local setkv = {}
	for i = 2, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
		if ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		elseif ARGV[i+1] == "incr" then
			redis.call('HINCRBY', KEYS[1], ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', KEYS[1], ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[1], unpack(setkv))
	end
	-- 返回最新的值
	return redis.call('HMGET', KEYS[1], unpack(fields))
`)

// 修改多个key数据，第一个key不存在返回值为空，其他可以不检查
// 第一个参数是有效期，第一个参数是检查的field是否存在 其他：num(后面field value的个数，必须是0或者3的倍数) field op value field op value ..  num field op value field op value ..
// 返回值 err=nil时 空：没加载数据 2: {"OR" or "NULL"} {field value field value ...} {field value field value ...} ...
// 当为OK时 后面的{}的个数和上面key对应  当为NULL时，表示没有当前自增字段值，后面的{}都不会填充了
var mhmodifyScript = goredis.NewScript(`
	for i = 1, #KEYS do
		local rst = redis.call('EXPIRE', KEYS[i], ARGV[1])
		if i ==1 and rst == 0 then
			return -- 第一个不存在 就空数据，另外过期要一块设置
		end
	end
	local resp = {}
	-- 判断自增key是否存在
	local rst = redis.call('HEXISTS', KEYS[1], ARGV[2])
	if tonumber(rst) == 0 then
		resp[1] = {"NULL"}
		return resp
	end
	resp[1] = {"OK"}
	local kvpos = 3
	for i = 1, #KEYS do
		resp[i+1] = {}
		local kvnum = tonumber(ARGV[kvpos])
		kvpos = kvpos + 1
		if kvnum > 0 then
			local fields = {}
			local setkv = {}
			for fi = 1, kvnum, 3 do
				fields[#fields+1] = ARGV[kvpos]
				if ARGV[kvpos+1] == "set" then
					setkv[#setkv+1] = ARGV[kvpos]
					setkv[#setkv+1] = ARGV[kvpos+2]
				elseif ARGV[kvpos+1] == "incr" then
					redis.call('HINCRBY', KEYS[i], ARGV[kvpos], ARGV[kvpos+2])
				elseif ARGV[kvpos+1] == "fincr" then
					redis.call('HINCRBYFLOAT', KEYS[i], ARGV[kvpos], ARGV[kvpos+2])
				end
			end
			kvpos = kvpos + 3
			if #setkv > 0 then
				redis.call('HMSET', KEYS[i], unpack(setkv))
			end
			local values = redis.call('HMGET', KEYS[i], unpack(fields))
			for n = 1, #fields do
				resp[i+1][n*2-1] = fields[n]
				resp[i+1][n*2] = values[n]
			end
		end
	end
	return resp
`)

// mhmodifyScript脚本简化 每个key只设置一个field
// 第一个参数是有效期，第一个参数是检查的field是否存在 其他：（每个key对应一个）field op value field op value ..  num field op value field op value ..
// 返回值 err=nil时 空：没加载数据 2: {"OR" or "NULL"} {field value} {field value} ...
// 当为OK时 后面的{}的个数和上面key对应  当为NULL时，表示没有当前自增字段值，后面的{}都不会填充了
var mhmodifyoneScript = goredis.NewScript(`
	for i = 1, #KEYS do
		local rst = redis.call('EXPIRE', KEYS[i], ARGV[1])
		if i ==1 and rst == 0 then
			return -- 第一个不存在 就空数据，另外过期要一块设置
		end
	end
	local resp = {}
	-- 判断自增key是否存在
	local rst = redis.call('HEXISTS', KEYS[1], ARGV[2])
	if tonumber(rst) == 0 then
		resp[1] = {"NULL"}
		return resp
	end
	resp[1] = {"OK"}
	local kvpos = 3
	for i = 1, #KEYS do
		resp[i+1] = {}
		resp[i+1][1] = ARGV[kvpos]
		if ARGV[kvpos+1] == "set" then
			redis.call('HSET', KEYS[i], ARGV[kvpos], ARGV[kvpos+2])
			resp[i+1][2] = ARGV[kvpos+2]
		elseif ARGV[kvpos+1] == "incr" then
			resp[i+1][2] = redis.call('HINCRBY', KEYS[i], ARGV[kvpos], ARGV[kvpos+2])
		elseif ARGV[kvpos+1] == "fincr" then
			resp[i+1][2] = redis.call('HINCRBYFLOAT', KEYS[i], ARGV[kvpos], ARGV[kvpos+2])
		else
			resp[i+1][2] = redis.call('HGET', KEYS[i], ARGV[kvpos])
		end
		kvpos = kvpos + 3
	end
	return resp
`)
