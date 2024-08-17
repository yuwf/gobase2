package mrcache

// https://github.com/yuwf/gobase2

import "gobase/goredis"

// 【注意】
// lua层返回return nil 或者直接return， goredis都会识别为空值，即redis.Nil
// local rst = redis.call 如果命令出错会直接返回error，不会再给rst了
// hmset的返回值有点坑，在lua中返回的table n['ok']='OK'
// 空值nil不要写入到redis中，给reids写nil值时，redis会写入空字符串，对一些自增类型的值，后面自增会有问题

// row /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 读取数据，key不存在返回值为空
// 第一个参数是有效期 其他：field field ..
// 返回值 err=nil时 1:空 数据为空   2:value value .. 和上面 field 对应，不存在对应的Value填充nil
var rowGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	return redis.call('HMGET', KEYS[1], select(2,unpack(ARGV)))
`)

// row 新增数据，直接保存
// 第一个参数是有效期 其他: field value field value ..
// 返回值 err=nil时 OK
var rowAddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// row 设置数据，key不存在返回值为空
// 第一个参数是有效期 其他: field value field value ..
// 返回值 err=nil时 1：空：数据为空  2：OK
var rowSetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	return 'OK'
`)

// row 修改数据，key不存在返回值为空
// 第一个参数是有效期 其他: field op value field op value ..
// 返回值 err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowModifyScript = goredis.NewScript(`
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

// rows /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// rows 读取数据，第一个key为索引key，redis不存在说明数据为空
// 第一个参数是有效期 其他：field field ..
// 返回值 err=nil时 1:空 数据为空  2:{value value ..} {value value ..} .. {}的个数为结果数量 每个{value value}的和上面的field对应，不存在对应的Value填充nil
var rowsGetScript = goredis.NewScript(`
	-- 先判断主key是否存在
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local datakeys = redis.call('HVALS', KEYS[1])
	-- 读取key
	local resp = {}
	for i = 1, #datakeys do
		local rst = redis.call('EXPIRE', datakeys[i], ARGV[1])
		if rst == 0 then
			return -- 数据不一致了 返回空 重新读
		end
		resp[i] = redis.call('HMGET', datakeys[i], select(2,unpack(ARGV)))
	end
	return resp
`)

// rows 新增数据，直接保存，总key放第一个位置
// 第一个参数是有效期 其他：num(后面field value的个数) field value field value ..  num field value field value ..
// 返回值 err=nil时 OK
var rowsAddScript = goredis.NewScript(`
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

// row 设置数据，第一个key为索引key，第二个为数据key, datakey不存在返回值为空
// 第一个参数是有效期 其他: field value field value ..
// 返回值 err=nil时 1：空：数据为空  2：OK
var rowsSetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[2], ARGV[1])
	if rst == 0 then
		return
	end
	redis.call('HMSET', KEYS[2], select(2,unpack(ARGV)))
	-- 设置索引key的倒计时
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// rows 修改数据，第一个key为索引key，第二个为datakey, datakey不存在返回值为空
// 第一个参数是有效期 其他: field op value field op value ..
// 返回值 err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowsModifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[2], ARGV[1])
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
			redis.call('HINCRBY', KEYS[2], ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', KEYS[2], ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[2], unpack(setkv))
	end
	-- 设置索引key的倒计时
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	-- 返回最新的值
	return redis.call('HMGET', KEYS[2], unpack(fields))
`)

// column /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// column 读取数据，第一个key为主key，redis不存在说明数据为空
// 第一个参数是有效期 第二个参数为主key的位置(lua索引位置)
// 返回值 err=nil时 1:空 数据为空  2:{field value field value ..} {field value field value ..} .. {}的个数和上面key对应 field就是自增字段
// 每个{}中的value数量可能不一致，因为nil不会接入redis
var columnGetScript = goredis.NewScript(`
	-- 先判断主key是否存在
	local rst = redis.call('EXPIRE', KEYS[ARGV[2]], ARGV[1])
	if rst == 0 then
		return
	end
	local resp = {}
	for i = 1, #KEYS do
		resp[i] = {} -- 先赋值
		local rst = 1
		if i ~= ARGV[2] then // 不是主key先设置过期时间
			rst = redis.call('EXPIRE', KEYS[i], ARGV[1])
		end
		if rst ~= 0 then
			resp[i] = redis.call('HGETALL', KEYS[i])
		end
	end
	return resp
`)

// column 读取一个数据，第一个key为主key，redis不存在说明数据为空
// 第一个参数是有效期，第二个参数为主key的位置(lua索引位置) 第三个参数为读取自增id
// 返回值 err=nil时 空：没加载数据 2: {"OK" or "NULL"}  {value  value ..}
// 当为OK时 后面的value的个数和上面key对应  当为NULL时，表示没有当前自增字段值，后面的{}都不会填充了
var columnGetOneScript = goredis.NewScript(`
	-- 先判断主key是否存在
	local rst = redis.call('EXPIRE', KEYS[ARGV[2]], ARGV[1])
	if rst == 0 then
		return
	end
	-- 其他key一块设置过期
	for i = 1, #KEYS do
		if i ~= ARGV[2] then
			redis.call('EXPIRE', KEYS[i], ARGV[1])
		end
	end
	local resp = {}
	-- 判断自增key是否存在
	local rst = redis.call('HEXISTS', KEYS[1], ARGV[3])
	if tonumber(rst) == 0 then
		resp[1] = {"NULL"}
		return resp
	end
	resp[1] = {"OK"}
	resp[2] = {}
	for i = 1, #KEYS do
		resp[2][i] = redis.call('HGET', KEYS[i], ARGV[3])
	end
	return resp
`)

// column 新增数据，直接保存
// 第一个参数是有效期 其他：num(后面field value的个数) field value field value ..  num field value field value ..
// 返回值 err=nil时 OK
var columnAddScript = goredis.NewScript(`
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

// column 设置数据，第一个key不存在返回值为空，其他可以不检查
// 第一个参数是有效期，第二个为自增字段 其他：（每个key对应一个）op value op value ..
// 返回值 err=nil时 1：空：数据为空  2：NULL 没有当前自增字段值  3：OK
var columnSetScript = goredis.NewScript(`
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
		if ARGV[kvpos] == "set" then
			redis.call('HSET', KEYS[i], ARGV[2], ARGV[kvpos+1])
		end
		kvpos = kvpos + 2
	end
	return 'OK'
`)

// columnModifyScript脚本简化 每个key只设置一个field
// 第一个参数是有效期，第二个为自增字段  其他：（每个key对应一个）op value op value ..
// 返回值 err=nil时 空：没加载数据 2: {"OK" or "NULL"} {value  value ..}
// 当为OK时 后面的value的个数和上面key对应  当为NULL时，表示没有当前自增字段值，后面的{}都不会填充了
var columnModifyOneScript = goredis.NewScript(`
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
	resp[2] = {}
	local kvpos = 3
	for i = 1, #KEYS do
		if ARGV[kvpos] == "set" then
			redis.call('HSET', KEYS[i], ARGV[2], ARGV[kvpos+1])
			resp[2][i] = ARGV[kvpos+1]
		elseif ARGV[kvpos] == "incr" then
			resp[2][i] = redis.call('HINCRBY', KEYS[i], ARGV[2], ARGV[kvpos+1])
		elseif ARGV[kvpos] == "fincr" then
			resp[2][i] = redis.call('HINCRBYFLOAT', KEYS[i], ARGV[2], ARGV[kvpos+1])
		else
			resp[2][i] = redis.call('HGET', KEYS[i], ARGV[2])
		end
		kvpos = kvpos + 2
	end
	return resp
`)
