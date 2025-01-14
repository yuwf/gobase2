package mrcache

// https://github.com/yuwf/gobase2

import "gobase/goredis"

// 【注意】
// lua层返回return nil 或者直接return， goredis都会识别为空值，即redis.Nil
// local rst = redis.call 如果命令出错会直接返回error，不会再给rst了
// hmset的返回值有点坑，在lua中返回的table n['ok']='OK'
// 空值nil不要写入到redis中，给reids写nil值时，redis会写入空字符串，对一些自增类型的值，后面自增会有问题

// 自增 总key
// 参数：第一个自增的field，正常情况用tablename，第二个参数表示拆表的个数(tablecount，0:不拆表)，第三个表示第几个表
// 返回值：err=nil时 自增值
var incrScript = goredis.NewScript(`
	local tableCount = tonumber(ARGV[2])
	local incrIndex = tonumber(ARGV[3])
	if tableCount == 0 then
		return redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
	end
	local rst = redis.call('HINCRBY', KEYS[1], ARGV[1], tableCount)
	local mod = rst % tableCount
	if mod ~= incrIndex then
		rst = rst - mod + incrIndex
		redis.call('HSET', KEYS[1], ARGV[1], rst)
	end
	return rst
`)

// row /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 读取数据
// key：生成的key key不存在返回值为空
// 参数：第一个是有效期 其他：field field ..
// 返回值：err=nil时 1:空 数据为空   2:value value .. 和上面 field 对应，不存在对应的Value填充nil
var rowGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	return redis.call('HMGET', KEYS[1], select(2,unpack(ARGV)))
`)

// row 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他: field value field value ..
// 返回值：err=nil时 OK
var rowAddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// row 设置数据，key不存在返回值为空
// 参数：第一个是有效期 其他: field op value field op value ..
// 返回值 err=nil时 1：空：数据为空  2：OK
var rowSetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	if #ARGV == 1 then -- 只有一个过期时间
		return 'OK'
	end
	local setkv = {}
	for i = 2, #ARGV, 3 do
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[1], ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[1], unpack(setkv))
	end
	return 'OK'
`)

// row 修改数据 有返回值
// key：生成的key，key不存在返回值为空
// 参数：第一个是有效期 其他: field op value field op value ..
// 返回值：err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowModifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local fields = {}
	local setkv = {}
	for i = 2, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[1], ARGV[i])
		elseif ARGV[i+1] == "set" then
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
	if #fields == 0 then
		return {}
	end
	-- 返回最新的值
	return redis.call('HMGET', KEYS[1], unpack(fields))
`)

// rows /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// rows 读取数据
// key：第一个key为索引key，redis不存在说明数据为空
// 参数：第一个是有效期 其他：field field ..
// 返回值：err=nil时 1:空 数据为空  2:{value value ..} {value value ..} .. {}的个数为结果数量 每个{value value}的和上面的field对应，不存在对应的value填充nil
var rowsGetAllScript = goredis.NewScript(`
	-- 先判断索引key是否存在
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

// rows 读取多条数据
// key：第一个key为索引key，redis不存在说明数据为空，后面的多个key为dataKeyValue, 任何一个dataKeyValue不存在返回值为空
// 参数：第一个是有效期 其他：field field ..
// 返回值：err=nil时 1:空 数据为空  2:{value value ..} {value value ..} .. {}的个数为结果数量 每个{value value}的和上面的field对应，不存在对应的Value填充nil
var rowsGetsScript = goredis.NewScript(`
	-- 先判断索引key是否存在
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local datakeys = redis.call('HMGET', KEYS[1], select(2,unpack(KEYS)))
	-- 读取key
	local resp = {}
	local idx = 1
	for i = 1, #datakeys do
		if datakeys[i] then -- 数据不为nil时才填充，nil时不填充
			local rst = redis.call('EXPIRE', datakeys[i], ARGV[1])
			if rst == 0 then
				return -- 数据不一致了 返回空 重新读
			end
			resp[idx] = redis.call('HMGET', datakeys[i], select(2,unpack(ARGV)))
			idx = idx + 1
		end
	end
	return resp
`)

// rows 读取数据
// key：第一个key为索引key，redis不存在说明数据为空
// 参数：第一个是有效期 第二个为数据dataKeyValue, dataKeyValue不存在返回值为空 其他：field field ..
// 返回值 err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowsGetScript = goredis.NewScript(`
	local datakey = redis.call('HGET', KEYS[1], ARGV[2])
	if not datakey then
		return
	end
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	redis.call('EXPIRE', datakey, ARGV[1])
	return redis.call('HMGET', datakey, select(3,unpack(ARGV)))
`)

// rows 新增数据
// key：第一个key为索引key 其他数据key列表， 每个key和下面的数据对应
// 参数：第一个是有效期 其他：num(后面field value的个数) field value field value ..  num field value field value ..
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

// rows 删除全部数据
// key：第一个key为索引key，第二个为数据key,
// 参数：dataValue
// 返回值：err=nil时 OK
var rowsDelScript = goredis.NewScript(`
	-- 删除索引key中的数据key
	local rst = redis.call('HDEL', KEYS[1], ARGV[1])
	if rst == 0 then
		return 'OK'
	end
	-- 删除数据key
	redis.call('DEL', KEYS[2])
	return 'OK'
`)

// rows 删除全部数据
// key：第一个key为索引key
// 返回值：err=nil时 OK
var rowsDelAllScript = goredis.NewScript(`
	local datakeys = redis.call('HVALS', KEYS[1])
	-- 删除索引key
	local rst = redis.call('DEL', KEYS[1])
	if rst == 0 then
		return 'OK'
	end
	-- 删除数据key
	redis.call('DEL', unpack(datakeys))
	return 'OK'
`)

// row 设置数据
// key：第一个key为索引key，第二个为数据key, 数据key不存在返回值为空
// 参数：第一个是有效期 其他: field value field value ..
// 返回值：err=nil时 1：空：数据为空  2：OK
var rowsSetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[2], ARGV[1])
	if rst == 0 then
		return
	end
	if #ARGV == 1 then -- 只有一个过期时间
		return 'OK'
	end
	local setkv = {}
	for i = 2, #ARGV, 3 do
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[2], ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[2], unpack(setkv))
	end
	
	-- 设置索引key的倒计时
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// rows 修改数据 有返回值
// key：第一个key为索引key，第二个为数据key, 数据key不存在返回值为空
// 参数：第一个是有效期 其他: field op value field op value ..
// 返回值：err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowsModifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[2], ARGV[1])
	if rst == 0 then
		return
	end
	local fields = {}
	local setkv = {}
	for i = 2, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[2], ARGV[i])
		elseif ARGV[i+1] == "set" then
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
// column 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他：field value field value
// 返回值：err=nil时 OK
var columnAddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// column 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他：field value
// 返回值：err=nil时 OK  空不存在
var columnSetScript = goredis.NewScript(`
	local exists = redis.call('HEXISTS', key, ARGV[2])
	if exists == 0 then
		return
	end
	redis.call('HMSET', KEYS[1], ARGV[2], ARGV[3])
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)
