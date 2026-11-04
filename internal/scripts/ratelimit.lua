-- ratelimit: take one token from the queue's GCRA rate limiter.
--
-- KEYS[1] cfg  KEYS[2] rate limiter state (theoretical arrival time, µs)
--
-- Returns {'OK'} | {'WAIT', ms until a token is available}.
--
-- GCRA (generic cell rate algorithm): with emission interval T = 1/rate and
-- burst B, a request is allowed if the new theoretical arrival time is no
-- more than B·T ahead of now. One key, O(1), no background refill.
-- Rate and burst are read from cfg on every call, so changes apply
-- immediately without restarting workers.
--
-- Atomic because: read-compare-write on the TAT from many workers at once
-- would otherwise let them all pass and exceed the rate.

local rate = tonumber(redis.call('HGET', KEYS[1], 'rate')) or 0
if rate <= 0 then
  return {'OK'}
end
local burst = tonumber(redis.call('HGET', KEYS[1], 'burst')) or 1
if burst < 1 then burst = 1 end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local T = 1000000 / rate

local tat = tonumber(redis.call('GET', KEYS[2])) or now
if tat < now then tat = now end
local new_tat = tat + T
local allow_at = new_tat - burst * T
if now < allow_at then
  return {'WAIT', tostring(math.ceil((allow_at - now) / 1000))}
end
redis.call('SET', KEYS[2], fmt(new_tat), 'PX', math.ceil((new_tat - now) / 1000) + 1000)
return {'OK'}
