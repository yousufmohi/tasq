-- fail: record a handler failure. Either schedules a retry or moves the job
-- to the dead-letter set.
--
-- KEYS[1] job hash  KEYS[2] stream  KEYS[3] sched  KEYS[4] dead
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] caller's lease token
-- ARGV[4] error message  ARGV[5] retry delay ms  ARGV[6] permanent ('1'|'0')
-- ARGV[7] allowed source states (csv, state.Fail == state.FailFinal sources)
--
-- Returns {'RETRY', run_at_ms} | {'DEAD'} | {'MISSING'} | {'LEASE_LOST'} |
--         {'INVALID_STATE', state}.
--
-- Atomic because: split, a crash between XACK and ZADD either loses the job
-- (acked, never rescheduled) or duplicates it (rescheduled, and also
-- reclaimed from the PEL).

if redis.call('EXISTS', KEYS[1]) == 0 then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'MISSING'}
end

local f = redis.call('HMGET', KEYS[1], 'state', 'lease_token', 'attempts', 'max_attempts')
if f[2] ~= ARGV[3] then
  return {'LEASE_LOST'}
end
if not allowed(f[1], ARGV[7]) then
  return {'INVALID_STATE', f[1]}
end
local attempts = tonumber(f[3]) or 0
local max = tonumber(f[4]) or 1

local now = now_ms()
ack_del(KEYS[2], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[1], 'stream_entry', 'lease_token')

if ARGV[6] ~= '1' and attempts < max then
  local run_at = now + tonumber(ARGV[5])
  redis.call('HSET', KEYS[1], 'state', 'retry', 'last_error', ARGV[4],
    'run_at_ms', fmt(run_at))
  redis.call('ZADD', KEYS[3], fmt(run_at), redis.call('HGET', KEYS[1], 'id'))
  return {'RETRY', fmt(run_at)}
end

redis.call('HSET', KEYS[1], 'state', 'dead', 'last_error', ARGV[4],
  'finished_ms', fmt(now))
redis.call('ZADD', KEYS[4], fmt(now), redis.call('HGET', KEYS[1], 'id'))
return {'DEAD'}
