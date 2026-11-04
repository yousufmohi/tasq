-- enqueue: create a job record and either its stream entry (run now) or its
-- sched ZSET member (run later), atomically, subject to the queue's depth
-- limit and an optional idempotency key.
--
-- KEYS[1] job hash  KEYS[2] sched  KEYS[3] cfg
-- KEYS[4] high stream  KEYS[5] default stream  KEYS[6] low stream
-- KEYS[7] idempotency key (ignored when ARGV[9] is empty)
-- ARGV[1] id  ARGV[2] queue  ARGV[3] type  ARGV[4] payload
-- ARGV[5] max_attempts  ARGV[6] priority (high|default|low)
-- ARGV[7] timeout ms (0 = none)
-- ARGV[8] delay ms (0 = run now), relative to the Redis clock
-- ARGV[9] idempotency key value ('' = none)  ARGV[10] its TTL in ms
--
-- Returns {'OK', created_ms} | {'DUP', existing job id} | {'EXISTS', ''} |
--         {'FULL', depth}.
--
-- The duplicate check comes before the depth check: retrying an enqueue
-- that already succeeded must return the original job, not QUEUE_FULL.
--
-- Depth = entries in all priority streams (undelivered + in flight) + sched
-- members. It counts every job not yet finished.
--
-- Atomic because: a crash between HSET and XADD/ZADD would leave a hash that
-- never runs; concurrent producers with the same idempotency key must not
-- both pass the existence check; and the depth check must see the same state
-- the write changes, or concurrent producers could overshoot.

if ARGV[9] ~= '' then
  local existing = redis.call('GET', KEYS[7])
  if existing then
    return {'DUP', existing}
  end
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'EXISTS', ''}
end

local stream
if ARGV[6] == 'high' then stream = KEYS[4]
elseif ARGV[6] == 'default' then stream = KEYS[5]
elseif ARGV[6] == 'low' then stream = KEYS[6]
else return redis.error_reply('taskq: unknown priority ' .. ARGV[6]) end

local max_depth = tonumber(redis.call('HGET', KEYS[3], 'max_depth')) or 0
if max_depth > 0 then
  local depth = redis.call('XLEN', KEYS[4]) + redis.call('XLEN', KEYS[5])
    + redis.call('XLEN', KEYS[6]) + redis.call('ZCARD', KEYS[2])
  if depth >= max_depth then
    return {'FULL', tostring(depth)}
  end
end

local now = now_ms()
local delay = tonumber(ARGV[8]) or 0
local created = fmt(now)

redis.call('HSET', KEYS[1],
  'id', ARGV[1], 'queue', ARGV[2], 'type', ARGV[3], 'payload', ARGV[4],
  'max_attempts', ARGV[5], 'priority', ARGV[6], 'timeout_ms', ARGV[7],
  'attempts', '0', 'created_ms', created)
if ARGV[9] ~= '' then
  redis.call('HSET', KEYS[1], 'idem_key', ARGV[9])
  redis.call('SET', KEYS[7], ARGV[1], 'PX', ARGV[10])
end

if delay > 0 then
  local run_at = fmt(now + delay)
  redis.call('ZADD', KEYS[2], run_at, ARGV[1])
  redis.call('HSET', KEYS[1], 'state', 'scheduled', 'run_at_ms', run_at)
else
  local eid = redis.call('XADD', stream, '*', 'id', ARGV[1])
  redis.call('HSET', KEYS[1], 'state', 'pending', 'run_at_ms', created,
    'stream_entry', eid)
end
return {'OK', created}
