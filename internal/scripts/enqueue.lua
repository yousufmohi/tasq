-- enqueue: create a job record and either its stream entry (run now) or its
-- sched ZSET member (run later), atomically.
--
-- KEYS[1] job hash   KEYS[2] stream   KEYS[3] sched
-- ARGV[1] id  ARGV[2] queue  ARGV[3] type  ARGV[4] payload
-- ARGV[5] max_attempts  ARGV[6] priority  ARGV[7] timeout ms (0 = none)
-- ARGV[8] delay ms (0 = run now), relative to the Redis clock
--
-- Returns {'OK', created_ms} or {'EXISTS', ''}.
--
-- Atomic because: a crash between HSET and XADD/ZADD would leave a hash that
-- never runs (or an entry pointing at no hash).

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'EXISTS', ''}
end

local now = now_ms()
local delay = tonumber(ARGV[8]) or 0
local created = fmt(now)

redis.call('HSET', KEYS[1],
  'id', ARGV[1], 'queue', ARGV[2], 'type', ARGV[3], 'payload', ARGV[4],
  'max_attempts', ARGV[5], 'priority', ARGV[6], 'timeout_ms', ARGV[7],
  'attempts', '0', 'created_ms', created)

if delay > 0 then
  local run_at = fmt(now + delay)
  redis.call('ZADD', KEYS[3], run_at, ARGV[1])
  redis.call('HSET', KEYS[1], 'state', 'scheduled', 'run_at_ms', run_at)
else
  local eid = redis.call('XADD', KEYS[2], '*', 'id', ARGV[1])
  redis.call('HSET', KEYS[1], 'state', 'pending', 'run_at_ms', created,
    'stream_entry', eid)
end
return {'OK', created}
