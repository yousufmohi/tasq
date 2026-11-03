-- enqueue: create a job record and its stream entry atomically.
--
-- KEYS[1] job hash   KEYS[2] stream
-- ARGV[1] id  ARGV[2] queue  ARGV[3] type  ARGV[4] payload
-- ARGV[5] max_attempts  ARGV[6] priority
--
-- Returns {'OK', created_ms} or {'EXISTS', ''}.
--
-- Atomic because: a crash between HSET and XADD would leave a hash that never
-- runs (or a stream entry pointing at no hash).

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'EXISTS', ''}
end

local now = fmt(now_ms())
local eid = redis.call('XADD', KEYS[2], '*', 'id', ARGV[1])
redis.call('HSET', KEYS[1],
  'id', ARGV[1], 'queue', ARGV[2], 'type', ARGV[3], 'payload', ARGV[4],
  'max_attempts', ARGV[5], 'priority', ARGV[6],
  'state', 'pending', 'attempts', '0',
  'created_ms', now, 'run_at_ms', now, 'stream_entry', eid)
return {'OK', now}
