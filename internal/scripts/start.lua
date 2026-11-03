-- start: turn a delivered stream entry into an active job with a new lease.
--
-- KEYS[1] job hash   KEYS[2] stream
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] consumer  ARGV[4] new lease token
-- ARGV[5] allowed source states (csv, from state.Start)
--
-- Returns {'OK', <HGETALL>} or {'SKIP', reason}. A skipped entry is acked
-- and deleted, because it can never become runnable:
--   missing      the hash expired or was purged
--   stale_entry  the hash points at a different entry (at most one live entry
--                per job is an invariant; only that entry may start it)
--   <state>      the job is terminal, or is scheduled/retry and will be
--                re-added to the stream by promote
--
-- Atomic because: check-state + attempts++ + token rotation must be one step,
-- or two workers could both believe they hold the current lease.

if redis.call('EXISTS', KEYS[1]) == 0 then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', 'missing'}
end

local f = redis.call('HMGET', KEYS[1], 'state', 'stream_entry')
local st, entry = f[1], f[2]

if entry ~= ARGV[2] then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', 'stale_entry'}
end
if not allowed(st, ARGV[5]) then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', st}
end

local now = fmt(now_ms())
redis.call('HINCRBY', KEYS[1], 'attempts', 1)
redis.call('HSET', KEYS[1],
  'state', 'active', 'lease_token', ARGV[4], 'lease_owner', ARGV[3],
  'started_ms', now)
return {'OK', redis.call('HGETALL', KEYS[1])}
