-- replay: return a dead job to its stream with a fresh attempt budget.
--
-- KEYS[1] job hash  KEYS[2] dead
-- ARGV[1] queue key prefix  ARGV[2] allowed source states (csv, state.Replay)
-- ARGV[3] job id
--
-- Returns {'OK'} | {'MISSING'} | {'INVALID_STATE', state}.
--
-- Atomic because: split, two concurrent replays could both XADD (duplicate
-- run), or a crash after ZREM and before XADD would lose the job.

if redis.call('EXISTS', KEYS[1]) == 0 then
  -- Drop a dangling index entry; there is nothing to replay.
  redis.call('ZREM', KEYS[2], ARGV[3])
  return {'MISSING'}
end
local f = redis.call('HMGET', KEYS[1], 'state', 'priority')
if not allowed(f[1], ARGV[2]) then
  return {'INVALID_STATE', f[1]}
end

local now = fmt(now_ms())
local eid = redis.call('XADD', ARGV[1] .. 's:' .. priority(f[2]), '*', 'id', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[3])
redis.call('HSET', KEYS[1], 'state', 'pending', 'attempts', '0',
  'stream_entry', eid, 'run_at_ms', now)
redis.call('HDEL', KEYS[1], 'finished_ms', 'lease_owner')
-- A dead job may carry a TTL set by an operator; a live job must not expire.
redis.call('PERSIST', KEYS[1])
return {'OK'}
