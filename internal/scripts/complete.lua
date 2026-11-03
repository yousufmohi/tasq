-- complete: mark an active job succeeded and remove its stream entry.
--
-- KEYS[1] job hash   KEYS[2] stream
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] caller's lease token
-- ARGV[4] retention ms (0 = keep forever)
-- ARGV[5] allowed source states (csv, from state.Complete)
--
-- Returns {'OK'} | {'MISSING'} | {'LEASE_LOST'} | {'INVALID_STATE', state}.
-- On LEASE_LOST nothing is written: another worker owns the job now, and
-- acking its entry would hide the job from crash recovery.
--
-- Atomic because: split, a crash between XACK and HSET leaves a job that is
-- acked but never marked succeeded, or marked succeeded but still pending
-- in the PEL (and so re-executed).

if redis.call('EXISTS', KEYS[1]) == 0 then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'MISSING'}
end

local f = redis.call('HMGET', KEYS[1], 'state', 'lease_token')
if f[2] ~= ARGV[3] then
  return {'LEASE_LOST'}
end
if not allowed(f[1], ARGV[5]) then
  return {'INVALID_STATE', f[1]}
end

ack_del(KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[1], 'state', 'succeeded', 'finished_ms', fmt(now_ms()))
redis.call('HDEL', KEYS[1], 'stream_entry')
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return {'OK'}
