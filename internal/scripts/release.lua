-- release: hand an active job back to its stream during graceful shutdown,
-- so another worker can start it immediately instead of after the
-- visibility timeout.
--
-- KEYS[1] job hash  KEYS[2] stream (the job's priority stream)
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] caller's lease token
-- ARGV[4] allowed source states (csv, state.Release)
-- ARGV[5] refund the attempt ('1'|'0')
--
-- Returns {'OK'} | {'MISSING'} | {'LEASE_LOST'} | {'INVALID_STATE', state}.
--
-- The lease token is cleared, so if the handler finishes after all (it
-- ignored cancellation), its complete/fail is rejected as LEASE_LOST. Its
-- side effect may still have happened: duplicate D5 in DESIGN.md.
--
-- Atomic because: split, a crash between XACK and XADD loses the job; the
-- other order leaves two live entries.

if redis.call('EXISTS', KEYS[1]) == 0 then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'MISSING'}
end
local f = redis.call('HMGET', KEYS[1], 'state', 'lease_token', 'attempts', 'id')
if f[2] ~= ARGV[3] then
  return {'LEASE_LOST'}
end
if not allowed(f[1], ARGV[4]) then
  return {'INVALID_STATE', f[1]}
end

local attempts = tonumber(f[3]) or 0
if ARGV[5] == '1' and attempts > 0 then
  attempts = attempts - 1
end
ack_del(KEYS[2], ARGV[1], ARGV[2])
local eid = redis.call('XADD', KEYS[2], '*', 'id', f[4])
redis.call('HSET', KEYS[1], 'state', 'pending', 'stream_entry', eid,
  'attempts', tostring(attempts), 'run_at_ms', fmt(now_ms()))
redis.call('HDEL', KEYS[1], 'lease_token', 'lease_owner', 'lease_until')
return {'OK'}
