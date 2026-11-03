-- heartbeat: extend the lease on an active job by resetting its PEL idle time.
--
-- KEYS[1] job hash   KEYS[2] stream
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] consumer  ARGV[4] lease token
-- ARGV[5] visibility timeout ms (recorded as lease_until for operators)
--
-- Returns {'OK'} | {'LEASE_LOST', reason}.
--
-- The lease *is* the PEL idle time: XAUTOCLAIM by another worker happens only
-- once an entry has been idle for the visibility timeout. XCLAIM with
-- min-idle 0 and JUSTID resets idle time without bumping the delivery count.
--
-- Atomic because: an unconditional XCLAIM would steal back an entry that
-- another worker has legitimately reclaimed. The ownership and token checks
-- must happen in the same step as the claim.

local p = redis.call('XPENDING', KEYS[2], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 then
  return {'LEASE_LOST', 'not_pending'}
end
if p[1][2] ~= ARGV[3] then
  return {'LEASE_LOST', 'not_owner'}
end
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[4] then
  return {'LEASE_LOST', 'token'}
end

redis.call('XCLAIM', KEYS[2], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
redis.call('HSET', KEYS[1], 'lease_until', fmt(now_ms() + tonumber(ARGV[5])))
return {'OK'}
