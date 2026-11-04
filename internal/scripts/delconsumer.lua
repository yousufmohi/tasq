-- delconsumer: remove a consumer from a group only if it owns no pending
-- entries.
--
-- KEYS[1] stream
-- ARGV[1] group  ARGV[2] consumer
--
-- Returns {'OK'} | {'BUSY', pending count (capped at 1)}.
--
-- Atomic because: XGROUP DELCONSUMER silently discards the consumer's PEL
-- entries, and those jobs would then never be reclaimed (lost). Checking
-- XPENDING and deleting in separate calls would race with a delivery to the
-- consumer in between.

local p = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2])
if #p > 0 then
  return {'BUSY', '1'}
end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return {'OK'}
