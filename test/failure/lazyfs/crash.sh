#!/bin/sh
# Simulate a host crash for Redis: SIGKILL redis-server, drop every write
# LazyFS holds that was never fsynced, then let entrypoint.sh restart Redis
# on what fsync actually made durable.
set -eu
touch /lazy/hold
pid=$(cat /lazy/redis.pid)
kill -KILL "$pid"
while kill -0 "$pid" 2>/dev/null; do sleep 0.01; done
# LazyFS keeps the completion FIFO open, so read exactly one line from it
# (the acknowledgement) rather than waiting for end-of-file.
timeout 30 head -n 1 /lazy/faults_done.fifo > /lazy/clear.out &
reader=$!
echo "lazyfs::clear-cache" > /lazy/faults.fifo
wait "$reader"
grep -q "finished::clear-cache" /lazy/clear.out
rm -f /lazy/hold
