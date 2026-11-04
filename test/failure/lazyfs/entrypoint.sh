#!/bin/sh
# Mount LazyFS on /lazy/mnt (backed by /lazy/root), then keep redis-server
# running with its data directory on the mount. When redis-server dies the
# loop restarts it, unless /lazy/hold exists (crash.sh holds it down while
# the unsynced data is dropped). Arguments are passed to redis-server.
set -eu
mkdir -p /lazy/root /lazy/mnt
cat > /lazy/config.toml <<CFG
[faults]
fifo_path="/lazy/faults.fifo"
fifo_path_completed="/lazy/faults_done.fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="2gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile="/lazy/lazyfs.log"
CFG
lazyfs /lazy/mnt --config-path /lazy/config.toml \
  -o allow_other -o modules=subdir -o subdir=/lazy/root &
i=0
until grep -q " /lazy/mnt fuse" /proc/mounts; do
  i=$((i + 1))
  if [ "$i" -gt 200 ]; then echo "lazyfs did not mount" >&2; exit 1; fi
  sleep 0.05
done
echo "lazyfs mounted"
while :; do
  redis-server --dir /lazy/mnt "$@" &
  echo $! > /lazy/redis.pid
  wait $! || true
  while [ -e /lazy/hold ]; do sleep 0.02; done
done
