#!/usr/bin/env bash
# Print the benchmark environment as JSON: hardware, OS, toolchain, Redis,
# git revision and the measured client->Redis round trip.
# Usage: bench/hardware.sh <redis-addr> <redis-container>
set -euo pipefail
addr=${1:-localhost:6390}
container=${2:-taskq-bench-redis}

json() { python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))'; }

cpu=$(lscpu | sed -n 's/^Model name:[[:space:]]*//p' | head -1)
threads=$(nproc)
cores=$(lscpu | sed -n 's/^Core(s) per socket:[[:space:]]*//p' | head -1)
mem=$(free -b | awk '/^Mem:/ {print $2}')
kernel=$(uname -r)
os=$(. /etc/os-release && echo "$PRETTY_NAME")
gov=$(go version | awk '{print $3}')
docker_server=$(docker version --format '{{.Server.Version}}')
docker_os=$(docker info --format '{{.OperatingSystem}}')
docker_cpus=$(docker info --format '{{.NCPU}}')
docker_mem=$(docker info --format '{{.MemTotal}}')
redis_version=$(docker exec "$container" redis-cli INFO server | sed -n 's/^redis_version:\(.*\)\r$/\1/p')
redis_fsync=$(docker exec "$container" redis-cli CONFIG GET appendfsync | tail -1)
sha=$(git rev-parse HEAD)
dirty=$( [ -z "$(git status --porcelain -- . ':!bench/results' ':!results')" ] && echo false || echo true )
rtt=$(bench/bin/taskq-bench -mode rtt -redis "$addr")

cat <<EOF
{
  "recorded_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$sha",
  "git_dirty": $dirty,
  "cpu": $(echo "$cpu" | json),
  "cpu_cores": ${cores:-null},
  "cpu_threads": $threads,
  "memory_bytes": $mem,
  "os": $(echo "$os" | json),
  "kernel": $(echo "$kernel" | json),
  "virtualization": "WSL2 guest; Redis runs in Docker Desktop's VM, reached through its port forwarding",
  "go": "$gov",
  "docker_server": "$docker_server",
  "docker_os": $(echo "$docker_os" | json),
  "docker_cpus": $docker_cpus,
  "docker_memory_bytes": $docker_mem,
  "redis_version": "$redis_version",
  "redis_appendfsync": "$redis_fsync",
  "redis_ping_rtt": $rtt
}
EOF
