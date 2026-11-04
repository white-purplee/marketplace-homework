#!/bin/sh
set -eu
# Redis rewrites this writable config during failover; preserve it on restart.
if [ ! -f /data/redis.conf ]; then
  cat > /data/redis.conf <<EOF
bind 0.0.0.0
protected-mode no
port 6379
dir /data
appendonly yes
replica-announce-ip ${NODE_IP}
replica-announce-port 6379
EOF
  if [ -n "${INITIAL_PRIMARY:-}" ]; then
    echo "replicaof ${INITIAL_PRIMARY} 6379" >> /data/redis.conf
  fi
fi
exec redis-server /data/redis.conf
