#!/bin/sh
set -eu
if [ ! -f /data/sentinel.conf ]; then
  cat > /data/sentinel.conf <<EOF
bind 0.0.0.0
protected-mode no
port 26379
dir /data
sentinel monitor flight-cache 172.30.87.10 6379 2
sentinel down-after-milliseconds flight-cache 3000
sentinel failover-timeout flight-cache 10000
sentinel parallel-syncs flight-cache 1
sentinel announce-ip ${NODE_IP}
sentinel announce-port 26379
EOF
fi
exec redis-sentinel /data/sentinel.conf
