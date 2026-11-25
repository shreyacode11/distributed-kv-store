#!/usr/bin/env bash
# Start a 5-node cluster on localhost:8001-8005 without Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs data/local
PEERS=""
for i in 1 2 3 4 5; do PEERS+="node$i=http://127.0.0.1:800$i,"; done
PEERS="${PEERS%,}"
: > logs/pids
for i in 1 2 3 4 5; do
  NODE_ID=node$i PEERS="$PEERS" DATA_DIR=data/local \
    python -m uvicorn kvnode.main:app --port 800$i --log-level warning > logs/node$i.log 2>&1 &
  echo "$!" >> logs/pids
done
for i in 1 2 3 4 5; do
  for _ in $(seq 1 50); do curl -sf localhost:800$i/health >/dev/null && break || sleep 0.2; done
done
echo "cluster up: http://localhost:8001 ... http://localhost:8005   (stop with scripts/stop_local.sh)"
