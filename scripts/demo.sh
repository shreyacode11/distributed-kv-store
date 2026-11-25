#!/usr/bin/env bash
# Walk-through for Docker Compose: replication, node failure, recovery, prefix scan.
# Usage: docker compose up -d --build && ./scripts/demo.sh
set -u
H=localhost
say() { printf "\n\033[1m== %s\033[0m\n" "$*"; }
put() { curl -s -X PUT "$H:$1/kv/$2" -H 'content-type: application/json' -d "{\"value\":\"$3\"}"; echo; }
get() { curl -s "$H:$1/kv/$2"; echo; }

say "1. Which 3 of the 5 nodes own key 'user:42'?"
curl -s $H:8001/cluster/locate/user:42; echo

say "2. Write via node1, read via node4 (any node can coordinate)"
put 8001 user:42 alice
get 8004 user:42

say "3. Kill one replica of the key (node3 may or may not be one; we kill it anyway)"
docker compose stop node3 >/dev/null 2>&1
sleep 1
say "   Writes and reads still succeed (W=2, R=2 of N=3 replicas)"
put 8001 user:42 alice-v2
get 8002 user:42

say "4. Prefix scan (index) across the cluster with a node down"
for i in 1 2 3 4 5; do put 8001 "user:$i" "name$i" >/dev/null; done
curl -s "$H:8001/scan?prefix=user:&limit=10"; echo

say "5. Bring node3 back: it recovers from its write-ahead log, read repair fixes anything it missed"
docker compose start node3 >/dev/null 2>&1
sleep 3
curl -s $H:8003/cluster/status; echo
get 8003 user:42
