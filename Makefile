.PHONY: up down demo test unit load local stop-local
up:        ; docker compose up -d --build
down:      ; docker compose down -v
demo:      ; ./scripts/demo.sh
unit:      ; pytest -m "not e2e" -q
test:      ; pytest -q
load:      ; python loadtest/loadtest.py --duration 15 --concurrency 32
local:     ; ./scripts/run_local.sh
stop-local:; ./scripts/stop_local.sh
