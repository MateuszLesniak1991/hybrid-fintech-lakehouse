#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/portfolio/hybrid-fintech-lakehouse}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs/realtime}"
PID_DIR="${PID_DIR:-$PROJECT_DIR/run/realtime}"

MIN_INTERVAL="${MIN_INTERVAL:-2}"
MAX_INTERVAL="${MAX_INTERVAL:-8}"

mkdir -p "$LOG_DIR" "$PID_DIR"
cd "$PROJECT_DIR"

[[ -x venv/bin/python ]] || {
  echo "[ERROR] Brak venv/bin/python"
  exit 1
}

source venv/bin/activate

stop_existing() {
  for pid_file in "$PID_DIR"/*.pid; do
    [[ -e "$pid_file" ]] || continue
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "[STOP] PID $pid ($(basename "$pid_file"))"
      kill "$pid"
    fi
    rm -f "$pid_file"
  done
}

cleanup() {
  echo
  echo "[INFO] Zatrzymywanie procesów realtime..."
  stop_existing
}
trap cleanup INT TERM EXIT

stop_existing

echo "[1/3] PostgreSQL outbox -> Redpanda"
nohup python streaming/realtime_outbox_to_redpanda.py \
  --poll-interval 0.5 \
  --fetch-size 20 \
  --log-level INFO \
  >> "$LOG_DIR/outbox_to_redpanda.log" 2>&1 &
echo $! > "$PID_DIR/outbox.pid"

echo "[2/3] Redpanda -> Azure Event Hub"
nohup python cloud/azure/redpanda_to_eventhub.py \
  --batch-size 100 \
  --batch-timeout 2 \
  --poll-timeout 0.5 \
  --idle-timeout 0 \
  --log-level INFO \
  >> "$LOG_DIR/redpanda_to_eventhub.log" 2>&1 &
echo $! > "$PID_DIR/bridge.pid"

sleep 2

echo "[3/3] Realtime banking generator"
nohup python streaming/realtime_banking_generator.py \
  --min-interval "$MIN_INTERVAL" \
  --max-interval "$MAX_INTERVAL" \
  --max-events 0 \
  --log-level INFO \
  >> "$LOG_DIR/banking_generator.log" 2>&1 &
echo $! > "$PID_DIR/generator.pid"

echo
echo "[OK] Realtime streaming działa."
echo
echo "PID-y:"
for f in "$PID_DIR"/*.pid; do
  printf "  %-24s %s\n" "$(basename "$f")" "$(cat "$f")"
done

echo
echo "Logi:"
echo "  tail -f $LOG_DIR/banking_generator.log"
echo "  tail -f $LOG_DIR/outbox_to_redpanda.log"
echo "  tail -f $LOG_DIR/redpanda_to_eventhub.log"
echo
echo "Zatrzymanie:"
echo "  bash scripts/stop_realtime_streaming.sh"

trap - INT TERM EXIT
