#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/portfolio/hybrid-fintech-lakehouse}"
PID_DIR="${PID_DIR:-$PROJECT_DIR/run/realtime}"

if [[ ! -d "$PID_DIR" ]]; then
  echo "[INFO] Brak katalogu PID. Nic do zatrzymania."
  exit 0
fi

found=0
for pid_file in "$PID_DIR"/*.pid; do
  [[ -e "$pid_file" ]] || continue
  found=1
  pid="$(cat "$pid_file")"

  if kill -0 "$pid" 2>/dev/null; then
    echo "[STOP] $(basename "$pid_file") PID=$pid"
    kill "$pid"
  else
    echo "[INFO] Proces PID=$pid już nie działa."
  fi

  rm -f "$pid_file"
done

if [[ "$found" -eq 0 ]]; then
  echo "[INFO] Brak aktywnych plików PID."
else
  echo "[OK] Procesy realtime zatrzymane."
fi
