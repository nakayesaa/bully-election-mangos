#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
go build -o bully bully.go
pids=()
cleanup() {
  trap - EXIT INT TERM
  if ((${#pids[@]})); then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for id in 0 1 2 3 4 5 6 7; do
  mode=live
  [[ $id != 4 ]] || mode=live_init
  [[ $id != 7 ]] || mode=dead
  ./bully "$id" "$mode" &
  pids+=("$!")
done
wait
