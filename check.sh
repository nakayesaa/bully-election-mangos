#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
logfile=$(mktemp)
trap 'rm -f "$logfile"' EXIT
status=0
timeout --signal=TERM 9s bash run.sh >"$logfile" 2>&1 || status=$?
cat "$logfile"
[[ $status == 0 || $status == 124 ]]
[[ $(grep -c 'MENJADI COORDINATOR' "$logfile") == 1 ]]
grep -q '\[node 6\].*MENJADI COORDINATOR: 6' "$logfile"
for id in 0 1 2 3 4 5; do
 grep -q "\[node $id\].*coordinator=6" "$logfile"
done
for edge in '4 5' '4 6' '4 7' '5 6' '5 7' '6 7'; do
 read -r from to <<<"$edge"
 grep -q "\[node $from\].*SEND ELECTION -> $to" "$logfile"
done
for edge in '4 5' '4 6' '5 6'; do
 read -r from to <<<"$edge"
 grep -q "\[node $from\].*RECV OK <- $to" "$logfile"
done
echo 'PASS: alur ELECTION/OK benar; hanya node 6 menang; node 0..5 menerima coordinator 6.'
