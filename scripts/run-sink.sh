#!/usr/bin/env bash
# Runs one load level against the canned-response sink (no real server, no DB).
#   run-sink.sh <vus> <ramp_s> <hold_s> <tag> <repo-dir> <loadgen-bin>
# Purpose: measure the load generator + kernel ceiling alone. If the loadgen
# fails here the same way it fails against the real server, the wall is the
# bench rig, not the submission.
set -uo pipefail
VUS=$1 RAMP=$2 HOLD=$3 TAG=$4 REPO=$5 LOADGEN=$6 TOKENS=$7
PORT=3100
B=/tmp/bench
rm -rf "$B"; mkdir -p "$B"

echo "== starting sink (2 threads, taskset 0-1)"
sudo -n bash -c "ulimit -n 1048576; taskset -c 0-1 $REPO/sink/sink \
  $PORT $REPO/sink/feed.json $REPO/sink/post.json 2" > "$B/sink.log" 2>&1 &

ok=0
for i in $(seq 60); do
  if curl -sf --max-time 2 "http://127.0.0.1:$PORT/health" > /dev/null; then ok=1; break; fi
  sleep 1
done
if [ "$ok" != 1 ]; then
  echo "sink never became healthy"; cat "$B/sink.log"; exit 1
fi
echo "== sink up"

echo "== bench: $TAG vus=$VUS ramp=${RAMP}s hold=${HOLD}s"
sudo -n bash -c "ulimit -n 1048576; taskset -c 2-3 $LOADGEN 127.0.0.1 $PORT \
  $VUS $RAMP $HOLD $TOKENS 2 16" > "$B/run.log" 2>&1
RC=$?
cat "$B/run.log"

sudo pkill -f "sink/sink" 2>/dev/null
sleep 1

echo "== sinkstats tail (last 6)"
grep -a "sinkstats" "$B/sink.log" | tail -6 || true
echo "== sink banner"; head -1 "$B/sink.log" || true

RESULT=$(grep -o 'RESULT: [A-Z]*' "$B/run.log" | tail -1 || true)
JSON=$(grep -o 'STATS_JSON: .*' "$B/run.log" | tail -1 | cut -d' ' -f2- || true)
echo "$JSON" > "$B/stats.json"
echo "BENCH_RESULT: $TAG $RESULT"
exit $RC
