#!/usr/bin/env bash
# Runs one benchmark level: server on cores 0-1, loadgen on cores 2-3.
#   run-bench.sh <vus> <ramp_s> <hold_s> <tag> <tdsc-dir> <loadgen-bin>
set -uo pipefail
VUS=$1 RAMP=$2 HOLD=$3 TAG=$4 TDSC=$5 LOADGEN=$6
PORT=3100
SUB="$TDSC/submissions/${SUBMISSION:-c-iouring-ssmurfgg04-gif}"
B=/tmp/bench
rm -rf "$B"; mkdir -p "$B"
cp "$TDSC/seed/feed.db" "$B/feed.db"

echo "== starting server (MAXCONN=450000, taskset 0-1)"
sudo -n bash -c "ulimit -n 1048576; SQLITE_PATH=$B/feed.db \
  JWT_SECRET=twelve-dollar-challenge HOST=127.0.0.1 PORT=$PORT MAXCONN=450000 \
  taskset -c 0-1 bash $SUB/start.sh" > "$B/server.log" 2>&1 &

ok=0
for i in $(seq 120); do
  if curl -sf "http://127.0.0.1:$PORT/health" > /dev/null; then ok=1; break; fi
  sleep 1
done
if [ "$ok" != 1 ]; then
  echo "server never became healthy"; cat "$B/server.log"; exit 1
fi
echo "== server up: $(head -c 200 "$B/server.log" | tr '\n' ' ')"

# CPU steal accounting: /proc/stat field 9 (steal jiffies) vs total jiffies.
# GitHub runner VMs live on oversubscribed hosts; a noisy neighbor shows up
# here as double-digit steal and inflates every latency number uniformly.
read -r _ u n sys idle iow irq sirq steal rest < <(awk '/^cpu /{print; exit}' /proc/stat)
J0=$((u+n+sys+idle+iow+irq+sirq+steal)); ST0=$steal

echo "== bench: $TAG vus=$VUS ramp=${RAMP}s hold=${HOLD}s"
sudo -n bash -c "ulimit -n 1048576; taskset -c 2-3 $LOADGEN 127.0.0.1 $PORT \
  $VUS $RAMP $HOLD $TDSC/seed/tokens.json 2 16" > "$B/run.log" 2>&1
RC=$?
cat "$B/run.log"

sudo pkill -f "$SUB/bin/server" 2>/dev/null
sleep 1

echo "== server log tail"
tail -15 "$B/server.log" || true

read -r _ u n sys idle iow irq sirq steal rest < <(awk '/^cpu /{print; exit}' /proc/stat)
J1=$((u+n+sys+idle+iow+irq+sirq+steal)); ST1=$steal
if [ $J1 -gt $J0 ]; then
    echo "RIG: cpu steal $(( 100 * (ST1 - ST0) / (J1 - J0) ))% over the run (100% = fully stolen VM)"
fi
echo "RIG: loadavg $(cat /proc/loadavg)"
grep -h . /proc/pressure/cpu 2>/dev/null | sed 's/^/RIG: psi /' || true

RESULT=$(grep -o 'RESULT: [A-Z]*' "$B/run.log" | tail -1 || true)
JSON=$(grep -o 'STATS_JSON: .*' "$B/run.log" | tail -1 | cut -d' ' -f2- || true)
echo "$JSON" > "$B/stats.json"
echo "BENCH_RESULT: $TAG $RESULT"
exit $RC
