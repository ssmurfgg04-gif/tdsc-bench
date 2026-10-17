#!/usr/bin/env bash
# Runs one benchmark level. Two modes:
#   default      server on cores 0-1, loadgen on cores 2-3
#   1cpu2g       server on ONE core (taskset -c 0) inside a cgroup capped at
#                2 GB with swap disabled, loadgen on cores 1-3. Maps the run
#                onto the challenge's 1 vCPU / 2 GB ($12) server assumption.
#   run-bench.sh <vus> <ramp_s> <hold_s> <tag> <tdsc-dir> <loadgen-bin> [1cpu2g]
set -uo pipefail
VUS=$1 RAMP=$2 HOLD=$3 TAG=$4 TDSC=$5 LOADGEN=$6
MODE=${7:-}
PORT=3100
SUB="$TDSC/submissions/${SUBMISSION:-c-iouring-ssmurfgg04-gif}"
B=/tmp/bench
rm -rf "$B"; mkdir -p "$B"
cp "$TDSC/seed/feed.db" "$B/feed.db"

if [ "$MODE" = "1cpu2g" ]; then
  echo "== constrained mode: server taskset -c 0, MemoryMax=2G, MemorySwapMax=0 (maps to the \$12 box: 1 vCPU, 2 GB, no swap)"
  # systemd-run --scope stays in the foreground of the backgrounded sudo call
  # and moves every child (the server) into a dedicated scope with the caps.
  sudo -n systemd-run --collect --unit=tdsc-server-"$$" \
    -p MemoryMax=2147483648 -p MemorySwapMax=0 \
    bash -c "ulimit -n 1048576; SQLITE_PATH=$B/feed.db \
      JWT_SECRET=twelve-dollar-challenge HOST=127.0.0.1 PORT=$PORT MAXCONN=450000 \
      taskset -c 0 bash $SUB/start.sh" > "$B/server.log" 2>&1 &
else
  echo "== starting server (MAXCONN=450000, taskset 0-1)"
  sudo -n bash -c "ulimit -n 1048576; SQLITE_PATH=$B/feed.db \
    JWT_SECRET=twelve-dollar-challenge HOST=127.0.0.1 PORT=$PORT MAXCONN=450000 \
    taskset -c 0-1 bash $SUB/start.sh" > "$B/server.log" 2>&1 &
fi

ok=0
for i in $(seq 120); do
  if curl -sf "http://127.0.0.1:$PORT/health" > /dev/null; then ok=1; break; fi
  sleep 1
done
if [ "$ok" != 1 ]; then
  echo "server never became healthy"; cat "$B/server.log"; exit 1
fi
echo "== server up: $(head -c 200 "$B/server.log" | tr '\n' ' ')"

# Prove the constraints actually apply to the running server process.
SPID=$(pgrep -f "$SUB/bin/server" | head -1)
if [ -n "$SPID" ]; then
  CGPATH=$(grep -o '0::.*' "/proc/$SPID/cgroup" 2>/dev/null | cut -c4-)
  echo "RIG: server pid $SPID cgroup: $CGPATH"
  if [ "$MODE" = "1cpu2g" ]; then
    MM=$(cat "/sys/fs/cgroup/$CGPATH/memory.max" 2>/dev/null || echo missing)
    MS=$(cat "/sys/fs/cgroup/$CGPATH/memory.swap.max" 2>/dev/null || echo missing)
    AFF=$(taskset -pc "$SPID" 2>/dev/null | grep -o '[0-9,]*$' || echo '?')
    echo "RIG: memory.max=$MM memory.swap.max=$MS server affinity=$AFF"
    if [ "$MM" != "2147483648" ]; then
      echo "FATAL: 2G memory cap not in effect (memory.max=$MM) — refusing to record numbers"
      exit 1
    fi
    if [ "$AFF" != "0" ]; then
      echo "FATAL: server not pinned to core 0 (affinity=$AFF) — refusing to record numbers"
      exit 1
    fi
  fi
  echo "RIG: server RSS at start: $(awk '/VmRSS/{print $2" kB"}' "/proc/$SPID/status" 2>/dev/null)"
fi

# CPU steal accounting: /proc/stat field 9 (steal jiffies) vs total jiffies.
# GitHub runner VMs live on oversubscribed hosts; a noisy neighbor shows up
# here as double-digit steal and inflates every latency number uniformly.
read -r _ u n sys idle iow irq sirq steal rest < <(awk '/^cpu /{print; exit}' /proc/stat)
J0=$((u+n+sys+idle+iow+irq+sirq+steal)); ST0=$steal

echo "== bench: $TAG vus=$VUS ramp=${RAMP}s hold=${HOLD}s"
if [ "$MODE" = "1cpu2g" ]; then
  sudo -n bash -c "ulimit -n 1048576; taskset -c 1-3 $LOADGEN 127.0.0.1 $PORT \
    $VUS $RAMP $HOLD $TDSC/seed/tokens.json 2 16" > "$B/run.log" 2>&1
else
  sudo -n bash -c "ulimit -n 1048576; taskset -c 2-3 $LOADGEN 127.0.0.1 $PORT \
    $VUS $RAMP $HOLD $TDSC/seed/tokens.json 2 16" > "$B/run.log" 2>&1
fi
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
echo "RIG: cpu $(lscpu 2>/dev/null | awk -F: '/Model name/{gsub(/^ +/,"",$2); print $2; exit}')"
grep -h . /proc/pressure/cpu 2>/dev/null | sed 's/^/RIG: psi /' || true

RESULT=$(grep -o 'RESULT: [A-Z]*' "$B/run.log" | tail -1 || true)
JSON=$(grep -o 'STATS_JSON: .*' "$B/run.log" | tail -1 | cut -d' ' -f2- || true)
echo "$JSON" > "$B/stats.json"
echo "BENCH_RESULT: $TAG $RESULT"
exit $RC
