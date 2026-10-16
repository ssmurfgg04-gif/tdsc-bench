# tdsc-bench

Benchmark rig for the [twelve-dollar-server-challenge](https://github.com/arjaythedev/twelve-dollar-server-challenge),
built to find the ceiling of our C submission ([PR #112](https://github.com/arjaythedev/twelve-dollar-server-challenge/pull/112))
beyond what the stock k6 harness can measure.

## why this exists

The official bench uses k6 against the server on one machine. A TCP client
that binds a single source address runs out of ephemeral ports near ~28k
connections, so k6 tops out long before the server does. This repo ships a
small C load generator that implements the exact same user model as
`bench/load.js`:

- GET /feed, think 3-7s, GET /posts/:id, think 3-8s, 15% like, 2% create,
  idle 5-15s, repeat
- pass bar: p95 < 500 ms, p99 < 1000 ms, under 1% failed requests, held for
  the whole hold window

...but spreads its connections across 16 loopback source addresses
(127.0.0.2 .. 127.0.0.17), giving each one its own port range. That plus a
per-VU timer heap instead of per-connection timers lets a single runner hold
hundreds of thousands of keep-alive users.

## how a run works

Each GitHub-hosted runner (4 vCPU, 16 GB) runs one level of the matrix:

1. clones the submission branch and applies `patches/maxconn.patch`
   (one-line env knob: `MAXCONN` lifts the 64k connection cap; default
   behavior is untouched)
2. builds the server with its PGO pipeline, seeds the official 500k-post
   database (checksum-verified)
3. tunes kernel limits (fd limits, backlog, port range, tcp memory)
4. starts the server pinned to cores 0-1 (io_uring mode), the loadgen pinned
   to cores 2-3
5. ramps to the target VU count, holds, then reports p95/p99/fail rate and
   PASS/FAIL against the official thresholds

Levels run as parallel jobs on separate runners, so 20k / 100k / 150k / 200k
are all measured at once.

## results

| level | VUs | reqs | p95 | p99 | fail % | verdict |
|-------|-----|------|-----|-----|--------|---------|
| (to be filled from runs) | | | | | | |

## caveats

- These are our own numbers for exploring the design's ceiling. Official
  scores come from the maintainer's k6 run on the actual $12 droplet, which
  is slower hardware and uses the stock client.
- The loadgen is behavior-matched to `bench/load.js` but is a different
  client; request rates per VU match, absolute numbers may differ slightly
  from k6.
- The `MAXCONN` patch only lives in this bench repo, never in the PR.

## local quick test

```bash
bash loadgen/build.sh
./loadgen/loadgen 127.0.0.1 3000 500 15 30 ../tdsc/seed/tokens.json 2 4
```
