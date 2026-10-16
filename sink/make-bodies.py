#!/usr/bin/env python3
"""Generate canned JSON bodies for the sink server, byte-compatible with what
the loadgen's response checker expects: feed needs "posts": then "id":N."""
import json, random, sys

random.seed(42)

PHRASES = [
    "the runner ceiling hunt continues",
    "counter go brrr",
    "io_uring vs epoll, the eternal flame war",
    "WAL says 0KB and means it",
    "load generator is also a program, who knew",
    "EAGAIN is a lifestyle",
    "kernel queueing is invisible until it is not",
    "p95 is a story we tell stakeholders",
]

def post(i):
    body = " ".join(random.choice(PHRASES) for _ in range(random.randint(6, 40)))
    return {
        "id": i,
        "body": body,
        "created_at": "2026-10-%02dT%02d:%02d:%02dZ" % (random.randint(1, 15), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
        "author": "user%d" % random.randint(1, 500),
        "like_count": random.randint(0, 500),
    }

feed = {"posts": [post(i) for i in range(1, 21)]}   # server uses LIMIT 20
one  = post(12345)

for path, obj in [(sys.argv[1], feed), (sys.argv[2], one)]:
    with open(path, "w") as f:
        f.write(json.dumps(obj, separators=(",", ":")))
    print(path, len(json.dumps(obj, separators=(",", ":"))), "bytes")
