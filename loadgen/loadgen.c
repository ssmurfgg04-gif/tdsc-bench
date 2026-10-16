/*
 * loadgen.c - a C load generator that mirrors the challenge's bench/load.js
 * user model exactly (same requests, same think times, same probabilities,
 * same pass thresholds), but scales far past k6 on a single machine:
 *
 *   - connections spread across 127.0.0.1 .. 127.0.0.<srcips> so each source
 *     address gets its own ephemeral port range (k6 binds one address and dies
 *     near ~28k connections)
 *   - one fixed-size struct per virtual user, keep-alive sockets, epoll
 *   - a per-thread timer heap drives think times instead of a per-VU timerfd
 *
 * usage:
 *   loadgen <ip> <port> <vus> <ramp_s> <hold_s> <tokens.json> <threads> [srcips] [watchdog_s]
 *
 * The official pass bar (from bench/load.js): p95 < 500 ms, p99 < 1000 ms,
 * fewer than 1% failed requests, held for the full hold window.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ---------------------------------------------------------------- config */

static const char *g_ip = "127.0.0.1";
static int g_port = 3000;
static long g_vus = 100000;
static long g_ramp_s = 150;
static long g_hold_s = 300;
static int g_threads = 2;
static int g_srcips = 16;
static const char *g_tokens_path = "tokens.json";
static long g_watchdog_s = 10;

static long long g_t0_us;          /* run start, monotonic */
static long long g_end_us;         /* ramp + hold, then stop */

/* ---------------------------------------------------------------- tokens */

typedef struct {
    char username[64];
    char token[256];
} tok_t;
static tok_t *g_tokens;
static int g_ntokens;

static void die(const char *m) { fprintf(stderr, "loadgen: %s\n", m); exit(1); }

static void load_tokens(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f) die("cannot open tokens.json");
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) die("oom reading tokens");
    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) die("short read tokens");
    buf[sz] = 0;
    fclose(f);

    /* count objects */
    int cap = 1024, n = 0;
    g_tokens = malloc((size_t)cap * sizeof *g_tokens);
    char *p = buf;
    while ((p = strchr(p, '{')) != NULL) {
        char *e = strchr(p, '}');
        if (!e) break;
        *e = 0;
        if (n == cap) {
            cap *= 2;
            g_tokens = realloc(g_tokens, (size_t)cap * sizeof *g_tokens);
        }
        tok_t *t = &g_tokens[n];
        memset(t, 0, sizeof *t);
        char *u = strstr(p, "\"username\":\"");
        char *k = strstr(p, "\"token\":\"");
        if (u && k) {
            u += 12;
            char *q = strchr(u, '"');
            size_t ul = q ? (size_t)(q - u) : 0;
            if (ul > sizeof t->username - 1) ul = sizeof t->username - 1;
            memcpy(t->username, u, ul);
            k += 9;
            q = strchr(k, '"');
            size_t kl = q ? (size_t)(q - k) : 0;
            if (kl > sizeof t->token - 1) kl = sizeof t->token - 1;
            memcpy(t->token, k, kl);
            n++;
        }
        p = e + 1;
    }
    free(buf);
    if (n < 1) die("no tokens parsed");
    g_ntokens = n;
    fprintf(stderr, "loadgen: %d tokens loaded\n", n);
}

/* ---------------------------------------------------------------- time */

static long long now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static void iso_now(char out[48])
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    gmtime_r(&ts.tv_sec, &tm);
    snprintf(out, 40, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, (int)(ts.tv_nsec / 1000000));
}

/* ---------------------------------------------------------------- rng */

typedef struct { uint64_t s; } rng_t;
static inline uint64_t rnd_next(rng_t *r)
{
    uint64_t x = r->s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    r->s = x ? x : 88172645463325252ULL;
    return r->s;
}
static inline double rnd_01(rng_t *r) { return (double)(rnd_next(r) >> 11) / 9007199254740992.0; }
static inline double rnd_between(rng_t *r, double lo, double hi)
{
    return lo + rnd_01(r) * (hi - lo);
}

/* ---------------------------------------------------------------- stats */

#define NBUCK 14
static const int buck_ms[NBUCK] = {1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000};

enum { EP_FEED = 0, EP_POST, EP_LIKE, EP_CREATE, NEP };
static const char *ep_name[NEP] = {"GET /feed", "GET /posts/:id", "POST like", "POST create"};

typedef struct {
    long long reqs;             /* requests completed (incl. failed) */
    long long fails;            /* network-level failures */
    long long check_fails;      /* wrong status / bad body */
    long long conn_fails;
    long long lat[NBUCK];       /* latency histogram, all requests */
    double lat_sum_ms;
    long long ep_reqs[NEP];
    long long ep_fails[NEP];
    long long ep_check[NEP];
    long long conns_open, conns_max_open;
} st_t;

static inline int lat_bucket(double ms)
{
    for (int i = 0; i < NBUCK; i++)
        if (ms <= buck_ms[i]) return i;
    return NBUCK - 1;
}

/* live snapshot counters (relaxed, only for progress prints) */
static atomic_llong g_live_conns, g_live_reqs, g_live_fails;

/* ---------------------------------------------------------------- vus */

enum { M_SLEEP, M_CONN, M_IO };
enum { S_NONE = 0, S_FEED, S_POST, S_LIKE, S_CREATE };
enum { TN_START, TN_POST, TN_AFTER_POST, TN_ITER };

typedef struct {
    int fd;                 /* -1 = not connected */
    uint8_t mode;
    uint8_t step;           /* request in flight (or about to be sent) */
    uint8_t think_next;     /* what to do when the current think ends */
    uint8_t do_like, do_create;
    uint8_t close_after;    /* server said Connection: close */
    uint16_t tok;
    uint32_t iter;
    uint32_t wake_tag;
    int post_id;
    int vuid;               /* 1-based global VU number, like k6's __VU */
    long long start_us;     /* when this VU enters the run (ramp pacing) */
    long long wake_us;      /* sleep until (valid when M_SLEEP) */
    long long req_start_us; /* watchdog + latency */
    /* pending request bytes */
    uint16_t woff, wlen;
    char wbuf[896];
    /* response accumulation */
    char *rbuf;
    uint32_t rcap, rlen;
    int status;
} vu_t;

/* ---------------------------------------------------------------- heap */

typedef struct { long long t; int idx; uint32_t tag; } he_t;
typedef struct { he_t *a; int n, cap; } heap_t;

static void heap_push(heap_t *h, long long t, int idx, uint32_t tag)
{
    if (h->n == h->cap) die("timer heap overflow");
    int i = h->n++;
    h->a[i].t = t; h->a[i].idx = idx; h->a[i].tag = tag;
    while (i > 0) {
        int p = (i - 1) / 2;
        if (h->a[p].t <= h->a[i].t) break;
        he_t tmp = h->a[p]; h->a[p] = h->a[i]; h->a[i] = tmp;
        i = p;
    }
}
static he_t heap_pop(heap_t *h)
{
    he_t top = h->a[0];
    h->a[0] = h->a[--h->n];
    int i = 0;
    for (;;) {
        int l = 2 * i + 1, r = l + 1, m = i;
        if (l < h->n && h->a[l].t < h->a[m].t) m = l;
        if (r < h->n && h->a[r].t < h->a[m].t) m = r;
        if (m == i) break;
        he_t tmp = h->a[m]; h->a[m] = h->a[i]; h->a[i] = tmp;
        i = m;
    }
    return top;
}

/* ---------------------------------------------------------------- thread */

typedef struct {
    int tid;
    int base;               /* global index of vus[0] */
    int nvus;
    vu_t *vus;
    int epfd;
    heap_t heap;
    rng_t rng;
    st_t st;
    long long last_reqs_pub;
    long long last_fails_pub;
    long long last_snap_us;
} thr_t;

static void vu_close(thr_t *T, vu_t *v)
{
    if (v->fd >= 0) {
        epoll_ctl(T->epfd, EPOLL_CTL_DEL, v->fd, NULL);
        close(v->fd);
        v->fd = -1;
        atomic_fetch_sub_explicit(&g_live_conns, 1, memory_order_relaxed);
    }
    v->rlen = 0;
    v->mode = M_SLEEP;
}

static void vu_sleep(thr_t *T, vu_t *v, double seconds, int think_next)
{
    v->mode = M_SLEEP;
    v->think_next = (uint8_t)think_next;
    v->wake_us = now_us() + (long long)(seconds * 1e6);
    v->wake_tag++;
    heap_push(&T->heap, v->wake_us, (int)(v - T->vus), v->wake_tag);
}

/* record a completed (or failed) request into the stats */
static void stat_req(thr_t *T, vu_t *v, double ms, int failed, int check_fail)
{
    int ep = v->step == S_FEED ? EP_FEED : v->step == S_POST ? EP_POST
           : v->step == S_LIKE ? EP_LIKE : EP_CREATE;
    T->st.reqs++;
    T->st.ep_reqs[ep]++;
    T->st.lat[lat_bucket(ms)]++;
    T->st.lat_sum_ms += ms;
    if (failed) { T->st.fails++; T->st.ep_fails[ep]++; }
    if (check_fail) { T->st.check_fails++; T->st.ep_check[ep]++; }
}

/* kill the in-flight request: counts as a failed request, restart iteration */
static void req_fail(thr_t *T, vu_t *v, const char *why)
{
    double ms = (double)(now_us() - v->req_start_us) / 1000.0;
    if (ms < 1) ms = 1;  /* a dropped attempt still counts with its bucket floor */
    stat_req(T, v, ms, 1, 0);
    vu_close(T, v);
    (void)why;
    /* like k6 on an errored iteration: give up and start a new one */
    v->iter++;
    v->do_like = 0;
    v->do_create = 0;
    vu_sleep(T, v, rnd_between(&T->rng, 1, 3), TN_START);
}

/* ---------------------------------------------------------------- requests */

static void build_request(thr_t *T, vu_t *v)
{
    tok_t *tk = &g_tokens[v->tok];
    int n = 0;
    char *p = v->wbuf;
    size_t cap = sizeof v->wbuf;
    switch (v->step) {
    case S_FEED:
        n = snprintf(p, cap, "GET /feed HTTP/1.1\r\nHost: %s:%d\r\n\r\n", g_ip, g_port);
        break;
    case S_POST:
        n = snprintf(p, cap, "GET /posts/%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n",
                     v->post_id, g_ip, g_port);
        break;
    case S_LIKE:
        n = snprintf(p, cap,
                     "POST /posts/%d/like HTTP/1.1\r\nHost: %s:%d\r\n"
                     "Authorization: Bearer %s\r\nContent-Type: application/json\r\n"
                     "Content-Length: 0\r\n\r\n",
                     v->post_id, g_ip, g_port, tk->token);
        break;
    case S_CREATE: {
        char iso[48], body[224];
        iso_now(iso);
        /* load.js: JSON.stringify({ body: "..." }) */
        int bl = snprintf(body, sizeof body,
                          "{\"body\":\"%s says hi at %s (VU %d, iter %u)\"}",
                          tk->username, iso, v->vuid, v->iter);
        n = snprintf(p, cap,
                     "POST /posts HTTP/1.1\r\nHost: %s:%d\r\n"
                     "Authorization: Bearer %s\r\nContent-Type: application/json\r\n"
                     "Content-Length: %d\r\n\r\n%s",
                     g_ip, g_port, tk->token, bl, body);
        break;
    }
    default:
        die("build_request with no step");
    }
    if (n <= 0 || (size_t)n >= cap) die("request too long for wbuf");
    v->wlen = (uint16_t)n;
    v->woff = 0;
}

static int src_ip_for(int vuid)
{
    /* 127.0.0.2 .. 127.0.0.<srcips+1>; keep .1 out of the pool */
    return 2 + (vuid % (g_srcips > 0 ? g_srcips : 1));
}

static void vu_connect(thr_t *T, vu_t *v)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) { req_fail(T, v, "socket"); return; }

    struct sockaddr_in src;
    memset(&src, 0, sizeof src);
    src.sin_family = AF_INET;
    src.sin_port = 0;
    src.sin_addr.s_addr = htonl((127U << 24) | (uint32_t)src_ip_for(v->vuid));
    if (bind(fd, (struct sockaddr *)&src, sizeof src) < 0) {
        close(fd);
        req_fail(T, v, "bind");
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof dst);
    dst.sin_family = AF_INET;
    dst.sin_port = htons((uint16_t)g_port);
    dst.sin_addr.s_addr = htonl((127U << 24) | 1U);
    int rc = connect(fd, (struct sockaddr *)&dst, sizeof dst);
    if (rc == 0) { /* instant (loopback can do this) */
        v->fd = fd;
        v->mode = M_IO;
        T->st.conns_open++;
        atomic_fetch_add_explicit(&g_live_conns, 1, memory_order_relaxed);
        /* fall through to writing below */
        struct epoll_event ev = { .events = EPOLLIN, .data.ptr = v };
        epoll_ctl(T->epfd, EPOLL_CTL_ADD, fd, &ev);
        /* send now */
        return; /* the caller writes when EPOLLOUT/next tick? no: write inline below */
    }
    if (errno != EINPROGRESS) {
        close(fd);
        req_fail(T, v, "connect");
        return;
    }
    v->fd = fd;
    v->mode = M_CONN;
    T->st.conns_open++;
    atomic_fetch_add_explicit(&g_live_conns, 1, memory_order_relaxed);
    struct epoll_event ev = { .events = EPOLLOUT, .data.ptr = v };
    if (epoll_ctl(T->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        vu_close(T, v);
        req_fail(T, v, "epoll add");
    }
}

/* try to write the pending request; arm EPOLLOUT if partial */
static void try_write(thr_t *T, vu_t *v)
{
    while (v->woff < v->wlen) {
        ssize_t w = send(v->fd, v->wbuf + v->woff, v->wlen - v->woff, MSG_NOSIGNAL);
        if (w > 0) {
            v->woff += (uint16_t)w;
            continue;
        }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct epoll_event ev = { .events = EPOLLIN | EPOLLOUT, .data.ptr = v };
            epoll_ctl(T->epfd, EPOLL_CTL_MOD, v->fd, &ev);
            return;
        }
        req_fail(T, v, "send");
        return;
    }
    /* fully written; only interested in readability now */
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = v };
    epoll_ctl(T->epfd, EPOLL_CTL_MOD, v->fd, &ev);
}

/* start the request currently in v->step (connect first if needed) */
static void send_step(thr_t *T, vu_t *v)
{
    build_request(T, v);
    v->rlen = 0;
    v->status = 0;
    v->req_start_us = now_us();
    if (v->fd < 0) {
        vu_connect(T, v);
        if (v->mode == M_IO) try_write(T, v);  /* instant connect path */
        return;
    }
    v->mode = M_IO;
    try_write(T, v);
}

/* pick a random post id out of a /feed response; -1 if none found */
static int feed_pick(thr_t *T, const char *b, uint32_t n)
{
    const char *posts = memmem(b, n, "\"posts\":", 8);
    if (!posts) return -1;
    int ids[128], cnt = 0;
    const char *p = posts + 8, *end = b + n;
    while (cnt < 128) {
        const char *id = memmem(p, (size_t)(end - p), "\"id\":", 5);
        if (!id || (size_t)(end - id) < 10) break;
        id += 5;
        char *qe;
        long val = strtol(id, &qe, 10);
        if (qe == id) { p = id + 1; continue; }
        if (val > 0 && val <= 500000000) ids[cnt++] = (int)val;
        p = qe;
    }
    if (!cnt) return -1;
    return ids[rnd_next(&T->rng) % (uint64_t)cnt];
}

static void parse_response(thr_t *T, vu_t *v)
{
    char *rb = v->rbuf;
    uint32_t n = v->rlen;

    const char *hdr_end = memmem(rb, n, "\r\n\r\n", 4);
    if (!hdr_end) {
        if (n > 65536) { req_fail(T, v, "no header end"); }
        return;  /* need more bytes */
    }
    uint32_t hlen = (uint32_t)(hdr_end - rb) + 4;

    /* status */
    int status = 0;
    if (n >= 12) status = atoi(rb + 9);
    v->status = status;

    /* headers: content-length, connection: close */
    long long cl = 0;
    int will_close = 0;
    const char *hp = rb, *he = hdr_end;
    while (hp < he) {
        const char *le = memmem(hp, (size_t)(he - hp), "\r\n", 2);
        if (!le) le = he;
        size_t ll = (size_t)(le - hp);
        if (ll > 16 && strncasecmp(hp, "Content-Length:", 15) == 0)
            cl = atoll(hp + 15);
        else if (ll >= 12 && strncasecmp(hp, "Connection:", 11) == 0) {
            const char *v0 = hp + 11;
            while (v0 < le && (*v0 == ' ' || *v0 == '\t')) v0++;
            if (strncasecmp(v0, "close", 5) == 0) will_close = 1;
        }
        hp = le + 2;
    }
    if (cl < 0) cl = 0;

    if ((uint32_t)(hlen + (uint64_t)cl) > n) return;  /* body incomplete */

    double ms = (double)(now_us() - v->req_start_us) / 1000.0;
    int failed = 0, check_fail = 0;
    switch (v->step) {
    case S_FEED:
        if (status != 200) check_fail = 1;
        v->post_id = feed_pick(T, rb + hlen, (uint32_t)cl);
        if (status == 200 && v->post_id < 0) check_fail = 1;
        break;
    case S_POST:
        if (status != 200) check_fail = 1;
        break;
    case S_LIKE:
        if (status != 200 && status != 201) check_fail = 1;
        break;
    case S_CREATE:
        if (status != 201) check_fail = 1;
        break;
    }
    if (check_fail && getenv("LOADGEN_DEBUG"))
        fprintf(stderr, "DBG %s status=%d body=%.*s\n",
                ep_name[v->step == S_FEED ? EP_FEED : v->step == S_POST ? EP_POST
                : v->step == S_LIKE ? EP_LIKE : EP_CREATE], status,
                (int)(cl > 120 ? 120 : cl), rb + hlen);
    stat_req(T, v, ms, failed, check_fail);

    if (will_close) {
        vu_close(T, v);
    } else {
        v->rlen = 0;
        v->mode = M_SLEEP;
    }

    /* advance the user loop */
    switch (v->step) {
    case S_FEED:
        if (check_fail) {          /* k6: bad feed -> sleep(3,7), next iteration */
            v->iter++;
            v->do_like = 0; v->do_create = 0;
            vu_sleep(T, v, rnd_between(&T->rng, 3, 7), TN_START);
        } else {
            vu_sleep(T, v, rnd_between(&T->rng, 3, 7), TN_POST);
        }
        break;
    case S_POST:
        vu_sleep(T, v, rnd_between(&T->rng, 3, 8), TN_AFTER_POST);
        break;
    case S_LIKE:
        if (v->do_create) { v->step = S_CREATE; send_step(T, v); }
        else vu_sleep(T, v, rnd_between(&T->rng, 5, 15), TN_START);
        break;
    case S_CREATE:
        vu_sleep(T, v, rnd_between(&T->rng, 5, 15), TN_START);
        break;
    }
}

static void on_readable(thr_t *T, vu_t *v)
{
    for (;;) {
        if (v->rcap - v->rlen < 4096) {
            uint32_t ncap = v->rcap ? v->rcap * 2 : 8192;
            while (ncap - v->rlen < 4096) ncap *= 2;
            if (ncap > (1u << 20)) { req_fail(T, v, "response too big"); return; }
            char *nb = realloc(v->rbuf, ncap);
            if (!nb) { req_fail(T, v, "oom rbuf"); return; }
            v->rbuf = nb;
            v->rcap = ncap;
        }
        ssize_t r = recv(v->fd, v->rbuf + v->rlen, v->rcap - v->rlen, 0);
        if (r > 0) {
            v->rlen += (uint32_t)r;
            if (v->rlen >= v->rcap) { /* buffer exactly full: keep parsing */ }
            parse_response(T, v);
            if (v->mode != M_IO) return;  /* done or failed */
            if ((size_t)r < 4096) return; /* drained for now */
            continue;
        }
        if (r == 0) { req_fail(T, v, "eof"); return; }
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        req_fail(T, v, "recv");
        return;
    }
}

/* think timer fired: figure out what this VU does next */
static void vu_wake(thr_t *T, vu_t *v)
{
    long long now = now_us();
    if (v->mode != M_SLEEP || v->wake_us > now + 1000) return;

    switch (v->think_next) {
    case TN_START:
        /* new iteration begins: roll the like/create dice, fetch the feed */
        v->iter++;
        v->do_like = rnd_01(&T->rng) < 0.15;
        v->do_create = rnd_01(&T->rng) < 0.02;
        v->step = S_FEED;
        send_step(T, v);
        break;
    case TN_POST:
        v->step = S_POST;
        send_step(T, v);
        break;
    case TN_AFTER_POST:
        if (v->do_like) {
            v->step = S_LIKE;
            send_step(T, v);
        } else if (v->do_create) {
            v->step = S_CREATE;
            send_step(T, v);
        } else {
            /* neither: two consecutive thinks, like the js loop does */
            vu_sleep(T, v, rnd_between(&T->rng, 5, 15), TN_START);
        }
        break;
    default:
        die("bad think_next");
    }
}

static void *thread_main(void *arg)
{
    thr_t *T = arg;
    T->epfd = epoll_create1(0);
    if (T->epfd < 0) die("epoll_create1");

    T->rng.s = 0x9E3779B97F4A7C15ULL ^ (uint64_t)(T->tid + 1);
    rnd_next(&T->rng);

    /* initial pacing: VU i enters at t0 + ramp*i/total (+0..5 s jitter) */
    for (int i = 0; i < T->nvus; i++) {
        vu_t *v = &T->vus[i];
        v->fd = -1;
        v->mode = M_SLEEP;
        v->think_next = TN_START;
        v->start_us = g_t0_us + (long long)((double)(T->base + i) * g_ramp_s * 1e6 / (double)g_vus);
        v->wake_us = v->start_us + (long long)(rnd_01(&T->rng) * 5e6);
        v->wake_tag = 1;
        v->tok = (uint16_t)((T->base + i) % g_ntokens);
        v->vuid = T->base + i + 1;
        heap_push(&T->heap, v->wake_us, i, v->wake_tag);
    }

    struct epoll_event evs[512];
    long long last_watchdog = now_us();
    long long open_max = 0;

    for (;;) {
        long long now = now_us();
        if (now >= g_end_us) break;

        int timeout_ms = 100;
        if (T->heap.n > 0) {
            long long d = (T->heap.a[0].t - now) / 1000;
            if (d < 0) d = 0;
            if (d > 100) d = 100;
            timeout_ms = (int)d + 1;
        }

        int ne = epoll_wait(T->epfd, evs, 512, timeout_ms);
        now = now_us();

        /* run every due timer */
        while (T->heap.n > 0 && T->heap.a[0].t <= now) {
            he_t e = heap_pop(&T->heap);
            if (e.tag != T->vus[e.idx].wake_tag) continue;   /* stale */
            vu_wake(T, &T->vus[e.idx]);
        }

        for (int i = 0; i < ne; i++) {
            vu_t *v = evs[i].data.ptr;
            uint32_t ev = evs[i].events;
            if (v->fd < 0) continue;
            if (v->mode == M_SLEEP) {
                /* idle keep-alive the server closed (or junk): drop it
                 * quietly, the next request reconnects. no stats hit. */
                int err = 0;
                socklen_t el = sizeof err;
                getsockopt(v->fd, SOL_SOCKET, SO_ERROR, &err, &el);
                if ((ev & (EPOLLERR | EPOLLHUP)) || err) { vu_close(T, v); continue; }
                char sink[512];
                ssize_t r;
                while ((r = recv(v->fd, sink, sizeof sink, 0)) > 0) {}
                if (r == 0) vu_close(T, v);   /* server hung up an idle conn */
                continue;
            }
            if (ev & (EPOLLERR | EPOLLHUP)) { req_fail(T, v, "err/hup"); continue; }
            if (v->mode == M_CONN && (ev & EPOLLOUT)) {
                int err = 0;
                socklen_t el = sizeof err;
                if (getsockopt(v->fd, SOL_SOCKET, SO_ERROR, &err, &el) < 0 || err) {
                    T->st.conn_fails++;
                    req_fail(T, v, "connect");
                    continue;
                }
                v->mode = M_IO;
                try_write(T, v);
                continue;
            }
            if ((ev & EPOLLOUT) && v->mode == M_IO && v->woff < v->wlen) {
                try_write(T, v);
                if (v->mode != M_IO) continue;
            }
            if (ev & EPOLLIN) {
                if (v->mode != M_IO) continue;  /* stray data on an idle conn */
                on_readable(T, v);
            }
        }

        /* watchdog sweep + connection census, twice a second */
        if (now - last_watchdog > 500000) {
            last_watchdog = now;
            long long limit = now - g_watchdog_s * 1000000LL;
            long long conn = 0;
            for (int i = 0; i < T->nvus; i++) {
                vu_t *v = &T->vus[i];
                if (v->fd < 0) continue;
                conn++;
                if ((v->mode == M_IO || v->mode == M_CONN) &&
                    v->req_start_us < limit)
                    req_fail(T, v, "watchdog");
            }
            if (conn > open_max) { open_max = conn; T->st.conns_max_open = conn; }
            if (now - T->last_snap_us > 1000000) {
                T->last_snap_us = now;
                atomic_fetch_add_explicit(&g_live_reqs, T->st.reqs - T->last_reqs_pub,
                                          memory_order_relaxed);
                T->last_reqs_pub = T->st.reqs;
                atomic_fetch_add_explicit(&g_live_fails, T->st.fails - T->last_fails_pub,
                                          memory_order_relaxed);
                T->last_fails_pub = T->st.fails;
            }
        }
    }

    /* teardown: close everything */
    for (int i = 0; i < T->nvus; i++)
        if (T->vus[i].fd >= 0) vu_close(T, &T->vus[i]);
    close(T->epfd);
    return NULL;
}

/* ---------------------------------------------------------------- main */

static double hist_pct(const long long *lat, long long total, double p)
{
    if (total <= 0) return 0;
    long long need = (long long)((double)total * p / 100.0);
    if (need < 1) need = 1;
    long long acc = 0;
    for (int i = 0; i < NBUCK; i++) {
        acc += lat[i];
        if (acc >= need) return buck_ms[i];
    }
    return buck_ms[NBUCK - 1];
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (argc > 1) g_ip = argv[1];
    if (argc > 2) g_port = atoi(argv[2]);
    if (argc > 3) g_vus = atol(argv[3]);
    if (argc > 4) g_ramp_s = atol(argv[4]);
    if (argc > 5) g_hold_s = atol(argv[5]);
    if (argc > 6) g_tokens_path = argv[6];
    if (argc > 7) g_threads = atoi(argv[7]);
    if (argc > 8) g_srcips = atoi(argv[8]);
    if (argc > 9) g_watchdog_s = atol(argv[9]);
    if (g_vus < 1 || g_threads < 1) die("bad args");

    struct rlimit rl;
    getrlimit(RLIMIT_NOFILE, &rl);
    if (rl.rlim_cur < (rlim_t)(g_vus + 512)) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
        getrlimit(RLIMIT_NOFILE, &rl);
    }
    fprintf(stderr, "loadgen: %ld VUs, %d threads, %d src ips, fd limit %llu, "
            "ramp %lds + hold %lds\n", g_vus, g_threads, g_srcips,
            (unsigned long long)rl.rlim_cur, g_ramp_s, g_hold_s);
    if (rl.rlim_cur < (rlim_t)(g_vus + 256))
        fprintf(stderr, "loadgen: WARNING fd limit below VU count\n");

    load_tokens(g_tokens_path);

    vu_t *vus = calloc((size_t)g_vus, sizeof *vus);
    if (!vus) die("oom allocating VUs");
    pthread_t *tids = calloc((size_t)g_threads, sizeof *tids);
    thr_t *ths = calloc((size_t)g_threads, sizeof *ths);
    if (!tids || !ths) die("oom threads");

    g_t0_us = now_us();
    g_end_us = g_t0_us + (g_ramp_s + g_hold_s) * 1000000LL;

    int per = (int)(g_vus / g_threads);
    int assigned = 0;
    for (int t = 0; t < g_threads; t++) {
        thr_t *T = &ths[t];
        T->tid = t;
        T->base = assigned;
        T->nvus = (t == g_threads - 1) ? (int)g_vus - assigned : per;
        T->vus = vus + assigned;
        T->heap.cap = T->nvus * 2 + 16;
        T->heap.a = malloc((size_t)T->heap.cap * sizeof *T->heap.a);
        if (!T->heap.a) die("oom heap");
        T->heap.n = 0;
        assigned += T->nvus;
        if (pthread_create(&tids[t], NULL, thread_main, T)) die("pthread_create");
    }

    /* progress line while it runs */
    for (;;) {
        long long now = now_us();
        if (now >= g_end_us) break;
        struct timespec ts = { 5, 0 };
        nanosleep(&ts, NULL);
        double el = (double)(now_us() - g_t0_us) / 1e6;
        long long c = atomic_load_explicit(&g_live_conns, memory_order_relaxed);
        long long r = atomic_load_explicit(&g_live_reqs, memory_order_relaxed);
        long long f = atomic_load_explicit(&g_live_fails, memory_order_relaxed);
        fprintf(stderr, "loadgen: t=%6.1fs conns=%lld reqs=%lld fails=%lld\n",
                el, c, r, f);
    }
    for (int t = 0; t < g_threads; t++)
        pthread_join(tids[t], NULL);

    /* aggregate */
    st_t tot;
    memset(&tot, 0, sizeof tot);
    for (int t = 0; t < g_threads; t++) {
        st_t *s = &ths[t].st;
        tot.reqs += s->reqs; tot.fails += s->fails; tot.check_fails += s->check_fails;
        tot.conn_fails += s->conn_fails;
        tot.lat_sum_ms += s->lat_sum_ms;
        tot.conns_open += s->conns_open;
        tot.conns_max_open += s->conns_max_open;   /* per-thread peaks sum up */
        for (int i = 0; i < NBUCK; i++) tot.lat[i] += s->lat[i];
        for (int e = 0; e < NEP; e++) {
            tot.ep_reqs[e] += s->ep_reqs[e];
            tot.ep_fails[e] += s->ep_fails[e];
            tot.ep_check[e] += s->ep_check[e];
        }
    }

    double p95 = hist_pct(tot.lat, tot.reqs, 95.0);
    double p99 = hist_pct(tot.lat, tot.reqs, 99.0);
    double avg = tot.reqs ? tot.lat_sum_ms / (double)tot.reqs : 0;
    double fail_rate = tot.reqs ? 100.0 * (double)tot.fails / (double)tot.reqs : 0;
    int pass = (p95 <= 500.0) && (p99 <= 1000.0) && (fail_rate < 1.0);

    double dur = (double)(g_ramp_s + g_hold_s);
    fprintf(stderr, "\nloadgen: FINAL REPORT (VUs=%ld, ramp %lds, hold %lds)\n",
            g_vus, g_ramp_s, g_hold_s);
    fprintf(stderr, "  requests:     %lld  (%.0f req/s avg over ramp+hold)\n",
            tot.reqs, tot.reqs / dur);
    fprintf(stderr, "  failed:       %lld  (%.3f%%)   conn errors: %lld\n",
            tot.fails, fail_rate, tot.conn_fails);
    fprintf(stderr, "  check fails:  %lld  (wrong status / bad body)\n", tot.check_fails);
    fprintf(stderr, "  latency:      avg %.1f ms   p95 %.0f ms   p99 %.0f ms\n", avg, p95, p99);
    fprintf(stderr, "  conns opened: %lld   peak concurrent: %lld\n",
            tot.conns_open, tot.conns_max_open);
    for (int e = 0; e < NEP; e++)
        fprintf(stderr, "    %-16s reqs=%-9lld fails=%-6lld check=%-6lld\n",
                ep_name[e], tot.ep_reqs[e], tot.ep_fails[e], tot.ep_check[e]);

    fprintf(stderr, "RESULT: %s (p95 %.0f ms, p99 %.0f ms, fail %.3f%%)\n",
            pass ? "PASS" : "FAIL", p95, p99, fail_rate);
    printf("STATS_JSON: {\"vus\":%ld,\"ramp_s\":%ld,\"hold_s\":%ld,\"reqs\":%lld,"
           "\"fails\":%lld,\"check_fails\":%lld,\"conn_fails\":%lld,"
           "\"avg_ms\":%.1f,\"p95_ms\":%.0f,\"p99_ms\":%.0f,\"fail_pct\":%.3f,"
           "\"peak_conns\":%lld,\"result\":\"%s\"}\n",
           g_vus, g_ramp_s, g_hold_s, tot.reqs, tot.fails, tot.check_fails,
           tot.conn_fails, avg, p95, p99, fail_rate, tot.conns_max_open,
           pass ? "PASS" : "FAIL");
    return 0;
}
