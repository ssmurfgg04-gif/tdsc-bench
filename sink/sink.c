/* sink.c - a measurement instrument, not a server.
 *
 * Answers the load generator with byte-identical canned responses so the
 * load generator + kernel ceiling can be measured without the real server
 * in the loop. If the loadgen also collapses at ~28-30k req/s against this,
 * the wall is the bench rig, not the server.
 *
 * Concurrency model: N threads, each with its own SO_REUSEPORT listener and
 * epoll. A connection fd is only ever touched by the thread that accepted
 * it, so all state and counters are per-thread with no locks, no atomics.
 * The stats thread just sums the per-thread counters.
 *
 * Every counter exists to answer one question when the rig stalls:
 *   accepts / accept_err   - is the accept path keeping up? (EMFILE = fd leak)
 *   fd_overflow            - fds beyond MAXFDS (would be silently dropped)
 *   req_* / resp_done      - requests parsed vs responses fully written
 *   write_eagain           - send-buffer backpressure events (kernel slow?)
 *   read_eagain / read_zero- read side health
 *   epoll_waits / events   - event-loop liveness (waits with 0 events = idle
 *                            while loadgen says "stalled" = kernel queueing)
 *   open_now               - live connection estimate (accepts - closes)
 *
 *   sink <port> <feed.json> <post.json> <threads>
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>

#define MAXFDS 400000
#define MAXTHREADS 16

/* one pending response per connection: canned bodies are immutable and
 * shared, so this is all the per-connection state there is */
typedef struct {
    const char *base;    /* response bytes (shared) */
    int len;
    int off;
    char *stash;         /* partial request line, rare */
    int stash_len;
} conn_t;

/* per-thread counters, no atomics needed (single owning thread) */
typedef struct {
    long long accepts, accept_eagain, accept_err, accept_emfile, fd_overflow;
    long long req_health, req_feed, req_post, req_like, req_create, req_unknown;
    long long resp_done, partial_write, write_eagain, write_err;
    long long read_eagain, read_zero, read_err, stash_hit, stash_drop;
    long long hup_close, close_total, bytes_out, bytes_in;
    long long epoll_waits, epoll_events, zero_event_waits;
    long long open_now;
} wc_t;

static conn_t *conns;
static wc_t WC[MAXTHREADS];
static int g_nthreads;
static volatile int g_done;

static char feed_resp[16384], post_resp[16384], like_resp[512], create_resp[16384], health_resp[512];
static int feed_len, post_len, like_len, create_len, health_len;

static char *file_read(const char *path, int *out_len)
{
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); exit(1); }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *b = malloc(n + 1);
    if (!b || fread(b, 1, n, f) != (size_t)n) { perror("fread"); exit(1); }
    fclose(f);
    b[n] = 0;
    *out_len = (int)n;
    return b;
}

static void build_responses(const char *feed_path, const char *post_path)
{
    int fl, pl;
    char *fb = file_read(feed_path, &fl);
    char *pb = file_read(post_path, &pl);
    feed_len   = snprintf(feed_resp, sizeof feed_resp,
                          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                          "Content-Length: %d\r\n\r\n", fl);
    memcpy(feed_resp + feed_len, fb, fl);
    feed_len += fl;
    post_len  = snprintf(post_resp, sizeof post_resp,
                         "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         "Content-Length: %d\r\n\r\n", pl);
    memcpy(post_resp + post_len, pb, pl);
    post_len += pl;
    like_len  = snprintf(like_resp, sizeof like_resp,
                         "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n"
                         "Content-Length: 28\r\n\r\n{\"post_id\":1,\"like_count\":1}");
    create_len = snprintf(create_resp, sizeof create_resp,
                          "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n"
                          "Content-Length: %d\r\n\r\n", pl);
    memcpy(create_resp + create_len, pb, pl);
    create_len += pl;
    health_len = snprintf(health_resp, sizeof health_resp,
                          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                          "Content-Length: 15\r\n\r\n{\"status\":\"ok\"}");
    free(fb);
    free(pb);
}

static void set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* decide which canned response the request line asks for */
static const char *pick(wc_t *wc, const char *line, int len, int *rlen)
{
    if (len >= 11 && !memcmp(line, "GET /health", 11)) { wc->req_health++; *rlen = health_len; return health_resp; }
    if (len >= 9 && !memcmp(line, "GET /feed", 9))    { wc->req_feed++;    *rlen = feed_len;    return feed_resp; }
    if (len >= 10 && !memcmp(line, "GET /posts", 10)) { wc->req_post++;    *rlen = post_len;    return post_resp; }
    if (len >= 5 && !memcmp(line, "POST ", 5)) {
        if (len >= 12 && !memcmp(line + 5, "/posts/", 7) && memmem(line, len, "/like", 5)) {
            wc->req_like++; *rlen = like_len; return like_resp;
        }
        wc->req_create++; *rlen = create_len; return create_resp;
    }
    wc->req_unknown++; *rlen = health_len; return health_resp; /* unknown: cheap answer */
}

/* close a connection: the owning thread calls this, never another thread */
static void drop(int fd, wc_t *wc)
{
    close(fd);
    if (conns[fd].stash) { free(conns[fd].stash); conns[fd].stash = NULL; }
    conns[fd].base = NULL;
    conns[fd].stash_len = 0;
    wc->close_total++;
    wc->open_now--;
}

static void *worker(void *arg)
{
    unsigned port = (unsigned)(uintptr_t)arg >> 16;
    int tid = (int)((unsigned)(uintptr_t)arg & 0xffff);
    wc_t *wc = &WC[tid];
    int epfd = epoll_create1(0);
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((unsigned short)port);
    if (bind(lfd, (struct sockaddr *)&a, sizeof a) < 0) { perror("bind"); exit(1); }
    if (listen(lfd, 65536) < 0) { perror("listen"); exit(1); }
    set_nonblock(lfd);
    struct epoll_event ev = { .events = EPOLLIN, .data.u32 = MAXFDS };
    epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev);

    struct epoll_event evs[512];
    char scratch[8192];
    for (;;) {
        int n = epoll_wait(epfd, evs, 512, 1000);
        wc->epoll_waits++;
        if (n == 0) { wc->zero_event_waits++; continue; }
        wc->epoll_events += n;
        for (int i = 0; i < n; i++) {
            if (evs[i].data.u32 == (uint32_t)MAXFDS) {
                for (;;) {
                    int fd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK);
                    if (fd < 0) {
                        if (errno == EAGAIN) { wc->accept_eagain++; break; }
                        if (errno == EINTR) continue;
                        wc->accept_err++;
                        if (errno == EMFILE || errno == ENFILE) {
                            /* listener stays readable: without a pause this
                             * becomes a 100% CPU spin that starves everything */
                            wc->accept_emfile++;
                            usleep(1000);
                        }
                        break;
                    }
                    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                    if (fd >= MAXFDS) {
                        /* can't track it: close and count, never leak */
                        wc->fd_overflow++;
                        close(fd);
                        continue;
                    }
                    conn_t *c = &conns[fd];
                    memset(c, 0, sizeof *c);
                    struct epoll_event ev = { .events = EPOLLIN, .data.u32 = (uint32_t)fd };
                    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
                    wc->accepts++;
                    wc->open_now++;
                }
                continue;
            }
            int fd = (int)evs[i].data.u32;
            if (fd < 0 || fd >= MAXFDS) continue;
            conn_t *c = &conns[fd];
            if (evs[i].events & (EPOLLERR | EPOLLHUP)) { wc->hup_close++; drop(fd, wc); continue; }
            if (evs[i].events & EPOLLIN) {
                for (;;) {
                    int r = read(fd, scratch, sizeof scratch);
                    if (r > 0) {
                        wc->bytes_in += r;
                        char *line = scratch; int line_len = r;
                        if (c->stash_len || !memmem(scratch, r, "\r\n", 2)) {
                            wc->stash_hit++;
                            if (!c->stash) c->stash = malloc(4096);
                            if (c->stash_len + r <= 4096) {
                                memcpy(c->stash + c->stash_len, scratch, r);
                                c->stash_len += r;
                            } else {
                                wc->stash_drop++;
                                c->stash_len = 0;
                            }
                            if (!memmem(c->stash, c->stash_len, "\r\n", 2)) break;
                            line = c->stash; line_len = c->stash_len;
                        }
                        const char *eol = memmem(line, line_len, "\r\n", 2);
                        if (eol) {
                            int rl;
                            const char *resp = pick(wc, line, (int)(eol - line), &rl);
                            c->base = resp; c->len = rl; c->off = 0;
                            c->stash_len = 0;
                        }
                        break; /* one request per read is plenty for the load model */
                    }
                    if (r == 0) { wc->read_zero++; drop(fd, wc); break; }
                    if (errno == EAGAIN) { wc->read_eagain++; break; }
                    if (errno == EINTR) continue;
                    wc->read_err++;
                    drop(fd, wc);
                    break;
                }
                /* if the read path dropped the fd, c->base is NULL and the
                 * write block below is skipped automatically */
            }
            if (c->base && c->off < c->len && (evs[i].events & (EPOLLOUT | EPOLLIN))) {
                for (;;) {
                    if (!c->base || c->off >= c->len) break;
                    int w = write(fd, c->base + c->off, c->len - c->off);
                    if (w > 0) {
                        c->off += w;
                        wc->bytes_out += w;
                        if (c->off >= c->len) {
                            c->base = NULL;
                            wc->resp_done++;
                            struct epoll_event ev = { .events = EPOLLIN, .data.u32 = (uint32_t)fd };
                            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
                            break;
                        }
                        wc->partial_write++;
                        continue;
                    }
                    if (errno == EAGAIN) {
                        wc->write_eagain++;
                        struct epoll_event ev = { .events = EPOLLIN | EPOLLOUT, .data.u32 = (uint32_t)fd };
                        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
                        break;
                    }
                    if (errno == EINTR) continue;
                    wc->write_err++;
                    drop(fd, wc);
                    break;
                }
            }
        }
    }
    return NULL;
}

static void *stats_thread(void *arg)
{
    (void)arg;
    long long t = 0;
    long long prev_reqs = 0, prev_done = 0, prev_acc = 0, prev_close = 0, prev_weag = 0, prev_ev = 0;
    for (;;) {
        sleep(5);
        t += 5;
        long long acc = 0, aeag = 0, aerr = 0, emf = 0, ovl = 0;
        long long rh = 0, rf = 0, rp = 0, rl = 0, rc = 0, ru = 0;
        long long done = 0, weag = 0, werr = 0, reag = 0, rzero = 0, rerr = 0;
        long long hup = 0, closes = 0, bout = 0, bin_ = 0, waits = 0, evs = 0, zev = 0, open = 0;
        for (int i = 0; i < g_nthreads; i++) {
            acc += WC[i].accepts; aeag += WC[i].accept_eagain; aerr += WC[i].accept_err;
            emf += WC[i].accept_emfile; ovl += WC[i].fd_overflow;
            rh += WC[i].req_health; rf += WC[i].req_feed; rp += WC[i].req_post;
            rl += WC[i].req_like; rc += WC[i].req_create; ru += WC[i].req_unknown;
            done += WC[i].resp_done; weag += WC[i].write_eagain; werr += WC[i].write_err;
            reag += WC[i].read_eagain; rzero += WC[i].read_zero; rerr += WC[i].read_err;
            hup += WC[i].hup_close; closes += WC[i].close_total;
            bout += WC[i].bytes_out; bin_ += WC[i].bytes_in;
            waits += WC[i].epoll_waits; evs += WC[i].epoll_events; zev += WC[i].zero_event_waits;
            open += WC[i].open_now;
        }
        long long reqs = rh + rf + rp + rl + rc + ru;
        fprintf(stderr,
            "sinkstats: t=%llds open=%lld acc=%lld (+%lld) closes=%lld (+%lld) "
            "reqs=%lld (+%lld) done=%lld (+%lld) pend=%lld | "
            "feed=%lld post=%lld like=%lld create=%lld health=%lld unk=%lld | "
            "weagain=%lld (+%lld) werr=%lld reag=%lld rzero=%lld rerr=%lld hup=%lld | "
            "aeagain=%lld aerr=%lld emfile=%lld oflow=%lld | "
            "bout=%.1fMB bin=%.1fMB ev5s=%lld waits=%lld idle=%lld\n",
            t, open, acc, acc - prev_acc, closes, closes - prev_close,
            reqs, reqs - prev_reqs, done, done - prev_done, reqs - done,
            rf, rp, rl, rc, rh, ru,
            weag, weag - prev_weag, werr, reag, rzero, rerr, hup,
            aeag, aerr, emf, ovl,
            bout / 1048576.0, bin_ / 1048576.0, evs - prev_ev, waits, zev);
        fflush(stderr);
        prev_reqs = reqs; prev_done = done; prev_acc = acc; prev_close = closes; prev_weag = weag; prev_ev = evs;
        if (g_done) break;
    }
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 5) { fprintf(stderr, "usage: sink <port> <feed.json> <post.json> <threads>\n"); return 1; }
    signal(SIGPIPE, SIG_IGN);
    int port = atoi(argv[1]);
    int nthreads = atoi(argv[4]);
    if (nthreads < 1 || nthreads > MAXTHREADS) { fprintf(stderr, "threads 1..%d\n", MAXTHREADS); return 1; }
    g_nthreads = nthreads;
    build_responses(argv[2], argv[3]);
    conns = calloc(MAXFDS, sizeof *conns);
    if (!conns) { perror("calloc"); return 1; }

    pthread_t st;
    pthread_create(&st, NULL, stats_thread, NULL);
    pthread_detach(st);

    for (int i = 0; i < nthreads; i++) {
        pthread_t t;
        uintptr_t arg = (uintptr_t)(((unsigned)port << 16) | (unsigned)i);
        if (pthread_create(&t, NULL, worker, (void *)arg) != 0) { perror("pthread_create"); return 1; }
        pthread_detach(t);
    }
    fprintf(stderr, "sink: %d threads on :%d (feed %dB post %dB like %dB create %dB)\n",
            nthreads, port, feed_len, post_len, like_len, create_len);
    for (;;) pause();
    return 0;
}
