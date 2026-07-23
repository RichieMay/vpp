/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * ldp2 — LD_PRELOAD 拦截层。libc socket 调用路由到 vcl2（合成 fd = base+handle），
 * 真 fd 直接 passthrough。仅拦截 AF_INET/INET6 + SOCK_STREAM。
 * 用法：LD_PRELOAD=libvcl2_ldpreload.so <app>
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <dlfcn.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <sys/time.h>
#include <fcntl.h>
#include <poll.h>
#include <time.h>
#include <signal.h>
#include <netinet/in.h>
#include <stdarg.h>

#include "vcl2.h"
#include "vcl2_private.h"

#define LDP2_DBG(...)                                                          \
  do {                                                                         \
    if (vcl2_debug) {                                                          \
      fprintf (stderr, "ldp2<%d>: ", (int) getpid ());                         \
      fprintf (stderr, __VA_ARGS__);                                           \
      fprintf (stderr, "\n");                                                  \
    }                                                                          \
  } while (0)

/* ---------- 真 libc 函数指针（懒解析） ---------- */
static int (*libc_socket) (int, int, int);
static int (*libc_connect) (int, const struct sockaddr *, socklen_t);
static ssize_t (*libc_read) (int, void *, size_t);
static ssize_t (*libc_write) (int, const void *, size_t);
static ssize_t (*libc_recv) (int, void *, size_t, int);
static ssize_t (*libc_send) (int, const void *, size_t, int);
static int (*libc_close) (int);
static int (*libc_epoll_create1) (int);
static int (*libc_epoll_ctl) (int, int, int, struct epoll_event *);
static int (*libc_epoll_wait) (int, struct epoll_event *, int, int);
static int (*libc_bind) (int, const struct sockaddr *, socklen_t);
static int (*libc_listen) (int, int);
static int (*libc_accept4) (int, struct sockaddr *, socklen_t *, int);
static int (*libc_accept) (int, struct sockaddr *, socklen_t *);
static int (*libc_setsockopt) (int, int, int, const void *, socklen_t);
static int (*libc_getsockopt) (int, int, int, void *, socklen_t *);
static int (*libc_getsockname) (int, struct sockaddr *, socklen_t *);
static int (*libc_getpeername) (int, struct sockaddr *, socklen_t *);
static int (*libc_poll) (struct pollfd *, nfds_t, int);
static int (*libc_select) (int, fd_set *, fd_set *, fd_set *, struct timeval *);
static int (*libc_pselect) (int, fd_set *, fd_set *, fd_set *,
                            const struct timespec *, const sigset_t *);
static int (*libc_fcntl) (int, int, ...);
static int (*libc_fcntl64) (int, int, ...);
static int (*libc_ioctl) (int, unsigned long, ...);
static ssize_t (*libc_writev) (int, const struct iovec *, int);
static ssize_t (*libc_readv) (int, const struct iovec *, int);
static ssize_t (*libc_sendfile) (int, int, off_t *, size_t);
static int (*libc_shutdown) (int, int);
static ssize_t (*libc_sendto) (int, const void *, size_t, int,
                               const struct sockaddr *, socklen_t);
static ssize_t (*libc_recvfrom) (int, void *, size_t, int,
                                 struct sockaddr *, socklen_t *);
static ssize_t (*libc_sendmsg) (int, const struct msghdr *, int);
static ssize_t (*libc_recvmsg) (int, struct msghdr *, int);

/* 对齐 VCL ldp_socket_wrapper.c：显式 dlopen libc 解析符号，
 * 不用 RTLD_NEXT（多层 LD_PRELOAD 下后者可能解析到错误的库）。
 * handle 保持打开（符号指针依赖它；进程生命周期内不 dlclose）。 */
static void *libc_handle;

static void ldp2_resolve_libc (void) {
  libc_handle = dlopen ("libc.so.6", RTLD_LAZY);
  if (!libc_handle)
    libc_handle = RTLD_NEXT; /* 回退：单 preload 场景仍可靠 */

#define RESOLVE(sym) libc_##sym = dlsym (libc_handle, #sym)
  RESOLVE (socket);
  RESOLVE (connect);
  RESOLVE (read);
  RESOLVE (write);
  RESOLVE (recv);
  RESOLVE (send);
  RESOLVE (close);
  RESOLVE (epoll_create1);
  RESOLVE (epoll_ctl);
  RESOLVE (epoll_wait);
  RESOLVE (bind);
  RESOLVE (listen);
  RESOLVE (accept4);
  RESOLVE (accept);
  RESOLVE (setsockopt);
  RESOLVE (getsockopt);
  RESOLVE (getsockname);
  RESOLVE (getpeername);
  RESOLVE (poll);
  RESOLVE (select);
  RESOLVE (pselect);
  RESOLVE (fcntl);
  RESOLVE (fcntl64);
  RESOLVE (ioctl);
  RESOLVE (writev);
  RESOLVE (readv);
  RESOLVE (sendfile);
  RESOLVE (shutdown);
  RESOLVE (sendto);
  RESOLVE (recvfrom);
  RESOLVE (sendmsg);
  RESOLVE (recvmsg);
#undef RESOLVE
}

/* vcl2 初始化：eager constructor（main 前 attach，失败则 _exit(1)）。
 * 预置 ldp2_init_done=1 防 attach 内 socket()/connect() 重入拦截死循环。 */
static int ldp2_init_done;

/* 返回 0=成功，非 0=失败（errno 风格）。成功时 ldp2_init_done=1。 */
static int ldp2_init (void) {
  int rv;
  const char *name;

  if (ldp2_init_done)
    return 0;

  ldp2_resolve_libc (); /* 确保 libc 指针就绪（attach 内部重入拦截器要用） */
  name = getenv ("VCL2_APP_NAME");
  rv = vcl2_init (name && name[0] ? name : "ldp2_app");
  if (rv) {
    LDP2_DBG ("vcl2_init failed: %d", rv);
    return rv;
  }

  ldp2_init_done = 1; /* ★ 预置：attach 内部重入 socket() 时不重入 init */
  rv = vcl2_app_attach ();
  if (rv) {
    LDP2_DBG (
      "vcl2_app_attach failed: %d (%s) — is VPP up with app-socket-api?", rv,
      strerror (-rv));
    ldp2_init_done = 0;
    return rv;
  }
  LDP2_DBG ("attached to VPP (app_index=%u wrk=%u)", vcl2_main.app_index,
            vcl2_main.app_wrk_index);
  return 0;
}

/* 拦截器入口调用：未初始化则尝试初始化（已 done 则 O(1) 跳过，含 attach 途中的重入） */
#define ldp2_init_check()                                                      \
  do {                                                                         \
    if (!ldp2_init_done)                                                       \
      ldp2_init ();                                                            \
  } while (0)

/* eager：库加载时（main 之前）即 attach VPP；失败则 _exit(1)（对齐 VCL ldp_constructor） */
__attribute__ ((constructor)) static void ldp2_ctor (void) {
  setvbuf (stderr, NULL, _IONBF,
           0); /* 无缓冲：nginx 重定向 fd2 后 vcl2 调试打印也能即时落盘 */
  if (ldp2_init () != 0) {
    fprintf (stderr, "\nldp2<%d>: ERROR: ldp2_ctor: attach VPP failed!\n",
             (int) getpid ());
    _exit (1);
  }
}

/* ---------- fd 分类 ---------- */
/* 合成 fd = fd_base + handle。仅当 handle 在 session 缓存里（已 create）才算 vcl2 fd。
 * 多线程：hash 查询需 sessions 读锁（与 alloc/close 的 hash 写并发安全）。fd<base 直接
 * 返回（真低 fd 不可能是 vcl2），不取锁。 */
static inline int ldp2_fd_is_vcl2 (int fd) {
  vcl2_handle_t h;
  int is_vcl2;
  if (!vcl2_is_init ())
    return 0;
  if (fd < (int) vcl2_main.fd_base)
    return 0;
  h = vcl2_fd_to_handle (fd);
  clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
  is_vcl2 = vcl2_session_get (h) != 0;
  clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
  return is_vcl2;
}

/* 前向声明（epoll 实现在 close() 之后） */
void ldp2_ep_close (int epfd);

/* ---------- 拦截器 ---------- */

int socket (int domain, int type, int protocol) {
  int t = type & ~(SOCK_CLOEXEC | SOCK_NONBLOCK);
  ldp2_init_check ();

  /* 只接 IPv4/IPv6 TCP；其余原样走 libc */
  if ((domain == AF_INET || domain == AF_INET6) && t == SOCK_STREAM &&
      vcl2_is_init () && vcl2_main.app_index) {
    int h =
      vcl2_session_create (VCL2_PROTO_TCP, (type & SOCK_NONBLOCK) ? 1 : 0);
    if (h < 0) {
      errno = -h;
      return -1;
    }
    int fd = vcl2_handle_to_fd ((vcl2_handle_t) h);
    LDP2_DBG ("socket(AF %d STREAM) -> synthetic fd=%d (handle=%d)", domain, fd,
              h);
    return fd;
  }
  return libc_socket (domain, type, protocol);
}

int connect (int fd, const struct sockaddr *addr, socklen_t len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd) && addr && len >= sizeof (struct sockaddr_in)) {
    vcl2_handle_t h = vcl2_fd_to_handle (fd);
    const struct sockaddr_in *a4 = (const struct sockaddr_in *) addr;
    uint8_t ip[16];
    uint8_t is_ip4;
    uint16_t port;
    int rv;

    if (a4->sin_family == AF_INET) {
      is_ip4 = 1;
      memcpy (ip, &a4->sin_addr, 4);
      port = ntohs (a4->sin_port);
    } else if (len >= sizeof (struct sockaddr_in6)) {
      const struct sockaddr_in6 *a6 = (const struct sockaddr_in6 *) addr;
      is_ip4 = 0;
      memcpy (ip, &a6->sin6_addr, 16);
      port = ntohs (a6->sin6_port);
    } else {
      errno = EAFNOSUPPORT;
      return -1;
    }
    LDP2_DBG ("connect fd=%d handle=%d -> %s:%u", fd, h, is_ip4 ? "ip4" : "ip6",
              port);
    rv = vcl2_session_connect (h, is_ip4, ip, port);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return 0;
  }
  return libc_connect (fd, addr, len);
}

ssize_t write (int fd, const void *buf, size_t n) {
  if (ldp2_fd_is_vcl2 (fd)) {
    ssize_t rv = vcl2_session_send (vcl2_fd_to_handle (fd), buf, n);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return rv;
  }
  return libc_write (fd, buf, n);
}

/* writev：gather iovec 到临时缓冲再 vcl2_session_send（nginx HTTP 响应用 writev）。 */
ssize_t writev (int fd, const struct iovec *iov, int iovcnt) {
  ldp2_init_check ();
  if (!ldp2_fd_is_vcl2 (fd))
    return libc_writev (fd, iov, iovcnt);
  size_t total = 0, i;
  uint8_t stack[8192], *buf = stack;
  uint8_t *heap = 0;
  for (i = 0; i < (size_t) iovcnt; i++)
    total += iov[i].iov_len;
  if (total > sizeof (stack)) {
    heap = malloc (total);
    buf = heap ? heap : stack;
  }
  size_t off = 0;
  for (i = 0; i < (size_t) iovcnt &&
              off < (total > sizeof (stack) && !heap ? sizeof (stack) : total);
       i++) {
    size_t n = iov[i].iov_len;
    if (off + n > (heap ? total : sizeof (stack)))
      n = (heap ? total : sizeof (stack)) - off;
    memcpy (buf + off, iov[i].iov_base, n);
    off += n;
  }
  ssize_t rv = vcl2_session_send (vcl2_fd_to_handle (fd), buf, off);
  free (heap);
  if (rv < 0) {
    errno = -rv;
    return -1;
  }
  return rv;
}

/* readv：vcl2_session_recv 到临时缓冲再 scatter 到 iovec。 */
ssize_t readv (int fd, const struct iovec *iov, int iovcnt) {
  ldp2_init_check ();
  if (!ldp2_fd_is_vcl2 (fd))
    return libc_readv (fd, iov, iovcnt);
  size_t total = 0, i;
  for (i = 0; i < (size_t) iovcnt; i++)
    total += iov[i].iov_len;
  uint8_t stack[8192], *buf = stack;
  uint8_t *heap = 0;
  if (total > sizeof (stack)) {
    heap = malloc (total);
    buf = heap ? heap : stack;
    if (!heap)
      total = sizeof (stack);
  }
  ssize_t rv = vcl2_session_recv (vcl2_fd_to_handle (fd), buf, total);
  if (rv > 0) {
    size_t rem = rv;
    for (i = 0; i < (size_t) iovcnt && rem > 0; i++) {
      size_t n = iov[i].iov_len < rem ? iov[i].iov_len : rem;
      memcpy (iov[i].iov_base, buf + (rv - rem), n);
      rem -= n;
    }
  }
  free (heap);
  if (rv < 0) {
    errno = -rv;
    return -1;
  }
  return rv;
}

/* sendfile：vcl2 fd 作 out_fd 时，读 in_fd（真文件）+ vcl2_session_send。
 * nginx 默认 sendfile off；若开，这里兜底（in_fd 必须是真文件 fd）。 */
ssize_t sendfile (int out_fd, int in_fd, off_t *offset, size_t count) {
  ldp2_init_check ();
  if (!ldp2_fd_is_vcl2 (out_fd))
    return libc_sendfile (out_fd, in_fd, offset, count);
  uint8_t stack[8192], *buf = stack;
  uint8_t *heap = 0;
  if (count > sizeof (stack)) {
    heap = malloc (count);
    buf = heap ? heap : stack;
    if (!heap)
      count = sizeof (stack);
  }
  off_t cur = offset ? *offset : 0;
  ssize_t r = pread (in_fd, buf, count, cur);
  if (r <= 0) {
    free (heap);
    return r;
  }
  ssize_t s = vcl2_session_send (vcl2_fd_to_handle (out_fd), buf, r);
  if (s > 0 && offset)
    *offset = cur + s;
  free (heap);
  if (s < 0) {
    errno = -s;
    return -1;
  }
  return s;
}

ssize_t read (int fd, void *buf, size_t n) {
  if (ldp2_fd_is_vcl2 (fd)) {
    ssize_t rv = vcl2_session_recv (vcl2_fd_to_handle (fd), buf, n);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return rv;
  }
  return libc_read (fd, buf, n);
}

ssize_t send (int fd, const void *buf, size_t n, int flags) {
  (void) flags;
  if (ldp2_fd_is_vcl2 (fd)) {
    ssize_t rv = vcl2_session_send (vcl2_fd_to_handle (fd), buf, n);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return rv;
  }
  return libc_send (fd, buf, n, flags);
}

ssize_t recv (int fd, void *buf, size_t n, int flags) {
  (void) flags;
  if (ldp2_fd_is_vcl2 (fd)) {
    ssize_t rv = vcl2_session_recv (vcl2_fd_to_handle (fd), buf, n);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return rv;
  }
  return libc_recv (fd, buf, n, flags);
}

int close (int fd) {
  ldp2_ep_close (fd); /* 若是我们创建的 epoll fd，清侧表 */
  if (ldp2_fd_is_vcl2 (fd)) {
    LDP2_DBG ("close synthetic fd=%d", fd);
    vcl2_session_close (vcl2_fd_to_handle (fd));
    return 0;
  }
  return libc_close (fd);
}

/* epoll：返回真 libc epoll fd。vcl2 fd 注册记侧表 regs[handle]，eventfd 加进真 epoll。
 * epoll_wait：查 vcl2 fifo 即时就绪 + 阻塞 libc_epoll_wait（eventfd 唤醒后 drain+复查）。 */

/* 单个 vcl2 fd 在某 epoll 上的注册 */
typedef struct {
  struct epoll_event ev; /* app 给的 events + data */
  uint8_t in_use;
} ldp2_evr_t;

/* 一个 epoll fd 的侧表 */
typedef struct {
  int libc_epfd; /* == 返回给 app 的真 fd */
  uint8_t in_use;
  uint8_t evtfd_added; /* app_event_queue 的 eventfd 是否已加入 libc_epfd */
  ldp2_evr_t *regs;    /* 按 handle 索引的 vcl2 fd 注册表 */
} ldp2_ep_t;

static ldp2_ep_t *ldp2_eps;
static uword *ldp2_ep_index; /* hash: libc_epfd -> ldp2_eps[] 下标 */

static ldp2_ep_t *ldp2_ep_get (int epfd) {
  uword *p = hash_get (ldp2_ep_index, epfd);
  if (!p)
    return 0;
  ldp2_ep_t *ep = vec_elt_at_index (ldp2_eps, p[0]);
  return ep->in_use ? ep : 0;
}

static ldp2_ep_t *ldp2_ep_alloc (int epfd) {
  ldp2_ep_t *ep;
  u32 idx;
  for (idx = 0; idx < vec_len (ldp2_eps); idx++)
    if (!ldp2_eps[idx].in_use) {
      ep = &ldp2_eps[idx];
      memset (ep, 0, sizeof (*ep));
      goto found;
    }
  vec_add2 (ldp2_eps, ep, 1);
  idx = ep - ldp2_eps;
  memset (ep, 0, sizeof (*ep));
found:
  ep->libc_epfd = epfd;
  ep->in_use = 1;
  hash_set (ldp2_ep_index, epfd, idx);
  return ep;
}

void ldp2_ep_close (int epfd) {
  ldp2_ep_t *ep = ldp2_ep_get (epfd);
  if (!ep)
    return;
  vec_free (ep->regs);
  ep->in_use = 0;
  ep->regs = 0;
  hash_unset (ldp2_ep_index, epfd);
}

/* app_event_queue 的 eventfd（VPP 投事件时 signal 它） */
static int ldp2_app_evt_fd (void) {
  if (!vcl2_is_init () || !vcl2_main.app_event_queue)
    return -1;
  return vcl2_main.app_event_queue->q.evtfd;
}

/* 把 eventfd 加进真 epoll（仅一次/epoll），用于 epoll_wait 唤醒 */
static void ldp2_ep_ensure_evtfd (ldp2_ep_t *ep) {
  int efd;
  struct epoll_event e;
  if (ep->evtfd_added)
    return;
  efd = ldp2_app_evt_fd ();
  if (efd < 0)
    return;
  e.events = EPOLLIN;
  e.data.fd = efd; /* wait 时据此识别 eventfd 事件 */
  if (libc_epoll_ctl (ep->libc_epfd, EPOLL_CTL_ADD, efd, &e) == 0)
    ep->evtfd_added = 1;
}

/* 排空 app_event_queue 并派发：ACCEPTED 入 listener 的 accept_q，其余 IO 事件
 *  靠 fifo 状态反映。同时让 eventfd 可被再次 arm。 */
static void ldp2_drain_app_events (void) {
  vcl2_dispatch_app_events ();
}

/* 查一个 vcl2 fd 的当前就绪事件。listener：accept_q 非空→EPOLLIN；
 * 数据 session：rx 有数据→IN，tx 有空间→OUT。 */
static uint32_t ldp2_session_ready (vcl2_session_t *s, uint32_t want) {
  uint32_t ev = 0;
  if (s->is_listener) {
    if ((want & EPOLLIN) && vec_len (s->accept_q) > 0)
      ev |= EPOLLIN;
    return ev;
  }
  /* peer 关：报 EPOLLIN 让 app 读取 → recv 返回 0(EOF)，app 关闭连接 */
  if (s->peer_closed && (want & EPOLLIN))
    ev |= EPOLLIN;
  if ((want & EPOLLIN) && s->rx_fifo &&
      svm_fifo_max_dequeue_cons (s->rx_fifo) > 0)
    ev |= EPOLLIN;
  if ((want & EPOLLOUT) && s->tx_fifo) {
    /* tx 有空间→EPOLLOUT；否则 arm want_deq_ntf，让 VPP 在 tx 空间释放时 signal
       * eventfd（这样事件循环无需分片轮询即可被 EPOLLOUT 唤醒）。一次性，每轮扫描重 arm。*/
    if (svm_fifo_max_enqueue_prod (s->tx_fifo) > 0)
      ev |= EPOLLOUT;
    else
      svm_fifo_add_want_deq_ntf (s->tx_fifo, SVM_FIFO_WANT_DEQ_NOTIF);
  }
  return ev;
}

/* 扫描 epoll 上所有 vcl2 fd 注册，把就绪的填进 events[]。返回新增数。
 * 持 sessions 读锁扫描（get/session_ready 用 session 指针；锁内指针稳定）。 */
static int ldp2_ep_collect_vcl2 (ldp2_ep_t *ep, struct epoll_event *events,
                                 int maxevents, int off) {
  int n = off;
  u32 h;
  clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
  for (h = 0; h < vec_len (ep->regs) && n < maxevents; h++) {
    ldp2_evr_t *r = &ep->regs[h];
    if (!r->in_use)
      continue;
    vcl2_session_t *s = vcl2_session_get (h);
    if (!s)
      continue;
    /* listener 即使无 fifo 也可就绪（accept_q）；数据 session 需已连接 */
    if (!s->is_listener && !s->rx_fifo)
      continue;
    uint32_t ev = ldp2_session_ready (s, r->ev.events);
    if (ev) {
      events[n].events = ev;
      events[n].data = r->ev.data;
      n++;
    }
  }
  clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
  return n;
}

int epoll_create1 (int flags) {
  int epfd;
  ldp2_init_check ();
  epfd = libc_epoll_create1 (flags);
  if (epfd < 0)
    return -1;
  ldp2_ep_alloc (epfd);
  LDP2_DBG ("epoll_create1 -> epfd=%d", epfd);
  return epfd;
}

int epoll_create (int size) {
  (void) size;
  return epoll_create1 (EPOLL_CLOEXEC);
}

int epoll_ctl (int epfd, int op, int fd, struct epoll_event *event) {
  ldp2_ep_t *ep;

  ldp2_init_check ();
  ep = ldp2_ep_get (epfd);
  if (!ep)
    return libc_epoll_ctl (epfd, op, fd, event); /* 不是我们的 epoll */

  if (ldp2_fd_is_vcl2 (fd)) {
    /* vcl2 fd：记侧表 + 确保 eventfd 进真 epoll */
    vcl2_handle_t h = vcl2_fd_to_handle (fd);
    if (op == EPOLL_CTL_DEL) {
      if (h < vec_len (ep->regs))
        ep->regs[h].in_use = 0;
      return 0;
    }
    if (!event) {
      errno = EFAULT;
      return -1;
    }
    vec_validate (ep->regs, h);
    ep->regs[h].ev = *event;
    ep->regs[h].in_use = 1;
    ldp2_ep_ensure_evtfd (ep);
    return 0;
  }
  /* 真 fd：直接进真 epoll */
  return libc_epoll_ctl (epfd, op, fd, event);
}

int epoll_wait (int epfd, struct epoll_event *events, int maxevents,
                int timeout) {
  ldp2_ep_t *ep;
  struct epoll_event tmp[64];
  int efd, n, m, i, tmpcap;
  long deadline_ms = -1; /* 超时截止（CLOCK_MONOTONIC 毫秒）；-1=无限 */

  ldp2_init_check ();
  if (maxevents <= 0 || timeout < -1) {
    errno = EINVAL;
    return -1;
  }
  ep = ldp2_ep_get (epfd);
  if (!ep)
    return libc_epoll_wait (epfd, events, maxevents,
                            timeout); /* 非 vcl2 epoll */

  efd = ldp2_app_evt_fd ();
  tmpcap = sizeof (tmp) / sizeof (tmp[0]);
  if (tmpcap > maxevents)
    tmpcap = maxevents;
  if (timeout > 0) {
    struct timespec t0;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 + timeout;
  }

  /* 事件驱动：每轮 drain+collect 后，用【完整剩余超时】阻塞在真 epoll（含 eventfd）
   * 上，被 VPP 信号/真 fd/超时唤醒。只在"唤醒却无就绪 fd"时（如 eventfd 是别的
   * session 的事件）才循环。无分片、无 usleep。eventfd 在 epoll_ctl ADD 时已加入
   * libc_epfd（ldp2_ep_ensure_evtfd）。 */
  for (;;) {
    int t;
    ldp2_drain_app_events ();
    n = ldp2_ep_collect_vcl2 (ep, events, maxevents, 0);
    if (n > 0 || timeout == 0)
      return n;

    if (deadline_ms < 0)
      t = -1;
    else {
      struct timespec now;
      long now_ms;
      clock_gettime (CLOCK_MONOTONIC, &now);
      now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
      if (now_ms >= deadline_ms)
        return 0;
      t = (int) (deadline_ms - now_ms);
    }

    m = libc_epoll_wait (ep->libc_epfd, tmp, tmpcap, t);
    if (m < 0 && errno != EINTR)
      return m;
    for (i = 0; i < m; i++) {
      if (efd >= 0 && tmp[i].data.fd == efd) {
        uint64_t b;
        read (efd, &b, sizeof (b)); /* 清 eventfd，下轮 drain+collect */
      } else if (n < maxevents)
        events[n++] = tmp[i]; /* 真 fd 事件原样透传 */
    }
    if (n > 0)
      return n; /* 真 fd 有事件 */
    /* 否则（eventfd 唤醒或 EINTR）：循环重新 drain+collect */
  }
}

int epoll_pwait (int epfd, struct epoll_event *events, int maxevents,
                 int timeout, const sigset_t *sigmask) {
  (void) sigmask; /* P4b：暂忽略 sigmask（nginx 默认不依赖） */
  return epoll_wait (epfd, events, maxevents, timeout);
}

/* ---------- server 侧：bind / listen / accept / setsockopt / fcntl ----------
 * bind：把本地地址存进 session（listen 时用）。vcl2 fd 无需真内核 bind。
 * listen：vcl2_session_listen（发 LISTEN 等 BOUND）。
 * accept/accept4：vcl2_session_accept（取 ACCEPTED 子 session → 新合成 fd）。
 * setsockopt：vcl2 fd 上大多是 no-op（SO_REUSEADDR 等 VPP 语义不同），返回 0。
 * fcntl：vcl2 fd 上记 nonblocking 标志（P4b 阻塞实现，flag 仅记录）。
 */

int bind (int fd, const struct sockaddr *addr, socklen_t len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd) && addr) {
    /* 写锁：存 lcl 地址进 session（mutate） */
    clib_rwlock_writer_lock (&vcl2_main.sessions_lock);
    vcl2_session_t *s = vcl2_session_get (vcl2_fd_to_handle (fd));
    if (!s) {
      clib_rwlock_writer_unlock (&vcl2_main.sessions_lock);
      errno = EBADF;
      return -1;
    }
    if (addr->sa_family == AF_INET && len >= sizeof (struct sockaddr_in)) {
      const struct sockaddr_in *a = (const struct sockaddr_in *) addr;
      s->lcl_is_ip4 = 1;
      memcpy (s->lcl_ip, &a->sin_addr, 4);
      s->lcl_port = ntohs (a->sin_port);
    } else if (addr->sa_family == AF_INET6 &&
               len >= sizeof (struct sockaddr_in6)) {
      const struct sockaddr_in6 *a = (const struct sockaddr_in6 *) addr;
      s->lcl_is_ip4 = 0;
      memcpy (s->lcl_ip, &a->sin6_addr, 16);
      s->lcl_port = ntohs (a->sin6_port);
    }
    clib_rwlock_writer_unlock (&vcl2_main.sessions_lock);
    return 0;
  }
  return libc_bind (fd, addr, len);
}

int listen (int fd, int backlog) {
  int rv;
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    rv = vcl2_session_listen (vcl2_fd_to_handle (fd), backlog);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return 0;
  }
  return libc_listen (fd, backlog);
}

static int ldp2_accept_common (int fd, struct sockaddr *addr,
                               socklen_t *addrlen, int flags) {
  vcl2_handle_t nh;
  int rv;

  ldp2_init_check ();
  if (!ldp2_fd_is_vcl2 (fd))
    return -2; /* 信号：不是 vcl2 fd */
  rv = vcl2_session_accept (vcl2_fd_to_handle (fd), &nh);
  LDP2_DBG ("ACCEPT lfd=%d -> rv=%d nh=%d (fd=%d)", fd, rv, nh,
            nh >= 0 ? (int) vcl2_main.fd_base + nh : -1);
  if (rv < 0) {
    errno = -rv;
    return -1;
  }
  /* 回填对端 sockaddr（nginx 等读 accept 返回的 peer addr；不填则拿到栈垃圾 → 连接被关）。
   * 读锁：取子 session 的 rmt 地址（读 session 字段）。 */
  if (addr && addrlen) {
    clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
    vcl2_session_t *cs = vcl2_session_get ((vcl2_handle_t) nh);
    if (cs && cs->rmt_is_ip4 &&
        *addrlen >= (socklen_t) sizeof (struct sockaddr_in)) {
      struct sockaddr_in a;
      memset (&a, 0, sizeof (a));
      a.sin_family = AF_INET;
      memcpy (&a.sin_addr, cs->rmt_ip, 4);
      a.sin_port = cs->rmt_port;
      memcpy (addr, &a, sizeof (a));
      *addrlen = sizeof (a);
    } else if (cs && *addrlen >= (socklen_t) sizeof (struct sockaddr_in6)) {
      struct sockaddr_in6 a6;
      memset (&a6, 0, sizeof (a6));
      a6.sin6_family = AF_INET6;
      memcpy (&a6.sin6_addr, cs->rmt_ip, 16);
      a6.sin6_port = cs->rmt_port;
      memcpy (addr, &a6, sizeof (a6));
      *addrlen = sizeof (a6);
    }
    clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
  }

  /* accept4 SOCK_NONBLOCK：设置子 session 非阻塞 */
  if (flags & SOCK_NONBLOCK) {
    clib_rwlock_writer_lock (&vcl2_main.sessions_lock);
    vcl2_session_t *cs = vcl2_session_get ((vcl2_handle_t) nh);
    if (cs)
      cs->nonblocking = 1;
    clib_rwlock_writer_unlock (&vcl2_main.sessions_lock);
  }

  return vcl2_handle_to_fd (nh);
}

int accept4 (int fd, struct sockaddr *addr, socklen_t *addrlen, int flags) {
  int r = ldp2_accept_common (fd, addr, addrlen, flags);
  if (r == -2)
    return libc_accept4 (fd, addr, addrlen, flags);
  return r;
}

int accept (int fd, struct sockaddr *addr, socklen_t *addrlen) {
  int r = ldp2_accept_common (fd, addr, addrlen, 0);
  if (r == -2)
    return libc_accept (fd, addr, addrlen);
  return r;
}

int setsockopt (int fd, int level, int optname, const void *optval,
                socklen_t optlen) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd))
    return 0; /* SO_REUSEADDR 等：vcl2/VPP 语义不同，静默成功 */
  return libc_setsockopt (fd, level, optname, optval, optlen);
}

/* shutdown：正常优雅半关闭。SHUT_RD 本地标记；SHUT_WR/RDWR 发 SHUTDOWN 给 VPP
 * （向 peer 发 FIN，连接仍可收）。镜像 VCL vls_shutdown。 */
int shutdown (int fd, int how) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    int rv = vcl2_session_shutdown (vcl2_fd_to_handle (fd), how);
    if (rv < 0) {
      errno = -rv;
      return -1;
    }
    return 0;
  }
  return libc_shutdown (fd, how);
}

int fcntl (int fd, int cmd, ...) {
  va_list ap;
  int argval;
  void *arg;
  va_start (ap, cmd);
  arg = va_arg (ap, void *);
  argval = (int) (long) arg;
  va_end (ap);
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    vcl2_handle_t h = vcl2_fd_to_handle (fd);
    if (cmd == F_SETFL) {
      clib_rwlock_writer_lock (&vcl2_main.sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      if (s)
        s->nonblocking = (argval & O_NONBLOCK) ? 1 : 0;
      clib_rwlock_writer_unlock (&vcl2_main.sessions_lock);
      return 0;
    }
    if (cmd == F_GETFL) {
      int nb = 0;
      clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      if (s)
        nb = s->nonblocking;
      clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
      return O_RDWR | (nb ? O_NONBLOCK : 0);
    }
    return 0;
  }
  return libc_fcntl (fd, cmd, arg);
}

/* ioctl：nginx 用 ioctl(FIONBIO) 设非阻塞（fcntl 的老式替代）。vcl2 fd 上记录标志。
 */
int ioctl (int fd, unsigned long cmd, ...) {
  va_list ap;
  void *arg;
  int *iarg;
  va_start (ap, cmd);
  arg = va_arg (ap, void *);
  va_end (ap);
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    if (cmd == FIONBIO && arg) {
      iarg = (int *) arg;
      int nb = *iarg ? 1 : 0;
      clib_rwlock_writer_lock (&vcl2_main.sessions_lock);
      vcl2_session_t *s = vcl2_session_get (vcl2_fd_to_handle (fd));
      if (s)
        s->nonblocking = nb;
      clib_rwlock_writer_unlock (&vcl2_main.sessions_lock);
    }
    return 0;
  }
  return libc_ioctl (fd, cmd, arg);
}

/* getsockopt：vcl2 fd 上返回合理值。SO_ERROR→0, SO_TYPE→SOCK_STREAM。 */
int getsockopt (int fd, int level, int optname, void *optval,
                socklen_t *optlen) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    if (optval && optlen && *optlen >= (socklen_t) sizeof (int)) {
      if (level == SOL_SOCKET &&
          (optname == SO_TYPE || optname == SO_DOMAIN))
        *(int *) optval = SOCK_STREAM;
      else
        *(int *) optval = 0; /* SO_ERROR / 各 int 型选项统一 0 */
    }
    return 0;
  }
  return libc_getsockopt (fd, level, optname, optval, optlen);
}

/* getsockname/getpeername：从 session 缓存回填真实地址。
 * ldp2_fill_name(fd, addr, len, is_peer=1) → peer 地址（rmt_*）
 * ldp2_fill_name(fd, addr, len, is_peer=0) → 本地地址（lcl_*） */
static int ldp2_fill_name (int fd, struct sockaddr *addr, socklen_t *len,
                           int is_peer) {
  vcl2_handle_t h = vcl2_fd_to_handle (fd);
  int ret = 0;

  if (!addr || !len)
    return 0;

  clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (s) {
    uint8_t is_ip4 = is_peer ? s->rmt_is_ip4 : s->lcl_is_ip4;
    uint8_t *ip = is_peer ? s->rmt_ip : s->lcl_ip;
    uint16_t port = is_peer ? s->rmt_port : s->lcl_port;

    if (is_ip4 && *len >= (socklen_t) sizeof (struct sockaddr_in)) {
      struct sockaddr_in a;
      memset (&a, 0, sizeof (a));
      a.sin_family = AF_INET;
      memcpy (&a.sin_addr, ip, 4);
      a.sin_port = port;
      memcpy (addr, &a, sizeof (a));
      *len = sizeof (a);
    } else if (*len >= (socklen_t) sizeof (struct sockaddr_in6)) {
      struct sockaddr_in6 a6;
      memset (&a6, 0, sizeof (a6));
      a6.sin6_family = AF_INET6;
      memcpy (&a6.sin6_addr, ip, 16);
      a6.sin6_port = port;
      memcpy (addr, &a6, sizeof (a6));
      *len = sizeof (a6);
    }
  } else {
    ret = -1;
    errno = EBADF;
  }
  clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
  return ret;
}

int getsockname (int fd, struct sockaddr *addr, socklen_t *len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd))
    return ldp2_fill_name (fd, addr, len, 0);
  return libc_getsockname (fd, addr, len);
}

int getpeername (int fd, struct sockaddr *addr, socklen_t *len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd))
    return ldp2_fill_name (fd, addr, len, 1);
  return libc_getpeername (fd, addr, len);
}

/* ---------- poll / select / pselect ----------
 * iperf3 等用 pselect/poll 等连接就绪（而非 epoll）。对含 vcl2 fd 的集合：
 * 先 dispatch（把 ACCEPTED 入 listener accept_q），再按 fifo/accept_q 状态判就绪；
 * 无就绪则阻塞在 eventfd 上（带真 fd 一起 poll），醒来重查。
 */

/* 把 vcl2 fd 的就绪位填进 pollfd->revents。要求调用者持有 sessions 读锁；s 非 NULL。 */
static int ldp2_poll_mark (vcl2_session_t *s, struct pollfd *pfd) {
  uint32_t want = 0, ev;
  pfd->revents = 0;
  if (pfd->events & POLLIN)
    want |= EPOLLIN;
  if (pfd->events & POLLOUT)
    want |= EPOLLOUT;
  ev = ldp2_session_ready (s, want);
  if (ev & EPOLLIN)
    pfd->revents |= POLLIN;
  if (ev & EPOLLOUT)
    pfd->revents |= POLLOUT;
  return pfd->revents != 0;
}

int poll (struct pollfd *fds, nfds_t nfds, int timeout) {
  int efd, has_vcl2 = 0, n, t;
  nfds_t i, nr;
  struct pollfd *rp = 0;
  int *map = 0;
  long deadline_ms = -1;

  ldp2_init_check ();
  /* 快判是否含 vcl2 候选（fd>=base）；精确性由 get 在锁内确认 */
  for (i = 0; i < nfds; i++) {
    fds[i].revents = 0;
    if (fds[i].fd >= (int) vcl2_main.fd_base)
      has_vcl2 = 1;
  }
  if (!has_vcl2)
    return libc_poll (fds, nfds, timeout);

  efd = ldp2_app_evt_fd ();
  vec_validate (rp, nfds + 1); /* 真 fd + 1 个 eventfd 槽 */
  vec_validate (map, nfds + 1);
  if (timeout > 0) {
    struct timespec t0;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 + timeout;
  }
  for (;;) {
    vcl2_dispatch_app_events ();
    n = 0;
    nr = 0;
    /* 读锁内：get 区分 vcl2 vs 真 fd；vcl2 标就绪，真 fd 收集到 rp/map */
    clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
    for (i = 0; i < nfds; i++) {
      vcl2_session_t *s = (fds[i].fd >= (int) vcl2_main.fd_base) ?
                            vcl2_session_get (vcl2_fd_to_handle (fds[i].fd)) :
                            0;
      if (s) {
        if (ldp2_poll_mark (s, &fds[i]))
          n++;
      } else {
        fds[i].revents = 0;
        if (fds[i].fd >= 0) {
          rp[nr] = fds[i];
          rp[nr].revents = 0;
          map[nr] = i;
          nr++;
        }
      }
    }
    clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
    if (n > 0 || timeout == 0) {
      /* vcl2 就绪（或 timeout==0）：顺带非阻塞查真 fd，一并上报 */
      if (nr)
        libc_poll (rp, nr, 0);
      for (i = 0; i < nr; i++) {
        fds[map[i]].revents = rp[i].revents;
        if (rp[i].revents)
          n++;
      }
      vec_free (rp);
      vec_free (map);
      return n;
    }
    /* 无就绪：把 eventfd 追加到 rp，与真 fd 一起【单次完整超时】阻塞 */
    if (efd < 0) {
      vec_free (rp);
      vec_free (map);
      errno = ENOSYS;
      return -1; /* 无 eventfd：不能事件驱动 */
    }
    rp[nr].fd = efd;
    rp[nr].events = POLLIN;
    rp[nr].revents = 0;
    map[nr] = -1; /* 标记 eventfd 槽 */
    if (deadline_ms < 0)
      t = -1;
    else {
      struct timespec now;
      long now_ms;
      clock_gettime (CLOCK_MONOTONIC, &now);
      now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
      if (now_ms >= deadline_ms) {
        vec_free (rp);
        vec_free (map);
        return 0;
      }
      t = (int) (deadline_ms - now_ms);
    }
    libc_poll (rp, nr + 1, t);
    if (rp[nr].revents & POLLIN) {
      uint64_t b;
      read (efd, &b, sizeof (b)); /* 清 eventfd，下轮 drain+重扫 */
    }
    for (i = 0; i < nr; i++) {
      fds[map[i]].revents = rp[i].revents;
      if (rp[i].revents)
        n++;
    }
    if (n > 0) {
      vec_free (rp);
      vec_free (map);
      return n; /* 真 fd 就绪 */
    }
    /* 否则 eventfd 唤醒（或超时）：循环重新 drain+重扫 vcl2 */
  }
}

/* select：翻译成对每个 fd 查就绪。vcl2 fd 用 ldp2_session_ready，真 fd 用 libc。 */
int select (int nfds, fd_set *rset, fd_set *wset, fd_set *eset,
            struct timeval *timeout) {
  fd_set orset, owset;
  int has_vcl2 = 0, n, fd, efd, wait_ms, sel_nfds;
  int timeout_ms;
  long deadline_ms = -1;
  struct timeval tv0 = {0, 0};

  ldp2_init_check ();
  /* 快判是否含 vcl2 候选 */
  for (fd = 0; fd < nfds; fd++) {
    int wr = rset && FD_ISSET (fd, rset);
    int ww = wset && FD_ISSET (fd, wset);
    if ((wr || ww) && fd >= (int) vcl2_main.fd_base)
      has_vcl2 = 1;
  }
  if (!has_vcl2)
    return libc_select (nfds, rset, wset, eset, timeout);

  timeout_ms =
    timeout ? (timeout->tv_sec * 1000 + timeout->tv_usec / 1000) : -1;
  efd = ldp2_app_evt_fd ();
  if (timeout_ms > 0) {
    struct timespec t0;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 + timeout_ms;
  }
  for (;;) {
    fd_set tr, tw;
    int maxr = -1;
    vcl2_dispatch_app_events ();
    FD_ZERO (&orset);
    FD_ZERO (&owset);
    FD_ZERO (&tr);
    FD_ZERO (&tw);
    n = 0;
    /* 读锁内：get 区分 vcl2 vs 真 fd；vcl2 查就绪，真 fd 收集到 tr/tw */
    clib_rwlock_reader_lock (&vcl2_main.sessions_lock);
    for (fd = 0; fd < nfds; fd++) {
      int wr = rset && FD_ISSET (fd, rset);
      int ww = wset && FD_ISSET (fd, wset);
      if (!wr && !ww)
        continue;
      vcl2_session_t *s = (fd >= (int) vcl2_main.fd_base) ?
                            vcl2_session_get (vcl2_fd_to_handle (fd)) :
                            0;
      if (s) {
        uint32_t want = 0;
        if (wr)
          want |= EPOLLIN;
        if (ww)
          want |= EPOLLOUT;
        uint32_t ev = ldp2_session_ready (s, want);
        if ((ev & EPOLLIN) && rset) {
          FD_SET (fd, &orset);
          n++;
        }
        if ((ev & EPOLLOUT) && wset) {
          FD_SET (fd, &owset);
          n++;
        }
      } else {
        if (wr)
          FD_SET (fd, &tr);
        if (ww)
          FD_SET (fd, &tw);
        if (fd > maxr)
          maxr = fd;
      }
    }
    clib_rwlock_reader_unlock (&vcl2_main.sessions_lock);
    if (maxr >= 0) {
      int rn = libc_select (maxr + 1, &tr, &tw, 0, &tv0); /* 非阻塞查真 fd */
      (void) rn;
      for (fd = 0; fd <= maxr; fd++) {
        if (FD_ISSET (fd, &tr)) {
          FD_SET (fd, &orset);
          n++;
        }
        if (FD_ISSET (fd, &tw)) {
          FD_SET (fd, &owset);
          n++;
        }
      }
    }
    if (n > 0 || (timeout && timeout->tv_sec == 0 && timeout->tv_usec == 0)) {
      if (rset)
        *rset = orset;
      if (wset)
        *wset = owset;
      if (eset)
        FD_ZERO (eset);
      return n;
    }
    /* 阻塞：把 eventfd 加入 tr，与真 fd 一起【单次完整超时】select */
    if (efd < 0) {
      if (rset)
        *rset = orset;
      if (wset)
        *wset = owset;
      if (eset)
        FD_ZERO (eset);
      errno = ENOSYS;
      return -1; /* 无 eventfd：不能事件驱动 */
    }
    sel_nfds = maxr + 1;
    if (efd >= sel_nfds)
      sel_nfds = efd + 1;
    FD_SET (efd, &tr); /* eventfd 加入读集 */
    if (deadline_ms < 0)
      wait_ms = -1;
    else {
      struct timespec now;
      long now_ms;
      clock_gettime (CLOCK_MONOTONIC, &now);
      now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
      if (now_ms >= deadline_ms) {
        if (rset)
          *rset = orset;
        if (wset)
          *wset = owset;
        if (eset)
          FD_ZERO (eset);
        return 0;
      }
      wait_ms = (int) (deadline_ms - now_ms);
    }
    {
      struct timeval wtv;
      wtv.tv_sec = wait_ms / 1000;
      wtv.tv_usec = (wait_ms % 1000) * 1000;
      libc_select (sel_nfds, &tr, &tw, 0, wait_ms >= 0 ? &wtv : 0);
    }
    if (FD_ISSET (efd, &tr)) {
      uint64_t b;
      read (efd, &b, sizeof (b)); /* 清 eventfd，下轮 drain+重扫 */
      FD_CLR (efd, &tr);
    }
    /* 真 fd 就绪：合并进 orset/owset 并返回 */
    for (fd = 0; fd <= maxr; fd++) {
      if (FD_ISSET (fd, &tr)) {
        FD_SET (fd, &orset);
        n++;
      }
      if (FD_ISSET (fd, &tw)) {
        FD_SET (fd, &owset);
        n++;
      }
    }
    if (n > 0) {
      if (rset)
        *rset = orset;
      if (wset)
        *wset = owset;
      if (eset)
        FD_ZERO (eset);
      return n;
    }
    /* 否则 eventfd 唤醒（或超时）：循环重新 drain+重扫 */
  }
}

int pselect (int nfds, fd_set *rset, fd_set *wset, fd_set *eset,
             const struct timespec *timeout, const sigset_t *sigmask) {
  (void) sigmask; /* P4：暂忽略 sigmask */
  struct timeval tv;
  if (timeout) {
    tv.tv_sec = timeout->tv_sec;
    tv.tv_usec = timeout->tv_nsec / 1000;
    return select (nfds, rset, wset, eset, &tv);
  }
  return select (nfds, rset, wset, eset, 0);
}

/* ==================== sendto / recvfrom / sendmsg / recvmsg ====================
 * TCP vcl2 fd 上等价于 send/recv/writev/readv（addr=NULL 的 connected socket）。
 * 非 vcl2 fd 透传 libc。 */

ssize_t sendto (int fd, const void *buf, size_t n, int flags,
                const struct sockaddr *addr, socklen_t addr_len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    (void) flags;
    (void) addr; /* TCP connected: addr 应为 NULL */
    (void) addr_len;
    return vcl2_session_send (vcl2_fd_to_handle (fd), buf, n);
  }
  return libc_sendto (fd, buf, n, flags, addr, addr_len);
}

ssize_t recvfrom (int fd, void *buf, size_t n, int flags,
                  struct sockaddr *addr, socklen_t *addr_len) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    ssize_t rv;
    (void) flags;
    rv = vcl2_session_recv (vcl2_fd_to_handle (fd), buf, n);
    if (rv >= 0 && addr && addr_len)
      ldp2_fill_name (fd, addr, addr_len, 1); /* 回填 peer 地址 */
    return rv;
  }
  return libc_recvfrom (fd, buf, n, flags, addr, addr_len);
}

ssize_t sendmsg (int fd, const struct msghdr *msg, int flags) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    if (PREDICT_FALSE (!msg)) {
      errno = EFAULT;
      return -1;
    }
    (void) flags;
    return writev (fd, msg->msg_iov, msg->msg_iovlen);
  }
  return libc_sendmsg (fd, msg, flags);
}

ssize_t recvmsg (int fd, struct msghdr *msg, int flags) {
  ldp2_init_check ();
  if (ldp2_fd_is_vcl2 (fd)) {
    if (PREDICT_FALSE (!msg)) {
      errno = EFAULT;
      return -1;
    }
    (void) flags;
    return readv (fd, msg->msg_iov, msg->msg_iovlen);
  }
  return libc_recvmsg (fd, msg, flags);
}
