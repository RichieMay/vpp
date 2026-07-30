/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 核心入口：vcl2_init 读配置，vcl2_app_attach 经 SAPI 完成 app+worker-0 注册。
 * 单侧所有权：进程死靠 SAPI close + VPP 回收，无 app 侧 teardown。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>

#include <vppinfra/clib_error.h>
#include <vppinfra/socket.h>
#include <vppinfra/mem.h>

#include <vnet/session/application_interface.h>

#include "vcl2.h"
#include "vcl2_private.h"

vcl2_main_t vcl2_main;

/* 调试开关（env VCL2_DEBUG=1 打开，默认关）。供 VCL2_DBG/LDP2_DBG 门控。*/
int vcl2_debug = 0;

/* ---------- 合成 fd <-> handle 编码（LDP2 用，可丢弃缓存 key） ---------- */
int vcl2_handle_to_fd (vcl2_handle_t h) {
  if (!VCL2_HANDLE_IS_VALID (h))
    return -EINVAL;
  return (int) vcl2_main.fd_base + (int) h;
}

vcl2_handle_t vcl2_fd_to_handle (int fd) {
  if (fd < (int) vcl2_main.fd_base)
    return VCL2_INVALID_HANDLE;
  return (vcl2_handle_t) (fd - (int) vcl2_main.fd_base);
}

int vcl2_is_init (void) {
  return vcl2_main.is_init;
}

/* ---------- SAPI UDS 连接（SEQPACKET client，复用 clib_socket） ---------- */
int vcl2_sapi_connect (void) {
  vcl2_main_t *vm = &vcl2_main;
  clib_socket_t *cs = &vm->sapi_sock;
  clib_error_t *err;

  if (vm->sapi_connected)
    return 0;

  memset (cs, 0, sizeof (*cs));
  cs->config = vm->sapi_socket_path;
  cs->flags =
    CLIB_SOCKET_F_IS_CLIENT | CLIB_SOCKET_F_SEQPACKET | CLIB_SOCKET_F_BLOCKING;

  err = clib_socket_init (cs);
  if (err) {
    clib_error_free (err);
    return -ECONNREFUSED;
  }
  /* SAPI socket 设 CLOEXEC（plan 要求）：app exec() 时不泄漏给子进程。
   * clib_socket_init 不设 CLOEXEC，这里补。atfork_child 关闭继承的副本并重开（也走本函数）。*/
  if (cs->fd >= 0)
    fcntl (cs->fd, F_SETFD, FD_CLOEXEC);
  vm->sapi_connected = 1;
  VCL2_DBG ("sapi connected: %s", vm->sapi_socket_path);
  return 0;
}

/* ---------- P1：app attach（复用 app_sapi_msg_t 协议） ----------
 * 镜像 src/vcl/vcl_sapi.c:vcl_api_send_attach 的 options 设置（已验证正确）。
 */
int vcl2_app_attach_locked (void) {
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t msg, rmp;
  app_sapi_attach_msg_t *mp = &msg.attach;
  app_sapi_attach_reply_msg_t *rp = &rmp.attach_reply;
  clib_error_t *err;
  int fds[8] = {0};
  int n = 0, rv;

  if ((rv = vcl2_sapi_connect ()))
    return rv;

  memset (&msg, 0, sizeof (msg));
  strncpy ((char *) mp->name, vm->app_name ? vm->app_name : "vcl2_app",
           sizeof (mp->name) - 1);

  /* options[APP_OPTIONS_FLAGS]：accept-redirect + add-segment + (eventfd?)。
   * ADD_SEGMENT：VPP 为 accept 的连接另开 memfd 段时，经 APP_ADD_SEGMENT 事件
   * （app_event_queue）+ SAPI socket SCM_RIGHTS 把段 fd 推给 app。vcl2_dispatch_app_events
   * 处理该事件时 recvmsg 取 fd 并 vcl2_segment_attach。必需，否则 accept 的 fifo 段无法映射。 */
  mp->options[APP_OPTIONS_FLAGS] =
    APP_OPTIONS_FLAGS_ACCEPT_REDIRECT | APP_OPTIONS_FLAGS_ADD_SEGMENT |
    (vm->use_mq_eventfd ? APP_OPTIONS_FLAGS_EVT_MQ_USE_EVENTFD : 0);
  mp->options[APP_OPTIONS_SEGMENT_SIZE] = vm->segment_size;
  mp->options[APP_OPTIONS_ADD_SEGMENT_SIZE] = vm->segment_size;
  mp->options[APP_OPTIONS_RX_FIFO_SIZE] = vm->rx_fifo_size;
  mp->options[APP_OPTIONS_TX_FIFO_SIZE] = vm->tx_fifo_size;
  mp->options[APP_OPTIONS_EVT_QUEUE_SIZE] = vm->evt_queue_size;

  msg.type = APP_SAPI_MSG_TYPE_ATTACH;
  pthread_mutex_lock (&vm->sapi_lock);
  err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
  if (err) {
    pthread_mutex_unlock (&vm->sapi_lock);
    clib_error_free (err);
    return -EIO;
  }

  memset (&rmp, 0, sizeof (rmp));
  err = clib_socket_recvmsg (&vm->sapi_sock, &rmp, sizeof (rmp), fds,
                             ARRAY_LEN (fds));
  pthread_mutex_unlock (&vm->sapi_lock);
  if (err) {
    clib_error_free (err);
    return -EIO;
  }
  if (PREDICT_FALSE (rmp.type != APP_SAPI_MSG_TYPE_ATTACH_REPLY)) {
    VCL2_DBG ("attach: unexpected reply type %d", (int) rmp.type);
    return -EPROTO;
  }
  if (PREDICT_FALSE (rp->retval)) {
    VCL2_DBG ("attach failed: retval %d", rp->retval);
    return -EINVAL;
  }

  vm->app_index = rp->app_index;
  vm->api_client_handle = rp->api_client_handle;
  vm->segment_handle = rp->segment_handle;

  /* 处理 VPP 返回的段 fd + mq 地址（镜像 vcl_api_attach_reply_handler） */
  if (rp->n_fds == 0) {
    VCL2_DBG ("attach: reply carried no fds");
    return -ENODATA;
  }
  if (rp->fd_flags & SESSION_FD_F_VPP_MQ_SEGMENT)
    if (vcl2_segment_attach (VCL2_VPP_WRK_SEG_HANDLE (0), "vpp-mq-seg",
                             fds[n++]))
      return -EINVAL;
  if (rp->fd_flags & SESSION_FD_F_MEMFD_SEGMENT) {
    char name[40];
    snprintf (name, sizeof (name), "memfd-%lu",
              (unsigned long) rp->segment_handle);
    if (vcl2_segment_attach (rp->segment_handle, name, fds[n++]))
      return -EINVAL;
  }
  /* app 事件 mq：VPP 投递事件给 app（app 轮询，不拥有） */
  if (vcl2_segment_attach_mq (rp->segment_handle, rp->app_mq, 0,
                              &vm->app_event_queue))
    return -EINVAL;
  if (rp->fd_flags & SESSION_FD_F_MQ_EVENTFD) {
    svm_msg_q_set_eventfd (vm->app_event_queue, fds[n]);
    /* eventfd 置非阻塞（对齐 VCL vcl_private.c:65）：否则多线程下主线程 select 的
       * read(efd) 与 worker svm_msg_q_timedwait 的 read(evtfd) 竞争同一计数，一方取走
       * 后另一方阻塞 read 永久挂起。svm_msg_q_timedwait 的 errno!=EAGAIN 分支专为非阻塞。*/
    fcntl (fds[n], F_SETFL, O_NONBLOCK);
    n++;
  }
  /* VPP 控制 mq：session listen/connect 等请求经它 */
  if (vcl2_segment_attach_mq (VCL2_VPP_WRK_SEG_HANDLE (0), rp->vpp_ctrl_mq,
                              rp->vpp_ctrl_mq_thread, &vm->ctrl_mq))
    return -EINVAL;

  VCL2_DBG ("attached: app_index=%u seg=%lu app_eq=%p ctrl_mq=%p",
            vm->app_index, (unsigned long) vm->segment_handle,
            vm->app_event_queue, vm->ctrl_mq);

  /* 复用 attach 隐式创建的 worker 0（不显式 ADD_DEL_WORKER，否则旧 worker 不被回收→泄漏）。
   * 仅 fork 子进程经 atfork_child 显式注册新 worker。 */
  vm->app_wrk_index = 0;
  {
    static int atfork_done;
    if (!atfork_done) {
      pthread_atfork (vcl2_atfork_prepare, vcl2_atfork_parent,
                      vcl2_atfork_child);
      atfork_done = 1;
    }
  }
  return 0;
}

/* ---------- P1：worker 注册（fork 子进程经 atfork child handler 也调它） ---------- */
int vcl2_worker_register_locked (void) {
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t msg, rmp;
  app_sapi_worker_add_del_msg_t *wp = &msg.worker_add_del;
  app_sapi_worker_add_del_reply_msg_t *rp = &rmp.worker_add_del_reply;
  clib_error_t *err;
  int fds[8] = {0};
  int n = 0, i;

  if (PREDICT_FALSE (!vm->sapi_connected))
    return -ENOTCONN;

  memset (&msg, 0, sizeof (msg));
  wp->app_index = vm->app_index;
  wp->wrk_index = vm->app_wrk_index; /* 0 = 新 worker */
  wp->is_add = 1;

  msg.type = APP_SAPI_MSG_TYPE_ADD_DEL_WORKER;
  pthread_mutex_lock (&vm->sapi_lock);
  err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
  if (err) {
    pthread_mutex_unlock (&vm->sapi_lock);
    clib_error_free (err);
    return -EIO;
  }

  memset (&rmp, 0, sizeof (rmp));
  err = clib_socket_recvmsg (&vm->sapi_sock, &rmp, sizeof (rmp), fds,
                             ARRAY_LEN (fds));
  pthread_mutex_unlock (&vm->sapi_lock);
  if (err) {
    clib_error_free (err);
    return -EIO;
  }
  if (PREDICT_FALSE (rmp.type != APP_SAPI_MSG_TYPE_ADD_DEL_WORKER_REPLY)) {
    VCL2_DBG ("worker-add: unexpected reply type %d", (int) rmp.type);
    return -EPROTO;
  }
  if (PREDICT_FALSE (rp->retval)) {
    VCL2_DBG ("worker-add failed: retval %d", rp->retval);
    return -EINVAL;
  }
  if (PREDICT_FALSE (!rp->is_add)) {
    VCL2_DBG ("worker-add: reply is not an add");
    return -EINVAL;
  }

  vm->app_wrk_index = rp->wrk_index;
  vm->api_client_handle = rp->api_client_handle;
  /* ctrl_mq 复用 attach 级别的（与 vcl 一致：worker reply 不重置 ctrl_mq） */

  if (rp->segment_handle == VCL2_INVALID_SEGMENT_HANDLE) {
    VCL2_DBG ("worker-add: invalid segment handle");
    return -EINVAL;
  }
  if (PREDICT_FALSE (!rp->n_fds)) {
    VCL2_DBG ("worker-add: reply carried no fds");
    return -ENODATA;
  }

  /* 镜像 vcl_api_add_del_worker_reply_handler：处理 worker 专属段 fd，
   * 并把 app_event_queue 重新指向【本 worker 的】memfd 段内的队列
   * （attach 级别那条是 worker-0 之前的临时指向，必须被本 worker 的覆盖，
   *  否则 VPP 的 CONNECTED 等事件会投到本 worker 的队列、而我们仍在轮询旧队列）。 */
  if (rp->fd_flags & SESSION_FD_F_VPP_MQ_SEGMENT)
    if (vcl2_segment_attach (VCL2_VPP_WRK_SEG_HANDLE (vm->app_wrk_index),
                             "vpp-worker-seg", fds[n++]))
      goto failed;
  if (rp->fd_flags & SESSION_FD_F_MEMFD_SEGMENT) {
    char name[40];
    snprintf (name, sizeof (name), "memfd-wrk-%lu",
              (unsigned long) rp->segment_handle);
    if (vcl2_segment_attach (rp->segment_handle, name, fds[n++]))
      goto failed;
  }
  /* 关键：app_event_queue 重新指向本 worker 段内的队列 */
  if (vcl2_segment_attach_mq (rp->segment_handle, rp->app_event_queue_address,
                              0, &vm->app_event_queue))
    goto failed;
  if (rp->fd_flags & SESSION_FD_F_MQ_EVENTFD) {
    svm_msg_q_set_eventfd (vm->app_event_queue, fds[n]);
    fcntl (fds[n], F_SETFL,
           O_NONBLOCK); /* 同 attach：eventfd 非阻塞（对齐 VCL）*/
    n++;
  }

  VCL2_DBG ("worker registered: wrk_index=%u client=%u seg=%lu app_eq=%p "
            "(n_fds=%u, used=%d)",
            vm->app_wrk_index, vm->api_client_handle,
            (unsigned long) rp->segment_handle, vm->app_event_queue, rp->n_fds,
            n);
  return 0;

failed:
  for (i = clib_max (n - 1, 0); i < rp->n_fds; i++)
    close (fds[i]);
  return -EINVAL;
}

/* cert/key 注册：经 SAPI 发 ADD_DEL_CERT_KEY，返回 VPP 分配的 ckpair_index（≥0）或负 errno。
 * 镜像 vcl_sapi_add_cert_key_pair：header sendmsg + cert|key blob sendmsg + recvmsg。
 * sapi_lock 包住全部三次（vcl2 共享单一 sapi_sock，区别于 VCL per-worker）。 */
int vcl2_tls_add_cert_key_pair (const char *cert, uint32_t cert_len,
                                const char *key, uint32_t key_len) {
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t msg, rmp;
  app_sapi_cert_key_add_del_msg_t *mp = &msg.cert_key_add_del;
  app_sapi_cert_key_add_del_reply_msg_t *rp = &rmp.cert_key_add_del_reply;
  clib_error_t *err;
  u8 *blob = 0;
  uint32_t cklen = cert_len + key_len;
  int rv = -EIO;

  if (PREDICT_FALSE (!vm->sapi_connected))
    return -ENOTCONN;

  memset (&msg, 0, sizeof (msg));
  mp->context = vm->app_wrk_index;
  mp->cert_len = cert_len;
  mp->certkey_len = cklen;
  mp->is_add = 1;
  msg.type = APP_SAPI_MSG_TYPE_ADD_DEL_CERT_KEY;

  vec_validate (blob, cklen - 1);
  clib_memcpy_fast (blob, cert, cert_len);
  clib_memcpy_fast (blob + cert_len, key, key_len);

  pthread_mutex_lock (&vm->sapi_lock);
  err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
  if (err) {
    clib_error_free (err);
    goto done;
  }
  err = clib_socket_sendmsg (&vm->sapi_sock, blob, cklen, 0, 0);
  if (err) {
    clib_error_free (err);
    goto done;
  }
  memset (&rmp, 0, sizeof (rmp));
  err = clib_socket_recvmsg (&vm->sapi_sock, &rmp, sizeof (rmp), 0, 0);
  if (err) {
    clib_error_free (err);
    goto done;
  }
  pthread_mutex_unlock (&vm->sapi_lock);

  vec_free (blob);
  if (PREDICT_FALSE (rmp.type != APP_SAPI_MSG_TYPE_ADD_DEL_CERT_KEY_REPLY)) {
    VCL2_DBG ("cert_key: bad reply type %d", (int) rmp.type);
    return -EPROTO;
  }
  if (PREDICT_FALSE (rp->retval)) {
    VCL2_DBG ("cert_key add failed retval=%d", (int) rp->retval);
    return -EINVAL;
  }
  VCL2_DBG ("cert_key registered index=%u", rp->index);
  return (int) rp->index;

done:
  pthread_mutex_unlock (&vm->sapi_lock);
  vec_free (blob);
  return rv;
}

/* 懒加载：首次 TLS connect/listen 前调用。读 cert/key 文件 + 注册 + 缓存 index。
 * ckpair 是 app 级（非 worker 级），fork 子进程继承 app_index 故 index 仍有效，无需重注。 */
int vcl2_tls_ensure_cert (void) {
  vcl2_main_t *vm = &vcl2_main;
  char cert[8192], key[8192];
  int cert_len, key_len, idx;
  FILE *fp;

  if (vm->tls_cert_loaded)
    return 0;
  if (!vm->tls_cert_file || !vm->tls_key_file) {
    VCL2_DBG ("tls: cert/key file env not set");
    return -ENOENT;
  }

  fp = fopen (vm->tls_cert_file, "r");
  if (!fp) {
    VCL2_DBG ("tls: cannot open cert %s", vm->tls_cert_file);
    return -ENOENT;
  }
  cert_len = (int) fread (cert, 1, sizeof (cert), fp);
  fclose (fp);

  fp = fopen (vm->tls_key_file, "r");
  if (!fp) {
    VCL2_DBG ("tls: cannot open key %s", vm->tls_key_file);
    return -ENOENT;
  }
  key_len = (int) fread (key, 1, sizeof (key), fp);
  fclose (fp);

  idx = vcl2_tls_add_cert_key_pair (cert, (uint32_t) cert_len, key,
                                    (uint32_t) key_len);
  if (idx < 0)
    return idx;
  vm->tls_ckpair_index = (uint32_t) idx;
  vm->tls_cert_loaded = 1;
  return 0;
}

/* fork child handler：子进程继承 sessions 缓存（含 listener）但需重建 SAPI 身份——
 * 开自己的 SAPI、注册为新 worker，拿到自己的 app_event_queue。在 fork() 返回前跑完。 */
void vcl2_atfork_child (void) {
  vcl2_main_t *vm = &vcl2_main;
  int rv;

  if (PREDICT_FALSE (!vm->is_init))
    return;

  /* 释放 prepare 持有的锁（逆序）——child 继承了锁状态，必须先释放才能后续操作 */
  clib_rwlock_writer_unlock (&vm->segment_table_lock);
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  pthread_mutex_unlock (&vm->sapi_lock);
  pthread_mutex_unlock (&vm->app_mq_lock);

  vm->pid = getpid ();

  /* 关掉继承来的父进程 SAPI 连接（子进程的 fd 副本），开自己的 */
  if (vm->sapi_connected) {
    clib_socket_close (&vm->sapi_sock);
    vm->sapi_connected = 0;
  }

  /* 重置 per-worker 状态（ctrl_mq 是 app 级，保留；app_index/config/segments/
   * sessions 缓存保留——子进程复用继承的 listener）。 */
  vm->app_event_queue = 0;
  vm->vpp_evt_q = 0;
  vm->app_wrk_index = 0;
  vm->api_client_handle = 0;

  if ((rv = vcl2_sapi_connect ())) {
    VCL2_DBG ("atfork child: sapi reconnect failed: %d", rv);
    return;
  }
  if ((rv = vcl2_worker_register_locked ())) {
    VCL2_DBG ("atfork child: worker register failed: %d", rv);
    return;
  }
  VCL2_DBG ("atfork child re-registered: pid=%d wrk=%u client=%u",
            (int) vm->pid, vm->app_wrk_index, vm->api_client_handle);

  /* 关键：子进程是【新】worker，不在任何 listener 的 accept 轮转位图里（al->workers）。
   * VPP 经 app_worker_start_listen 把 worker 加入位图——条件是该 worker 调过 listen。
   * nginx worker 只继承 listener、不会再次 listen，故这里替它对每个继承来的 listener
   * 重发 LISTEN（同 ip/port 已绑定，VPP 仅把本 worker 加入 al->workers，不重新 bind）。 */
  {
    u32 i;
    for (i = 0; i < vec_len (vm->sessions); i++) {
      vcl2_session_t *s = &vm->sessions[i];
      if (s->in_use && s->is_listener) {
        int lrv = vcl2_session_listen (s->handle, 0);
        VCL2_DBG ("atfork child: re-listen handle=%u -> %d", s->handle, lrv);
      }
    }
  }
}

/* fork prepare：acquire 所有锁，使 child 继承干净（未锁）状态。
 * POSIX fork 只复制调用线程——若有其他线程持锁，child 永久死锁。
 * prepare 在 fork 前跑（父进程），acquire → parent/child 各自 release。
 * 锁序：app_mq_lock → sapi_lock → sessions_lock → segment_table_lock */
void vcl2_atfork_prepare (void) {
  vcl2_main_t *vm = &vcl2_main;
  if (PREDICT_FALSE (!vm->is_init))
    return;
  pthread_mutex_lock (&vm->app_mq_lock);
  pthread_mutex_lock (&vm->sapi_lock);
  clib_rwlock_writer_lock (&vm->sessions_lock);
  clib_rwlock_writer_lock (&vm->segment_table_lock);
}

/* fork 后在父进程跑：master 从所有 listener 的 accept 轮转中摘除自己（unlisten），
 * 否则 VPP 会把 ACCEPTED 轮给不排空 event queue 的 master → 卡死。
 * child handler 与 parent handler 分别在各自进程中跑，无先后保证。 */
void vcl2_atfork_parent (void) {
  vcl2_main_t *vm = &vcl2_main;
  vcl2_handle_t *listeners = NULL;
  u32 i;

  if (PREDICT_FALSE (!vm->is_init))
    return;

  /* 释放 prepare 持有的锁（逆序） */
  clib_rwlock_writer_unlock (&vm->segment_table_lock);
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  pthread_mutex_unlock (&vm->sapi_lock);
  pthread_mutex_unlock (&vm->app_mq_lock);

  /* 读锁内收集 listener handle（防 vec realloc），锁外逐个 unlisten */
  clib_rwlock_reader_lock (&vm->sessions_lock);
  for (i = 0; i < vec_len (vm->sessions); i++)
    if (vm->sessions[i].in_use && vm->sessions[i].is_listener)
      vec_add1 (listeners, vm->sessions[i].handle);
  clib_rwlock_reader_unlock (&vm->sessions_lock);

  vec_foreach_index (i, listeners)
    vcl2_session_unlisten (listeners[i]);
  vec_free (listeners);
}

/* recvmsg 一个 fd（SCM_RIGHTS）从 SAPI socket。VPP 在投 APP_ADD_SEGMENT 事件到
 * app_event_queue 之前，先经 SAPI socket sendmsg 把段 fd 推过来（空 app_sapi_msg_t +
 * 附属 fd）。处理 ADD_SEGMENT 事件时调本函数取 fd。 */
int vcl2_sapi_recv_fd (int *out_fd) {
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t dummy;
  clib_error_t *err;
  int fds[1] = {0};

  if (PREDICT_FALSE (!vm->sapi_connected))
    return -ENOTCONN;
  pthread_mutex_lock (&vm->sapi_lock);
  err = clib_socket_recvmsg (&vm->sapi_sock, &dummy, sizeof (dummy), fds,
                             ARRAY_LEN (fds));
  pthread_mutex_unlock (&vm->sapi_lock);
  if (err) {
    clib_error_free (err);
    return -EIO;
  }
  *out_fd = fds[0];
  return 0;
}

/* ---------- 公共 API ---------- */

/*
 * vppinfra 工作堆。注意：这不是"需要跨进程协调 teardown 的数据面资源"
 * （sessions/fifos/segments 才是 —— 那些由 VPP 拥有）。这只是 clib 基础设施
 * （clib_socket / vec / hash 等）所需的进程本地内存，进程死即由内核回收，
 * 单侧、无协调。与单侧所有权原则不冲突。
 */
#define VCL2_HEAP_SIZE (64 << 20)
static void *vcl2_heap_base;

static int vcl2_heap_alloc (void) {
  void *mem, *heap;
  mem = mmap (0, VCL2_HEAP_SIZE, PROT_READ | PROT_WRITE,
              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mem == MAP_FAILED)
    return -ENOMEM;
  heap = clib_mem_init (mem, VCL2_HEAP_SIZE);
  if (!heap) {
    munmap (mem, VCL2_HEAP_SIZE);
    vcl2_heap_base = 0;
    return -ENOMEM;
  }
  vcl2_heap_base = mem;
  return 0;
}

/* 解析 "size" env：支持 123 / 4K / 16M / 2G（1024 进制）。空/非法 → 返回 def。
 * 用于让 rx/tx fifo、segment、evt_queue 可按负载调（iperf 大 fifo，nginx 多连接小 fifo）。*/
static u64
vcl2_cfg_size (const char *name, u64 def)
{
  const char *s = getenv (name);
  if (!s || !s[0])
    return def;
  char *end = NULL;
  unsigned long long v = strtoull (s, &end, 0);
  if (end == s) /* 无数字 */
    return def;
  if (end && *end)
    {
      switch (*end)
	{
	case 'k': case 'K': v <<= 10; break;
	case 'm': case 'M': v <<= 20; break;
	case 'g': case 'G': v <<= 30; break;
	default: break;
	}
    }
  return (u64) v;
}

int vcl2_init (const char *app_name) {
  vcl2_main_t *vm = &vcl2_main;
  const char *s;
  int rv;

  if (vm->is_init)
    return 0;

  /* 先建 vppinfra 工作堆（clib_socket / vec 等内部需要） */
  if ((rv = vcl2_heap_alloc ()))
    return rv;

  memset (vm, 0, sizeof (*vm));
  clib_rwlock_init (&vm->segment_table_lock);
  clib_rwlock_init (&vm->sessions_lock);
  pthread_mutex_init (&vm->app_mq_lock, NULL);
  pthread_mutex_init (&vm->sapi_lock, NULL);
  vm->fd_base = VCL2_FD_BASE_DEFAULT;
  vm->app_name = strdup (app_name ? app_name : "vcl2_app");
  vm->pid = getpid ();

  /* 调试开关：VCL2_DEBUG=1（或 LDP2_DEBUG=1）打开 VCL2_DBG/LDP2_DBG 输出，默认关 */
  s = getenv ("VCL2_DEBUG");
  if (!s || !s[0])
    s = getenv ("LDP2_DEBUG");
  vcl2_debug = (s && (s[0] == '1' || s[0] == 'y' || s[0] == 'Y')) ? 1 : 0;

  s = getenv ("VCL2_SAPI_SOCKET");
  vm->sapi_socket_path = strdup (s && s[0] ? s : VCL2_SAPI_SOCKET_DEFAULT);

  /* transparent_tls：VCL2_TRANSPARENT_TLS/LDP_TRANSPARENT_TLS=1 开启；
   * cert/key 文件接受 VCL2_ 与 LDP_ 两套名（drop-in 兼容 VCL）。cert 首次 TLS
   * connect/listen 时懒注册（vcl2_tls_ensure_cert）。*/
  s = getenv ("VCL2_TRANSPARENT_TLS");
  if (!s || !s[0])
    s = getenv ("LDP_TRANSPARENT_TLS");
  vm->tls_enabled = (s && (s[0] == '1' || s[0] == 'y' || s[0] == 'Y')) ? 1 : 0;
  if (vm->tls_enabled) {
    s = getenv ("VCL2_TLS_CERT_FILE");
    if (!s || !s[0])
      s = getenv ("LDP_TLS_CERT_FILE");
    vm->tls_cert_file = s && s[0] ? strdup (s) : NULL;
    s = getenv ("VCL2_TLS_KEY_FILE");
    if (!s || !s[0])
      s = getenv ("LDP_TLS_KEY_FILE");
    vm->tls_key_file = s && s[0] ? strdup (s) : NULL;
  }
  vm->tls_ckpair_index = ~0;
  vm->unlisten_ctx = ~0;

  /* 配置：默认适合高吞吐单/少流（iperf）；nginx 等多连接负载用 env 调小 fifo。
   * VCL2_RX_FIFO_SIZE / VCL2_TX_FIFO_SIZE / VCL2_SEGMENT_SIZE / VCL2_EVT_QUEUE_SIZE */
  vm->rx_fifo_size = vcl2_cfg_size ("VCL2_RX_FIFO_SIZE", VCL2_RX_FIFO_SIZE_DEFAULT);
  vm->tx_fifo_size = vcl2_cfg_size ("VCL2_TX_FIFO_SIZE", VCL2_TX_FIFO_SIZE_DEFAULT);
  vm->segment_size = vcl2_cfg_size ("VCL2_SEGMENT_SIZE", VCL2_SEGMENT_SIZE_DEFAULT);
  vm->evt_queue_size = vcl2_cfg_size ("VCL2_EVT_QUEUE_SIZE", VCL2_EVT_QUEUE_SIZE_DEFAULT);
  vm->use_mq_eventfd = 1;

  /* 段管理器初始化（VPP-owned 段经 fifo_segment_attach 接入） */
  fifo_segment_main_init (&vm->segment_main, (u64) ~0, 20 /* timeout s */);

  vm->is_init = 1;
  VCL2_DBG ("init: app=%s sapi=%s", vm->app_name, vm->sapi_socket_path);
  return 0;
}

int vcl2_app_attach (void) {
  int rv;
  if (!vcl2_main.is_init)
    return -EINVAL;
  /* attach 在 constructor（main 前）/ atfork child（子进程单线程）跑，不与数据面线程
   * 并发；其段操作内部已用 segment_table_lock 保护，故无需外层锁。*/
  rv = vcl2_app_attach_locked ();
  return rv;
}

int vcl2_worker_register (void) {
  int rv;
  if (!vcl2_main.is_init)
    return -EINVAL;
  rv = vcl2_worker_register_locked ();
  return rv;
}

void vcl2_destroy (void) {
  vcl2_main_t *vm = &vcl2_main;
  if (PREDICT_FALSE (!vm->is_init))
    return;

  /* 正常优雅 detach（app 显式调用时）：向 VPP 发 ADD_DEL_WORKER(is_add=0) 回收本 worker。
   * VPP 收到后经 worker barrier 回收该 worker 的全部 session/fifo/listener/segment。
   * 这是【正常操作】的控制消息；异常退出（kill -9/segfault）不会跑到这里，由 VPP 经
   * SAPI UDS close 单侧兜底回收（已 P6 验证）。两条路径都回收，互不依赖。*/
  if (vm->sapi_connected) {
    app_sapi_msg_t msg;
    app_sapi_worker_add_del_msg_t *mp = &msg.worker_add_del;
    clib_error_t *err;
    memset (&msg, 0, sizeof (msg));
    msg.type = APP_SAPI_MSG_TYPE_ADD_DEL_WORKER;
    mp->app_index = vm->app_index;
    mp->wrk_index = vm->app_wrk_index;
    mp->is_add = 0;
    pthread_mutex_lock (&vm->sapi_lock);
    err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
    pthread_mutex_unlock (&vm->sapi_lock);
    if (err)
      clib_error_free (err);
    clib_socket_close (&vm->sapi_sock);
    vm->sapi_connected = 0;
  }

  /* 丢弃 app 侧可丢弃缓存（进程本地；即便不 free，进程退出内核也会回收）*/
  vec_free (vm->segments);
  vec_free (vm->sessions);
  hash_free (vm->handle_to_session);
  hash_free (vm->vpp_handle_to_session);
  free (vm->app_name);
  free (vm->sapi_socket_path);
  clib_rwlock_free (&vm->segment_table_lock);
  clib_rwlock_free (&vm->sessions_lock);
  pthread_mutex_destroy (&vm->app_mq_lock);
  pthread_mutex_destroy (&vm->sapi_lock);
  memset (vm, 0, sizeof (*vm));
}

/* ---------- session 操作（vcl2_session_connect / create / send / recv / close /
 * listen / accept / event_poll_once / dispatch 见 vcl2_session.c）。 ---------- */
