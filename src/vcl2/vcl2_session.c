/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 session 操作（控制面 + 数据面）。
 * 指针稳定性：sessions_lock（rwlock）保护 sessions vec（alloc 可能 realloc），
 * get/alloc 假定调用者持锁；阻塞等待前必须解锁。锁序 sessions_lock→segment_table_lock。
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h> /* htons */

#include <vppinfra/clib_error.h>
#include <vnet/session/application_interface.h> /* app_alloc/send_ctrl_evt, *_msg_t */
#include <vnet/session/session_types.h> /* session_event_t, SESSION_CTRL_EVT_* */

#include "vcl2_private.h"

/* 阻塞 recv/send 的事件驱动等待兜底超时（秒）。事件驱动下几乎不触发——VPP 在 RX 入队
 * / tx 空间释放时 signal eventfd，timedwait 会被即时唤醒；此值只是防"信号丢失"的
 * 周期重查。不用短分片（0.1s 轮询）。*/
#define VCL2_BLOCK_TIMEOUT 60.0
/* 控制面（connect/listen/accept）等回复的总超时（秒）——正常回复经 eventfd 即时到达。*/
#define VCL2_CTRL_TIMEOUT 5.0

/* ---------- 可丢弃 session 缓存（假定调用者持有 sessions_lock）---------- */

/* 要求：调用者持有 sessions 【写】锁。分配/复用一个槽，登记 hash。 */
vcl2_session_t *vcl2_session_alloc (vcl2_handle_t h) {
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  u32 i;

  for (i = 0; i < vec_len (vm->sessions); i++)
    if (!vm->sessions[i].in_use) {
      s = &vm->sessions[i];
      memset (s, 0, sizeof (*s));
      goto found;
    }
  vec_add2 (vm->sessions, s, 1); /* 第三参数是【个数】，必须为 1 */
  memset (s, 0, sizeof (*s));
found:
  s->handle = h;
  s->in_use = 1;
  hash_set (vm->handle_to_session, (uword) h, s - vm->sessions);
  return s;
}

/* 要求：调用者持有 sessions 读锁（或写锁）。指针仅在锁持有期间有效。 */
vcl2_session_t *vcl2_session_get (vcl2_handle_t h) {
  vcl2_main_t *vm = &vcl2_main;
  uword *p = hash_get (vm->handle_to_session, (uword) h);
  if (!p)
    return 0;
  vcl2_session_t *s = vec_elt_at_index (vm->sessions, p[0]);
  return s->in_use ? s : 0;
}

/* 要求：调用者持有 sessions 读锁（或写锁）。 */
vcl2_session_t *vcl2_session_get_by_vpp_handle (u64 vpp_handle) {
  vcl2_main_t *vm = &vcl2_main;
  u32 i;
  for (i = 0; i < vec_len (vm->sessions); i++)
    if (vm->sessions[i].in_use && vm->sessions[i].vpp_handle == vpp_handle)
      return &vm->sessions[i];
  return 0;
}

static vcl2_handle_t vcl2_next_handle;

/* socket() 路径：预留一个 handle 槽（尚未 connect）。写锁内 ++handle + alloc。 */
int vcl2_session_create (vcl2_proto_t proto, uint8_t is_nonblocking) {
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  vcl2_handle_t h;
  (void) proto;
  (void) is_nonblocking;
  clib_rwlock_writer_lock (&vm->sessions_lock);
  h = ++vcl2_next_handle; /* 写锁独占，自增安全 */
  s = vcl2_session_alloc (h);
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  if (!s)
    return -ENOMEM;
  return (int) h;
}

/* fifo 提取公共核心（CONNECTED/ACCEPTED 共用）。要求：调用者持有 sessions 写锁
 * （因 mutate session 字段，且首附 vpp_evt_q）。内部 segment 操作取 segment 读锁。 */
int vcl2_session_attach_fifos (vcl2_session_t *s, u64 vpp_handle, u64 seg,
                               uword rxf_off, uword txf_off, uword vpp_eq_off,
                               u32 mq_index) {
  vcl2_main_t *vm = &vcl2_main;

  s->vpp_handle = vpp_handle;
  s->vpp_session_index = session_index_from_handle (vpp_handle);

  s->rx_fifo = vcl2_segment_alloc_fifo (seg, rxf_off);
  s->tx_fifo = vcl2_segment_alloc_fifo (seg, txf_off);
  if (PREDICT_FALSE (!s->rx_fifo || !s->tx_fifo)) {
    VCL2_DBG ("fifo map failed rx=%p tx=%p (seg=0x%llx)", s->rx_fifo,
              s->tx_fifo, (unsigned long long) seg);
    return -EINVAL;
  }
  if (PREDICT_FALSE (!vm->vpp_evt_q)) {
    if (vcl2_segment_attach_mq (VCL2_VPP_WRK_SEG_HANDLE (0), vpp_eq_off,
                                mq_index, &vm->vpp_evt_q)) {
      VCL2_DBG ("vpp_evt_q attach failed");
      return -EINVAL;
    }
  }
  s->rx_fifo->vpp_session_index = s->vpp_session_index;
  s->tx_fifo->vpp_session_index = s->vpp_session_index;
  s->rx_fifo->segment_index = vcl2_segment_lookup (seg);
  s->tx_fifo->segment_index = s->rx_fifo->segment_index;
  s->rx_fifo->signals = &s->rx_fifo->shr->signals;
  s->tx_fifo->signals = &s->tx_fifo->shr->signals;
  return 0;
}

static int vcl2_session_attach_connected (vcl2_session_t *s,
                                          session_connected_msg_t *cm) {
  int rv = vcl2_session_attach_fifos (
    s, cm->handle, cm->segment_handle, cm->server_rx_fifo, cm->server_tx_fifo,
    cm->vpp_event_queue_address, cm->mq_index);
  if (rv)
    return rv;
  VCL2_DBG ("session attached handle=%u vpp=0x%llx", s->handle,
            (unsigned long long) s->vpp_handle);
  return 0;
}

/*
 * connect：发 CONNECT，阻塞等 CONNECTED。等待时不持锁；每轮重取。
 */
int vcl2_session_connect (vcl2_handle_t h, uint8_t is_ip4, const uint8_t *ip,
                          uint16_t port) {
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_connect_msg_t *mp;
  int i, et;

  if (PREDICT_FALSE (!vm->ctrl_mq || !vm->app_event_queue))
    return -ENOTCONN;
  if (!VCL2_HANDLE_IS_VALID (h))
    return -EINVAL;

  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_CONNECT);
  mp = (session_connect_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = is_ip4;
  mp->proto = TRANSPORT_PROTO_TCP;
  mp->port = htons (port);
  if (ip) {
    /* ipv4 存在 ip46 offset 12（pad[3] 之后），必须用 set_ip4 */
    if (is_ip4)
      ip46_address_set_ip4 (&mp->ip, (const ip4_address_t *) ip);
    else
      clib_memcpy_fast (&mp->ip, ip, 16);
  }
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 CONNECTED：dispatch 统一处理所有事件类型（含 CONNECTED，置 ctrl_done），
   * 本循环只检测标志——不再内联 drain 丢弃其它事件。deadline 限定 + 1ms 有界重查。*/
  {
    struct timespec t0, now;
    long deadline_ms, now_ms;
    double rem;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 +
                  (long) (VCL2_CTRL_TIMEOUT * 1000);
    for (;;) {
      uint8_t done;
      int rv;

      clib_rwlock_reader_lock (&vm->sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      done = s ? s->ctrl_done : 0;
      rv = s ? s->ctrl_rv : -EINVAL;
      clib_rwlock_reader_unlock (&vm->sessions_lock);

      if (done) {
        /* 清标志（防复用；connect 每 session 一次，清掉无副作用）*/
        clib_rwlock_writer_lock (&vm->sessions_lock);
        s = vcl2_session_get (h);
        if (s)
          s->ctrl_done = 0;
        clib_rwlock_writer_unlock (&vm->sessions_lock);
        return rv;
      }
      clock_gettime (CLOCK_MONOTONIC, &now);
      now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
      if (now_ms >= deadline_ms)
        return -ETIMEDOUT;
      rem = (double) (deadline_ms - now_ms) / 1000.0;
      if (rem > 0.001)
        rem = 0.001; /* cap 1ms 有界重查 */
      vcl2_mq_wait_dispatch (rem);
    }
  }
}

/*
 * send：写 tx_fifo + 通知 VPP。持读锁做 enqueue（指针稳定）；无空间则解锁、arm ntf、
 * 等（不持锁），下一轮重取。
 */
int vcl2_session_send (vcl2_handle_t h, const void *buf, uint32_t len) {
  vcl2_main_t *vm = &vcl2_main;
  int n;

  if (!len)
    return 0;

  for (;;) {
    int have_space;

    clib_rwlock_reader_lock (&vm->sessions_lock);
    vcl2_session_t *s = vcl2_session_get (h);

    if (!s) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -EINVAL;
    }
    if (s->wr_shutdown || !s->tx_fifo) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -EPIPE;
    }
    if (PREDICT_FALSE (!vm->vpp_evt_q)) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -ENOTCONN;
    }

    have_space = svm_fifo_max_enqueue_prod (s->tx_fifo) >= (int) len;
    if (s->nonblocking && !have_space) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -EAGAIN;
    }

    if (have_space) {
      n = app_send_stream_raw (s->tx_fifo, vm->vpp_evt_q, (u8 *) buf, len,
                               SESSION_IO_EVT_TX, 1, 0);
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return n < 0 ? -EAGAIN : n;
    }

    /* 无空间：arm ntf，解锁后等 */
    svm_fifo_add_want_deq_ntf (s->tx_fifo, SVM_FIFO_WANT_DEQ_NOTIF);
    clib_rwlock_reader_unlock (&vm->sessions_lock);
    vcl2_mq_wait_dispatch (0.001);
  }
  return -EAGAIN; /* not reached */
}

/*
 * recv：读 rx_fifo。持读锁 dequeue；空则解锁、等（不持锁），下一轮重取。
 */
int vcl2_session_recv (vcl2_handle_t h, void *buf, uint32_t len) {
  vcl2_main_t *vm = &vcl2_main;
  int n;

  for (;;) {
    uint8_t pc, nb;

    clib_rwlock_reader_lock (&vm->sessions_lock);
    vcl2_session_t *s = vcl2_session_get (h);

    if (!s) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -EINVAL;
    }
    if (s->rd_shutdown) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return 0;
    }
    /* rx_fifo NULL = DISCONNECTED 置 NULL（防 VPP 已清零 fifo 的 crash）*/
    if (PREDICT_FALSE (!s->rx_fifo)) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return s->peer_closed ? 0 : -EINVAL;
    }

    n = app_recv_stream_raw (s->rx_fifo, (u8 *) buf, len, 1, 0);
    pc = s->peer_closed;
    nb = s->nonblocking;
    clib_rwlock_reader_unlock (&vm->sessions_lock);

    if (n > 0)
      return n;
    if (pc)
      return 0;
    if (nb)
      return -EAGAIN;

    /* 阻塞：1ms 有界重查 + 排空 */
    vcl2_mq_wait_dispatch (0.001);
  }
  return -ETIMEDOUT; /* not reached */
}

/*
 * close：写锁内发控制消息（UNLISTEN/DISCONNECT）+ 移除缓存条目。控制消息发送非阻塞。
 */
int vcl2_session_close (vcl2_handle_t h) {
  vcl2_main_t *vm = &vcl2_main;
  u64 *child_disc = NULL; /* accept_q 子 session 的 vpp_handle，锁外发 DISCONNECT */

  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s) {
    clib_rwlock_writer_unlock (&vm->sessions_lock);
    return -EINVAL;
  }
  VCL2_DBG ("close handle=%u vpp=0x%llx%s", s->handle,
            (unsigned long long) s->vpp_handle,
            s->is_listener ? " (listener)" : "");

  uint8_t is_listener = s->is_listener;
  uint64_t vpp_handle = s->vpp_handle;

  /* listener：清理 accept_q 里未取走的子 session（已 accept 的不在此列） */
  if (is_listener) {
    u32 i;
    for (i = 0; i < vec_len (s->accept_q); i++) {
      vcl2_session_t *cs = vcl2_session_get (s->accept_q[i]);
      if (cs && cs->vpp_handle) {
        vec_add1 (child_disc, cs->vpp_handle);
        hash_unset (vm->handle_to_session, (uword) s->accept_q[i]);
        cs->in_use = 0;
        cs->rx_fifo = cs->tx_fifo = 0;
        cs->vpp_handle = 0;
      }
    }
  }

  hash_unset (vm->handle_to_session, (uword) h);
  vec_free (s->accept_q);
  s->in_use = 0;
  s->rx_fifo = s->tx_fifo = 0;
  s->is_listener = 0;
  s->vpp_handle = 0;
  clib_rwlock_writer_unlock (&vm->sessions_lock);

  /* 锁外发 ctrl（避免 send 阻塞持写锁） */
  if (is_listener && vm->ctrl_mq) {
    app_session_evt_t ae;
    session_unlisten_msg_t *mp;
    app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &ae, SESSION_CTRL_EVT_UNLISTEN);
    mp = (session_unlisten_msg_t *) ae.evt->data;
    memset (mp, 0, sizeof (*mp));
    mp->client_index = vm->api_client_handle;
    mp->context = h;
    mp->wrk_index = vm->app_wrk_index;
    mp->handle = vpp_handle;
    app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &ae);
  } else if (!is_listener && vpp_handle && vm->vpp_evt_q) {
    app_session_evt_t ae;
    session_disconnect_msg_t *mp;
    app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae, SESSION_CTRL_EVT_DISCONNECT);
    mp = (session_disconnect_msg_t *) ae.evt->data;
    memset (mp, 0, sizeof (*mp));
    mp->client_index = vm->api_client_handle;
    mp->handle = vpp_handle;
    app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
    VCL2_DBG ("DISCONNECT sent handle=%u vpp=0x%llx", h,
              (unsigned long long) vpp_handle);
  }

  /* listener 关闭时，给 accept_q 里未取走的子 session 各发 DISCONNECT */
  if (child_disc) {
    u64 *vh;
    vec_foreach (vh, child_disc) {
      if (vm->vpp_evt_q) {
        app_session_evt_t ae;
        session_disconnect_msg_t *mp;
        app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
                                   SESSION_CTRL_EVT_DISCONNECT);
        mp = (session_disconnect_msg_t *) ae.evt->data;
        memset (mp, 0, sizeof (*mp));
        mp->client_index = vm->api_client_handle;
        mp->handle = *vh;
        app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      }
    }
    vec_free (child_disc);
  }

  return 0;
}

/*
 * shutdown：写锁内设标志 + 发 SHUTDOWN（SHUT_WR/RDWR）。SHUT_RD 纯本地。
 */
int vcl2_session_shutdown (vcl2_handle_t h, int how) {
  vcl2_main_t *vm = &vcl2_main;
  uint64_t vpp_handle;
  uint8_t send_shutdown;

  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s || s->is_listener) {
    clib_rwlock_writer_unlock (&vm->sessions_lock);
    return -EINVAL;
  }
  if (how == SHUT_RD || how == SHUT_RDWR)
    s->rd_shutdown = 1;
  send_shutdown = (how == SHUT_WR || how == SHUT_RDWR);
  if (send_shutdown)
    s->wr_shutdown = 1;
  vpp_handle = s->vpp_handle;
  clib_rwlock_writer_unlock (&vm->sessions_lock);

  if (send_shutdown && vpp_handle && vm->vpp_evt_q) {
    app_session_evt_t ae;
    session_shutdown_msg_t *mp;
    app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae, SESSION_CTRL_EVT_SHUTDOWN);
    mp = (session_shutdown_msg_t *) ae.evt->data;
    memset (mp, 0, sizeof (*mp));
    mp->client_index = vm->api_client_handle;
    mp->handle = vpp_handle;
    app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
    VCL2_DBG ("SHUTDOWN sent handle=%u vpp=0x%llx how=%d", h,
              (unsigned long long) vpp_handle, how);
  }
  return 0;
}

/*
 * unlisten：写锁内发 UNLISTEN（atfork_parent 用）。close 内 listener 路径已内联发 UNLISTEN。
 */
int vcl2_session_unlisten (vcl2_handle_t h) {
  vcl2_main_t *vm = &vcl2_main;
  uint8_t is_listener;
  uint64_t vpp_handle;

  /* 锁内取字段，解锁后再发 ctrl（避免 send 阻塞持写锁）*/
  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  is_listener = s && s->is_listener;
  vpp_handle = s ? s->vpp_handle : 0;
  clib_rwlock_writer_unlock (&vm->sessions_lock);

  if (is_listener && vm->ctrl_mq) {
    app_session_evt_t ae;
    session_unlisten_msg_t *mp;
    app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &ae, SESSION_CTRL_EVT_UNLISTEN);
    mp = (session_unlisten_msg_t *) ae.evt->data;
    memset (mp, 0, sizeof (*mp));
    mp->client_index = vm->api_client_handle;
    mp->context = h;
    mp->wrk_index = vm->app_wrk_index;
    mp->handle = vpp_handle;
    app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &ae);
    return 0;
  }
  return -EINVAL;
}

/*
 * listen：发 LISTEN，阻塞等 BOUND。BOUND 成功后写锁内记 vpp_handle+is_listener。
 */
int vcl2_session_listen (vcl2_handle_t h, uint32_t q_len) {
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_listen_msg_t *mp;
  int i;
  (void) q_len;

  if (PREDICT_FALSE (!vm->ctrl_mq || !vm->app_event_queue))
    return -ENOTCONN;

  /* 取 lcl 地址（读锁）填 LISTEN 消息 */
  uint8_t lcl_is_ip4;
  uint8_t lcl_ip[16];
  uint16_t lcl_port;
  clib_rwlock_reader_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s) {
    clib_rwlock_reader_unlock (&vm->sessions_lock);
    return -EINVAL;
  }
  lcl_is_ip4 = s->lcl_is_ip4;
  clib_memcpy_fast (lcl_ip, s->lcl_ip, 16);
  lcl_port = s->lcl_port;
  clib_rwlock_reader_unlock (&vm->sessions_lock);

  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_LISTEN);
  mp = (session_listen_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = lcl_is_ip4;
  mp->port = htons (lcl_port);
  mp->proto = TRANSPORT_PROTO_TCP;
  if (lcl_is_ip4) {
    ip4_address_t ip4;
    memcpy (&ip4, lcl_ip, 4);
    ip46_address_set_ip4 (&mp->ip, &ip4);
  } else
    clib_memcpy_fast (&mp->ip, lcl_ip, 16);
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 BOUND：dispatch 统一处理（含 BOUND，置 ctrl_done + listener 身份），
   * 本循环只检测标志。deadline 限定 + 1ms 有界重查。*/
  {
    struct timespec t0, now;
    long deadline_ms, now_ms;
    double rem;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 +
                  (long) (VCL2_CTRL_TIMEOUT * 1000);
    for (;;) {
      uint8_t done;
      int rv;

      clib_rwlock_reader_lock (&vm->sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      done = s ? s->ctrl_done : 0;
      rv = s ? s->ctrl_rv : -EINVAL;
      clib_rwlock_reader_unlock (&vm->sessions_lock);

      if (done) {
        clib_rwlock_writer_lock (&vm->sessions_lock);
        s = vcl2_session_get (h);
        if (s)
          s->ctrl_done = 0;
        clib_rwlock_writer_unlock (&vm->sessions_lock);
        return rv;
      }
      clock_gettime (CLOCK_MONOTONIC, &now);
      now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
      if (now_ms >= deadline_ms)
        return -ETIMEDOUT;
      rem = (double) (deadline_ms - now_ms) / 1000.0;
      if (rem > 0.001)
        rem = 0.001; /* cap 1ms 有界重查 */
      vcl2_mq_wait_dispatch (rem);
    }
  }
}

/*
 * accept：取 listener accept_q 一个子 session。读锁内查 accept_q + 回 ACCEPTED_REPLY
 * （accept_q pop 改为写锁）；无则解锁、dispatch（内部加锁）、等。
 */
int vcl2_session_accept (vcl2_handle_t listener_h, vcl2_handle_t *out) {
  vcl2_main_t *vm = &vcl2_main;
  int i;

  if (!out)
    return -EINVAL;
  /* 先确认是 listener（读锁） */
  clib_rwlock_reader_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (listener_h);
  uint8_t is_listener = s && s->is_listener;
  clib_rwlock_reader_unlock (&vm->sessions_lock);
  if (!is_listener)
    return -EINVAL;

  for (;;) {
    vcl2_handle_t ch = 0;
    int got = 0, have_cs = 0;
    uint32_t accept_context = 0, cs_vpp_session_index = 0;
    uint64_t cs_vpp_handle = 0;
    clib_rwlock_writer_lock (
      &vm->sessions_lock); /* pop accept_q，取子 session 字段 */
    s = vcl2_session_get (listener_h);
    if (s && vec_len (s->accept_q) > 0) {
      ch = s->accept_q[0];
      vec_delete (s->accept_q, 1, 0);
      vcl2_session_t *cs = vcl2_session_get (ch);
      if (cs) {
        accept_context = cs->accept_context;
        cs_vpp_handle = cs->vpp_handle;
        cs_vpp_session_index = cs->vpp_session_index;
        have_cs = 1;
      }
      got = 1;
    }
    clib_rwlock_writer_unlock (&vm->sessions_lock);
    if (got) {
      /* 解锁后回 ACCEPTED_REPLY（避免 send 阻塞持写锁）*/
      if (have_cs && vm->vpp_evt_q) {
        app_session_evt_t ae;
        session_accepted_reply_msg_t *rm;
        app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
                                   SESSION_CTRL_EVT_ACCEPTED_REPLY);
        rm = (session_accepted_reply_msg_t *) ae.evt->data;
        rm->context = accept_context;
        rm->retval = 0;
        rm->handle = cs_vpp_handle;
        rm->app_session_index = cs_vpp_session_index;
        app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      }
      *out = ch;
      return 0;
    }
    /* 等 ACCEPTED：1ms 有界重查 + 排空（app_mq_lock 串行）。dispatch 入 accept_q，
       * 下一轮 pop。*/
    vcl2_mq_wait_dispatch (0.001);
  }
  return -EAGAIN; /* not reached */
}

/* mq 排空与分发。timedwait + drain + 就地处理在 app_mq_lock 内（串行化 wait+drain，
 * 避免锁外并发 poll 丢唤醒）。锁序单向 app_mq_lock→sessions_lock（drain 内取 W），无环。
 * session_event_t 是头+data[]柔性数组，payload 在 mq buffer 内，必须 free_msg 前就地读。 */
void vcl2_mq_wait_dispatch (double timeout_s) {
  vcl2_main_t *vm = &vcl2_main;
  svm_msg_q_t *mq = vm->app_event_queue;
  svm_msg_q_msg_t msg;

  if (!mq)
    return;
  /* timedwait 在锁外：仅 poll eventfd。worker 空闲 recv 循环若持锁等 1ms，会占据
   * app_mq_lock 饿死主线程 select 的 dispatch（iperf end-of-test 等 ctrl 事件处理不了
   * →client 挂；gdb 暂停 worker 即解套，证明确是 liveness/饿死）。锁外等待让主线程能
   * 拿到锁做 dispatch。eventfd 已 O_NONBLOCK，锁外 timedwait 内部 read 不阻塞。*/
  if (timeout_s > 0)
    svm_msg_q_timedwait (mq, timeout_s);
  pthread_mutex_lock (&vm->app_mq_lock);
  while (!svm_msg_q_sub (mq, &msg, SVM_Q_NOWAIT, 0)) {
    session_event_t *e = svm_msg_q_msg_data (mq, &msg);
    if (e->event_type == SESSION_CTRL_EVT_APP_ADD_SEGMENT) {
      session_app_add_segment_msg_t *sm =
        (session_app_add_segment_msg_t *) e->data;
      int fd = -1;
      if (sm->fd_flags)
        vcl2_sapi_recv_fd (&fd);
      int rv =
        vcl2_segment_attach (sm->segment_handle, (char *) sm->segment_name, fd);
      VCL2_DBG ("ADD_SEGMENT handle=0x%llx fd=%d -> %d",
                (unsigned long long) sm->segment_handle, fd, rv);
    } else if (e->event_type == SESSION_CTRL_EVT_DISCONNECTED) {
      session_disconnected_msg_t *dm = (session_disconnected_msg_t *) e->data;
      clib_rwlock_writer_lock (&vm->sessions_lock);
      vcl2_session_t *ds = vcl2_session_get_by_vpp_handle (dm->handle);
      if (ds) {
        ds->peer_closed = 1;
        /* 【关键·crash 根因修复】peer 断开后 VPP 会释放/清零 session 的 fifo
	       * （end_chunk→0）。在写锁内【先于 DISCONNECTED_REPLY】把 rx/tx fifo 指针
	       * 置 NULL，使后续 recv/send 的 !fifo 检查命中、直接返回 EOF/EPIPE，绝不
	       * 解引用已被 VPP 清零的 fifo（f_chunk_end(c=0x0) SIGSEGV）。写锁排斥所有
	       * 正在 recv/send 的读锁持有者，故无"中途解引用"。代价：丢弃 fifo 里残留
	       * 的尾部数据（peer 已断，可接受）。*/
        ds->rx_fifo = 0;
        ds->tx_fifo = 0;
        VCL2_DBG (
          "DISCONNECTED handle=%u vpp=0x%llx (peer_closed, fifos nulled)",
          ds->handle, (unsigned long long) dm->handle);
      }
      clib_rwlock_writer_unlock (&vm->sessions_lock);
      if (vm->vpp_evt_q) {
        app_session_evt_t ae;
        session_disconnected_reply_msg_t *rm;
        app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
                                   SESSION_CTRL_EVT_DISCONNECTED_REPLY);
        rm = (session_disconnected_reply_msg_t *) ae.evt->data;
        rm->context = vm->api_client_handle;
        rm->retval = 0;
        rm->handle = dm->handle;
        app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      }
    } else if (e->event_type == SESSION_CTRL_EVT_ACCEPTED) {
      session_accepted_msg_t *am = (session_accepted_msg_t *) e->data;
      clib_rwlock_writer_lock (&vm->sessions_lock);
      vcl2_session_t *ls = vcl2_session_get_by_vpp_handle (am->listener_handle);
      if (ls) {
        vcl2_handle_t lh = ls->handle;
        vcl2_handle_t nh = ++vcl2_next_handle;
        vcl2_session_t *ns = vcl2_session_alloc (nh);
        if (ns) {
          ns->accept_context = am->context;
          ns->rmt_is_ip4 = am->rmt.is_ip4;
          if (am->rmt.is_ip4)
            memcpy (ns->rmt_ip, &am->rmt.ip.ip4, 4);
          else
            memcpy (ns->rmt_ip, &am->rmt.ip, 16);
          ns->rmt_port = am->rmt.port;
          if (!vcl2_session_attach_fifos (
                ns, am->handle, am->segment_handle, am->server_rx_fifo,
                am->server_tx_fifo, am->vpp_event_queue_address,
                am->mq_index)) {
            vcl2_session_t *ls2 =
              vcl2_session_get (lh); /* 重取（vec 可能已移动） */
            if (ls2)
              vec_add1 (ls2->accept_q, nh);
            VCL2_DBG ("child=%u attached, queued to listener=%u", nh, lh);
          } else
            VCL2_DBG ("accept fifo attach failed");
        }
      } else
        VCL2_DBG ("ACCEPTED no listener vpp=0x%llx",
                  (unsigned long long) am->listener_handle);
      clib_rwlock_writer_unlock (&vm->sessions_lock);
    } else if (e->event_type == SESSION_CTRL_EVT_CONNECTED) {
      /* connect 回复：context = vcl2 handle（connect 请求所设）。attach fifos + 置
	   * ctrl_done，让 vcl2_session_connect 的等待循环检测到并返回。*/
      session_connected_msg_t *cm = (session_connected_msg_t *) e->data;
      clib_rwlock_writer_lock (&vm->sessions_lock);
      vcl2_session_t *cs = vcl2_session_get (cm->context);
      if (cs) {
        if (cm->retval)
          cs->ctrl_rv = (int) cm->retval;
        else
          cs->ctrl_rv = vcl2_session_attach_connected (cs, cm);
        cs->ctrl_done = 1;
        VCL2_DBG ("CONNECTED handle=%u rv=%d", cs->handle, cs->ctrl_rv);
      }
      clib_rwlock_writer_unlock (&vm->sessions_lock);
    } else if (e->event_type == SESSION_CTRL_EVT_BOUND) {
      /* listen 回复：context = vcl2 handle。置 listener 身份 + ctrl_done。*/
      session_bound_msg_t *bm = (session_bound_msg_t *) e->data;
      clib_rwlock_writer_lock (&vm->sessions_lock);
      vcl2_session_t *bs = vcl2_session_get (bm->context);
      if (bs) {
        if (bm->retval)
          bs->ctrl_rv = (int) bm->retval;
        else {
          bs->vpp_handle = bm->handle;
          bs->is_listener = 1;
          bs->ctrl_rv = 0;
        }
        bs->ctrl_done = 1;
        VCL2_DBG ("BOUND handle=%u rv=%d", bs->handle, bs->ctrl_rv);
      }
      clib_rwlock_writer_unlock (&vm->sessions_lock);
    }
    svm_msg_q_free_msg (mq, &msg);
  }
  pthread_mutex_unlock (&vm->app_mq_lock);
}

/* 非阻塞排空 + 分发（ldp2 select/poll/epoll 唤醒后用）。无等待。*/
void vcl2_dispatch_app_events (void) {
  vcl2_mq_wait_dispatch (0);
}
