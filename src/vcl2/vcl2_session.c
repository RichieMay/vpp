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
#include <sys/socket.h>
#include <netinet/in.h>
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
  uword *p = hash_get (vm->vpp_handle_to_session, (uword) vpp_handle);
  if (p) {
    vcl2_session_t *s = &vm->sessions[p[0]];
    if (s->in_use && s->vpp_handle == vpp_handle)
      return s;
  }
  return 0;
}

static vcl2_handle_t vcl2_next_handle;

/* socket() 路径：预留一个 handle 槽（尚未 connect）。写锁内 ++handle + alloc。 */
int vcl2_session_create (vcl2_proto_t proto, uint8_t is_nonblocking) {
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  vcl2_handle_t h;
  (void) is_nonblocking;
  clib_rwlock_writer_lock (&vm->sessions_lock);
  h = ++vcl2_next_handle;
  s = vcl2_session_alloc (h);
  if (s) {
    s->is_dgram = (proto == VCL2_PROTO_UDP) ? 1 : 0;
    s->is_tls = (proto == VCL2_PROTO_TLS) ? 1 : 0;
  }
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
  hash_set (vm->vpp_handle_to_session, (uword) vpp_handle,
            (uword) (s - vm->sessions));

  s->rx_fifo = vcl2_segment_alloc_fifo (seg, rxf_off);
  s->tx_fifo = vcl2_segment_alloc_fifo (seg, txf_off);
  if (PREDICT_FALSE (!s->rx_fifo || !s->tx_fifo)) {
    VCL2_DBG ("fifo map failed rx=%p tx=%p (seg=0x%llx)", s->rx_fifo,
              s->tx_fifo, (unsigned long long) seg);
    return -EINVAL;
  }
  /* 【多核修复】每个 session 绑定【自己的】RX mq：vpp_eq_off/mq_index 指向该
   * session 所属 VPP worker 线程的 mq（CONNECTED/ACCEPTED/BOUND 回复各带）。
   * send/DISCONNECT/SHUTDOWN/ACCEPTED_REPLY 等经它回 VPP，落在正确线程，
   * 否则 app_worker_add_event 断言 s->thread_index==cur 触发 panic。
   * vpp_eq_off/mq_index 此前只在首次回填了【全局】vm->vpp_evt_q（= 某一个线程），
   * 多核 RSS 下其余线程的 session 用它即错线程。 */
  if (vcl2_segment_attach_mq (VCL2_VPP_WRK_SEG_HANDLE (0), vpp_eq_off,
                              mq_index, &s->vpp_evt_q)) {
    VCL2_DBG ("per-session vpp_evt_q attach failed (seg=0x%llx off=%lu)",
              (unsigned long long) seg, (unsigned long) vpp_eq_off);
    s->vpp_evt_q = vm->vpp_evt_q; /* 回退全局（兼容单核/早期） */
  }
  /* 兼容：全局 vpp_evt_q 首次初始化（个别早期路径仍引用它作回退） */
  if (PREDICT_FALSE (!vm->vpp_evt_q))
    vm->vpp_evt_q = s->vpp_evt_q;
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

/* TLS ext_config：在 worker 段分配 chunk，写入 transport_endpt_ext_cfg_t
 * （type=CRYPTO，crypto.ckpair_index），回填 chunk 偏移。connect/listen 经 mp->ext_config
 * 传给 VPP，VPP 据此为本会话启用 TLS（终止）。镜像 VCL vcl_msg_add_ext_config。*/
static int vcl2_session_build_ext_config (uint32_t ckpair_index, uword *offset) {
  transport_endpt_ext_cfg_t ec;
  svm_fifo_chunk_t *c;

  memset (&ec, 0, sizeof (ec));
  ec.type = TRANSPORT_ENDPT_EXT_CFG_CRYPTO;
  ec.len = sizeof (ec);
  ec.crypto.ckpair_index = ckpair_index;
  if (vcl2_segment_alloc_chunk (VCL2_VPP_WRK_SEG_HANDLE (0), 0, ec.len, offset,
                                &c))
    return -1;
  clib_memcpy_fast (c->data, &ec, ec.len);
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
  uint8_t proto = TRANSPORT_PROTO_TCP, is_tls = 0;
  uword tls_ext_off = 0;

  if (PREDICT_FALSE (!vm->ctrl_mq || !vm->app_event_queue))
    return -ENOTCONN;
  if (!VCL2_HANDLE_IS_VALID (h))
    return -EINVAL;

  /* 写锁内：取 proto（is_dgram/is_tls 三选一）+ 记对端地址；UDP listener→connect 时
   * 先撤销 listener 身份 + 登记 UNLISTEN。*/
  uint8_t need_unlisten = 0;
  uint64_t unlisten_vh = 0;
  uint16_t lcl_port = 0;
  uint8_t lcl_ip[16];
  uint8_t lcl_is_ip4 = 0;
  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *tmp = vcl2_session_get (h);
  if (!tmp) {
    clib_rwlock_writer_unlock (&vm->sessions_lock);
    return -EINVAL;
  }
  proto = tmp->is_dgram ? TRANSPORT_PROTO_UDP :
                          (tmp->is_tls ? TRANSPORT_PROTO_TLS :
                                         TRANSPORT_PROTO_TCP);
  is_tls = tmp->is_tls;
  tmp->rmt_is_ip4 = is_ip4;
  tmp->rmt_port = htons (port);
  if (ip)
    memcpy (tmp->rmt_ip, ip, is_ip4 ? 4 : 16);
  /* 本地地址（CONNECT 的源地址 = mp->lcl_*；iperf3 -u 把已 bind 的 listener connect 给
   * 客户端时，连接会话必须保留原源端口 5201，否则 VPP 分配临时端口 → 客户端发 5201 的
   * 数据落不到该会话。未 bind 时为 0 → VPP 自选。镜像 VCL vcl_send_session_connect。*/
  lcl_port = tmp->lcl_port;
  lcl_is_ip4 = tmp->lcl_is_ip4;
  memcpy (lcl_ip, tmp->lcl_ip, 16);
  /* 连接无连接 UDP listener：先 UNLISTEN 释放端口（镜像 VCL vppcom_session_connect 的
   * LISTEN-state 分支）。iperf3 -u 服务端 iperf_udp_accept 先 connect(listener,peer) 再
   * netannounce 同端口新 listener；不先 UNLISTEN 旧 listener 则 VPP 端口仍占用 → 新 LISTEN
   * 返 PORTINUSE(-17) → 测试挂起。TCP listener connect 返 EINVAL（镜像 VCL）。*/
  if (tmp->is_listener) {
    if (!tmp->is_dgram) {
      clib_rwlock_writer_unlock (&vm->sessions_lock);
      return -EINVAL;
    }
    unlisten_vh = tmp->vpp_handle;
    if (unlisten_vh) {
      hash_unset (vm->vpp_handle_to_session, (uword) unlisten_vh);
      tmp->vpp_handle = 0;
    }
    tmp->is_listener = 0;
    need_unlisten = 1;
  }
  clib_rwlock_writer_unlock (&vm->sessions_lock);

  /* 锁外：先发 UNLISTEN（异步；依赖 ctrl_mq FIFO——先于 CONNECT 入队 → VPP 先处理 → 端口
   * 释放后 CONNECT 才建连，随后 iperf3 netannounce 的 LISTEN 才能绑同端口成功）。*/
  if (need_unlisten && unlisten_vh) {
    app_session_evt_t uae;
    session_unlisten_msg_t *ump;
    app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &uae, SESSION_CTRL_EVT_UNLISTEN);
    ump = (session_unlisten_msg_t *) uae.evt->data;
    memset (ump, 0, sizeof (*ump));
    ump->client_index = vm->api_client_handle;
    ump->context = h;
    ump->wrk_index = vm->app_wrk_index;
    ump->handle = unlisten_vh;
    app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &uae);
    VCL2_DBG ("connect: UNLISTEN old listener vpp=0x%llx (free port)",
              (unsigned long long) unlisten_vh);
  }

  /* TLS：确保 cert 已注册，挂 ext_config（crypto.ckpair_index）到 worker 段 chunk */
  if (is_tls) {
    int erv = vcl2_tls_ensure_cert ();
    if (erv)
      return erv;
    if (vcl2_session_build_ext_config (vm->tls_ckpair_index, &tls_ext_off))
      return -ENOMEM;
  }

  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_CONNECT);
  mp = (session_connect_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = is_ip4;
  mp->proto = proto;
  mp->port = htons (port);
  if (ip) {
    /* ipv4 存在 ip46 offset 12（pad[3] 之后），必须用 set_ip4 */
    if (is_ip4)
      ip46_address_set_ip4 (&mp->ip, (const ip4_address_t *) ip);
    else
      clib_memcpy_fast (&mp->ip, ip, 16);
  }
  /* 源地址：bind 过则保留（iperf3 -u 的 5201），否则 0 由 VPP 自选。镜像 VCL。*/
  mp->lcl_port = htons (lcl_port);
  if (lcl_is_ip4)
    ip46_address_set_ip4 (&mp->lcl_ip, (const ip4_address_t *) lcl_ip);
  else
    clib_memcpy_fast (&mp->lcl_ip, lcl_ip, 16);
  mp->flags |= TRANSPORT_CFG_F_CONNECTED;
  if (is_tls)
    mp->ext_config = tls_ext_off;
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 CONNECTED：dispatch 统一处理所有事件类型（含 CONNECTED，置 ctrl_done），
   * 本循环只检测标志——不再内联 drain 丢弃其它事件。deadline 限定 + 1ms 有界重查。*/
  {
    long deadline_ms, now_ms;
    double rem;
    deadline_ms = vcl2_now_ms () + (long) (VCL2_CTRL_TIMEOUT * 1000);
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
      now_ms = vcl2_now_ms ();
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
    int have_space, overhead;

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
    if (PREDICT_FALSE (!s->vpp_evt_q)) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -ENOTCONN;
    }

    overhead = s->is_dgram ? SESSION_CONN_HDR_LEN : 0;
    have_space = svm_fifo_max_enqueue_prod (s->tx_fifo) >= (int) len + overhead;
    if (s->nonblocking && !have_space) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return -EAGAIN;
    }

    if (have_space) {
      if (s->is_dgram) {
        /* dgram：dest = session rmt（sendto/connect 设），lcl = 本地。
         * 每个 dgram 带 dgram 头，VPP 据头里的 rmt 投递到对端。*/
        app_session_transport_t at;
        memset (&at, 0, sizeof (at));
        at.is_ip4 = s->rmt_is_ip4;
        at.rmt_port = s->rmt_port;
        if (s->rmt_is_ip4)
          memcpy (&at.rmt_ip.ip4, s->rmt_ip, 4);
        else
          memcpy (&at.rmt_ip.ip6, s->rmt_ip, 16);
        at.lcl_port = htons (s->lcl_port);
        if (s->lcl_is_ip4)
          memcpy (&at.lcl_ip.ip4, s->lcl_ip, 4);
        else
          memcpy (&at.lcl_ip.ip6, s->lcl_ip, 16);
        n = app_send_dgram_raw (s->tx_fifo, &at, s->vpp_evt_q, (u8 *) buf,
                                len, SESSION_IO_EVT_TX, 1, 0);
      } else
        n = app_send_stream_raw (s->tx_fifo, s->vpp_evt_q, (u8 *) buf, len,
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
 * recv：读 rx_fifo。语义同 recvfrom(addr=NULL) —— 合并实现，避免重复。
 */
int vcl2_session_recv (vcl2_handle_t h, void *buf, uint32_t len) {
  return vcl2_session_recvfrom (h, buf, len, NULL, NULL);
}

/* 回填 sockaddr（is_ip4 取 ip 前4字节，否则16字节；port 网络序）。
 * vcl2_session_recvfrom / ldp2 getsockname/getpeername/accept 共用，去重 3 处。*/
void vcl2_fill_sockaddr_from_ip (struct sockaddr *addr, socklen_t *addr_len,
                                 uint8_t is_ip4, const uint8_t *ip,
                                 uint16_t port) {
  if (is_ip4 && *addr_len >= sizeof (struct sockaddr_in)) {
    struct sockaddr_in a;
    memset (&a, 0, sizeof (a));
    a.sin_family = AF_INET;
    memcpy (&a.sin_addr, ip, 4);
    a.sin_port = port;
    memcpy (addr, &a, sizeof (a));
    *addr_len = sizeof (a);
  } else if (*addr_len >= sizeof (struct sockaddr_in6)) {
    struct sockaddr_in6 a6;
    memset (&a6, 0, sizeof (a6));
    a6.sin6_family = AF_INET6;
    memcpy (&a6.sin6_addr, ip, 16);
    a6.sin6_port = port;
    memcpy (addr, &a6, sizeof (a6));
    *addr_len = sizeof (a6);
  }
}

/* recvfrom：recv + 回填源地址。语义同 vcl2_session_recv（阻塞/非阻塞）。
 * TCP at = session 对端；UDP at = app_recv_dgram_raw 的 per-packet 源。*/
int vcl2_session_recvfrom (vcl2_handle_t h, void *buf, uint32_t len,
                           struct sockaddr *addr, socklen_t *addr_len) {
  vcl2_main_t *vm = &vcl2_main;
  int n;

  for (;;) {
    uint8_t pc, nb;
    app_session_transport_t at;

    memset (&at, 0, sizeof (at));
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
    if (PREDICT_FALSE (!s->rx_fifo)) {
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      return s->peer_closed ? 0 : -EINVAL;
    }

    if (s->is_dgram) {
      n = app_recv_dgram_raw (s->rx_fifo, (u8 *) buf, len, &at, 1, 0);
    } else {
      n = app_recv_stream_raw (s->rx_fifo, (u8 *) buf, len, 1, 0);
      /* 仅当调用者要回填源地址时才取对端（recv 路径 addr=NULL 不付此开销）*/
      if (addr && addr_len) {
        at.is_ip4 = s->rmt_is_ip4;
        at.rmt_port = s->rmt_port;
        if (s->rmt_is_ip4)
          memcpy (&at.rmt_ip.ip4, s->rmt_ip, 4);
        else
          memcpy (&at.rmt_ip.ip6, s->rmt_ip, 16);
      }
    }
    pc = s->peer_closed;
    nb = s->nonblocking;
    clib_rwlock_reader_unlock (&vm->sessions_lock);

    if (n > 0) {
      if (addr && addr_len)
        vcl2_fill_sockaddr_from_ip (addr, addr_len, at.is_ip4,
                                    (const uint8_t *) &at.rmt_ip, at.rmt_port);
      return n;
    }
    if (pc)
      return 0;
    if (nb)
      return -EAGAIN;

    /* 阻塞：1ms 有界重查 + 排空 */
    vcl2_mq_wait_dispatch (0.001);
  }
  return -ETIMEDOUT; /* not reached */
}

/* 发 DISCONNECT 到指定 evt_q（多核下各 session 经其所属线程 mq）。client_index 取
 * 全局；去重 close 内本 session + accept_q 未取走 child 两处消息构造。*/
static void vcl2_send_disconnect (svm_msg_q_t *mq, uint64_t vpp_handle) {
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t ae;
  session_disconnect_msg_t *mp;
  app_alloc_ctrl_evt_to_vpp (mq, &ae, SESSION_CTRL_EVT_DISCONNECT);
  mp = (session_disconnect_msg_t *) ae.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->handle = vpp_handle;
  app_send_ctrl_evt_to_vpp (mq, &ae);
}

/*
 * close：写锁内发控制消息（UNLISTEN/DISCONNECT）+ 移除缓存条目。控制消息发送非阻塞。
 */
int vcl2_session_close (vcl2_handle_t h) {
  vcl2_main_t *vm = &vcl2_main;
  /* 子 session 的 DISCONNECT 必须各经其【所属线程】的 evt_q 发回（多核 RSS），
   * 故连 evt_q 一起缓存，锁外逐个发送。 */
  struct { uint64_t vpp_handle; svm_msg_q_t *evt_q; } *child_disc = NULL,
                                                                 cd_ent;
  svm_msg_q_t *disc_mq = 0; /* 本 session 的 evt_q（非 listener DISCONNECT 用） */

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
  uint8_t peer_closed = s->peer_closed;
  disc_mq = s->vpp_evt_q ? s->vpp_evt_q : vm->vpp_evt_q;
  uint8_t disc_is_global = (disc_mq == vm->vpp_evt_q);

  /* listener：清理 accept_q 里未取走的子 session（已 accept 的不在此列） */
  if (is_listener) {
    u32 i;
    for (i = 0; i < vec_len (s->accept_q); i++) {
      vcl2_session_t *cs = vcl2_session_get (s->accept_q[i]);
      if (cs && cs->vpp_handle) {
        cd_ent.vpp_handle = cs->vpp_handle;
        cd_ent.evt_q = cs->vpp_evt_q ? cs->vpp_evt_q : vm->vpp_evt_q;
        vec_add1 (child_disc, cd_ent);
        hash_unset (vm->handle_to_session, (uword) s->accept_q[i]);
        hash_unset (vm->vpp_handle_to_session, (uword) cs->vpp_handle);
        cs->in_use = 0;
        cs->rx_fifo = cs->tx_fifo = 0;
        cs->vpp_evt_q = 0;
        cs->vpp_handle = 0;
      }
    }
  }

  hash_unset (vm->handle_to_session, (uword) h);
  if (vpp_handle)
    hash_unset (vm->vpp_handle_to_session, (uword) vpp_handle);
  vec_free (s->accept_q);
  s->in_use = 0;
  s->rx_fifo = s->tx_fifo = 0;
  s->vpp_evt_q = 0;
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

    /* 同步等 UNLISTEN_REPLY 再返回：镜像内核同步 close 语义——确保 VPP 完全拆除
     * 旧 listener 后，app 才能 rebind 同端口新 listener。否则 VPP 在 UNLISTEN 未
     * 完成时处理新 LISTEN，会话层状态错乱 → 顺序客户端（iperf3 -s 每客户端重 listen）
     * 在若干轮后停止投递 ACCEPTED → 挂起。deadline 2s 兜底（VPP 正常 <1ms 回）。
     * listener close 在服务循环里单线程发生（iperf3/nginx worker），全局 ctx/done 安全。*/
    vm->unlisten_ctx = h;
    vm->unlisten_done = 0;
    {
      long deadline_ms = vcl2_now_ms () + 2000;
      while (!vm->unlisten_done) {
        if (vcl2_now_ms () >= deadline_ms)
          break;
        vcl2_mq_wait_dispatch (0.001);
      }
      vm->unlisten_ctx = ~0;
    }
  } else if (!is_listener && vpp_handle && disc_mq) {
    vcl2_send_disconnect (disc_mq, vpp_handle);
    VCL2_DBG ("DISCONNECT sent handle=%u vpp=0x%llx peer_closed=%u "
              "disc_is_global=%u pid=%d", h, (unsigned long long) vpp_handle,
              peer_closed, disc_is_global, (int) getpid ());
  }

  /* listener 关闭时，给 accept_q 里未取走的子 session 各发 DISCONNECT（各经其
   * 所属线程的 evt_q，否则多核下错线程触发 app_worker_add_event 断言 panic） */
  if (child_disc) {
    __typeof__ (child_disc) cd;
    vec_foreach (cd, child_disc) {
      if (cd->evt_q) {
        vcl2_send_disconnect (cd->evt_q, cd->vpp_handle);
        VCL2_DBG ("DISCONNECT(child) sent vpp=0x%llx disc_is_global=%u pid=%d",
                  (unsigned long long) cd->vpp_handle,
                  (cd->evt_q == vm->vpp_evt_q), (int) getpid ());
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
  svm_msg_q_t *mq = 0;

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
  mq = s->vpp_evt_q ? s->vpp_evt_q : vm->vpp_evt_q;
  clib_rwlock_writer_unlock (&vm->sessions_lock);

  if (send_shutdown && vpp_handle && mq) {
    app_session_evt_t ae;
    session_shutdown_msg_t *mp;
    app_alloc_ctrl_evt_to_vpp (mq, &ae, SESSION_CTRL_EVT_SHUTDOWN);
    mp = (session_shutdown_msg_t *) ae.evt->data;
    memset (mp, 0, sizeof (*mp));
    mp->client_index = vm->api_client_handle;
    mp->handle = vpp_handle;
    app_send_ctrl_evt_to_vpp (mq, &ae);
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
  (void) q_len;

  if (PREDICT_FALSE (!vm->ctrl_mq || !vm->app_event_queue))
    return -ENOTCONN;

  /* 取 lcl 地址 + proto（读锁）填 LISTEN 消息 */
  uint8_t lcl_is_ip4, is_dgram, is_tls;
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
  is_dgram = s->is_dgram;
  is_tls = s->is_tls;
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
  /* proto 按 session：UDP/TLS/TCP（原硬编码 TCP，UDP listener 误请求 TCP——顺带修）*/
  mp->proto = is_dgram ? TRANSPORT_PROTO_UDP :
                         (is_tls ? TRANSPORT_PROTO_TLS : TRANSPORT_PROTO_TCP);
  if (lcl_is_ip4) {
    ip4_address_t ip4;
    memcpy (&ip4, lcl_ip, 4);
    ip46_address_set_ip4 (&mp->ip, &ip4);
  } else
    clib_memcpy_fast (&mp->ip, lcl_ip, 16);
  /* TLS listener：挂 ext_config（服务端证书 ckpair） */
  if (is_tls) {
    uword off;
    int erv = vcl2_tls_ensure_cert ();
    if (erv)
      return erv;
    if (vcl2_session_build_ext_config (vm->tls_ckpair_index, &off))
      return -ENOMEM;
    mp->ext_config = off;
  }
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 BOUND：dispatch 统一处理（含 BOUND，置 ctrl_done + listener 身份），
   * 本循环只检测标志。deadline 限定 + 1ms 有界重查。*/
  {
    long deadline_ms, now_ms;
    double rem;
    deadline_ms = vcl2_now_ms () + (long) (VCL2_CTRL_TIMEOUT * 1000);
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
      now_ms = vcl2_now_ms ();
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
    svm_msg_q_t *cs_mq = 0;
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
        /* 【多核修复】ACCEPTED_REPLY 必须经该子 session 所属线程的 evt_q 回 VPP，
         * 否则 session_mq_accepted_reply_handler→app_worker_rx_notify→
         * app_worker_add_event 断言 s->thread_index==cur 触发 panic。
         * 此前用全局 vm->vpp_evt_q（= 某一个线程），多核 RSS 下错线程。 */
        cs_mq = cs->vpp_evt_q ? cs->vpp_evt_q : vm->vpp_evt_q;
        have_cs = 1;
      }
      got = 1;
    }
    clib_rwlock_writer_unlock (&vm->sessions_lock);
    if (got) {
      /* 解锁后回 ACCEPTED_REPLY（避免 send 阻塞持写锁）*/
      if (have_cs && cs_mq) {
        app_session_evt_t ae;
        session_accepted_reply_msg_t *rm;
        app_alloc_ctrl_evt_to_vpp (cs_mq, &ae,
                                   SESSION_CTRL_EVT_ACCEPTED_REPLY);
        rm = (session_accepted_reply_msg_t *) ae.evt->data;
        rm->context = accept_context;
        rm->retval = 0;
        rm->handle = cs_vpp_handle;
        rm->app_session_index = cs_vpp_session_index;
        app_send_ctrl_evt_to_vpp (cs_mq, &ae);
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
      svm_msg_q_t *ds_mq = 0;
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
        /* DISCONNECTED_REPLY 同样须经该 session 所属线程的 evt_q（多核 RSS） */
        ds_mq = ds->vpp_evt_q ? ds->vpp_evt_q : vm->vpp_evt_q;
        VCL2_DBG (
          "DISCONNECTED handle=%u vpp=0x%llx (peer_closed, fifos nulled)",
          ds->handle, (unsigned long long) dm->handle);
      }
      clib_rwlock_writer_unlock (&vm->sessions_lock);
      if (ds_mq) {
        app_session_evt_t ae;
        session_disconnected_reply_msg_t *rm;
        app_alloc_ctrl_evt_to_vpp (ds_mq, &ae,
                                   SESSION_CTRL_EVT_DISCONNECTED_REPLY);
        rm = (session_disconnected_reply_msg_t *) ae.evt->data;
        rm->context = vm->api_client_handle;
        rm->retval = 0;
        rm->handle = dm->handle;
        app_send_ctrl_evt_to_vpp (ds_mq, &ae);
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
          /* 子 session 继承 listener 的 proto（UDP/TLS）；数据路径据此分支 */
          ns->is_dgram = ls->is_dgram;
          ns->is_tls = ls->is_tls;
          ns->rmt_is_ip4 = am->rmt.is_ip4;
          if (am->rmt.is_ip4)
            memcpy (ns->rmt_ip, &am->rmt.ip.ip4, 4);
          else
            memcpy (ns->rmt_ip, &am->rmt.ip, 16);
          ns->rmt_port = am->rmt.port;
          /* 本地地址（getsockname 用）：端口网络序原样拷 */
          ns->lcl_is_ip4 = am->lcl.is_ip4;
          ns->lcl_port = am->lcl.port;
          if (am->lcl.is_ip4)
            memcpy (ns->lcl_ip, &am->lcl.ip.ip4, 4);
          else
            memcpy (ns->lcl_ip, &am->lcl.ip, 16);
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
        else {
          cs->ctrl_rv = vcl2_session_attach_connected (cs, cm);
          /* 回填本地地址（getsockname 用）：端口网络序，照 rmt 原样拷 */
          cs->lcl_is_ip4 = cm->lcl.is_ip4;
          cs->lcl_port = cm->lcl.port;
          if (cm->lcl.is_ip4)
            memcpy (cs->lcl_ip, &cm->lcl.ip.ip4, 4);
          else
            memcpy (cs->lcl_ip, &cm->lcl.ip, 16);
        }
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
          /* listener 的 vpp_handle 也登记反向 hash（ACCEPTED 按 listener_handle 查它）*/
          hash_set (vm->vpp_handle_to_session, (uword) bm->handle,
                    (uword) (bs - vm->sessions));
          /* BOUND 带 rx/tx fifo（对齐 VCL vcl_session_bound_handler 的
           * vcl_segment_attach_session）。UDP listener 据此 recvfrom；TCP listener
           * 的 fifo 不用（数据走子 session），无害。rx_fifo==0 则跳过。*/
          if (bm->rx_fifo) {
            bs->ctrl_rv = vcl2_session_attach_fifos (
              bs, bm->handle, bm->segment_handle, bm->rx_fifo, bm->tx_fifo,
              bm->vpp_evt_q, bm->mq_index);
            /* connectionless（UDP）listener 的数据面身份是 cl_sh_handle，不是
             * listener handle（后者为 0）。attach_fifos 按 listener handle 设了
             * vpp_session_index=0（错）；此处改用 cl_sh_handle（对齐 VCL 的
             * fifo->vpp_sh = cl_sh_handle），否则 tx 通知引用错误 session →
             * 数据报发不出。*/
            if (bs->is_dgram && bm->cl_sh_handle) {
              u32 cl_idx = session_index_from_handle (bm->cl_sh_handle);
              bs->rx_fifo->vpp_sh = bm->cl_sh_handle;
              bs->tx_fifo->vpp_sh = bm->cl_sh_handle;
              bs->rx_fifo->vpp_session_index = cl_idx;
              bs->tx_fifo->vpp_session_index = cl_idx;
            }
          }
        }
        bs->ctrl_done = 1;
        VCL2_DBG ("BOUND handle=%u vpp=0x%llx rv=%d", bs->handle,
                  (unsigned long long) bm->handle, bs->ctrl_rv);
      }
      clib_rwlock_writer_unlock (&vm->sessions_lock);
    } else if (e->event_type == SESSION_CTRL_EVT_UNLISTEN_REPLY) {
      /* unlisten 回复：close(listener) 同步等它，确保 VPP 拆除旧 listener 后再 rebind
       *（镜像内核同步 close 语义；避免 VPP 在 UNLISTEN 未完成时处理新 LISTEN →
       * 顺序客户端挂起）。按 context 匹配正在等待的 close。*/
      session_unlisten_reply_msg_t *urm =
        (session_unlisten_reply_msg_t *) e->data;
      if (urm->context == vm->unlisten_ctx) {
        vm->unlisten_done = 1;
        VCL2_DBG ("UNLISTEN_REPLY handle=%u rv=%d", urm->context,
                  (int) urm->retval);
      }
    }
    svm_msg_q_free_msg (mq, &msg);
  }
  pthread_mutex_unlock (&vm->app_mq_lock);
}

/* 非阻塞排空 + 分发（ldp2 select/poll/epoll 唤醒后用）。无等待。*/
void vcl2_dispatch_app_events (void) {
  vcl2_mq_wait_dispatch (0);
}
