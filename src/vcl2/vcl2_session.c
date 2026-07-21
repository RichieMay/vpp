/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 session 操作（P3a：控制面 connect + app_event_queue 轮询）。
 *
 * 关键：vcl2 维护一个【可丢弃】的薄缓存（vcl2_session_t）—— 只持指向 VPP-owned
 * 段内的 fifo/mq 指针 + correlation key + 少量语义标志（nonblocking/peer_closed/
 * shutdown/listener）。它【不是】VCL 那种带状态机的 session 对象（无 VCL_STATE_* 状态机、
 * 不拥有任何需跨进程释放的资源）。session 的真实状态/资源全在 VPP；进程死缓存即随进程
 * 消失，VPP 单侧回收。
 *
 * P3a 范围：经 ctrl_mq 发 connect，并轮询 app_event_queue 验证 VPP 的 CONNECTED
 * 回复能收到（证明双向 mq 通）。fifo 提取 + recv/send 是下一步。
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>		/* htons */

#include <vppinfra/clib_error.h>
#include <vnet/session/application_interface.h>	/* app_alloc/send_ctrl_evt, *_msg_t */
#include <vnet/session/session_types.h>		/* session_event_t, SESSION_CTRL_EVT_* */

#include "vcl2_private.h"

/* ---------- 可丢弃 session 缓存 ----------
 * 单侧所有权：缓存条目只持【VPP-owned 段内】的 fifo/mq 指针 + correlation key，
 * 不持任何需跨进程释放的资源。fork 子进程丢弃重建；进程死即随进程消失。
 */
vcl2_session_t *
vcl2_session_alloc (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  u32 i;

  /* 复用空闲槽 */
  for (i = 0; i < vec_len (vm->sessions); i++)
    if (!vm->sessions[i].in_use)
      {
	s = &vm->sessions[i];
	memset (s, 0, sizeof (*s));
	goto found;
      }
  /* 无空闲槽：追加一个（vec_add2 第三参数是【个数】，必须为 1） */
  vec_add2 (vm->sessions, s, 1);
  memset (s, 0, sizeof (*s));
found:
  s->handle = h;
  s->in_use = 1;
  hash_set (vm->handle_to_session, (uword) h, s - vm->sessions);
  return s;
}

vcl2_session_t *
vcl2_session_get (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  uword *p = hash_get (vm->handle_to_session, (uword) h);
  if (!p)
    return 0;
  vcl2_session_t *s = vec_elt_at_index (vm->sessions, p[0]);
  return s->in_use ? s : 0;
}

/*
 * 创建一个 session 缓存槽（预留 handle，尚未 connect）。镜像 vcl 的
 * vppcom_session_create：纯 app 侧，无 VPP 往返。LDP 的 socket() 调它拿到合成 fd。
 * 返回 handle（>0）或负 errno。
 */
static vcl2_handle_t vcl2_next_handle;

int
vcl2_session_create (vcl2_proto_t proto, uint8_t is_nonblocking)
{
  vcl2_handle_t h;
  vcl2_session_t *s;
  (void) proto;
  (void) is_nonblocking;	/* P4a 暂不区分阻塞；后续按需存 */
  h = ++vcl2_next_handle;
  s = vcl2_session_alloc (h);
  if (!s)
    return -ENOMEM;
  return (int) h;
}

/*
 * 从 CONNECTED 回复提取 fifo/mq 并填入缓存条目（镜像 vcl_segment_attach_session）。
 * - session 的 rx/tx fifo 与 vpp_evt_q 都在【worker memfd 段 / vpp-worker 段】内，
 *   这些段已在 attach/worker-add 时 mmap。这里只按偏移还原指针。
 * - vpp_session_index 显式从 handle 解出（IO 事件回填需要，builtin app 也这么做）。
 */
static int
vcl2_session_attach_connected (vcl2_session_t * s,
			       session_connected_msg_t * cm)
{
  vcl2_main_t *vm = &vcl2_main;
  int rv;

  rv = vcl2_session_attach_fifos (s, cm->handle, cm->segment_handle,
				  cm->server_rx_fifo, cm->server_tx_fifo,
				  cm->vpp_event_queue_address, cm->mq_index);
  if (rv)
    return rv;
  VCL2_DBG ("session attached handle=%u vpp=0x%llx", s->handle, (unsigned long long) s->vpp_handle);
  (void) vm;
  return 0;
}

/*
 * fifo 提取的公共核心（CONNECTED / ACCEPTED 共用）：在 session 段内按偏移还原
 * rx/tx svm_fifo，attach vpp_evt_q（单 worker 下复用），回填 fifo 的 vpp_session_index。
 */
int
vcl2_session_attach_fifos (vcl2_session_t * s, u64 vpp_handle, u64 seg,
			   uword rxf_off, uword txf_off, uword vpp_eq_off,
			   u32 mq_index)
{
  vcl2_main_t *vm = &vcl2_main;

  s->vpp_handle = vpp_handle;
  s->vpp_session_index = session_index_from_handle (vpp_handle);

  s->rx_fifo = vcl2_segment_alloc_fifo (seg, rxf_off);
  s->tx_fifo = vcl2_segment_alloc_fifo (seg, txf_off);
  if (!s->rx_fifo || !s->tx_fifo)
    {
      VCL2_DBG ("fifo map failed rx=%p tx=%p (seg=0x%llx)", s->rx_fifo, s->tx_fifo,
	       (unsigned long long) seg);
      return -EINVAL;
    }
  if (!vm->vpp_evt_q)
    {
      if (vcl2_segment_attach_mq (VCL2_VPP_WRK_SEG_HANDLE (0), vpp_eq_off,
				  mq_index, &vm->vpp_evt_q))
	{
	  VCL2_DBG ("vpp_evt_q attach failed\n", (int) getpid ());
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

/*
 * 从 app_event_queue 读一个事件，按 event_type 分发。
 * 返回 event_type（>0）或 0（无事件/超时）或负 errno。
 */
int
vcl2_event_poll_once (double timeout_s)
{
  vcl2_main_t *vm = &vcl2_main;
  svm_msg_q_msg_t msg;
  session_event_t *e;
  session_connected_msg_t *cm;
  int et;

  if (!vm->app_event_queue)
    return -ENOTCONN;

  if (svm_msg_q_timedwait (vm->app_event_queue, timeout_s))
    return 0;	/* 超时，无事件 */

  if (svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
    return 0;

  e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
  et = e->event_type;
  VCL2_DBG ("event received: type=%u as_u64[0]=0x%llx "
	   "as_u64[1]=0x%llx", et,
	   (unsigned long long) e->as_u64[0], (unsigned long long) e->as_u64[1]);
  /* CONNECTED：读 session_connected_msg_t（外部 app 路径）。注意 vnet_connect
   * 失败时也会投 type=CONNECTED（err 藏在 payload）。判据：context==我的 handle
   * 且 retval==0 才是真成功。 */
  if (et == SESSION_CTRL_EVT_CONNECTED)
    {
      cm = (session_connected_msg_t *) e->data;
      VCL2_DBG ("  CONNECTED payload: context=%u retval=%d "
	       "handle=0x%llx seg=0x%llx rx_fifo=0x%llx tx_fifo=0x%llx", cm->context, cm->retval,
	       (unsigned long long) cm->handle,
	       (unsigned long long) cm->segment_handle,
	       (unsigned long long) cm->server_rx_fifo,
	       (unsigned long long) cm->server_tx_fifo);
    }
  svm_msg_q_free_msg (vm->app_event_queue, &msg);
  return et;
}

/*
 * 发起 connect：经 ctrl_mq 发 SESSION_CTRL_EVT_CONNECT，然后等 app_event_queue
 * 上的 CONNECTED 回复。ip 为 4 或 16 字节（按 is_ip4）。port 为 host 字序。
 */
int
vcl2_session_connect (vcl2_handle_t h, uint8_t is_ip4, const uint8_t * ip,
		      uint16_t port)
{
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_connect_msg_t *mp;
  int i, et;

  if (!vm->ctrl_mq || !vm->app_event_queue)
    return -ENOTCONN;
  if (!VCL2_HANDLE_IS_VALID (h))
    return -EINVAL;

  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_CONNECT);
  mp = (session_connect_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;		/* correlation key，VPP 回复时带回 */
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = is_ip4;
  mp->proto = TRANSPORT_PROTO_TCP;
  mp->port = htons (port);
  if (ip)
    {
      /* 关键：VPP 的 ip46_address_t 把 ipv4 存在 offset 12（pad[3] 之后），
       * 不是 offset 0。直接 memcpy 4 字节会落到 pad 区，VPP 的 ip_is_zero
       * 检查 ip4.as_u32(@12) 读到全 0 → SESSION_E_INVALID_RMT_IP。
       * 必须用 ip46_address_set_ip4：清 pad + 写 ip4 到 offset 12。 */
      if (is_ip4)
	ip46_address_set_ip4 (&mp->ip, (const ip4_address_t *) ip);
      else
	clib_memcpy_fast (&mp->ip, ip, 16);	/* ipv6 占满 16 字节 @0 */
    }
  VCL2_DBG ("connect msg: client=%u ctx=%u wrk=%u is_ip4=%u "
	   "proto=%u port=0x%04x rmt=%u.%u.%u.%u (ip4@off12 %u.%u.%u.%u)", mp->client_index, mp->context, mp->wrk_index,
	   mp->is_ip4, mp->proto, mp->port, ip[0], ip[1], ip[2], ip[3],
	   mp->ip.as_u8[12], mp->ip.as_u8[13], mp->ip.as_u8[14],
	   mp->ip.as_u8[15]);
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  VCL2_DBG ("connect sent handle=%u (context=%u)", h, h);

  /* 等 CONNECTED 回复（最多 ~5s）。注意：vnet_connect 失败时 VPP 也投
   * type=CONNECTED 事件，err 藏在 session_connected_msg_t.retval。必须查 retval。 */
  for (i = 0; i < 50; i++)
    {
      svm_msg_q_msg_t msg;
      session_event_t *e;
      session_connected_msg_t *cm;

      if (svm_msg_q_timedwait (vm->app_event_queue, 0.1))
	continue;		/* 超时，继续等 */
      if (svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	continue;
      e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
      et = e->event_type;
      if (et == SESSION_CTRL_EVT_CONNECTED)
	{
	  int crv;
	  cm = (session_connected_msg_t *) e->data;
	  VCL2_DBG ("CONNECTED ctx=%u retval=%d handle=0x%llx "
		   "seg=0x%llx rx_fifo=0x%llx tx_fifo=0x%llx vpp_eq=0x%llx "
		   "mqidx=%u", cm->context, cm->retval, (unsigned long long) cm->handle,
		   (unsigned long long) cm->segment_handle,
		   (unsigned long long) cm->server_rx_fifo,
		   (unsigned long long) cm->server_tx_fifo,
		   (unsigned long long) cm->vpp_event_queue_address, cm->mq_index);
	  svm_msg_q_free_msg (vm->app_event_queue, &msg);
	  /* retval==0 才是真成功；否则把 session_error_t 透传给调用者 */
	  if (cm->retval)
	    return (int) cm->retval;
	  /* 成功：提取 fifo/mq 进可丢弃缓存，供 send/recv 直接解引用。
	   * 优先复用 socket() 预留的槽（LDP 路径）；否则创建（直连 API 路径）。 */
	  vcl2_session_t *s = vcl2_session_get (h);
	  if (!s)
	    s = vcl2_session_alloc (h);
	  crv = vcl2_session_attach_connected (s, cm);
	  return crv;
	}
      VCL2_DBG ("(connect wait) event type=%u", et);
      svm_msg_q_free_msg (vm->app_event_queue, &msg);
      if (et < 0)
	return et;
    }
  return -ETIMEDOUT;
}

/*
 * send：把数据写入 tx_fifo（app→peer），并经 vpp_evt_q 发 SESSION_IO_EVT_TX
 * 通知 VPP 取走发送。镜像 vcl app_send_stream_raw。返回写入字节数或负 errno。
 */
int
vcl2_session_send (vcl2_handle_t h, const void *buf, uint32_t len)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  int n;

  s = vcl2_session_get (h);
  if (!s || !s->tx_fifo)
    return -EINVAL;
  if (s->wr_shutdown)
    return -EPIPE;		/* shutdown(SHUT_WR/RDWR) 后再写 → EPIPE */
  if (!vm->vpp_evt_q)
    return -ENOTCONN;
  if (!len)
    return 0;

  /* 非阻塞 + 无空间：立即 EAGAIN（nginx/iperf 的 ET 事件循环依赖此语义） */
  if (s->nonblocking && svm_fifo_max_enqueue_prod (s->tx_fifo) < len)
    return -EAGAIN;

  /* 阻塞：等待 tx_fifo 有空间（与 vcl 一致用 want-deq-ntf 机制） */
  while (svm_fifo_max_enqueue_prod (s->tx_fifo) < len)
    {
      svm_fifo_add_want_deq_ntf (s->tx_fifo, SVM_FIFO_WANT_DEQ_NOTIF);
      if (svm_msg_q_timedwait (vm->app_event_queue, 0.1))
	continue;
      /* 排空任何事件（tx 有空间时 VPP 会投事件；这里只消费，数据在 fifo 里） */
      svm_msg_q_msg_t msg;
      while (!svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	svm_msg_q_free_msg (vm->app_event_queue, &msg);
    }

  n = app_send_stream_raw (s->tx_fifo, vm->vpp_evt_q, (u8 *) buf, len,
			   SESSION_IO_EVT_TX, /*do_evt */ 1, /*noblock */ 0);
  if (n < 0)
    return -EAGAIN;
  return n;
}

/*
 * recv：从 rx_fifo 读 peer→app 数据（阻塞，最多 ~5s）。VPP 收到数据后写入 rx_fifo
 * 并投 SESSION_IO_EVT_RX 到 app_event_queue；这里轮询事件、读到即返回。
 * 返回读取字节数或负 errno。
 */
int
vcl2_session_recv (vcl2_handle_t h, void *buf, uint32_t len)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  int i, n;

  s = vcl2_session_get (h);
  if (!s || !s->rx_fifo)
    return -EINVAL;
  if (s->rd_shutdown)
    return 0;			/* shutdown(SHUT_RD/RDWR) 后再读 → EOF */

  /* 先试一次（非阻塞下若空则 EAGAIN） */
  n = app_recv_stream_raw (s->rx_fifo, (u8 *) buf, len, /*clear_evt */ 1,
			   /*peek */ 0);
  if (n > 0)
    return n;
  /* 无数据：peer 已关 → EOF(0)；否则非阻塞 EAGAIN / 阻塞等待 */
  if (s->peer_closed)
    return 0;
  if (s->nonblocking)
    return -EAGAIN;

  for (i = 0; i < 50; i++)
    {
      n = app_recv_stream_raw (s->rx_fifo, (u8 *) buf, len, /*clear_evt */ 1,
			       /*peek */ 0);
      if (n > 0)
	return n;

      /* 无数据：等 VPP 投 RX 事件 */
      if (svm_msg_q_timedwait (vm->app_event_queue, 0.1))
	continue;
      svm_msg_q_msg_t msg;
      while (!svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	{
	  session_event_t *e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
	  VCL2_DBG ("(recv wait) event type=%u sid=%u", e->event_type, e->session_index);
	  svm_msg_q_free_msg (vm->app_event_queue, &msg);
	}
    }
  return -ETIMEDOUT;
}

/*
 * close：正常优雅关闭（控制面消息）。镜像 VCL vcl_send_session_disconnect。
 *   - listener：发 UNLISTEN（ctrl_mq），把本 worker 移出 accept 轮转位图。
 *   - 连接：发 DISCONNECT（vpp_evt_q，SESSION_CTRL_EVT_DISCONNECT +
 *     session_disconnect_msg_t{client_index,handle}）。VPP 收到后关闭 transport
 *     （向 peer 发 FIN）并回收该 session。
 *
 * 设计边界（单侧所有权原则只约束【进程退出】清理）：
 *   - 正常优雅关闭【必须】发控制消息（与 connect/listen/accept 同属控制面，非 teardown 协议）。
 *   - 进程退出（含 kill -9 / segfault 等异常）【不】依赖本消息——app 异常时无法发送，
 *     由 VPP 经 SAPI UDS close 单侧回收（已 P6 验证：app 死 → VPP 回收，无 app 侧消息）。
 *     故本函数即使没跑到（进程被杀），也不会泄漏：VPP 的 UDS 检测兜底。
 *
 * 发完控制消息后丢弃 app 侧缓存条目（in_use=0 + 移出 hash）。真正的 session/fifo 回收
 * 由 VPP 在 transport close 后完成（它拥有这些资源）。
 */
int
vcl2_session_close (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;

  s = vcl2_session_get (h);
  if (!s)
    return -EINVAL;
  VCL2_DBG ("close handle=%u vpp=0x%llx%s", s->handle, (unsigned long long) s->vpp_handle,
	   s->is_listener ? " (listener)" : "");

  if (s->is_listener)
    {
      /* listener 优雅关闭：从 accept 轮转摘除本 worker */
      vcl2_session_unlisten (h);
    }
  else if (s->vpp_handle && vm->vpp_evt_q)
    {
      /* 连接优雅关闭：请 VPP 关 transport（发 FIN 给 peer）+ 回收 session */
      app_session_evt_t ae;
      session_disconnect_msg_t *mp;
      app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
				 SESSION_CTRL_EVT_DISCONNECT);
      mp = (session_disconnect_msg_t *) ae.evt->data;
      memset (mp, 0, sizeof (*mp));
      mp->client_index = vm->api_client_handle;
      mp->handle = s->vpp_handle;
      app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      VCL2_DBG ("DISCONNECT sent handle=%u vpp=0x%llx", h, (unsigned long long) s->vpp_handle);
    }

  hash_unset (vm->handle_to_session, (uword) h);
  vec_free (s->accept_q);
  s->in_use = 0;
  s->rx_fifo = s->tx_fifo = 0;
  s->is_listener = 0;
  return 0;
}

/*
 * shutdown：正常优雅半关闭（控制面，镜像 VCL vppcom_session_shutdown）。
 *   SHUT_RD      ：本地标记 rd_shutdown（recv 返回 EOF），不发消息（VPP 无需知道）。
 *   SHUT_WR/RDWR ：本地标记 wr_shutdown（send 返回 -EPIPE），并向 VPP 发
 *                  SESSION_CTRL_EVT_DISCONNECT 等价的 SHUTDOWN（session_shutdown_msg_t）
 *                  → VPP 关【发送方向】transport（向 peer 发 FIN），但连接仍可收。
 * 与 close() 一样属正常操作；异常退出（kill -9）不依赖它——UDS 兜底。
 */
int
vcl2_session_shutdown (vcl2_handle_t h, int how)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;

  s = vcl2_session_get (h);
  if (!s)
    return -EINVAL;
  if (s->is_listener)
    return -EINVAL;		/* 不能对 listener shutdown（镜像 VCL） */

  if (how == SHUT_RD || how == SHUT_RDWR)
    {
      s->rd_shutdown = 1;
      if (how == SHUT_RD)
	return 0;		/* 只读半关：纯本地 */
    }
  s->wr_shutdown = 1;

  /* 写半关：通知 VPP（向 peer 发 FIN，连接仍可收） */
  if (s->vpp_handle && vm->vpp_evt_q)
    {
      app_session_evt_t ae;
      session_shutdown_msg_t *mp;
      app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
				 SESSION_CTRL_EVT_SHUTDOWN);
      mp = (session_shutdown_msg_t *) ae.evt->data;
      memset (mp, 0, sizeof (*mp));
      mp->client_index = vm->api_client_handle;
      mp->handle = s->vpp_handle;
      app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      VCL2_DBG ("SHUTDOWN sent handle=%u vpp=0x%llx how=%d", h, (unsigned long long) s->vpp_handle, how);
    }
  return 0;
}

/*
 * unlisten：把【本 worker】从 listener 的 accept 轮转位图（al->workers）移除。
 * 不销毁 listener（其它 worker 仍在位图里则 listener 保留）。发 SESSION_CTRL_EVT_UNLISTEN，
 * 不等回复。用于 nginx master：fork 后 master 不 accept，需把自己摘出，
 * 否则 VPP 把 ACCEPTED 轮到 master（master 不排空 mq）→ 连接卡死。
 */
int
vcl2_session_unlisten (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_unlisten_msg_t *mp;
  vcl2_session_t *s;

  s = vcl2_session_get (h);
  if (!s || !s->is_listener || !vm->ctrl_mq)
    return -EINVAL;
  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_UNLISTEN);
  mp = (session_unlisten_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->handle = s->vpp_handle;
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);
  VCL2_DBG ("unlisten handle=%u vpp=0x%llx (remove self from "
	   "accept rotor)", h,
	   (unsigned long long) s->vpp_handle);
  return 0;
}

/* 按 VPP handle 查 session（用于 ACCEPTED 的 listener_handle 反查） */
vcl2_session_t *
vcl2_session_get_by_vpp_handle (u64 vpp_handle)
{
  vcl2_main_t *vm = &vcl2_main;
  u32 i;
  for (i = 0; i < vec_len (vm->sessions); i++)
    if (vm->sessions[i].in_use && vm->sessions[i].vpp_handle == vpp_handle)
      return &vm->sessions[i];
  return 0;
}

/*
 * listen：经 ctrl_mq 发 SESSION_CTRL_EVT_LISTEN，等 BOUND。本地地址取自 bind() 存入
 * session 的 lcl_*（ldp2 bind 调用）。BOUND 成功后置 is_listener + 记 vpp_handle。
 * 镜像 vcl_send_session_listen。返回 0 或负 errno。
 */
int
vcl2_session_listen (vcl2_handle_t h, uint32_t q_len)
{
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_listen_msg_t *mp;
  vcl2_session_t *s;
  int i;

  (void) q_len;
  s = vcl2_session_get (h);
  if (!s)
    return -EINVAL;
  if (!vm->ctrl_mq || !vm->app_event_queue)
    return -ENOTCONN;

  memset (&app_evt, 0, sizeof (app_evt));
  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt, SESSION_CTRL_EVT_LISTEN);
  mp = (session_listen_msg_t *) app_evt.evt->data;
  memset (mp, 0, sizeof (*mp));
  mp->client_index = vm->api_client_handle;
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = s->lcl_is_ip4;
  mp->port = htons (s->lcl_port);
  mp->proto = TRANSPORT_PROTO_TCP;
  if (s->lcl_is_ip4)
    {
      ip4_address_t ip4;
      memcpy (&ip4, s->lcl_ip, 4);
      ip46_address_set_ip4 (&mp->ip, &ip4);	/* ipv4 @ offset 12 */
    }
  else
    clib_memcpy_fast (&mp->ip, s->lcl_ip, 16);
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  VCL2_DBG ("listen sent handle=%u port=%u", h, s->lcl_port);

  /* 等 BOUND（最多 ~5s） */
  for (i = 0; i < 50; i++)
    {
      svm_msg_q_msg_t msg;
      session_event_t *e;
      session_bound_msg_t *bm;

      if (svm_msg_q_timedwait (vm->app_event_queue, 0.1))
	continue;
      if (svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	continue;
      e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
      if (e->event_type == SESSION_CTRL_EVT_BOUND)
	{
	  bm = (session_bound_msg_t *) e->data;
	  svm_msg_q_free_msg (vm->app_event_queue, &msg);
	  if (bm->retval)
	    return (int) bm->retval;
	  s->vpp_handle = bm->handle;
	  s->is_listener = 1;
	  VCL2_DBG ("BOUND listener handle=%u vpp=0x%llx", h, (unsigned long long) bm->handle);
	  return 0;
	}
      svm_msg_q_free_msg (vm->app_event_queue, &msg);
    }
  return -ETIMEDOUT;
}

/*
 * accept：取走 listener accept_q 里一个已 ACCEPTED 的子 session。无则轮询
 * app_event_queue（经 vcl2_dispatch_app_events 派发，会把 ACCEPTED 入队）。
 * 返回 0 并把新 handle 写入 *out；或负 errno（-EAGAIN 超时）。
 */
int
vcl2_session_accept (vcl2_handle_t listener_h, vcl2_handle_t * out)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  int i;

  if (!out)
    return -EINVAL;
  s = vcl2_session_get (listener_h);
  if (!s || !s->is_listener)
    return -EINVAL;

  for (i = 0; i < 50; i++)
    {
      if (vec_len (s->accept_q) > 0)
	{
	  vcl2_handle_t ch = s->accept_q[0];
	  vec_delete (s->accept_q, 1, 0);
	  /* 关键：向 VPP 回 ACCEPTED_REPLY，否则 VPP 认为该子 session 未被 app 接受，
	   * 不 drain tx_fifo（镜像 vcl_send_session_accepted_reply）。 */
	  vcl2_session_t *cs = vcl2_session_get (ch);
	  if (cs && vm->vpp_evt_q)
	    {
	      app_session_evt_t ae;
	      session_accepted_reply_msg_t *rm;
	      app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae,
					 SESSION_CTRL_EVT_ACCEPTED_REPLY);
	      rm = (session_accepted_reply_msg_t *) ae.evt->data;
	      rm->context = cs->accept_context;
	      rm->retval = 0;
	      rm->handle = cs->vpp_handle;
	      rm->app_session_index = cs->vpp_session_index;
	      app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
	      VCL2_DBG ("ACCEPTED_REPLY child=%u vpp=0x%llx", ch, (unsigned long long) cs->vpp_handle);
	    }
	  *out = ch;
	  return 0;
	}
      if (svm_msg_q_timedwait (vm->app_event_queue, 0.1))
	continue;
      vcl2_dispatch_app_events ();
    }
  return -EAGAIN;
}

/*
 * 排空 app_event_queue 并按 event_type 派发。ACCEPTED 建新 session（attach 其 fifo）
 * 并入对应 listener 的 accept_q；其余 IO 事件无需处理（fifo 状态是就绪判据）。
 * ldp2 的 epoll_wait / accept 共用本函数。
 */
void
vcl2_dispatch_app_events (void)
{
  vcl2_main_t *vm = &vcl2_main;
  svm_msg_q_t *mq = vm->app_event_queue;
  svm_msg_q_msg_t msg;

  if (!mq)
    return;
  while (!svm_msg_q_sub (mq, &msg, SVM_Q_NOWAIT, 0))
    {
      session_event_t *e = svm_msg_q_msg_data (mq, &msg);
      if (e->event_type == SESSION_CTRL_EVT_APP_ADD_SEGMENT)	{
	  /* VPP 为新连接另开的 memfd 段：fd 已先经 SAPI socket 推来，这里 recvmsg
	   * 取 fd 并 attach。必须在用该段的 ACCEPTED 之前完成（VPP 按 fd→event 顺序投）。 */
	  session_app_add_segment_msg_t *sm =
	    (session_app_add_segment_msg_t *) e->data;
	  int fd = -1, rv;
	  if (sm->fd_flags)
	    vcl2_sapi_recv_fd (&fd);
	  rv = vcl2_segment_attach (sm->segment_handle, (char *) sm->segment_name,
				    fd);
	  VCL2_DBG ("ADD_SEGMENT handle=0x%llx fd=%d -> %d", (unsigned long long) sm->segment_handle, fd,
		   rv);
	}
      if (e->event_type == SESSION_CTRL_EVT_DISCONNECTED)
	{
	  /* peer 关闭（FIN）：镜像 vcl_session_disconnected_handler。标记 session
	   * peer_closed（recv 排空后返回 EOF=0，epoll/poll 报 EPOLLIN 唤醒 app 读取），
	   * 并回 DISCONNECTED_REPLY 完成 VPP 侧关闭握手。 */
	  session_disconnected_msg_t *dm =
	    (session_disconnected_msg_t *) e->data;
	  vcl2_session_t *ds = vcl2_session_get_by_vpp_handle (dm->handle);
	  if (ds)
	    {
	      ds->peer_closed = 1;
	      VCL2_DBG ("DISCONNECTED handle=%u vpp=0x%llx "
		       "(peer_closed)", ds->handle,
		       (unsigned long long) dm->handle);
	    }
	  if (vm->vpp_evt_q)
	    {
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
	}
      if (e->event_type == SESSION_CTRL_EVT_ACCEPTED)
	{
	  session_accepted_msg_t *am = (session_accepted_msg_t *) e->data;
	  vcl2_session_t *ls = vcl2_session_get_by_vpp_handle (am->listener_handle);
	  if (ls)
	    {
	      /* 注意 vec 悬空：vcl2_session_create 可能 vec_add2 重排 sessions 向量，
	       * 使 ls/ns 指针失效。故只取 listener 的 handle（值），create 后重取指针。 */
	      vcl2_handle_t lh = ls->handle;
	      int nh = vcl2_session_create (VCL2_PROTO_TCP, 0);
	      VCL2_DBG ("ACCEPTED child_vpp=0x%llx listener=%u "
		       "-> child=%d seg=0x%llx", (unsigned long long) am->handle, lh, nh,
		       (unsigned long long) am->segment_handle);
	      if (nh >= 0)
		{
		  vcl2_session_t *ns = vcl2_session_get ((vcl2_handle_t) nh);
		  vcl2_session_t *ls2 = vcl2_session_get (lh);	/* 重取（vec 可能已移动） */
		  if (ns)
		    {
		      ns->accept_context = am->context;
		      /* 记对端地址，accept() 回填 sockaddr 用（nginx 等读 accept 返回的 peer addr） */
		      ns->rmt_is_ip4 = am->rmt.is_ip4;
		      if (am->rmt.is_ip4)
			memcpy (ns->rmt_ip, &am->rmt.ip.ip4, 4);
		      else
			memcpy (ns->rmt_ip, &am->rmt.ip, 16);
		      ns->rmt_port = am->rmt.port;	/* 网络字序 */
		    }
		  if (ns && !vcl2_session_attach_fifos (
				ns, am->handle, am->segment_handle, am->server_rx_fifo,
				am->server_tx_fifo, am->vpp_event_queue_address,
				am->mq_index))
		    {
		      vec_add1 (ls2->accept_q, (vcl2_handle_t) nh);
		      VCL2_DBG ("child=%d attached, queued to "
			       "listener=%u accept_q len=%u", nh, lh, (u32) vec_len (ls2->accept_q));
		    }
		  else
		    VCL2_DBG ("accept fifo attach failed");
		}
	    }
	  else
	    VCL2_DBG ("ACCEPTED no listener vpp=0x%llx", (unsigned long long) am->listener_handle);
	}
      svm_msg_q_free_msg (mq, &msg);
    }
}

