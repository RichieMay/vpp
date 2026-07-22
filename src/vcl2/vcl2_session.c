/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 session 操作（控制面 + 数据面）。
 *
 * 多线程（方案 C）：单一共享 worker + sessions_lock（rwlock）保护 sessions 缓存。
 * 关键不变量（指针稳定性）：vcl2_session_get/get_by_vpp_handle 返回指向 sessions vec
 * 的指针；vec 可能在 alloc（vec_add2）时 realloc。故：
 *   - vcl2_session_get / get_by_vpp_handle / alloc 【假定调用者已持有 sessions_lock】
 *     （读锁或写锁）；返回的指针仅在锁持有期间有效。
 *   - 所有公开操作（recv/send/connect/listen/accept/close/shutdown/dispatch）自行加锁，
 *     围绕"取指针→用指针"段；【阻塞等待（svm_msg_q_timedwait）前必须解锁】，下一轮重新
 *     取锁+取指针。
 *   - 锁序：sessions_lock（外）→ segment_table_lock（内）。vcl2_segment_* 只取 segment 锁。
 * 单侧所有权不变：被锁的全是可丢弃缓存，无 app 拥有的资源。
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>		/* htons */

#include <vppinfra/clib_error.h>
#include <vnet/session/application_interface.h>	/* app_alloc/send_ctrl_evt, *_msg_t */
#include <vnet/session/session_types.h>		/* session_event_t, SESSION_CTRL_EVT_* */

#include "vcl2_private.h"

/* 阻塞 recv/send 的事件驱动等待兜底超时（秒）。事件驱动下几乎不触发——VPP 在 RX 入队
 * / tx 空间释放时 signal eventfd，timedwait 会被即时唤醒；此值只是防"信号丢失"的
 * 周期重查。不用短分片（0.1s 轮询）。*/
#define VCL2_BLOCK_TIMEOUT 60.0
/* 控制面（connect/listen/accept）等回复的总超时（秒）——正常回复经 eventfd 即时到达。*/
#define VCL2_CTRL_TIMEOUT 5.0

/* ---------- 可丢弃 session 缓存（假定调用者持有 sessions_lock）---------- */

/* 要求：调用者持有 sessions 【写】锁。分配/复用一个槽，登记 hash。 */
vcl2_session_t *
vcl2_session_alloc (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  u32 i;

  for (i = 0; i < vec_len (vm->sessions); i++)
    if (!vm->sessions[i].in_use)
      {
	s = &vm->sessions[i];
	memset (s, 0, sizeof (*s));
	goto found;
      }
  vec_add2 (vm->sessions, s, 1);	/* 第三参数是【个数】，必须为 1 */
  memset (s, 0, sizeof (*s));
found:
  s->handle = h;
  s->in_use = 1;
  hash_set (vm->handle_to_session, (uword) h, s - vm->sessions);
  return s;
}

/* 要求：调用者持有 sessions 读锁（或写锁）。指针仅在锁持有期间有效。 */
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

/* 要求：调用者持有 sessions 读锁（或写锁）。 */
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

static vcl2_handle_t vcl2_next_handle;

/* socket() 路径：预留一个 handle 槽（尚未 connect）。写锁内 ++handle + alloc。 */
int
vcl2_session_create (vcl2_proto_t proto, uint8_t is_nonblocking)
{
  vcl2_main_t *vm = &vcl2_main;
  vcl2_session_t *s;
  vcl2_handle_t h;
  (void) proto;
  (void) is_nonblocking;
  clib_rwlock_writer_lock (&vm->sessions_lock);
  h = ++vcl2_next_handle;		/* 写锁独占，自增安全 */
  s = vcl2_session_alloc (h);
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  if (!s)
    return -ENOMEM;
  return (int) h;
}

/* fifo 提取公共核心（CONNECTED/ACCEPTED 共用）。要求：调用者持有 sessions 写锁
 * （因 mutate session 字段，且首附 vpp_evt_q）。内部 segment 操作取 segment 读锁。 */
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

static int
vcl2_session_attach_connected (vcl2_session_t * s, session_connected_msg_t * cm)
{
  int rv = vcl2_session_attach_fifos (s, cm->handle, cm->segment_handle,
				      cm->server_rx_fifo, cm->server_tx_fifo,
				      cm->vpp_event_queue_address, cm->mq_index);
  if (rv)
    return rv;
  VCL2_DBG ("session attached handle=%u vpp=0x%llx", s->handle,
	   (unsigned long long) s->vpp_handle);
  return 0;
}

int
vcl2_event_poll_once (double timeout_s)
{
  vcl2_main_t *vm = &vcl2_main;
  svm_msg_q_msg_t msg;
  session_event_t *e;
  int et;

  if (!vm->app_event_queue)
    return -ENOTCONN;
  if (svm_msg_q_timedwait (vm->app_event_queue, timeout_s))
    return 0;
  if (svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
    return 0;
  e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
  et = e->event_type;
  VCL2_DBG ("event received: type=%u", et);
  svm_msg_q_free_msg (vm->app_event_queue, &msg);
  return et;
}

/*
 * connect：发 CONNECT，阻塞等 CONNECTED。等待时不持锁；每轮重取。
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
  mp->context = h;
  mp->wrk_index = vm->app_wrk_index;
  mp->is_ip4 = is_ip4;
  mp->proto = TRANSPORT_PROTO_TCP;
  mp->port = htons (port);
  if (ip)
    {
      /* ipv4 存在 ip46 offset 12（pad[3] 之后），必须用 set_ip4 */
      if (is_ip4)
	ip46_address_set_ip4 (&mp->ip, (const ip4_address_t *) ip);
      else
	clib_memcpy_fast (&mp->ip, ip, 16);
    }
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 CONNECTED：deadline 限定（VCL2_CTRL_TIMEOUT），每次唤醒彻底排空找目标。
   * CONNECTED 经 eventfd 即时到达，无短分片。*/
  {
    struct timespec t0;
    long deadline_ms;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 +
		  (long) (VCL2_CTRL_TIMEOUT * 1000);
    for (;;)
      {
	struct timespec now;
	long now_ms;
	double rem;
	svm_msg_q_msg_t msg;
	session_event_t *e;
	clock_gettime (CLOCK_MONOTONIC, &now);
	now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
	if (now_ms >= deadline_ms)
	  return -ETIMEDOUT;
	rem = (double) (deadline_ms - now_ms) / 1000.0;
	if (svm_msg_q_timedwait (vm->app_event_queue, rem))
	  continue;
	while (!svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	  {
	    e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
	    et = e->event_type;
	    if (et == SESSION_CTRL_EVT_CONNECTED)
	      {
		int crv;
		session_connected_msg_t *cm = (session_connected_msg_t *) e->data;
		svm_msg_q_free_msg (vm->app_event_queue, &msg);
		if (cm->retval)
		  return (int) cm->retval;
		/* 成功：写锁内 get-or-alloc + attach fifos */
		clib_rwlock_writer_lock (&vm->sessions_lock);
		vcl2_session_t *s = vcl2_session_get (h);
		if (!s)
		  s = vcl2_session_alloc (h);
		crv = s ? vcl2_session_attach_connected (s, cm) : -ENOMEM;
		clib_rwlock_writer_unlock (&vm->sessions_lock);
		return crv;
	      }
	    svm_msg_q_free_msg (vm->app_event_queue, &msg);
	    if (et < 0)
	      return et;
	  }
      }
  }
}

/*
 * send：写 tx_fifo + 通知 VPP。持读锁做 enqueue（指针稳定）；无空间则解锁、arm ntf、
 * 等（不持锁），下一轮重取。
 */
int
vcl2_session_send (vcl2_handle_t h, const void *buf, uint32_t len)
{
  vcl2_main_t *vm = &vcl2_main;
  int n;

  if (!len)
    return 0;
  for (;;)
    {
      int have_space;
      clib_rwlock_reader_lock (&vm->sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      if (!s || !s->tx_fifo)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return -EINVAL;
	}
      if (s->wr_shutdown)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return -EPIPE;
	}
      if (!vm->vpp_evt_q)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return -ENOTCONN;
	}
      have_space = svm_fifo_max_enqueue_prod (s->tx_fifo) >= (int) len;
      if (s->nonblocking && !have_space)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return -EAGAIN;
	}
      if (have_space)
	{
	  n = app_send_stream_raw (s->tx_fifo, vm->vpp_evt_q, (u8 *) buf, len,
				   SESSION_IO_EVT_TX, 1, 0);
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return n < 0 ? -EAGAIN : n;
	}
      /* 无空间：arm want-deq-ntf，解锁后【事件驱动】等——VPP 在 tx 空间释放时 signal
       * eventfd，timedwait 即时唤醒。无短分片。*/
      svm_fifo_add_want_deq_ntf (s->tx_fifo, SVM_FIFO_WANT_DEQ_NOTIF);
      clib_rwlock_reader_unlock (&vm->sessions_lock);
      svm_msg_q_timedwait (vm->app_event_queue, VCL2_BLOCK_TIMEOUT);
      vcl2_dispatch_app_events ();
    }
  return -EAGAIN;			/* not reached */
}

/*
 * recv：读 rx_fifo。持读锁 dequeue；空则解锁、等（不持锁），下一轮重取。
 */
int
vcl2_session_recv (vcl2_handle_t h, void *buf, uint32_t len)
{
  vcl2_main_t *vm = &vcl2_main;
  int n;

  for (;;)
    {
      uint8_t pc, nb;
      clib_rwlock_reader_lock (&vm->sessions_lock);
      vcl2_session_t *s = vcl2_session_get (h);
      if (!s || !s->rx_fifo)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return -EINVAL;
	}
      if (s->rd_shutdown)
	{
	  clib_rwlock_reader_unlock (&vm->sessions_lock);
	  return 0;
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
	return -EAGAIN;		/* 非阻塞 + 空：立即 EAGAIN */
      /* 【事件驱动】阻塞：VPP 在 RX 入队时 signal eventfd（无条件），timedwait 即时
       * 唤醒。无短分片、无忙等。peer 死→DISCONNECTED→dispatch 置 peer_closed→下一轮 EOF。*/
      svm_msg_q_timedwait (vm->app_event_queue, VCL2_BLOCK_TIMEOUT);
      vcl2_dispatch_app_events ();
    }
  return -ETIMEDOUT;			/* not reached */
}

/*
 * close：写锁内发控制消息（UNLISTEN/DISCONNECT）+ 移除缓存条目。控制消息发送非阻塞。
 */
int
vcl2_session_close (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;

  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s)
    {
      clib_rwlock_writer_unlock (&vm->sessions_lock);
      return -EINVAL;
    }
  VCL2_DBG ("close handle=%u vpp=0x%llx%s", s->handle,
	   (unsigned long long) s->vpp_handle, s->is_listener ? " (listener)" : "");
  if (s->is_listener)
    {
      if (vm->ctrl_mq)
	{
	  app_session_evt_t ae;
	  session_unlisten_msg_t *mp;
	  app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &ae, SESSION_CTRL_EVT_UNLISTEN);
	  mp = (session_unlisten_msg_t *) ae.evt->data;
	  memset (mp, 0, sizeof (*mp));
	  mp->client_index = vm->api_client_handle;
	  mp->context = h;
	  mp->wrk_index = vm->app_wrk_index;
	  mp->handle = s->vpp_handle;
	  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &ae);
	}
    }
  else if (s->vpp_handle && vm->vpp_evt_q)
    {
      app_session_evt_t ae;
      session_disconnect_msg_t *mp;
      app_alloc_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae, SESSION_CTRL_EVT_DISCONNECT);
      mp = (session_disconnect_msg_t *) ae.evt->data;
      memset (mp, 0, sizeof (*mp));
      mp->client_index = vm->api_client_handle;
      mp->handle = s->vpp_handle;
      app_send_ctrl_evt_to_vpp (vm->vpp_evt_q, &ae);
      VCL2_DBG ("DISCONNECT sent handle=%u vpp=0x%llx", h,
	       (unsigned long long) s->vpp_handle);
    }
  hash_unset (vm->handle_to_session, (uword) h);
  vec_free (s->accept_q);
  s->in_use = 0;
  s->rx_fifo = s->tx_fifo = 0;
  s->is_listener = 0;
  s->vpp_handle = 0;
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  return 0;
}

/*
 * shutdown：写锁内设标志 + 发 SHUTDOWN（SHUT_WR/RDWR）。SHUT_RD 纯本地。
 */
int
vcl2_session_shutdown (vcl2_handle_t h, int how)
{
  vcl2_main_t *vm = &vcl2_main;
  uint64_t vpp_handle;
  uint8_t send_shutdown;

  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s || s->is_listener)
    {
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

  if (send_shutdown && vpp_handle && vm->vpp_evt_q)
    {
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
int
vcl2_session_unlisten (vcl2_handle_t h)
{
  vcl2_main_t *vm = &vcl2_main;
  int rv = -EINVAL;

  clib_rwlock_writer_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (s && s->is_listener && vm->ctrl_mq)
    {
      app_session_evt_t ae;
      session_unlisten_msg_t *mp;
      app_alloc_ctrl_evt_to_vpp (vm->ctrl_mq, &ae, SESSION_CTRL_EVT_UNLISTEN);
      mp = (session_unlisten_msg_t *) ae.evt->data;
      memset (mp, 0, sizeof (*mp));
      mp->client_index = vm->api_client_handle;
      mp->context = h;
      mp->wrk_index = vm->app_wrk_index;
      mp->handle = s->vpp_handle;
      app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &ae);
      rv = 0;
    }
  clib_rwlock_writer_unlock (&vm->sessions_lock);
  return rv;
}

/*
 * listen：发 LISTEN，阻塞等 BOUND。BOUND 成功后写锁内记 vpp_handle+is_listener。
 */
int
vcl2_session_listen (vcl2_handle_t h, uint32_t q_len)
{
  vcl2_main_t *vm = &vcl2_main;
  app_session_evt_t app_evt;
  session_listen_msg_t *mp;
  int i;
  (void) q_len;

  if (!vm->ctrl_mq || !vm->app_event_queue)
    return -ENOTCONN;

  /* 取 lcl 地址（读锁）填 LISTEN 消息 */
  uint8_t lcl_is_ip4;
  uint8_t lcl_ip[16];
  uint16_t lcl_port;
  clib_rwlock_reader_lock (&vm->sessions_lock);
  vcl2_session_t *s = vcl2_session_get (h);
  if (!s)
    {
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
  if (lcl_is_ip4)
    {
      ip4_address_t ip4;
      memcpy (&ip4, lcl_ip, 4);
      ip46_address_set_ip4 (&mp->ip, &ip4);
    }
  else
    clib_memcpy_fast (&mp->ip, lcl_ip, 16);
  app_send_ctrl_evt_to_vpp (vm->ctrl_mq, &app_evt);

  /* 事件驱动等 BOUND：deadline 限定，每次唤醒彻底排空找目标。经 eventfd 即时到达。*/
  {
    struct timespec t0;
    long deadline_ms;
    clock_gettime (CLOCK_MONOTONIC, &t0);
    deadline_ms = (long) t0.tv_sec * 1000 + t0.tv_nsec / 1000000 +
		  (long) (VCL2_CTRL_TIMEOUT * 1000);
    for (;;)
      {
	struct timespec now;
	long now_ms;
	double rem;
	svm_msg_q_msg_t msg;
	session_event_t *e;
	clock_gettime (CLOCK_MONOTONIC, &now);
	now_ms = (long) now.tv_sec * 1000 + now.tv_nsec / 1000000;
	if (now_ms >= deadline_ms)
	  return -ETIMEDOUT;
	rem = (double) (deadline_ms - now_ms) / 1000.0;
	if (svm_msg_q_timedwait (vm->app_event_queue, rem))
	  continue;
	while (!svm_msg_q_sub (vm->app_event_queue, &msg, SVM_Q_NOWAIT, 0))
	  {
	    e = svm_msg_q_msg_data (vm->app_event_queue, &msg);
	    if (e->event_type == SESSION_CTRL_EVT_BOUND)
	      {
		session_bound_msg_t *bm = (session_bound_msg_t *) e->data;
		svm_msg_q_free_msg (vm->app_event_queue, &msg);
		if (bm->retval)
		  return (int) bm->retval;
		clib_rwlock_writer_lock (&vm->sessions_lock);
		vcl2_session_t *ls = vcl2_session_get (h);
		if (ls)
		  {
		    ls->vpp_handle = bm->handle;
		    ls->is_listener = 1;
		  }
		clib_rwlock_writer_unlock (&vm->sessions_lock);
		return ls ? 0 : -EINVAL;
	      }
	    svm_msg_q_free_msg (vm->app_event_queue, &msg);
	  }
      }
  }
}

/*
 * accept：取 listener accept_q 一个子 session。读锁内查 accept_q + 回 ACCEPTED_REPLY
 * （accept_q pop 改为写锁）；无则解锁、dispatch（内部加锁）、等。
 */
int
vcl2_session_accept (vcl2_handle_t listener_h, vcl2_handle_t * out)
{
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

  for (;;)
    {
      vcl2_handle_t ch = 0;
      int got = 0;
      clib_rwlock_writer_lock (&vm->sessions_lock);	/* pop accept_q + 回 reply */
      s = vcl2_session_get (listener_h);
      if (s && vec_len (s->accept_q) > 0)
	{
	  ch = s->accept_q[0];
	  vec_delete (s->accept_q, 1, 0);
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
	    }
	  got = 1;
	}
      clib_rwlock_writer_unlock (&vm->sessions_lock);
      if (got)
	{
	  *out = ch;
	  return 0;
	}
      /* 事件驱动等 ACCEPTED：经 eventfd 即时到达，dispatch 入 accept_q，下一轮 pop。*/
      svm_msg_q_timedwait (vm->app_event_queue, VCL2_BLOCK_TIMEOUT);
      vcl2_dispatch_app_events ();
    }
  return -EAGAIN;			/* not reached */
}

/*
 * dispatch：排空 app_event_queue（sub 原子）。ADD_SEGMENT→segment attach（内部 segment
 * 写锁）；DISCONNECTED→写锁设 peer_closed + 回 DISCONNECTED_REPLY；ACCEPTED→写锁内建子
 * session + attach fifos + 入 accept_q。
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
      if (e->event_type == SESSION_CTRL_EVT_APP_ADD_SEGMENT)
	{
	  session_app_add_segment_msg_t *sm =
	    (session_app_add_segment_msg_t *) e->data;
	  int fd = -1;
	  if (sm->fd_flags)
	    vcl2_sapi_recv_fd (&fd);
	  int rv = vcl2_segment_attach (sm->segment_handle,
					(char *) sm->segment_name, fd);
	  VCL2_DBG ("ADD_SEGMENT handle=0x%llx fd=%d -> %d",
		   (unsigned long long) sm->segment_handle, fd, rv);
	}
      else if (e->event_type == SESSION_CTRL_EVT_DISCONNECTED)
	{
	  session_disconnected_msg_t *dm = (session_disconnected_msg_t *) e->data;
	  clib_rwlock_writer_lock (&vm->sessions_lock);
	  vcl2_session_t *ds = vcl2_session_get_by_vpp_handle (dm->handle);
	  if (ds)
	    {
	      ds->peer_closed = 1;
	      VCL2_DBG ("DISCONNECTED handle=%u vpp=0x%llx (peer_closed)",
		       ds->handle, (unsigned long long) dm->handle);
	    }
	  clib_rwlock_writer_unlock (&vm->sessions_lock);
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
      else if (e->event_type == SESSION_CTRL_EVT_ACCEPTED)
	{
	  session_accepted_msg_t *am = (session_accepted_msg_t *) e->data;
	  clib_rwlock_writer_lock (&vm->sessions_lock);
	  vcl2_session_t *ls = vcl2_session_get_by_vpp_handle (am->listener_handle);
	  if (ls)
	    {
	      vcl2_handle_t lh = ls->handle;
	      vcl2_handle_t nh = ++vcl2_next_handle;
	      vcl2_session_t *ns = vcl2_session_alloc (nh);
	      if (ns)
		{
		  ns->accept_context = am->context;
		  ns->rmt_is_ip4 = am->rmt.is_ip4;
		  if (am->rmt.is_ip4)
		    memcpy (ns->rmt_ip, &am->rmt.ip.ip4, 4);
		  else
		    memcpy (ns->rmt_ip, &am->rmt.ip, 16);
		  ns->rmt_port = am->rmt.port;
		  if (!vcl2_session_attach_fifos
		      (ns, am->handle, am->segment_handle, am->server_rx_fifo,
		       am->server_tx_fifo, am->vpp_event_queue_address,
		       am->mq_index))
		    {
		      vcl2_session_t *ls2 = vcl2_session_get (lh);	/* 重取（vec 可能已移动） */
		      if (ls2)
			vec_add1 (ls2->accept_q, nh);
		      VCL2_DBG ("child=%u attached, queued to listener=%u", nh,
			       lh);
		    }
		  else
		    VCL2_DBG ("accept fifo attach failed");
		}
	    }
	  else
	    VCL2_DBG ("ACCEPTED no listener vpp=0x%llx",
		     (unsigned long long) am->listener_handle);
	  clib_rwlock_writer_unlock (&vm->sessions_lock);
	}
      svm_msg_q_free_msg (mq, &msg);
    }
}
