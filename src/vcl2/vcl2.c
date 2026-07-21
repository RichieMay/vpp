/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 核心入口（P0 骨架 + P1 SAPI 控制面）。
 *
 * 对比原 VCL（src/vcl/vppcom.c）：本文件【不】分配 sessions pool、accept_evts_fifo、
 * 私有堆。vcl2_init 只读配置；vcl2_app_attach 经 SAPI(SEQPACKET) 复用 app-socket-api
 * 消息完成 app 注册 + worker-0 注册，拿到 app_index / app_wrk_index，并暂存 VPP 返回的
 * 段 fd（P2 mmap）。进程死 = 关 sapi_sock（+ 内核 munmap 映射段 + VPP barrier 单侧回收），
 * 无 app 侧多步 teardown。
 *
 * P1 复用 src/vcl/vcl_sapi.c 的同一协议（app_sapi_msg_t），只是客户端更薄。
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
int
vcl2_handle_to_fd (vcl2_handle_t h)
{
  if (!VCL2_HANDLE_IS_VALID (h))
    return -EINVAL;
  return (int) vcl2_main.fd_base + (int) h;
}

vcl2_handle_t
vcl2_fd_to_handle (int fd)
{
  if (fd < (int) vcl2_main.fd_base)
    return VCL2_INVALID_HANDLE;
  return (vcl2_handle_t) (fd - (int) vcl2_main.fd_base);
}

int
vcl2_is_init (void)
{
  return vcl2_main.is_init;
}

/* ---------- SAPI UDS 连接（SEQPACKET client，复用 clib_socket） ---------- */
int
vcl2_sapi_connect (void)
{
  vcl2_main_t *vm = &vcl2_main;
  clib_socket_t *cs = &vm->sapi_sock;
  clib_error_t *err;

  if (vm->sapi_connected)
    return 0;

  memset (cs, 0, sizeof (*cs));
  cs->config = vm->sapi_socket_path;
  cs->flags = CLIB_SOCKET_F_IS_CLIENT | CLIB_SOCKET_F_SEQPACKET |
	      CLIB_SOCKET_F_BLOCKING;

  err = clib_socket_init (cs);
  if (err)
    {
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
int
vcl2_app_attach_locked (void)
{
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t msg, rmp;
  app_sapi_attach_msg_t *mp = &msg.attach;
  app_sapi_attach_reply_msg_t *rp = &rmp.attach_reply;
  clib_error_t *err;
  int fds[8] = { 0 };
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
  err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
  if (err)
    {
      clib_error_free (err);
      return -EIO;
    }

  memset (&rmp, 0, sizeof (rmp));
  err = clib_socket_recvmsg (&vm->sapi_sock, &rmp, sizeof (rmp), fds,
			     ARRAY_LEN (fds));
  if (err)
    {
      clib_error_free (err);
      return -EIO;
    }
  if (rmp.type != APP_SAPI_MSG_TYPE_ATTACH_REPLY)
    {
      VCL2_DBG ("attach: unexpected reply type %d", (int) rmp.type);
      return -EPROTO;
    }
  if (rp->retval)
    {
      VCL2_DBG ("attach failed: retval %d", rp->retval);
      return -EINVAL;
    }

  vm->app_index = rp->app_index;
  vm->api_client_handle = rp->api_client_handle;
  vm->segment_handle = rp->segment_handle;

  /* 处理 VPP 返回的段 fd + mq 地址（镜像 vcl_api_attach_reply_handler） */
  if (rp->n_fds == 0)
    {
      VCL2_DBG ("attach: reply carried no fds");
      return -ENODATA;
    }
  if (rp->fd_flags & SESSION_FD_F_VPP_MQ_SEGMENT)
    if (vcl2_segment_attach (VCL2_VPP_WRK_SEG_HANDLE (0), "vpp-mq-seg", fds[n++]))
      return -EINVAL;
  if (rp->fd_flags & SESSION_FD_F_MEMFD_SEGMENT)
    {
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
  if (rp->fd_flags & SESSION_FD_F_MQ_EVENTFD)
    svm_msg_q_set_eventfd (vm->app_event_queue, fds[n++]);
  /* VPP 控制 mq：session listen/connect 等请求经它 */
  if (vcl2_segment_attach_mq (VCL2_VPP_WRK_SEG_HANDLE (0), rp->vpp_ctrl_mq,
			      rp->vpp_ctrl_mq_thread, &vm->ctrl_mq))
    return -EINVAL;

  VCL2_DBG ("attached: app_index=%u seg=%lu app_eq=%p ctrl_mq=%p",
	    vm->app_index, (unsigned long) vm->segment_handle,
	    vm->app_event_queue, vm->ctrl_mq);

  /* 镜像 VCL：attach 已在 VPP 侧隐式创建 worker 0（vnet_application_attach 内
   * application_alloc_worker_and_init）。本进程（master / 单进程 app）直接复用
   * worker 0，【不】再显式 ADD_DEL_WORKER——否则会创建第二个 worker，而 attach 创建
   * 的 worker 0 永不被 UDS 检测回收（sapi_socket_detach 只摘 aah_app_wrk_index 指向的
   * 那个 worker）→ application_n_workers 永不为 0 → app entry 泄漏（2026-07-21 P6 根因）。
   * 只有 fork 出来的子进程才需要显式 add（vcl2_atfork_child→vcl2_worker_register_locked）。
   *
   * 单侧所有权一致：app 退出【不】发任何清理消息（违背设计）；VPP 靠 SAPI UDS close
   * 单侧回收。master 用 worker 0 后，其 socket 的 aah_app_wrk_index 在 VPP attach 路径
   * 设置，close 时被正确 detach。*/
  vm->app_wrk_index = 0;
  {
    /* 一次性注册 fork handler：子进程经 vcl2_atfork_child 重建 worker 身份 */
    static int atfork_done;
    if (!atfork_done)
      {
	pthread_atfork (0, vcl2_atfork_parent, vcl2_atfork_child);
	atfork_done = 1;
      }
  }
  return 0;
}

/* ---------- P1：worker 注册（fork 子进程经 atfork child handler 也调它） ---------- */
int
vcl2_worker_register_locked (void)
{
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t msg, rmp;
  app_sapi_worker_add_del_msg_t *wp = &msg.worker_add_del;
  app_sapi_worker_add_del_reply_msg_t *rp = &rmp.worker_add_del_reply;
  clib_error_t *err;
  int fds[8] = { 0 };
  int n = 0, i;

  if (!vm->sapi_connected)
    return -ENOTCONN;

  memset (&msg, 0, sizeof (msg));
  wp->app_index = vm->app_index;
  wp->wrk_index = vm->app_wrk_index;	/* 0 = 新 worker */
  wp->is_add = 1;

  msg.type = APP_SAPI_MSG_TYPE_ADD_DEL_WORKER;
  err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
  if (err)
    {
      clib_error_free (err);
      return -EIO;
    }

  memset (&rmp, 0, sizeof (rmp));
  err = clib_socket_recvmsg (&vm->sapi_sock, &rmp, sizeof (rmp), fds,
			     ARRAY_LEN (fds));
  if (err)
    {
      clib_error_free (err);
      return -EIO;
    }
  if (rmp.type != APP_SAPI_MSG_TYPE_ADD_DEL_WORKER_REPLY)
    {
      VCL2_DBG ("worker-add: unexpected reply type %d", (int) rmp.type);
      return -EPROTO;
    }
  if (rp->retval)
    {
      VCL2_DBG ("worker-add failed: retval %d", rp->retval);
      return -EINVAL;
    }
  if (!rp->is_add)
    {
      VCL2_DBG ("worker-add: reply is not an add");
      return -EINVAL;
    }

  vm->app_wrk_index = rp->wrk_index;
  vm->api_client_handle = rp->api_client_handle;
  /* ctrl_mq 复用 attach 级别的（与 vcl 一致：worker reply 不重置 ctrl_mq） */

  if (rp->segment_handle == VCL2_INVALID_SEGMENT_HANDLE)
    {
      VCL2_DBG ("worker-add: invalid segment handle");
      return -EINVAL;
    }
  if (!rp->n_fds)
    {
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
  if (rp->fd_flags & SESSION_FD_F_MEMFD_SEGMENT)
    {
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
  if (rp->fd_flags & SESSION_FD_F_MQ_EVENTFD)
    svm_msg_q_set_eventfd (vm->app_event_queue, fds[n++]);

  VCL2_DBG ("worker registered: wrk_index=%u client=%u seg=%lu app_eq=%p "
	    "(n_fds=%u, used=%d)", vm->app_wrk_index, vm->api_client_handle,
	    (unsigned long) rp->segment_handle, vm->app_event_queue,
	    rp->n_fds, n);
  return 0;

failed:
  for (i = clib_max (n - 1, 0); i < rp->n_fds; i++)
    close (fds[i]);
  return -EINVAL;
}

/*
 * fork 子进程的 atfork child handler。
 *
 * 关键（单侧所有权 + fd继承/身份不继承）：fork 后子进程【继承】了父进程的
 * vcl2_main 内存——包括 app_index、ctrl_mq（app 级、MAP_SHARED 段映射，子进程
 * 共享有效）、sessions 缓存（含 listener，子进程可直接 accept）。但子进程的
 * SAPI 连接是父进程的 app-worker 身份，必须【重建身份】：开自己的 SAPI、向 VPP
 * 注册为一个新 worker（同 app）→ 拿到自己的 app_event_queue / vpp_evt_q / wrk_index。
 *
 * 在 fork() 返回给 app 前（pthread_atfork child 回调）跑完 → nginx worker 看到就绪的 vcl2。
 */
void
vcl2_atfork_child (void)
{
  vcl2_main_t *vm = &vcl2_main;
  int rv;

  if (!vm->is_init)
    return;

  vm->pid = getpid ();

  /* 关掉继承来的父进程 SAPI 连接（子进程的 fd 副本），开自己的 */
  if (vm->sapi_connected)
    {
      clib_socket_close (&vm->sapi_sock);
      vm->sapi_connected = 0;
    }

  /* 重置 per-worker 状态（ctrl_mq 是 app 级，保留；app_index/config/segments/
   * sessions 缓存保留——子进程复用继承的 listener）。 */
  vm->app_event_queue = 0;
  vm->vpp_evt_q = 0;
  vm->app_wrk_index = 0;
  vm->api_client_handle = 0;

  if ((rv = vcl2_sapi_connect ()))
    {
      VCL2_DBG ("atfork child: sapi reconnect failed: %d", rv);
      return;
    }
  if ((rv = vcl2_worker_register_locked ()))
    {
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
    for (i = 0; i < vec_len (vm->sessions); i++)
      {
	vcl2_session_t *s = &vm->sessions[i];
	if (s->in_use && s->is_listener)
	  {
	    int lrv = vcl2_session_listen (s->handle, 0);
	    VCL2_DBG ("atfork child: re-listen handle=%u -> %d", s->handle, lrv);
	  }
      }
  }
}

/*
 * fork 后在【父进程（master）】跑：nginx master 自己不 accept（只管理 worker），
 * 但它创建了 listener、在 accept 轮转位图 al->workers 里。若不摘出，VPP 会把
 * ACCEPTED 轮给 master，而 master 不排空 app_event_queue → 连接卡死。
 * 故 master 把自己从所有 listener 的 al->workers 摘除（unlisten），只留 worker 接受。
 * （worker 已在 child handler 里 re-listen 加入位图；pthread_atfork child 先于 parent 跑。）
 */
void
vcl2_atfork_parent (void)
{
  vcl2_main_t *vm = &vcl2_main;
  u32 i;
  if (!vm->is_init)
    return;
  for (i = 0; i < vec_len (vm->sessions); i++)
    if (vm->sessions[i].in_use && vm->sessions[i].is_listener)
      vcl2_session_unlisten (vm->sessions[i].handle);
}

/* recvmsg 一个 fd（SCM_RIGHTS）从 SAPI socket。VPP 在投 APP_ADD_SEGMENT 事件到
 * app_event_queue 之前，先经 SAPI socket sendmsg 把段 fd 推过来（空 app_sapi_msg_t +
 * 附属 fd）。处理 ADD_SEGMENT 事件时调本函数取 fd。 */
int
vcl2_sapi_recv_fd (int *out_fd)
{
  vcl2_main_t *vm = &vcl2_main;
  app_sapi_msg_t dummy;
  clib_error_t *err;
  int fds[1] = { 0 };

  if (!vm->sapi_connected)
    return -ENOTCONN;
  err = clib_socket_recvmsg (&vm->sapi_sock, &dummy, sizeof (dummy), fds,
			     ARRAY_LEN (fds));
  if (err)
    {
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

static int
vcl2_heap_alloc (void)
{
  void *mem, *heap;
  mem = mmap (0, VCL2_HEAP_SIZE, PROT_READ | PROT_WRITE,
	      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mem == MAP_FAILED)
    return -ENOMEM;
  heap = clib_mem_init (mem, VCL2_HEAP_SIZE);
  if (!heap)
    {
      munmap (mem, VCL2_HEAP_SIZE);
      vcl2_heap_base = 0;
      return -ENOMEM;
    }
  vcl2_heap_base = mem;
  return 0;
}

int
vcl2_init (const char *app_name)
{
  vcl2_main_t *vm = &vcl2_main;
  const char *s;
  int rv;

  if (vm->is_init)
    return 0;

  /* 先建 vppinfra 工作堆（clib_socket / vec 等内部需要） */
  if ((rv = vcl2_heap_alloc ()))
    return rv;

  memset (vm, 0, sizeof (*vm));
  pthread_mutex_init (&vm->lock, 0);
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

  /* 配置默认（P1 用默认；后续可读 VCL2_CONFIG） */
  vm->rx_fifo_size = VCL2_RX_FIFO_SIZE_DEFAULT;
  vm->tx_fifo_size = VCL2_TX_FIFO_SIZE_DEFAULT;
  vm->segment_size = VCL2_SEGMENT_SIZE_DEFAULT;
  vm->evt_queue_size = VCL2_EVT_QUEUE_SIZE_DEFAULT;
  vm->use_mq_eventfd = 1;

  /* 段管理器初始化（VPP-owned 段经 fifo_segment_attach 接入） */
  fifo_segment_main_init (&vm->segment_main, (u64) ~0, 20 /* timeout s */);

  vm->is_init = 1;
  VCL2_DBG ("init: app=%s sapi=%s", vm->app_name, vm->sapi_socket_path);
  return 0;
}

int
vcl2_app_attach (void)
{
  int rv;
  if (!vcl2_main.is_init)
    return -EINVAL;
  pthread_mutex_lock (&vcl2_main.lock);
  rv = vcl2_app_attach_locked ();
  pthread_mutex_unlock (&vcl2_main.lock);
  return rv;
}

int
vcl2_worker_register (void)
{
  int rv;
  if (!vcl2_main.is_init)
    return -EINVAL;
  pthread_mutex_lock (&vcl2_main.lock);
  rv = vcl2_worker_register_locked ();
  pthread_mutex_unlock (&vcl2_main.lock);
  return rv;
}

void
vcl2_destroy (void)
{
  vcl2_main_t *vm = &vcl2_main;
  if (!vm->is_init)
    return;

  /* 正常优雅 detach（app 显式调用时）：向 VPP 发 ADD_DEL_WORKER(is_add=0) 回收本 worker。
   * VPP 收到后经 worker barrier 回收该 worker 的全部 session/fifo/listener/segment。
   * 这是【正常操作】的控制消息；异常退出（kill -9/segfault）不会跑到这里，由 VPP 经
   * SAPI UDS close 单侧兜底回收（已 P6 验证）。两条路径都回收，互不依赖。*/
  if (vm->sapi_connected)
    {
      app_sapi_msg_t msg;
      app_sapi_worker_add_del_msg_t *mp = &msg.worker_add_del;
      clib_error_t *err;
      memset (&msg, 0, sizeof (msg));
      msg.type = APP_SAPI_MSG_TYPE_ADD_DEL_WORKER;
      mp->app_index = vm->app_index;
      mp->wrk_index = vm->app_wrk_index;
      mp->is_add = 0;
      err = clib_socket_sendmsg (&vm->sapi_sock, &msg, sizeof (msg), 0, 0);
      if (err)
	clib_error_free (err);
      clib_socket_close (&vm->sapi_sock);
      vm->sapi_connected = 0;
    }

  /* 丢弃 app 侧可丢弃缓存（进程本地；即便不 free，进程退出内核也会回收）*/
  vec_free (vm->segments);
  vec_free (vm->fd_cache);
  vec_free (vm->sessions);
  hash_free (vm->handle_to_cache_index);
  hash_free (vm->handle_to_session);
  free (vm->app_name);
  free (vm->sapi_socket_path);
  pthread_mutex_destroy (&vm->lock);
  memset (vm, 0, sizeof (*vm));
}

/* ---------- session 操作（vcl2_session_connect / create / send / recv / close /
 * listen / accept / event_poll_once / dispatch 见 vcl2_session.c）。 ---------- */
