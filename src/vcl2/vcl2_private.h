/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 内部结构。vcl2_main_t 无 sessions pool/私有堆，只有 SAPI socket + app index
 * + 段映射表 + 可丢弃缓存。
 */

#ifndef included_vcl2_private_h
#define included_vcl2_private_h

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <vppinfra/hash.h>
#include <vppinfra/vec.h>
#include <vppinfra/lock.h>
#include <vppinfra/socket.h>
#include <svm/fifo_segment.h>
#include <svm/message_queue.h>
#include <svm/svm_fifo.h>
#include "vcl2.h"

/* 调试开关：env VCL2_DEBUG=1 打开（默认关）。所有 VCL2_DBG/LDP2_DBG 受此门控，
 * 避免拦截每个 socket 调用时无条件 fprintf 的开销与日志污染。定义在 vcl2.c。*/
extern int vcl2_debug;
#define VCL2_DBG(...)                                                          \
  do {                                                                         \
    if (vcl2_debug) {                                                          \
      fprintf (stderr, "vcl2<%d>: ", (int) getpid ());                         \
      fprintf (stderr, __VA_ARGS__);                                           \
      fprintf (stderr, "\n");                                                  \
    }                                                                          \
  } while (0)

/* 默认 SAPI socket 路径（VPP 实际创建处） */
#define VCL2_SAPI_SOCKET_DEFAULT "/run/vpp/app_ns_sockets/default"

/* 合成 fd 编码（LDP2 用）：fd = base + handle_index。仅 app 侧可丢弃缓存的 key */
#define VCL2_FD_BASE_DEFAULT 256

/* 默认配置（镜像 vcl 合理默认；可被 env/VCL2_CONFIG 覆盖，P1 用默认） */
#define VCL2_RX_FIFO_SIZE_DEFAULT (4 << 20)
#define VCL2_TX_FIFO_SIZE_DEFAULT (4 << 20)
#define VCL2_SEGMENT_SIZE_DEFAULT (4 << 26)
#define VCL2_EVT_QUEUE_SIZE_DEFAULT 100000

/**
 * 一个【VPP-owned】段的 app 侧映射。app 只 mmap 了它，不拥有。
 * 进程退出内核自动 munmap；段由 VPP 分配/回收。
 */
typedef struct {
  int fd;          /* 段 fd（SCM_RIGHTS 收到） */
  void *base;      /* mmap 基址（P2） */
  uint64_t size;   /* mmap 大小 */
  uint64_t handle; /* VPP 段 handle */
  uint8_t in_use;
} vcl2_segment_t;

/**
 * 可丢弃的 session 缓存条目。
 *
 * 注意（单侧所有权）：这里【不】持有任何需要跨进程释放的资源——rx_fifo/tx_fifo
 * 与 vpp_evt_q 都是指向【VPP-owned 段内】的指针（app 只 mmap 了段，映射进程死由
 * 内核回收）。本结构只是把这些指针 + correlation key 放进进程本地可丢弃缓存，
 * 供 recv/send 直接解引用。fork 子进程丢弃重建；进程死即随进程消失。无 free 协议。
 */
typedef struct {
  vcl2_handle_t
    handle; /* app 侧 correlation key（= connect/listen 的 context） */
  uint64_t vpp_handle;        /* session_handle_t（VPP 返回） */
  uint32_t vpp_session_index; /* handle 低 32 位（IO 事件回填用） */
  svm_fifo_t *rx_fifo;        /* 读：peer→app 数据（VPP-owned 段内指针） */
  svm_fifo_t *tx_fifo;        /* 写：app→peer 数据（VPP-owned 段内指针） */
  uint8_t in_use;
  uint8_t
    nonblocking; /* fcntl(F_SETFL O_NONBLOCK)/ioctl(FIONBIO) 设置：recv/send 无数据/空间时返回 EAGAIN */
  uint8_t
    peer_closed; /* 收到 DISCONNECTED：recv 排空后返回 0(EOF)，epoll/poll 报 EPOLLIN */
  uint8_t
    rd_shutdown; /* shutdown(SHUT_RD/RDWR)：app 不再读，recv 返回 0(EOF) */
  uint8_t
    wr_shutdown; /* shutdown(SHUT_WR/RDWR)：app 不再写，send 返回 -EPIPE；并已发 SHUTDOWN 给 VPP */
  /* 控制面完成标志（dispatch 处理 CONNECTED/BOUND 后置位；connect/listen 轮询它）。
   * 避免 connect/listen 内联 drain 丢弃其它事件——统一由 dispatch 处理全部类型。*/
  uint8_t ctrl_done; /* 0=等待中；1=CONNECTED/BOUND 已到（见 ctrl_rv） */
  int ctrl_rv;       /* CONNECTED/BOUND 结果（retval 或 -errno） */
  /* server 侧（listen/accept） */
  uint8_t is_listener; /* BOUND 后置位 */
  uint8_t is_dgram;    /* UDP：recv 用 app_recv_dgram_raw */
  uint8_t is_tls;      /* TLS：connect/listen 用 TRANSPORT_PROTO_TLS + 挂 ext_config */
  uint8_t lcl_is_ip4;  /* bind() 存的本地地址，listen() 用 */
  uint8_t lcl_ip[16];
  uint16_t lcl_port;
  uint8_t rmt_is_ip4; /* ACCEPTED 的对端地址，accept() 回填 sockaddr 用 */
  uint8_t rmt_ip[16];
  uint16_t rmt_port;
  vcl2_handle_t
    *accept_q; /* 已 ACCEPTED、待 accept() 取走的新 session handle 队列 */
  uint32_t
    accept_context; /* 子 session 的 ACCEPTED.context，回 ACCEPTED_REPLY 用 */
} vcl2_session_t;

/**
 * vcl2 全局状态。刻意极小 —— 没有 app 拥有的资源。
 */
typedef struct {
  /* 配置 */
  char *app_name;
  char *sapi_socket_path;
  uint32_t fd_base;
  uint64_t rx_fifo_size, tx_fifo_size, segment_size, evt_queue_size;
  uint8_t use_mq_eventfd;

  /* 控制面：唯一控制通道（SAPI UDS, SOCK_SEQPACKET） */
  clib_socket_t sapi_sock;
  uint8_t sapi_connected;
  pthread_mutex_t sapi_lock; /* 保护 sapi_sock 的 send/recv 配对（防并发乱序） */
  uint32_t app_index;     /* VPP 分配（attach reply） */
  uint32_t app_wrk_index; /* 当前进程 app-worker（worker-add reply） */
  uint32_t api_client_handle;
  pid_t pid;

  /* 段管理（VPP-owned，app 经 fifo_segment_attach 只 mmap，不拥有） */
  fifo_segment_main_t segment_main;
  uword *segment_table; /* hash: segment_handle -> fifo_segment index */
  svm_msg_q_t *ctrl_mq; /* VPP 控制 mq（session listen/connect 等请求） */
  svm_msg_q_t *app_event_queue; /* VPP 投递事件给 app 的 mq（app 轮询） */
  svm_msg_q_t *
    vpp_evt_q; /* 每 VPP-worker 的 RX mq：send 后通知 VPP 取 tx_fifo（CONNECTED 回填） */
  uint64_t segment_handle; /* 主 memfd 段 handle（attach reply） */

  /* 可丢弃 session 缓存（recv/send 用，非资源所有权） */
  vcl2_session_t *sessions;
  uword *handle_to_session; /* hash: handle -> sessions[] index */
  uword *vpp_handle_to_session; /* hash: vpp_handle -> sessions[] index（dispatch 查找用）*/

  /* 数据面映射（VPP-owned，app 只 mmap） */
  vcl2_segment_t *segments;

  /* 状态 */
  uint8_t is_init;
  /* transparent_tls：VPP 终止 TLS。cert/key 经 SAPI 注册一次，ckpair_index 全局缓存；
   * connect/listen 时据此挂 ext_config。fork 子进程清 loaded 重新注册。 */
  uint8_t tls_enabled;     /* env VCL2_TRANSPARENT_TLS=1 */
  uint8_t tls_cert_loaded; /* ckpair_index 已注册 */
  uint32_t tls_ckpair_index;
  const char *tls_cert_file;
  const char *tls_key_file;
  /* close(listener) 同步等 UNLISTEN_REPLY 用：unlisten_ctx=正在等的 vcl2 handle(~0=无) */
  uint32_t unlisten_ctx;
  uint8_t unlisten_done;
  /* 多线程锁（方案 C：单一共享 worker + 锁）。所有被锁结构都是【可丢弃缓存】，
   * 非跨进程资源 —— 单侧所有权不变。
   *  - segment_table_lock：保护 segment_table hash + segment_main（段映射）
   *  - sessions_lock：保护 sessions vec + handle_to_session hash（session 缓存） */
  clib_rwlock_t segment_table_lock;
  clib_rwlock_t sessions_lock;
  /* 多线程 mq 串行化锁：app_event_queue 的唯一守卫。
   * 锁序：app_mq_lock（外）→ sessions_lock（内）。vcl2_mq_wait_dispatch 持 app_mq_lock
   * 期间在 drain 分支取 sessions_lock(W)。单向锁序，无环、无死锁。 */
  pthread_mutex_t app_mq_lock;
} vcl2_main_t;

extern vcl2_main_t vcl2_main;

#define VCL2_INVALID_SEGMENT_HANDLE ((u64) ~0)
/* VPP worker-mq-segment 的 handle 约定（镜像 vcl_vpp_worker_segment_handle） */
#define VCL2_VPP_WRK_SEG_HANDLE(wrk)                                           \
  (VCL2_INVALID_SEGMENT_HANDLE - (u64) (wrk) - 1)
#define VCL2_INVALID_SEG_INDEX ((u32) ~0)

/* 内部 helper */
int vcl2_sapi_connect (void);
int vcl2_app_attach_locked (void);
int vcl2_worker_register_locked (void);
int vcl2_sapi_recv_fd (int *fd);
void vcl2_atfork_prepare (void); /* fork 前：acquire 所有锁，保证 child 继承干净状态 */
void vcl2_atfork_parent (void);  /* fork 后父进程：释放锁 + unlisten */
void vcl2_atfork_child (void);   /* fork 后子进程：释放锁 + 重建 worker 身份 */
void vcl2_atfork_parent (
  void); /* fork 父进程（master）：从 accept 转发摘除自己 */
int vcl2_session_unlisten (
  vcl2_handle_t h); /* 把本 worker 移出 listener 的 accept 轮转 */
int vcl2_handle_to_fd (vcl2_handle_t h);
vcl2_handle_t vcl2_fd_to_handle (int fd);
/* 段管理（vcl2_segment.c） */
int vcl2_segment_attach (u64 handle, char *name, int fd);
u32 vcl2_segment_lookup (u64 handle);
int vcl2_segment_attach_mq (u64 handle, uword offset, u32 idx,
                            svm_msg_q_t **mq);
svm_fifo_t *vcl2_segment_alloc_fifo (u64 handle, uword offset);
/* 在已映射段内分配任意 chunk（ext_config 用），返回 chunk 偏移。 */
int vcl2_segment_alloc_chunk (u64 handle, u32 slice, u32 size, uword *offset,
                              svm_fifo_chunk_t **chunk);

/* transparent_tls（vcl2.c）：cert/key 经 SAPI 注册 VPP，拿 ckpair_index。 */
int vcl2_tls_add_cert_key_pair (const char *cert, uint32_t cert_len,
                                const char *key, uint32_t key_len);
/* 懒加载：首次 TLS connect/listen 前调用，读文件 + 注册 + 缓存 index。 */
int vcl2_tls_ensure_cert (void);

/* session 缓存（vcl2_session.c） */
vcl2_session_t *vcl2_session_get (vcl2_handle_t h);
/* recvfrom：recv + 回填源地址 sockaddr（UDP per-packet / TCP 对端）。*/
int vcl2_session_recvfrom (vcl2_handle_t h, void *buf, uint32_t len,
                           struct sockaddr *addr, socklen_t *addr_len);
vcl2_session_t *vcl2_session_alloc (vcl2_handle_t h);
vcl2_session_t *vcl2_session_get_by_vpp_handle (u64 vpp_handle);
int vcl2_session_attach_fifos (vcl2_session_t *s, u64 vpp_handle, u64 seg,
                               uword rxf_off, uword txf_off, uword vpp_eq_off,
                               u32 mq_index);
/* 排空 app_event_queue 并按类型分发（ldp2 epoll_wait / accept 共用）。
 * ACCEPTED 事件建新 session 入对应 listener 的 accept_q。
 * 非阻塞：timedwait(0)，立即排空当前在队事件。*/
void vcl2_dispatch_app_events (void);
/* 阻塞"等+排空+分发"：app_mq_lock 内 timedwait(timeout)+drain，锁外 process。
 * - 数据路径（recv/send）：timeout=0.001（1ms 有界重查，防丢唤醒）。
 * - 非阻塞 drain：timeout=0。
 * 锁序：mq_lock 与 sessions_lock 永不重叠。*/
void vcl2_mq_wait_dispatch (double timeout_s);

#endif /* included_vcl2_private_h */
