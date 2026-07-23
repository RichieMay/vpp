/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 — 单侧所有权的 App 接入库（公共 API）。
 *
 * 设计原则：VPP 拥有全部数据面资源；app 侧零拥有（只有可丢弃缓存 + VPP-owned 段映射）。
 * 详见 README.md 与 plan 文件 vpp-vcl2-single-ownership.md。
 */

#ifndef included_vcl2_h
#define included_vcl2_h

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * vcl2 句柄 —— 不透明。LDP2 会把它编码进合成 fd 给 app。
 * 它只是 app 侧可丢弃缓存里的一个 key；真正的 session/fifo 资源由 VPP 拥有。
 */
typedef uint32_t vcl2_handle_t;
#define VCL2_INVALID_HANDLE ((vcl2_handle_t) ~0)
#define VCL2_HANDLE_IS_VALID(h) ((h) != VCL2_INVALID_HANDLE)

/* 协议（与 session 层一致） */
typedef enum {
  VCL2_PROTO_TCP = 0,
  VCL2_PROTO_UDP,
} vcl2_proto_t;

/**
 * 庝始化 vcl2：读配置（env / VCL2_CONFIG），连接 SAPI（默认
 * /run/vpp/app_ns_sockets/default）。失败返回负 errno。
 * 多次调用幂等（已初始化则直接返回 0）。
 */
int vcl2_init (const char *app_name);

/**
 * 向 VPP 注册 app + 当前进程作为 app-worker 0。拿到 app_index / app_wrk_index。
 * （P1 实现：发送 SAPI attach + worker-add 消息。）
 */
int vcl2_app_attach (void);

/**
 * （重新）注册当前进程为 app-worker。
 * - 首次：由 vcl2_app_attach 内部调用。
 * - fork 后的子进程：由 pthread_atfork child handler 调用 —— 子进程是独立 PID，
 *   必须向 VPP 重新注册为自己的 app-worker（身份不继承），并重建可丢弃缓存。
 * 这是"fd 继承、身份不继承"分离机制的核心。
 */
int vcl2_worker_register (void);

/**
 * 析构 —— 【正常优雅 detach】（app 显式调用）：向 VPP 发 ADD_DEL_WORKER(is_add=0)，
 * VPP 在 worker barrier 下回收本 worker 的全部 session/fifo/listener/segment。
 *
 * 设计边界：这是【正常操作】路径的控制消息。异常退出（kill -9 / segfault）不会跑到
 * 这里 —— 那种情况下由 VPP 经 SAPI UDS close 单侧兜底回收（已验证：app 死 → VPP 回收，
 * 无 app 侧消息）。两条路径都回收，互不依赖；本函数即使没跑到也不泄漏。
 */
void vcl2_destroy (void);

/* ---- session 操作 ----
 * 所有操作经 VPP-owned 段（app 只 mmap + 直读直写 svm_fifo），不在 app 侧持有 session 结构。
 * P3a：connect 已实现（ctrl_mq 发送 + 等 CONNECTED 回复）。其余为桩。
 */
int vcl2_session_create (vcl2_proto_t proto, uint8_t is_nonblocking);
int vcl2_session_listen (vcl2_handle_t sh, uint32_t q_len);
int vcl2_session_accept (vcl2_handle_t listener_sh, vcl2_handle_t *accepted_sh);

/* connect：ip 为 4/16 字节（按 is_ip4），port 为 host 字序。
 * 经 ctrl_mq 发 connect，阻塞等 VPP 的 CONNECTED 回复。0=成功。 */
int vcl2_session_connect (vcl2_handle_t sh, uint8_t is_ip4, const uint8_t *ip,
                          uint16_t port);

int vcl2_session_send (vcl2_handle_t sh, const void *buf, uint32_t len);
int vcl2_session_recv (vcl2_handle_t sh, void *buf, uint32_t len);
int vcl2_session_close (vcl2_handle_t sh);

/* shutdown：正常优雅半关闭（how=SHUT_RD/WR/RDWR）。SHUT_WR/RDWR 向 VPP 发 SHUTDOWN
 * （peer 收到 FIN，连接仍可收）；SHUT_RD 仅本地标记。正常操作的控制消息，非退出清理。 */
int vcl2_session_shutdown (vcl2_handle_t sh, int how);

/* 库是否已初始化 */
int vcl2_is_init (void);

#ifdef __cplusplus
}
#endif

#endif /* included_vcl2_h */
