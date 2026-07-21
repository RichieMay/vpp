# vcl2 — 单侧所有权的 App 接入库（重写 VCL，方案 B）

## 这是什么
`vcl2` 是一个**独立的** app 侧库（与 `src/vcl/` 完全独立、互不影响），按"单侧所有权"原则重写，
替代原 VCL 的多步、非原子跨进程资源 teardown（`vcl_worker_cleanup` 那串 ~10 步 free 是 VCL 崩溃族的根源）。

## 北极星原则
**VPP 拥有全部数据面资源（session / fifo / segment / event queue / 元数据）；
app 进程"需要显式释放的拥有资源"为零 —— 只持可丢弃缓存 + VPP-owned shmem 的映射。**

推论：app 死 → VPP 在自己锁（worker barrier）下单侧回收 + 内核自动 munmap app 侧 shmem 映射。
两侧各自单侧、无协调协议 → 无竞态 → 原 VCL 那类堆破坏崩溃从结构上消失。
本质 = 内核 socket 的所有权模型（内核拥有 socket，进程只拿 fd；进程死内核单侧回收）。

## 所有权契约
| 资源 | 原 VCL（崩溃源） | vcl2 |
|---|---|---|
| session 元数据 | app 本地 `sessions` pool | **VPP `session_t`** |
| accept 事件队列 | app `accept_evts_fifo` | **VPP `app_worker_t.wrk_evts`** |
| 数据 fifo | VPP segment_manager 段 | **不变（VPP 拥有）** |
| app 事件 mq | app 各 worker 本地向量 | **VPP-owned 段内 mq** |
| fd→handle 映射 | app 本地哈希（拥有） | **app 侧仅为可丢弃缓存**（能从 VPP 重建） |
| app-owned session 账本（sessions pool/accept_evts_fifo/mq 向量） | app 拥有，需多步 free | **删除**（这些资源全归 VPP） |
| vppinfra 工作堆（clib_mem） | app 拥有 | **保留**（进程本地、单侧：进程死内核回收，非跨进程 teardown 资源；clib_socket/vec 等需要） |
| 清理协议 | 跨进程多步非原子 teardown | **单侧**：VPP barrier 回收 + 内核 munmap |

**vcl2 内部状态（全部）**：SAPI clib_socket + app/app_wrk index + 段映射表 + 可丢弃 fd-cache + 一个 vppinfra 工作堆（进程本地、单侧）。
**没有**：sessions pool、accept_evts_fifo、mq 向量、bitmap —— 即**没有 app-owned 数据面账本**（这些全归 VPP）。vppinfra 工作堆仅用于 clib 基础设施内部，进程死内核回收，非跨进程 teardown 资源。
进程退出 = 关 SAPI fd（内核自动 munmap 映射段）+ 丢缓存；VPP 侧 barrier 回收自己拥有的资源。

## fork / 身份分离（nginx 多 worker 可行性）
两个不能混为一谈的东西：
- **fd（nginx 句柄，合成编码）**：**继承**（fork 复制进程内存 → fd 号 + 可丢弃缓存复制给子进程）。
- **VPP 侧身份（app-worker）**：**不继承**（故意）→ 子进程经 `pthread_atfork` child handler 向 VPP **重新注册**为自己的 app-worker，并查 VPP 重建可丢弃缓存。
child handler 在 `fork()` 返回给 app 前跑完 → app 看到就绪的 vcl2。附带根治原 VCL 的 ④（身份不靠继承 fd）。

## 资源释放：正常优雅 vs 异常退出（设计边界，重要）
单侧所有权原则**只约束进程退出/死亡后的清理**。两条路径都回收、互不依赖：
- **正常优雅操作（必须支持，发控制面消息）**：`close()` 连接 → `SESSION_CTRL_EVT_DISCONNECT`（VPP 发 FIN + 回收）；`close()` listener → `UNLISTEN`；`shutdown(SHUT_WR/RDWR)` → `SESSION_CTRL_EVT_SHUTDOWN`（半关闭 FIN）；`vcl2_destroy`（显式 detach）→ `ADD_DEL_WORKER(is_add=0)`。这些都镜像 VCL，与 connect/listen/accept 同属控制面，**不是** teardown 协议。
- **异常退出（kill -9 / segfault / 未跑 close）**：app 无法发送任何消息 → **只依赖 VPP 的 SAPI UDS close 检测**单侧回收（`sapi_socket_detach` → worker barrier → `application_free`）。**无 atexit / 无析构**（刻意：不能依赖异常时跑不到的 app 侧消息）。
本质 = 内核 socket 模型：`close()` 优雅发 FIN；进程被杀则内核单侧清理 socket。

## 实现说明（与早期 plan 的细化/澄清）
- **vcl2_session_t 是薄可丢弃缓存**，非 VCL 的状态机 session 对象：持 fifo/mq 指针（指向 VPP-owned 段）+ handle + 少量语义标志（nonblocking/peer_closed/rd/wr_shutdown/is_listener）。无 VCL_STATE_* 状态机、不拥有需跨进程释放的资源。
- **`accept_q`（listener 上已 ACCEPTED、待 `accept()` 取走的子 handle 队列）**：plan 早期写"accept 事件队列 = VPP app_worker_t.wrk_evts"。实际 vcl2 需在收到 ACCEPTED 事件后、app 调 `accept()` 前，缓冲子 handle —— 这是 `accept()` API 模型不可避免。**关键区别**：原 VCL 的 `accept_evts_fifo` 存的是 session 事件结构（app 拥有、需多步 free、非原子 teardown 的崩溃源）；vcl2 的 `accept_q` 只存**可丢弃 handle**（真正的 session/fifo 归 VPP）。故崩溃族风险已消除，只是名字上仍叫"队列"。
- **SAPI socket 设 `CLOEXEC`**（plan 要求）：`clib_socket_init` 不设，vcl2 在 `vcl2_sapi_connect` 里 `fcntl(F_SETFD, FD_CLOEXEC)` 补上，防 app `exec()` 泄漏。
- **线程模型**：vcl2 假设**一进程一 worker**（nginx 多进程模型）。数据路径（send/recv/accept/dispatch）不加锁 → **不支持单进程内多线程并发**用同一 vcl2 实例（需多线程的应用请用 fork 或自加锁）。多进程（fork）下每进程独立 vcl2_main 副本，安全。
- **调试输出**：默认关。`VCL2_DEBUG=1`（或 `LDP2_DEBUG=1`）打开 `VCL2_DBG`/`LDP2_DBG`（stderr）。

## 构建产物
- `libvcl2.so` — vcl2 核心库（app 直接链接）。
- `libvcl2_ldpreload.so` — LD_PRELOAD 拦截器（把未改 app 的 socket 调用接到 vcl2）。
独立于 `libvppcom.so` / `libvcl_ldpreload.so`；env 变量 `VCL2_*`（如 `VCL2_APP_NAME`、`VCL2_CONFIG`）。

## 分阶段实现（见 plan 文件 `vpp-vcl2-single-ownership.md`）
- **P0 骨架**（当前）：目录 + CMake + 头文件（API 面 + 内部结构）+ vcl2.c 入口（init/SAPI connect）。
- P1 控制面：app attach + worker add（SAPI 消息）。
- P2 数据面：SCM_RIGHTS 收 fifo 段 fd → mmap → svm_fifo recv/send。
- P3 事件面：轮询 VPP-owned event queue + epoll。
- P4 LDP2 拦截器。
- P5 fork/atfork。
- P6 多进程死亡/清理验证（无 core、无泄漏）。
- P7 nginx 端到端（validate.sh 全绿、零 core）。

## VPP 侧
尽量**不动 VPP 核心**：复用现有 SAPI / app_worker / segment_manager / `sapi_socket_detach`→worker barrier 清理。
可选增量（后置）：SAPI accept 加 `SO_PEERCRED`、per-worker pidfd —— 纯增量、不挡 vcl2 主体。
