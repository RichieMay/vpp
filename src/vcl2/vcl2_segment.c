/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 段管理（P2）。
 *
 * 所有段都是【VPP-owned】：VPP 经 segment_manager 分配，把 fd 经 SCM_RIGHTS 发给 app；
 * app 这里只 mmap（fifo_segment_attach），不拥有。进程退出内核自动 munmap；
 * VPP 侧检测 app 死后自行回收。无 app 侧段 free 协议。
 *
 * 复用 libsvm 的 fifo_segment_* （与 src/vcl/vcl_private.c 同一套机制）。
 */

#include <string.h>
#include <stdio.h>

#include <vppinfra/clib_error.h>
#include <svm/ssvm.h>		/* SSVM_SEGMENT_MEMFD */

#include "vcl2_private.h"

/* mmap 一个 VPP 发来的段 fd，登记 handle -> segment_index */
int
vcl2_segment_attach (u64 handle, char *name, int fd)
{
  fifo_segment_create_args_t a;
  int rv;

  memset (&a, 0, sizeof (a));
  a.segment_name = name;
  a.segment_type = SSVM_SEGMENT_MEMFD;
  a.memfd_fd = fd;

  rv = fifo_segment_attach (&vcl2_main.segment_main, &a);
  if (rv)
    {
      VCL2_DBG ("segment_attach('%s') failed: %d", name, rv);
      return rv;
    }
  hash_set (vcl2_main.segment_table, handle, a.new_segment_indices[0]);
  VCL2_DBG ("segment attached '%s' handle=%lu -> idx=%u", name, (unsigned long) handle,
	   a.new_segment_indices[0]);
  return 0;
}

u32
vcl2_segment_lookup (u64 handle)
{
  uword *p = hash_get (vcl2_main.segment_table, handle);
  return p ? (u32) p[0] : VCL2_INVALID_SEG_INDEX;
}

/* 在已映射的段内、按偏移定位一个 svm_msg_q（VPP 拥有，app 只 attach 引用） */
int
vcl2_segment_attach_mq (u64 handle, uword offset, u32 idx, svm_msg_q_t ** mq)
{
  fifo_segment_t *fs;
  u32 fi;

  fi = vcl2_segment_lookup (handle);
  if (fi == VCL2_INVALID_SEG_INDEX)
    {
      VCL2_DBG ("attach_mq: segment %lu not attached", (unsigned long) handle);
      return -1;
    }
  fs = fifo_segment_get_segment (&vcl2_main.segment_main, fi);
  *mq = fifo_segment_msg_q_attach (fs, offset, idx);
  if (!*mq)
    return -1;
  return 0;
}

/* 在已映射的段内、按偏移 attach 一个已存在的 svm_fifo（VPP 创建，app 只引用）。
 * 镜像 fifo_segment_alloc_fifo_w_offset —— 用于从 CONNECTED 回复的 rx/tx fifo 偏移
 * 还原出 svm_fifo_t *。返回的 fifo 仅在对应段映射存活期间有效。 */
svm_fifo_t *
vcl2_segment_alloc_fifo (u64 handle, uword offset)
{
  fifo_segment_t *fs;
  u32 fi;

  fi = vcl2_segment_lookup (handle);
  if (fi == VCL2_INVALID_SEG_INDEX)
    {
      VCL2_DBG ("alloc_fifo: segment %lu not attached", (unsigned long) handle);
      return 0;
    }
  fs = fifo_segment_get_segment (&vcl2_main.segment_main, fi);
  return fifo_segment_alloc_fifo_w_offset (fs, offset);
}
