/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 vpp_runtime
 *
 * vcl2 段管理。段为 VPP-owned（app 只 mmap，退出内核自动 munmap，VPP 回收）。
 * segment_table_lock（rwlock）保护 hash + segment_main。锁序 sessions_lock→segment_table_lock。
 */

#include <string.h>
#include <stdio.h>

#include <vppinfra/clib_error.h>
#include <svm/ssvm.h> /* SSVM_SEGMENT_MEMFD */

#include "vcl2_private.h"

/* mmap 一个 VPP 发来的段 fd，登记 handle -> segment_index */
int vcl2_segment_attach (u64 handle, char *name, int fd) {
  vcl2_main_t *vm = &vcl2_main;
  fifo_segment_create_args_t a;
  int rv;

  memset (&a, 0, sizeof (a));
  a.segment_name = name;
  a.segment_type = SSVM_SEGMENT_MEMFD;
  a.memfd_fd = fd;

  clib_rwlock_writer_lock (&vm->segment_table_lock);
  rv = fifo_segment_attach (&vm->segment_main, &a);
  if (rv) {
    clib_rwlock_writer_unlock (&vm->segment_table_lock);
    VCL2_DBG ("segment_attach('%s') failed: %d", name, rv);
    return rv;
  }
  hash_set (vm->segment_table, handle, a.new_segment_indices[0]);
  clib_rwlock_writer_unlock (&vm->segment_table_lock);
  VCL2_DBG ("segment attached '%s' handle=%lu -> idx=%u", name,
            (unsigned long) handle, a.new_segment_indices[0]);
  return 0;
}

u32 vcl2_segment_lookup (u64 handle) {
  vcl2_main_t *vm = &vcl2_main;
  uword *p;
  u32 idx;
  clib_rwlock_reader_lock (&vm->segment_table_lock);
  p = hash_get (vm->segment_table, handle);
  idx = p ? (u32) p[0] : VCL2_INVALID_SEG_INDEX;
  clib_rwlock_reader_unlock (&vm->segment_table_lock);
  return idx;
}

/* 在已映射的段内、按偏移定位一个 svm_msg_q（VPP 拥有，app 只 attach 引用） */
int vcl2_segment_attach_mq (u64 handle, uword offset, u32 idx,
                            svm_msg_q_t **mq) {
  vcl2_main_t *vm = &vcl2_main;
  fifo_segment_t *fs;
  uword *p;

  clib_rwlock_reader_lock (&vm->segment_table_lock);
  p = hash_get (vm->segment_table, handle);
  if (PREDICT_FALSE (!p)) {
    clib_rwlock_reader_unlock (&vm->segment_table_lock);
    VCL2_DBG ("attach_mq: segment %lu not attached", (unsigned long) handle);
    return -1;
  }
  fs = fifo_segment_get_segment (&vm->segment_main, p[0]);
  *mq = fifo_segment_msg_q_attach (fs, offset, idx);
  clib_rwlock_reader_unlock (&vm->segment_table_lock);
  if (!*mq)
    return -1;
  return 0;
}

/* 在已映射的段内、按偏移 attach 一个已存在的 svm_fifo（VPP 创建，app 只引用）。
 * 镜像 fifo_segment_alloc_fifo_w_offset —— 用于从 CONNECTED 回复的 rx/tx fifo 偏移
 * 还原出 svm_fifo_t *。返回的 fifo 仅在对应段映射存活期间有效。 */
svm_fifo_t *vcl2_segment_alloc_fifo (u64 handle, uword offset) {
  vcl2_main_t *vm = &vcl2_main;
  fifo_segment_t *fs;
  uword *p;
  svm_fifo_t *f;

  clib_rwlock_reader_lock (&vm->segment_table_lock);
  p = hash_get (vm->segment_table, handle);
  if (PREDICT_FALSE (!p)) {
    clib_rwlock_reader_unlock (&vm->segment_table_lock);
    VCL2_DBG ("alloc_fifo: segment %lu not attached", (unsigned long) handle);
    return 0;
  }
  fs = fifo_segment_get_segment (&vm->segment_main, p[0]);
  f = fifo_segment_alloc_fifo_w_offset (fs, offset);
  clib_rwlock_reader_unlock (&vm->segment_table_lock);
  return f;
}
