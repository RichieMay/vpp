# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 vpp_runtime
#
# vcl2 —— 单侧所有权的 App 接入库。与 src/vcl/ 独立，互不影响。

option(VPP_BUILD_VCL2 "Build vcl2 (single-ownership app library)" ON)
if(NOT VPP_BUILD_VCL2)
  return()
endif()

##############################################################################
# libvcl2 —— 核心库（app 直接链接；P0: vcl2.c）
# 刻意只依赖 vppinfra/svm（svm 用于 P2 svm_fifo）。不引入 vlibmemoryclient 等，
# 保持 app 侧薄。
##############################################################################
add_vpp_library(vcl2
  SOURCES
  vcl2.c
  vcl2_segment.c
  vcl2_session.c

  LINK_LIBRARIES
  vppinfra svm rt pthread
)

# P4 起加入 ldp2.c（LD_PRELOAD 拦截器），届时启用：
# add_vpp_library(vcl2_ldpreload
#   SOURCES ldp2.c
#   LINK_LIBRARIES vppinfra svm rt pthread vcl2 dl
# )

add_vpp_headers(vcl2
  vcl2.h
  vcl2_private.h
)
