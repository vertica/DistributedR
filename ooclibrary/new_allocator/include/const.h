#ifndef __CONST_H_
#define __CONST_H_

#include <limits.h>

#define NOTEST
#define DEFAULT_LARGE_THRESHOLD (4UL * 1024UL * 1024UL) // 4MB
#define M (1024UL * 1024UL)
#define UUID_LENGTH (32)
#define USE_MADVISE
#define TRACE_MEM
#define DEFAULT_DISK_PATH "/tmp/R/"
#define ENV_ALLOCATOR_LOG_LOCATION "ALLOCATOR_LOG_LOCATION"
#define MULTI_LOAD 8
#define META_IN_MEM

// if defined, we use separate allocator for internal allocation
#define CUSTOM_ALLOCATION

// if defined, we build with internal tracer support
// whether enable tracer or not depends on environment variable
#define USE_TRACER

// alignment size, should be larger than object size (2^n times)
#define ALIGNMENT (1UL << 21)

// define if we use clock algorithm
// #define USE_CLOCK_ALGORITHM
#define CLOCK_ALGORITHM_MASK_NUM 8
// if defined NON_UNIFORM_META_SIZE, the first page (4KB) is in memory, otherwsie first object (2MB) is in memory
// #define NON_UNIFORM_META_SIZE

// if defined, instead of ftruncate, we manually write out zeros until size fits
// #define O_DIRECT_TRUNCATE

#ifdef CUSTOM_ALLOCATION
#define container_allocator meta_allocator
#else
#define container_allocator std::allocator
#endif

#endif
