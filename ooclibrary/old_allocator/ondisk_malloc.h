#ifndef __ONDISK_MALLOC_H_
#define __ONDISK_MALLOC_H_

#include "common.h"
#include <malloc.h>
#include "mmap_malloc.h"

#define HAVE_MORECORE 0

#ifndef DYNAMIC_LIB
#define USE_DL_PREFIX
#endif

#define DEFAULT_MMAP_THRESHOLD DEFAULT_LARGE_THRESHOLD
#define HAVE_USR_INCLUDE_MALLOC_H
#define HAVE_MREMAP 0
#define DEFAULT_GRANULARITY (DEFAULT_LARGE_THRESHOLD / 2)
#define MMAP(s) dlmmap(s, 0)
#define DIRECT_MMAP(s) dlmmap(s, 1)
#define MUNMAP dlmunmap


#ifdef __cplusplus
extern "C" {
#endif

void* dlmalloc(size_t bytes);
void dlfree(void* ptr);
void* dlrealloc(void* oldmem, size_t bytes);
int dlposix_memalign(void** pp, size_t alignment, size_t bytes);

#ifdef __cplusplus
}
#endif

#endif