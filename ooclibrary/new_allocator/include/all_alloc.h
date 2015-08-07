// Header file to be included in dlmalloc.c to replace mmap

#ifndef __ONDISK_MALLOC_H_
#define __ONDISK_MALLOC_H_

#include <malloc.h>
#include "const.h"
#include "wrapper.h"

#define HAVE_MORECORE 0

#ifdef ALL_IN_MEM // If defined ALL_IN_MEM, every allocation is done in memory; this is for meta_allocator
#include <sys/mman.h>
#include "common.h"
#define USE_DL_PREFIX

inline void* mem_mmap(size_t size, int mode){
	TEST_PRINT("mem_mmap called %zu %d\n", size, mode);
	return mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
}

inline int mem_munmap(void* ptr, size_t size){
	TEST_PRINT("mem_munmap called %p %zu\n", ptr, size);
	return munmap(ptr, size);
}

#define MMAP(s) 	mem_mmap(s, 0)
#define DIRECT_MMAP(s) 	mem_mmap(s, 1)
#define MUNMAP(p, s) 	mem_munmap(p, s)
#else



#define MMAP(s)			mmap_dispatch(s, 0)
#define DIRECT_MMAP(s) 	mmap_dispatch(s, 1)
#define MUNMAP(p, s) 	munmap_dispatch(p, s)
#endif

#define DEFAULT_MMAP_THRESHOLD DEFAULT_LARGE_THRESHOLD
#define HAVE_USR_INCLUDE_MALLOC_H
#define HAVE_MREMAP 0
#define DEFAULT_GRANULARITY (DEFAULT_LARGE_THRESHOLD / 2)

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