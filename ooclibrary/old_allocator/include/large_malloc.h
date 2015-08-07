#ifndef __LARGE_MALLOC_H_
#define __LARGE_MALLOC_H_

#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <uuid/uuid.h>
#include <sys/mman.h>
#include "common.h"

typedef struct malloc_meta {
	size_t offset;
	size_t header_chunk, data_chunk;
	// mmap_handle handle;
} malloc_meta;

void* mmap_large(void* addr, size_t header_size, size_t data_size, mmap_handle handle, int madv_flag);
int munmap_large(void* ptr, size_t page_total);
#endif