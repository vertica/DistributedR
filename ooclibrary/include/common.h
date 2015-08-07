#ifndef __COMMON_H_
#define __COMMON_H_
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "const.h"

#ifdef DEBUG
#define DEBUG_PRINT(...) fprintf(stderr, __VA_ARGS__)
#else
#define DEBUG_PRINT(...) do {} while (0)
#endif

#ifndef NOTEST
#define TEST_PRINT(...) fprintf(stderr, __VA_ARGS__)
#else
#define TEST_PRINT(...) do {} while (0)
#endif

#define ERROR_PRINT(...) fprintf(stderr, __VA_ARGS__)

#define PRINT_TIME() 	do { \
		struct timeval t; \
		gettimeofday(&t, NULL); \
		printf("%zu seconds %zu microseconds\n", t.tv_sec, t.tv_usec); \
	} while (0)

static inline size_t min(size_t l, size_t r){
	return (l < r) ? l : r;
}

static inline size_t max(size_t l, size_t r){
	return (l < r) ? r : l;
}

typedef struct mmap_handle {
	int fd;
	size_t offset;
} mmap_handle;
#endif