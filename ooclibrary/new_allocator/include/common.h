#ifndef __COMMON_H_
#define __COMMON_H_
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "const.h"
#include "gprintf.h"

#ifdef DEBUG
#define DEBUG_PRINT(...) gprintf(stderr, __VA_ARGS__)
#else
#define DEBUG_PRINT(...) do {} while (0)
#endif

#ifndef NOTEST
#define TEST_PRINT(...) gprintf(stderr, __VA_ARGS__)
#else
#define TEST_PRINT(...) do {} while (0)
#endif

#define FPRINTF(...) gprintf(__VA_ARGS__)

#define ERROR_PRINT(...) gprintf(stderr, __VA_ARGS__)

#define VOLATILE_SNPRINTF(dst, size, args...) do {\
	char* non_volatile_buffer = new char[size]; \
	snprintf(non_volatile_buffer, size, args); \
	for (size_t i = 0; i < size && non_volatile_buffer[i] != '\0'; i++){ \
		dst[i] = non_volatile_buffer[i]; \
	} \
	delete[] non_volatile_buffer; \
} while (0)

static inline size_t min(size_t l, size_t r){
	return (l < r) ? l : r;
}

static inline size_t max(size_t l, size_t r){
	return (l < r) ? r : l;
}

static inline volatile char* volatile_strncpy(volatile char* dst, const char* src, size_t n){
	size_t i;
	for (i = 0; i < n && src[i] != '\0'; i++){
		dst[i] = src[i];
	}
	return dst;
}

#endif