#ifndef __GPRINTF_H__
#define __GPRINTF_H__ 

#include <stdio.h>
#include <stdarg.h>

#define VA_MAXIMUM 16

#ifdef __cplusplus
extern "C" {
#endif

int gprintf(FILE* fd, const char* format, ...);
int gsprintf(char* s, const char* format, ...);

#ifdef __cplusplus
}
#endif

#define GTEST_PRINT(...) gprintf(stderr, __VA_ARGS__)

#endif