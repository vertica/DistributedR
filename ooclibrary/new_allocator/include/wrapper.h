#ifndef __WRAPPER_H__
#define __WRAPPER_H__

#include <signal.h>

#define OBJECT_SIZE (2UL * 1024UL * 1024UL)
// #define OBJECT_CACHE_SIZE (0)
#define PAGE_CACHE_SIZE (20UL * 1024UL * 1024UL)

#ifdef __cplusplus
extern "C" {
#endif

void enable_mmap_hooks();
void disable_mmap_hooks();

int init_tracer();

void init_mmap_arena(const char* path, size_t object_cache_size, size_t object_size);
void init_mmap_arena_shared(const char* path, size_t object_cache_size, size_t object_size, const char* shm_name);
void destroy_mmap_arena();

void* mmap_dispatch(size_t size, int mode);
int munmap_dispatch(void* ptr, size_t size);

void* arena_mmap(void* ptr, size_t size, int mode);
void* arena_shm_mmap(const char* name, void* ptr, size_t size, int mode);
int arena_munmap(void* ptr, size_t size);
void handle_sigsegv(int signum, siginfo_t* si, void* unused);

int arena_reserve(size_t size);
int arena_release(size_t size);

void print_mmap_status();
#ifdef __cplusplus
}
#endif
#endif