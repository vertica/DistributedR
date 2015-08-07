#ifndef __MMAP_MALLOC_H_
#define __MMAP_MALLOC_H_

#ifdef __cplusplus
extern "C" {
#endif

void print_mmap_status();
void enable_mmap_hooks();
void lock_all_segment();
void set_total_memory(size_t size);
void* dlmmap(size_t size, int mode);
int dlmunmap(void* ptr, size_t size);

#ifdef __cplusplus
}
#endif

#endif