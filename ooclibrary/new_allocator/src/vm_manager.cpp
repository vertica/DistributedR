#include "new_allocator.h"

#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdint.h>
#include <memory.h>
#include <sys/mman.h>
#include <errno.h>

#include "profiling.h"

void* vm_manager_t::vm_align_mmap(size_t size){
	// page size alignment
	size = page_align(size);

	size_t aligned_size = (size + ALIGNMENT - 1 + page_size - 1) & ~(page_size - 1);

	void* ptr = mmap(nullptr, aligned_size, PROT_NONE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
	void* aligned_ptr = reinterpret_cast<void*>((reinterpret_cast<size_t>(ptr) + ALIGNMENT - 1) & ~(ALIGNMENT - 1));
	size_t before = static_cast<char*>(aligned_ptr) - static_cast<char*>(ptr);
	size_t after = aligned_size - size - before;
	munmap(ptr, before);
	munmap(static_cast<char*>(aligned_ptr) + size, after);
	
	if (reinterpret_cast<size_t>(aligned_ptr) % ALIGNMENT != 0){
		ERROR_PRINT("alignment failed\n");
	}

	TEST_PRINT("%lx: %p %lx %p %lx\n", size, ptr, before, aligned_ptr, after);
	return aligned_ptr;
}

int vm_manager_t::vm_munmap(void* ptr, size_t size){
	size = page_align(size);

	return munmap(ptr, size);
}

int vm_manager_t::vm_clean_mmap(void* ptr, size_t size){
	int ret;
	ret = munmap(ptr, size);
	if (ret < 0){
		ERROR_PRINT("munmap failed: %d %d\n", ret, errno);
		return -1;
	}
	void* nptr = mmap(ptr, size, PROT_NONE, MAP_PRIVATE | MAP_FIXED | MAP_ANON | MAP_NORESERVE, -1, 0);
	if (nptr != ptr){
		ERROR_PRINT("mmap fixed failed %p %p\n", nptr, ptr);
		return -1;
	}
	return 0;
}

int vm_manager_t::vm_mask(void* ptr, size_t size){
	size = page_align(size);

	return mprotect(ptr, size, PROT_NONE);
}

int vm_manager_t::vm_readonly_mask(void* ptr, size_t size){
	size = page_align(size);

	return mprotect(ptr, size, PROT_READ);
}

int vm_manager_t::vm_unmask(void* ptr, size_t size){
	size = page_align(size);

	return mprotect(ptr, size, PROT_READ | PROT_WRITE);
}
