#include "large_malloc.h"

static size_t pagesize = 0;

static inline void _init_pagesize(){
	if (pagesize == 0){
		pagesize = getpagesize();
	}
}

static inline size_t truncate_div(size_t num, size_t denom){
	return (num + denom - 1) / denom;
}

void* mmap_large(void* addr, size_t header_size, size_t data_size, mmap_handle handle, int madv_flag){
	_init_pagesize();

	size_t total_size = header_size + data_size;

	// TODO: DEBUG ONLY
	header_size = 0;
	data_size = total_size;

	char* raw;

	// if (addr == NULL){
		raw = (char*) mmap(
			NULL, 
			header_size + data_size, 
			PROT_READ | PROT_WRITE,
			MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, 
			-1,
			0);
	// }else{
	// 	raw = (char*) mmap(
	// 		addr, 
	// 		header_size + data_size, 
	// 		PROT_READ | PROT_WRITE,
	// 		MAP_SHARED | MAP_ANON | MAP_FIXED | MAP_NORESERVE, 
	// 		-1,
	// 		0);
	// }

	if (raw == MAP_FAILED){
		ERROR_PRINT("mmap failed, error: %s\n", strerror(errno));
		return NULL;
	}

	int truncate_ret = ftruncate(handle.fd, handle.offset + total_size);

	if (truncate_ret != 0){
		ERROR_PRINT("truncate failed, error: %s\n", strerror(errno));
		return NULL;
	}

	// if (header_size != 0){
	// 	int ret = munmap(raw, header_size);

	// 	if (ret < 0){
	// 		ERROR_PRINT("munmap failed, error: %s\n", strerror(errno));
	// 	}

	// 	raw = (char*) mmap(
	// 			raw, 
	// 			header_size, 
	// 			PROT_READ | PROT_WRITE,
	// 			MAP_PRIVATE | MAP_FIXED, 
	// 			-1,
	// 			0);

	// 	if (raw == MAP_FAILED){
	// 		ERROR_PRINT("mmap failed, error: %s\n", strerror(errno));
	// 		return NULL;
	// 	}
	// }

	void* ret = mmap(
		raw + header_size, 
		data_size, 
		PROT_READ | PROT_WRITE,
		MAP_SHARED | MAP_FIXED,
		handle.fd,
		handle.offset + header_size);

	if (ret == MAP_FAILED){
		ERROR_PRINT("mmap failed, error: %s\n", strerror(errno));
		return NULL;
	}

	if (madv_flag != 0){
		madvise(ret, data_size, madv_flag);
	}

	DEBUG_PRINT("On disk mmap: %p %zu %zu -> %p\n", addr, header_size, data_size, raw);

	return raw;
}

int munmap_large(void* ptr, size_t total_size){
	_init_pagesize();

	return munmap(ptr, total_size);
}