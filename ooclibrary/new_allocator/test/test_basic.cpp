#include "wrapper.h"
#include "profiling.h"

#include <string.h>
#include <sys/mman.h>

#include <string>
#include <cstdlib>
#include <vector>

// const size_t M = 1024UL * 1024UL;

void* original_mmap(void* addr, size_t size){
	return mmap(addr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
}

int original_munmap(void* addr, size_t size){
	return munmap(addr, size);
}

#ifndef USE_ORIGINAL
#define mymmap(p, s) arena_mmap(p, s, 1)
#define mymunmap arena_munmap
#else
#define mymmap original_mmap
#define mymunmap original_munmap
#endif

int main(int argc, char* argv[]){

	#ifndef USE_ORIGINAL

	if (argc < 8){
		fprintf(stderr, "Usage: %s alloc_num test_num access_each_round object_size(M)\n", argv[0]);
		fprintf(stderr, "\tobject_cache_size(M) page_cache_size(M) object_size(K)\n");
		return 0;
	}
	#else
	if (argc < 5){
		fprintf(stderr, "Usage: %s alloc_num test_num access_each_round object_size(M)\n", argv[0]);
		return 0;
	}
	#endif

	size_t alloc_num = atoll(argv[1]);
	size_t test_num = atoll(argv[2]);
	size_t access_each_round = atoll(argv[3]);
	size_t object_size = atoll(argv[4]) * M;

	printf("Task: %zu allocation of %zuM, %zu random access\n", alloc_num, object_size / M, test_num);

	#ifndef USE_ORIGINAL

	size_t object_cache_size = atoll(argv[5]) * M;
	size_t page_cache_size = atoll(argv[6]) * M;
	size_t mmap_object_size = atoll(argv[7]) * 1024UL;

	init_mmap_arena("/tmp/R/", object_cache_size, mmap_object_size);
	print_mmap_status();
	#endif

	std::vector<void*> ptrs;

	timer sec1, sec2, sec3;
	timer access;

	sec1.start();
	for (size_t i = 0; i < alloc_num; i++){
		void* ptr = mymmap(NULL, object_size);
		unsigned char* cptr = static_cast<unsigned char*>(ptr);
		// printf("mmap ptr: %p\n", ptr);
		for (size_t i = 0; i < object_size; i++){
			cptr[i] = i % 256;
		}
		ptrs.push_back(ptr);
	}
	sec1.stop();

	sec2.start();
	for (size_t i = 0; i < test_num; i++){
		void* ptr = ptrs[rand() % ptrs.size()];
		unsigned char* cptr = static_cast<unsigned char*>(ptr);
		for (size_t j = 0; j < access_each_round; j++){
			size_t index = (rand()) % object_size;
			timer_scope _scope(access);
			if (cptr[index] != index % 256){
				printf("Error: not equal\n");
				exit(1);
			}
		}
	}
	sec2.stop();

	sec3.start();
	for (auto it = ptrs.begin(); it != ptrs.end(); it++){
		void* ptr = *it;
		mymunmap(ptr, object_size);
		// printf("munmap ptr: %p\n", ptr);
	}
	sec3.stop();

	print_mmap_status();
	print_timer("\t", "sec1", sec1);
	print_timer("\t", "sec2", sec2);
	print_timer("\t", "sec3", sec3);
	print_timer("\t", "access", access);

	destroy_mmap_arena();
}