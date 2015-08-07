#include <string>
#include <sstream>
#include <iterator>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <malloc.h>
#include <string.h>
// #include "ondisk_malloc.h"
#include "symbol_finder.cpp"

/**
 * Test random memory allocation function
 *
 * Operations:
 *
 * 1. malloc and add to list
 * 2. memalign and add to list
 * 3. free randomly from list
 * 4. realloc randomly from list
 */


// #define malloc dlmalloc
// #define free dlfree
// #define realloc dlrealloc
// #define posix_memalign dlposix_memalign

/**
 * Statically allocated pointer array
 */
#define PTR_COUNT 256
static void* data_ptrs[PTR_COUNT];
static size_t ptr_count = 0;

#define PRINT_STATE() printf("testing %s, ptr_count: %zu\n", \
							 __func__, ptr_count)

inline void* pop_random(){
	int to_free = rand() % ptr_count;
	void* ptr = data_ptrs[to_free];
	std::copy(&data_ptrs[to_free + 1], &data_ptrs[ptr_count], &data_ptrs[to_free]);
	ptr_count--;
	return ptr;
}

inline size_t gen_size_random(){
	int choice = rand();
	if (choice % 3 == 0){
		return rand() % 1024;
	}else if (choice % 3 == 1){
		return (size_t)(rand() % 1024) + 8UL * 1024UL * 1024UL;
	}else {
		return (size_t)(rand() % 256) * 4096 + 8UL * 1024UL * 1024UL;
	}
}

inline void fill(void* ptr, size_t size){
	memset(ptr, 0, size);
}

inline void malloc_action(){
	PRINT_STATE();
	if (ptr_count == PTR_COUNT){
		return;
	}

	void* ptr;

	size_t size = gen_size_random();
	ptr = malloc(size);
	fill(ptr, size);

	data_ptrs[ptr_count++] = ptr;
}

inline void memalign_action(){
	PRINT_STATE();
	if (ptr_count == PTR_COUNT){
		return;
	}

	size_t size = gen_size_random();
	// printf("size: %zu\n", size);

	void* ptr;
	posix_memalign(&ptr, getpagesize(), size);
	fill(ptr, size);
		

	data_ptrs[ptr_count++] = ptr;
}

inline void free_action(){
	PRINT_STATE();
	if (ptr_count == 0){
		return;
	}

	void* ptr = pop_random();
	free(ptr);
}

inline void realloc_action(){
	PRINT_STATE();
	if (ptr_count == 0){
		return;
	}

	void* ptr = pop_random();

	size_t size = gen_size_random();
	ptr = realloc(ptr, size);
	fill(ptr, size);
	data_ptrs[ptr_count++] = ptr;
}

int test_random_action(size_t times, unsigned int seed = 0){

	if (seed == 0){
		seed = time(NULL);
	}
	srand(seed);
	for (size_t i = 0; i < times; i++){
		int action = rand() % 5;

		switch(action){
			case 0:
			malloc_action();
			break;

			case 1:
			memalign_action();
			break;

			case 2:
			case 3:
			free_action();
			break;

			case 4:
			realloc_action();
			break;
		}
	}

	for (size_t i = 0; i < ptr_count; i++){
		free(data_ptrs[i]);
	}
	printf("Freed all\n");
	return 0;
}

int main(int argc, char** argv){
	size_t times = 256;
	char* end;

	enable_mmap();
	set_mem(128UL * 1024UL * 1024UL);

	if (argc == 2){
		times = strtol(argv[1], &end, 10);
		if (*end != 0){
			printf("Error in arguments\n");
			exit(1);
		}
	}
	test_random_action(times);
}