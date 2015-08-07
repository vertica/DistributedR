#include <string>
#include <sstream>
#include <iterator>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <map>
#include <malloc.h>
#include <string.h>
#include "common.h"
#include "timer.h"
#include "allocator.h"

/**
 * Test memory access performance with large allocation
 *
 * view the memory region as either:
 * 1. vector
 * 2. R style matrix on single vector
 *
 * and store 2 * mat at
 * 1. original vector
 * 2. new vector
 *
 * array_size should be a multiplication of col_num and row_chunk_num
 */



template<typename DataType>
void test_original_vector(size_t array_size){
	printf("testing %s with array_size: %zu, mem_size: %zu\n", 
		__func__, array_size, array_size * sizeof(DataType));
	timer t;

	DataType* data = (DataType*) malloc(array_size * sizeof(DataType));

	TIMER_PRINT_RESET();
	memset(data, 0, array_size * sizeof(DataType));
	
	TIMER_PRINT_RESET();

	for (size_t i = 0; i < array_size; i++){
		data[i] = data[i] * 2;
	}

	TIMER_PRINT_RESET();

	free(data);
	
	TIMER_PRINT_RESET();
	printf("===============\n");
}

template<typename DataType>
void test_new_vector(size_t array_size){
	printf("testing %s with array_size: %zu, mem_size: %zu\n", 
		__func__, array_size, array_size * sizeof(DataType));
	timer t;
	DataType* data = (DataType*) malloc(array_size * sizeof(DataType));
	DataType* data2 = (DataType*) malloc(array_size * sizeof(DataType));

	TIMER_PRINT_RESET();
	memset(data, 0, array_size * sizeof(DataType));
	memset(data2, 0, array_size * sizeof(DataType));
	
	TIMER_PRINT_RESET();

	for (size_t i = 0; i < array_size; i++){
		data2[i] = data[i] * 2;
	}

	TIMER_PRINT_RESET();

	free(data);
	free(data2);

	TIMER_PRINT_RESET();
	printf("===============\n");

}

template<typename DataType>
void test_original_mat(size_t array_size, size_t col_num, size_t row_chunk_num){
	printf("testing %s with array_size: %zu, mem_size: %zu\n, col_num: %zu, row_chunk_num: %zu\n", 
		__func__, array_size, array_size * sizeof(DataType), col_num, row_chunk_num);
	timer t;
	DataType* data = (DataType*) malloc(array_size * sizeof(DataType));


	TIMER_PRINT_RESET();
	memset(data, 0, array_size * sizeof(DataType));

	TIMER_PRINT_RESET();

	size_t chunk_count = array_size / (col_num * row_chunk_num);

	for (size_t i = 0; i < chunk_count; i++){
		for (size_t j = 0; j < col_num; j++){
			size_t offset = col_num * row_chunk_num * i + row_chunk_num * j;
			for (size_t k = 0; k < row_chunk_num; k++){
				data[offset + k] = data[offset + k] * 2;
			}
		}
	}

	TIMER_PRINT_RESET();

	free(data);
	TIMER_PRINT_RESET();
	printf("===============\n");

}

template<typename DataType>
void test_new_mat(size_t array_size, size_t col_num, size_t row_chunk_num){
	printf("testing %s with array_size: %zu, mem_size: %zu\n, col_num: %zu, row_chunk_num: %zu\n", 
		__func__, array_size, array_size * sizeof(DataType), col_num, row_chunk_num);
	
	timer t;

	DataType* data = (DataType*) malloc(array_size * sizeof(DataType));
	DataType* data2 = (DataType*) malloc(array_size * sizeof(DataType));

	TIMER_PRINT_RESET();
	memset(data, 0, array_size * sizeof(DataType));
	memset(data2, 0, array_size * sizeof(DataType));
	
	TIMER_PRINT_RESET();

	size_t chunk_count = array_size / (col_num * row_chunk_num);

	for (size_t i = 0; i < chunk_count; i++){
		for (size_t j = 0; j < row_chunk_num; j++){
			size_t offset = col_num * row_chunk_num * i + row_chunk_num * j;
			for (size_t k = 0; k < row_chunk_num; k++){
				data2[offset + k] = data[offset + k] * 2;
			}
		}
	}

	TIMER_PRINT_RESET();

	free(data);
	free(data2);
	TIMER_PRINT_RESET();
	printf("===============\n");
}

struct mem_region{
	void** ptrs;
	size_t size;
	size_t count;
};

mem_region alloc_mem(size_t num, size_t size){
	mem_region region;
	region.ptrs = (void**) calloc(num, sizeof(void*));
	region.size = size;
	region.count = num;

	for (size_t i = 0; i < region.count; i++){
		region.ptrs[i] = malloc(size);
	}

	return region;
}

void access_mem(mem_region& region){
	for (size_t i = 0; i < region.count; i++){
		memset(region.ptrs[i], 0, region.size);
	}
}

void free_mem(mem_region& region){
	for (size_t i = 0; i < region.count; i++){
		free(region.ptrs[i]);
	}

	free(region.ptrs);
}

static const size_t SMALL = 2UL * 1024UL;
static const size_t LARGE = 64UL * 1024UL * 1024UL;

void test_combine(std::vector<size_t> pattern){
	int state = 0;

	for (std::vector<size_t>::iterator it = pattern.begin();
									   it != pattern.end();
									   it++){
		mem_region region;

		if (state == 0){
			region = alloc_mem(*it, SMALL);
		}else{
			region = alloc_mem(*it, LARGE);
		}

		access_mem(region);	
	}
}

size_t readsize(std::istream& stream){
	std::string buffer;
	stream >> buffer;

	const size_t K = 1024UL;
	const size_t M = 1024UL * 1024UL;
	const size_t G = 1024UL * 1024UL * 1024UL;

	size_t unit = 0;
	size_t size = atol(buffer.substr(0, buffer.length() - 1).c_str());

	switch (buffer[buffer.length() - 1]){
	case 'K':
		unit = K;
		break;
	case 'M':
		unit = M;
		break;
	case 'G':
		unit = G;
		break;
	}
	size *= unit;

	return size;
}

void run_dsl(std::istream& stream){
	std::vector<mem_region> regions;
	std::string buf;
	struct timeval tbegin, tend;

	while (std::getline(stream, buf)){
		printf("Step %s ", buf.c_str());
		gettimeofday(&tbegin, NULL);

		std::stringstream buf_stream(buf);
		char type;
		size_t idx;
		buf_stream >> type;
		buf_stream >> idx;
		if (type == 'S'){
			
			assert(idx == regions.size());
			size_t size = readsize(buf_stream);

			mem_region region = alloc_mem(size / SMALL, SMALL);
			regions.push_back(region);
			access_mem(region);

		}else if (type == 'L'){

			assert(idx == regions.size());
			size_t size = readsize(buf_stream);

			mem_region region = alloc_mem(size / LARGE, LARGE);
			regions.push_back(region);
			access_mem(region);

		}else if (type == 'A'){

			access_mem(regions[idx]);

		}else if (type == 'F'){

			free_mem(regions[idx]);

		}


		gettimeofday(&tend, NULL);
		printf("use %f seconds\n", double(tend.tv_sec - tbegin.tv_sec) + double(tend.tv_usec - tbegin.tv_usec) / 1000000);
	}
}

int main(int argc, char** argv){

	size_t array_size, col_num, row_chunk_num;

	if (argc < 4){
		printf("Using default arguments\n");
		printf("Usage: %s array_size col_num row_chunk_num temp_folder\n", argv[0]);

		array_size = 128UL * 1024UL * 1024UL;
		col_num = 32UL;
		row_chunk_num = 64UL;
	}else{
		printf("Using custom arguemnts\n");

		char* end;
		array_size = strtol(argv[1], &end, 10);
		if (*end != 0){
			printf("Error in arguments\n");
			exit(1);
		}

		col_num = strtol(argv[2], &end, 10);
		if (*end != 0){
			printf("Error in arguments\n");
			exit(1);
		}

		row_chunk_num = strtol(argv[3], &end, 10);
		if (*end != 0){
			printf("Error in arguments\n");
			exit(1);
		}
	}

	/*if (argc >= 5){
		printf("Using mmap\n");
		initHook();
		hookMallocLarge();
		setFolderPath(argv[4]);
	}else{
		printf("Using vanilla\n");
	}*/
	

	// test_original_vector<int>(array_size);
	// test_new_vector<int>(array_size);
	// test_original_mat<int>(array_size, col_num, row_chunk_num);
	// test_new_mat<int>(array_size, col_num, row_chunk_num);

	// test_combine(std::vector<size_t>{256 * K,5,5,5,5,6});
	
	std::ifstream f(argv[1]);

	run_dsl(f);

}
