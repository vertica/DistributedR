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
#include "allocator.h"

// int main(int argc, char** argv){

// 	size_t array_size, col_num, row_chunk_num;

// 	if (argc < 4){
// 		printf("Using default arguments\n");

// 		array_size = 128UL * 1024UL * 1024UL;
// 		col_num = 32UL;
// 		row_chunk_num = 64UL;
// 	}else{
// 		printf("Using custom arguemnts\n");

// 		char* end;
// 		array_size = strtol(argv[1], &end, 10);
// 		if (*end != 0){
// 			printf("Error in arguments\n");
// 			exit(1);
// 		}

// 		col_num = strtol(argv[2], &end, 10);
// 		if (*end != 0){
// 			printf("Error in arguments\n");
// 			exit(1);
// 		}

// 		row_chunk_num = strtol(argv[3], &end, 10);
// 		if (*end != 0){
// 			printf("Error in arguments\n");
// 			exit(1);
// 		}
// 	}


// 	initHook();
// 	hookMallocLarge();

// 	test_original_vector<int>(array_size);
// 	test_new_vector<int>(array_size);
// 	test_original_mat<int>(array_size, col_num, row_chunk_num);
// 	test_new_mat<int>(array_size, col_num, row_chunk_num);

// }