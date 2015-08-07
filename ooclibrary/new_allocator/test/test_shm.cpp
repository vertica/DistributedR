#include "wrapper.h"
#include "profiling.h"

#include <string.h>
#include <sys/mman.h>

#include <string>
#include <sstream>
#include <cstdlib>
#include <vector>
#include <tuple>
#include <map>

typedef std::map<std::tuple<void*, size_t>, unsigned char> modify_map;

int main(){
	init_mmap_arena_shared("/tmp/R/", 40 * M, 256UL * 1024UL, "coord_shm");
	init_tracer();
	std::vector<void*> ptrs;
	modify_map modifications;

	timer sec1, sec2, sec3;
	// timer access;

	print_mmap_status();

	// void* ptr = arena_shm_mmap("shm", NULL, 2345, 0);
	// unsigned char* cptr = static_cast<unsigned char*>(ptr);
	// cptr[0] = 1;
	// arena_munmap(ptr, 2345);

	// char buf[256];
	// memcpy(buf, "/tmp/R/", sizeof("/tmp/R/"));


	// auto a = std::string(buf);
	// auto b = a + std::string(buf);

	// void* ptr = arena_mmap(NULL, 5 * M);
	// arena_munmap(ptr, 5 * M);

	sec1.start();
	for (size_t i = 0; i < 128; i++){
		std::stringstream ss;
		ss << "shm" << "." << i;
		std::string s = ss.str();
		void* ptr = arena_shm_mmap(s.c_str(), NULL, 2 * M, 1);
		unsigned char* cptr = static_cast<unsigned char*>(ptr);
		// printf("mmap ptr: %p\n", ptr);
		for (size_t j = 0; j < 2 * M; j++){
			cptr[j] = j % 256;
			// printf("Ping\n");
		}
		ptrs.push_back(ptr);
	}
	sec1.stop();

	// // print_mmap_status();

	// sec2.start();
	// for (size_t i = 0; i < 512; i++){
	// 	void* ptr = ptrs[rand() % ptrs.size()];
	// 	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	// 	for (size_t j = 0; j < 100; j++){
	// 		size_t index = (rand()) % 5 * M;
	// 		// timer_scope _scope(access);
	// 		auto it = modifications.find(std::tuple<void*, size_t>(ptr, index));
	// 		if (it != modifications.end()){
	// 			if (cptr[index] != it->second){
	// 				printf("Error mod0: not equal, %zu, %d, %d, %d\n", index, char(index % 256), cptr[index], it->second);
	// 				exit(1);
	// 			}
	// 		}else if (cptr[index] != index % 256){
	// 			printf("Error: not equal, %zu, %d, %d, %d\n", index, char(index % 256), cptr[index]);
	// 			exit(1);
	// 		}

	// 		unsigned char val = rand() % 256;
	// 		cptr[index] = val;
	// 		modifications[std::tuple<void*, size_t>(ptr, index)] = val;
	// 	}
	// }
	// sec2.stop();

	// // print_mmap_status();

	// // Validate modifications
	
	// for (auto it = modifications.begin(); it !=modifications.end(); it++){
	// 	unsigned char* cptr = static_cast<unsigned char*>(std::get<0>(it->first));
	// 	size_t index = std::get<1>(it->first);
	// 	if (cptr[index] != it->second){
	// 		printf("Error mod1: not equal, %zu, %d, %d\n", index, char(index % 256), cptr[index]);
	// 		exit(1);
	// 	}
	// }
	// modifications.clear();

	// sec3.start();
	// for (auto it = ptrs.begin(); it != ptrs.end(); it++){
	// 	void* ptr = *it;
	// 	arena_munmap(ptr, 5 * M);
	// 	// printf("munmap ptr: %p\n", ptr);
	// }
	// sec3.stop();

	print_mmap_status();
	print_timer("\t", "sec1", sec1);
	print_timer("\t", "sec2", sec2);
	print_timer("\t", "sec3", sec3);
	// print_timer("\t", "access", access);

	destroy_mmap_arena();
}