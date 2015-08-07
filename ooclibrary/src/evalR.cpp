#include <malloc.h>
#include <string.h>
#include <dlfcn.h>
#include <pthread.h>

#include <string>
#include <sstream>
#include <iterator>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <map>


#include <Rinternals.h>
#include <Rcpp.h>

#include "common.h"
#include "symbol_finder.h"

// // [[Rcpp::export]]
// void SetParameter(size_t threshold, std::string path){
// 	// printf("%p\n", &_allocator.config.large_threshold);

// 	_allocator.config.folder = path;
// 	_allocator.config.large_threshold = threshold;

// 	// size_t* p = &_allocator.config.large_threshold;
// 	// printf("%p %zu\n", p, *p);

// 	// printf("%zu %s\n", _allocator.config.large_threshold, _allocator.config.folder.c_str());
// }

static void get_allocator(){
	void* (*get_allocator)();
	get_allocator = (void* (*)()) dlsym(RTLD_DEFAULT, "get_allocator");
	printf("%p\n", (void*) get_allocator);

	if (get_allocator != NULL){
		get_allocator();
	}
}

void set_mem(size_t size){
	// void (*set_mem_func)(size_t);
	// set_mem_func = (void (*)(size_t)) dlsym(RTLD_DEFAULT, "set_total_memory");
	// printf("%p\n", (void*) set_mem_func);

	// if (set_mem_func != NULL){
	// 	set_mem_func(size);
	// }
	
	CALL_LIB_FUNC_SIGN(set_total_memory, size_t, size);
}



// // [[Rcpp::export]]
// void EnableMMapHooks(){
// 	void (*enable_mmap_hooks)();
// 	enable_mmap_hooks = (void (*)()) dlsym(RTLD_DEFAULT, "enable_mmap_hooks");
// 	printf("%p\n", (void*) enable_mmap_hooks);

// 	if (enable_mmap_hooks != NULL){
// 		enable_mmap_hooks();
// 	}
// }

// [[Rcpp::export]]
void EnableMMapHooks() {
	CALL_LIB_FUNC(enable_mmap_hooks);
}

// [[Rcpp::export]]
void PrintMMapStatus(){
	CALL_LIB_FUNC(print_mmap_status);
}

// [[Rcpp::export]]
void SetMemoryLimit(int mega){

	CALL_LIB_FUNC_SIGN(set_total_memory, size_t, size_t(mega) * 1024UL * 1024UL);
}

// [[Rcpp::export]]
void LockAll(){
	CALL_LIB_FUNC(lock_all_segment);
}

void* _lockAllPeriodically(void* _time){

	void (*lock_func)();
	lock_func = (void (*)()) dlsym(RTLD_DEFAULT, "lock_all_segment");
	printf("%p\n", (void*) lock_func);

	unsigned int t = *(static_cast<unsigned int*>(_time));
	free(_time);

	if (lock_func == NULL){
		return NULL;
	}

	while (true) {
		lock_func();
		sleep(t);
	}

	return NULL;
}

// [[Rcpp::export]]
void LockAllPeriodically(size_t t){
	// TODO(che): Make it thread safe
	unsigned int* _time = (unsigned int*) malloc(sizeof(unsigned int));
	*_time = t;
	pthread_t pt;
	pthread_create(&pt, NULL, _lockAllPeriodically, static_cast<void*>(_time));
}