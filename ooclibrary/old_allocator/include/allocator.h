#ifndef __ALLOCATOR_H_
#define __ALLOCATOR_H_

#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>

#include <cstdint>
#include <string>
#include <sstream>
#include <iterator>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <map>
#include <list>
#include <memory>

#include "common.h"
#include "helper.h"
#include "timer.h"
// extern "C"{
#include "mmap_malloc.h"
#include "large_malloc.h"
// }


class mutex_lock{
	pthread_mutex_t* _mutex;
public:
	inline mutex_lock(pthread_mutex_t* mutex):_mutex(mutex){
		pthread_mutex_lock(_mutex);
	}

	~mutex_lock(){
		pthread_mutex_unlock(_mutex);
	}
};

struct meta_info{
	void* ptr;
	size_t size;
	char* path;
	mmap_handle handle;
	int detached; // detached if directly in memory rather than mmaped in
	// int can_lock;

	// inline ~meta_info(){
	// 	free(path);
	// }

	inline bool is_file_backed() const{
		return path != NULL;
	}
};

typedef std::shared_ptr<meta_info> meta_info_ptr;

class file_deleter{
public:
	~file_deleter();
	void recycle();
};

typedef std::map<void*, std::shared_ptr<meta_info> > direct_malloc_map_t;

class large_only_malloc_map {
public:
	typedef direct_malloc_map_t::value_type value_type;
	typedef direct_malloc_map_t::key_type key_type;
	typedef direct_malloc_map_t::mapped_type mapped_type;
	typedef direct_malloc_map_t::iterator iterator;
	typedef direct_malloc_map_t::const_iterator const_iterator;

	inline iterator find(const key_type& key){
		return _malloc_map.find(key);
	}
	inline iterator begin(){
		return _malloc_map.begin();
	}
	inline iterator end(){
		return _malloc_map.end();
	}

	inline void erase(iterator it){
		_malloc_map.erase(it);
		return;
	}

	inline std::pair<iterator, bool> insert(const value_type& pair){
		if (!pair.second->is_file_backed()){
			return std::pair<iterator, bool>(end(), false);
		}else{
			return _malloc_map.insert(pair);
		}
	}

private:
	direct_malloc_map_t _malloc_map;
};

typedef direct_malloc_map_t malloc_map_t; // never free malloc_map_t

static const int tab64[64] = {
    63,  0, 58,  1, 59, 47, 53,  2,
    60, 39, 48, 27, 54, 33, 42,  3,
    61, 51, 37, 40, 49, 18, 28, 20,
    55, 30, 34, 11, 43, 14, 22,  4,
    62, 57, 46, 52, 38, 26, 32, 41,
    50, 36, 17, 19, 29, 10, 13, 21,
    56, 45, 25, 31, 35, 16,  9, 12,
    44, 24, 15,  8, 23,  7,  6,  5};

static int log2_64 (uint64_t value)
{
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    return tab64[((uint64_t)((value - (value >> 1))*0x07EDD5E59A4E28C2)) >> 58];
}

struct mmap_status{
	size_t distribution[65];
	size_t in_mem;
	size_t current;
	size_t total;

	inline void init(){
		in_mem = 0;
		current = 0;
		total = 0;
		memset(distribution, 0, sizeof(size_t) * 65);
	}

	inline void print_status(){
		TEST_PRINT("[mmap_status] Allocation distribution:\n");
		for (int i = 0; i <= 64; i++){
			if (distribution[i] == 0){
				int j = i;
				while (distribution[j] == 0 && j <= 64){
					j++;
				}
				j--;
				TEST_PRINT("[mmap_status] \t2^%d ~ 2^%d: %zu\n", i, j, 0);
				i = j;
			}else{
				TEST_PRINT("[mmap_status] \t2^%d ~ 2^%d: %zu\n", i, i+1, distribution[i]);
			}
		}
	}

	inline void small_alloc(size_t size){
		in_mem += size;
		current += size;
		total += size;
		distribution[log2_64(size)]++;
	}

	inline void large_alloc(size_t size){
		current += size;
		total += size;
		distribution[log2_64(size)]++;
	}

	inline void small_free(size_t size){
		in_mem -= size;
		current -= size;
	}

	inline void large_free(size_t size){
		current -= size;
	}
};

#define allocator_status mmap_status

struct allocator_config{
	std::string folder;
	size_t in_memory_size;
	// size_t large_threshold;
	size_t total_memory;
};

struct allocator_t{
	malloc_map_t* malloc_map;
	allocator_config config;
	allocator_status status;
	timer mmap_init_timer, logging_timer, truncate_timer;
	FILE* _log_fd;
	pthread_mutex_t mutex;
	struct sigaction sa;

	allocator_t();
	allocator_t(FILE* log_fd);

	inline void report_mem_usage(){
		TEST_PRINT("in_mem: %zu, current: %zu, history: %zu, allowed: %zu\n", status.in_mem, status.current, status.total, config.total_memory);
	}

	inline void report_timer(){
		TEST_PRINT("mmap_init: %zuus, logging: %zuus, truncate: %zuus\n", mmap_init_timer.elapsed(), logging_timer.elapsed(), truncate_timer.elapsed());
	}

	void report_incore_status();

	meta_info_ptr find_segment(void* ptr);
	void lock_all();
	void set_watch_handler();

	void* mmap_dispatch(void* addr, size_t size, int mode);
	int munmap_dispatch(void* ptr, size_t size);
	void msync_dispatch(void* ptr, size_t size, bool sync);
	void detach_segment(void* ptr);
	void attach_segment(void* ptr);
};
extern allocator_t* _allocator;


#endif