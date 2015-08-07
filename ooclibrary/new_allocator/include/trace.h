#ifndef __TRACE_H__
#define __TRACE_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <execinfo.h>
#include <errno.h>
#include <string.h>

#include <algorithm>

#include "const.h"
#include "common.h"
#include "timer.h"

#define SKIP 3
#define TRACE 3
#define TRACE_SIZE (SKIP + TRACE)
#define WRITE_BUFFER_SIZE 4096
#define INTERVAL 5

typedef std::basic_string<char, std::char_traits<char>, container_allocator<char> > mem_safe_string;

template<typename idx_type>
class _tracer_t{

	bool _valid;
	timer t;
	FILE* fd;
	FILE* symbol_fd;
	void* _buffer[TRACE_SIZE];

	std::unordered_map<void*, size_t, 
							std::hash<void*>, 
							std::equal_to<void*>, 
							container_allocator<std::pair<const void*, unsigned int> > > histogram;

	std::vector<void*, container_allocator<void*> > address_array;

public:

	inline _tracer_t(const char* path, const char* symbol_path):_valid(true) {

		fd = fopen(path, "w");
		if (fd < 0){
			ERROR_PRINT("open trace file failed, %s\n", strerror(errno));
			_valid = false;
			return;
		}
		// symbol_fd = open(symbol_path, O_CREAT | O_RDWR, S_IRWXU);
		symbol_fd = fopen(symbol_path, "w+");
		if (symbol_fd == NULL){
			ERROR_PRINT("open trace symbol fd failed, %s\n", strerror(errno));
			fclose(fd);
			_valid = false;
		}

		t.start();
	}

	inline ~_tracer_t(){
		if (_valid){
				fclose(fd);
				fclose(symbol_fd);
		}
	}

	inline bool valid(){
		return _valid;
	}

	inline void clear(){
		histogram.clear();
		address_array.clear();
		t.clear();
	}

	static std::vector<mem_safe_string, container_allocator<mem_safe_string> > backtrace_symbols_unsafe(void* addrs[], size_t size){
		char** strs = backtrace_symbols(addrs, size);
		std::vector<mem_safe_string, container_allocator<mem_safe_string> > ret(size, mem_safe_string());
		std::copy(&strs[0], &strs[size], ret.begin());
		return ret;
	}

	void backtrace_symbols_safe(){
		fseek(symbol_fd, 0, SEEK_SET);
		backtrace_symbols_fd(&address_array[0], address_array.size(), fileno(symbol_fd));
		fseek(symbol_fd, 0, SEEK_SET);
		char buffer[1024];
		for (size_t i = 0; i < address_array.size(); i++){
			char* loc = &buffer[0];
			size_t size = 1024;
			getline(&loc, &size, symbol_fd);
			FPRINTF(fd, "%zu %s", histogram[address_array[i]], buffer);
		}
		FPRINTF(fd, "\n\n");
	}

	inline void dump(){

		fflush(fd);
		auto comp = [this](void* p, void* q) {return (histogram[p] > histogram[q]);};

		std::sort(address_array.begin(), address_array.end(), comp);

		if (address_array.size() == 0){
			return;
		}

		backtrace_symbols_safe();

		clear();

	}

	inline void hit(void* addr){

		auto it = histogram.find(addr);
		if (it != histogram.end()){
			it->second++;
		}else{
			histogram[addr] = 1;
			address_array.push_back(addr);
		}
	}

	inline void trace(){
		t.collect();
		if (t.elapsed() > INTERVAL * 1000000){
			dump();
		}

		int ret = backtrace(_buffer, TRACE_SIZE);
		if (ret < 0){
			ERROR_PRINT("backtrace failed: %d\n", ret);
		}

		for (int i = SKIP; i < ret; i++){
			hit(_buffer[i]);
		}
	}

	// inline void save_trace_symbol(){
	// 	fflush(symbol_fd);
	// 	backtrace_symbols_fd(&inverse_map[0], inverse_map.size(), fileno(symbol_fd));
	// }
};

typedef _tracer_t<unsigned int> tracer_t;

#endif
