#include "wrapper.h"

#include <sys/mman.h>
#include <unistd.h>

#include "const.h"
#include "common.h"
#include "new_allocator.h"
#include "profiling.h"
#include "trace.h"
#include "meta_allocator.h"

mmap_arena* arena;
// static mmap_arena::config conf;

static int _inited = 0;
static int _enabled = 0;

static struct sigaction old_sa;

#ifdef USE_TRACER
static tracer_t* _tracer = nullptr;
#endif

#undef TEST_PRINT
#define TEST_PRINT(...) fprintf(stderr, __VA_ARGS__)

inline long getenv_long(const char* name, long default_value){
	char* str = getenv(name);
	if (str == NULL){
		return default_value;
	}
	return atol(str);
}

void print_mmap_status(){
	if (!_inited){
		TEST_PRINT("arena not inited\n");
		return;
	}

	TEST_PRINT("mmap_arena:\n");
	TEST_PRINT("\tobject_cache:\t%zuM / %zuM\n", arena->_allocator->used() / M, arena->_allocator->total() / M);
	print_timers("\t");

#ifdef USE_TRACER
	if (_tracer != nullptr){
		_tracer->trace();
	}

#endif

}

int init_tracer(){
	char* trace_file = getenv("MMAP_TRACE_FILE");
	char* trace_symbol_file = getenv("MMAP_TRACE_SYMBOL_FILE");

	if (trace_file != nullptr && trace_symbol_file != nullptr){
		TEST_PRINT("Initializing tracer\n");
		tracer_t* mem = meta_allocator<tracer_t>().allocate(1);
		_tracer = new(mem) tracer_t(trace_file, trace_symbol_file);

		if (!_tracer->valid()){
			_tracer = nullptr;
			ERROR_PRINT("tracer intialization failed\n");
			meta_allocator<tracer_t>().deallocate(mem, 1);
		}
		TEST_PRINT("Initialized\n");

	}
	return 0;
}

int try_init_from_env(){
	char* folder = getenv("MMAP_FOLDER");
	size_t object_cache_size = getenv_long("MMAP_MEMORY_LIMIT", 256UL * M);
	size_t object_size = getenv_long("MMAP_OBJECT_SIZE", DEFAULT_LARGE_THRESHOLD / 2);

	if (folder == NULL){
		return -1;
	}

	init_mmap_arena(folder, object_cache_size, object_size);
	TEST_PRINT("Successfully inited mmap arena\n");

#ifdef USE_TRACER
	init_tracer();
#endif


	return 0;
}

void enable_mmap_hooks(){
	if (!_inited){
		if (try_init_from_env() == 0){
			_enabled = 1;
		}
		return;
	}

	_enabled = 1;
}

void disable_mmap_hooks(){
	_enabled = 0;
}

class fileback_disabler {

	int enabled;
	
public:

	inline fileback_disabler(){ enabled = _enabled; _enabled = 0; }
	inline ~fileback_disabler() { _enabled = enabled; }
};

void init_from_config(const char* path, mmap_arena::config conf){
	arena = new mmap_arena(path, conf);

	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sigaddset(&sa.sa_mask, SIGSEGV);
	sa.sa_sigaction = handle_sigsegv;

	sigaction(SIGSEGV, &sa, &old_sa);

	_inited = 1;
}

void init_mmap_arena(const char* path, size_t object_cache_size, size_t object_size){

	mmap_arena::config conf;
	conf.object_cache_size = object_cache_size;
	conf.object_size = object_size;
	conf.page_cache_size = 0;
	conf.enable_meta_in_memory = 1;

	conf.use_shm = false;
	conf.shm_name = nullptr;

	init_from_config(path, conf);
}

void init_mmap_arena_shared(const char* path, size_t object_cache_size, size_t object_size, const char* shm_name){

	mmap_arena::config conf;
	conf.object_cache_size = object_cache_size;
	conf.object_size = object_size;
	conf.page_cache_size = 0;
	conf.enable_meta_in_memory = 1;

	conf.use_shm = true;
	conf.shm_name = shm_name;

	init_from_config(path, conf);
}

// TODO
void destroy_mmap_arena(){
}

// bool should_mmap_disk(size_t size){
// 	return size >= 2 * conf.object_size;
// }

void* mmap_in_mem(void* addr, size_t size){
	void* ptr;
	if (addr == NULL){
		ptr = mmap(
			NULL, 
			size, 
			PROT_READ | PROT_WRITE,
			MAP_SHARED | MAP_ANON,
			-1,
			0);
	}else{
		ptr = mmap(
			addr, 
			size, 
			PROT_READ | PROT_WRITE,
			MAP_SHARED | MAP_ANON | MAP_FIXED,
			-1,
			0);
	}
	return ptr;
}

void* mmap_dispatch(size_t size, int mode){
	// TEST_PRINT("DISPATCH MMAP: %zu, %d, %d\n", size, _inited, _enabled);
	if (!_inited || !_enabled){
		return mmap_in_mem(NULL, size);
	}
	// TEST_PRINT("DISPATCH MMAP: using arena mmap\n");
	fileback_disabler disabler;
	return arena_mmap(NULL, size, mode);
}

int munmap_dispatch(void* ptr, size_t size){
	// TEST_PRINT("DISPATCH MUNMAP: %p %zu\n", ptr, size);
	if (!_inited || !_enabled){
		return munmap(ptr, size);
	}

	fileback_disabler disabler;

	int ret = arena_munmap(ptr, size);
	if (ret >= 0){
		return ret;
	}
	else{
		return munmap(ptr, size);
	}
}

void* arena_mmap(void* ptr, size_t size, int mode){
	auto handle = arena->mmap(ptr, size, mode);
	return handle->_start_ptr;
}

int arena_munmap(void* ptr, size_t size){
	return arena->partial_munmap(ptr, size);
}

void* arena_shm_mmap(const char* name, void* ptr, size_t size, int mode){
	auto handle = arena->shm_create(ptr, name, size);
	return handle->_start_ptr;
}

// int arena_reserve(size_t size){
// 	return arena->reserve(size);
// }

// int arena_release(size_t size){
// 	return arena->release(size);
// }

void handle_sigsegv(int signum, siginfo_t* si, void* unused){
	timer_scope _scope(timers.handler_total);
#ifdef USE_TRACER
	if (_tracer != nullptr){
		timer_scope _scope(timers.trace_log);
		_tracer->trace();
	}
#endif

	int ret;
	if ((ret = arena->_page_cache->load(si->si_addr)) < 0){
		ERROR_PRINT("FATAL: SEGV NOT HANDLED for %p\n", si->si_addr);
		old_sa.sa_sigaction(signum, si, unused);
	}
}