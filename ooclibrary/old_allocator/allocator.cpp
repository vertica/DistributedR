#include "allocator.h"

#include "gprintf.h"

#include <ctime>
#include <cstdlib>
#include <sys/mman.h>

#include <memory>
#include <vector>

allocator_t* _allocator;

static const char _file_folder[] = DEFAULT_DISK_PATH;
static const size_t _in_memory_size = 4UL * 1024UL;
// static const size_t _large_threshold = 2 * DEFAULT_LARGE_THRESHOLD;
static const size_t _total_memory = 1UL << 63;

static int _inited = 0;
static size_t pagesize;

static inline void _init_pagesize(){
	if (pagesize == 0){
		pagesize = getpagesize();
	}
}

static inline long getenv_long(const char* name, long default_value){
	char* str = getenv(name);
	if (str == NULL){
		return default_value;
	}
	return atol(str);
}

void _init(){
	_init_pagesize();

	_allocator = NULL;
	char* allocator_log = getenv(ENV_ALLOCATOR_LOG_LOCATION);

	if (allocator_log != nullptr){
		FILE* f = fopen(allocator_log, "wb");
		if (f != nullptr){
			_allocator = new allocator_t(f);
		}
	}

	if (_allocator == NULL){
		_allocator = new allocator_t();
	}

	char* folder = getenv("MMAP_FOLDER");
	size_t mem_allowed = getenv_long("MMAP_MEMORY_LIMIT", 2048UL * M);

	if (folder == NULL){
		ERROR_PRINT("no MMAP_FOLDER\n");
		return;
	}


	_allocator->config.folder = folder;
	_allocator->config.in_memory_size = _in_memory_size;
	// _allocator->config.large_threshold = _large_threshold;
	_allocator->config.total_memory = mem_allowed;

	_allocator->status.init();

	_inited = 1;

	print_mmap_status();

}

void set_folder_path(const char* path){
	_allocator->config.folder = path;
}

void set_total_memory(size_t size){
	_allocator->config.total_memory = size;
}

void print_mmap_status(){
	if (!_inited){
		ERROR_PRINT("allocator not inited\n");
		return;
	}
	TEST_PRINT("Config: Total %zuMB\n", _allocator->config.total_memory / M);
	_allocator->status.print_status();
	_allocator->report_timer();
}

void lock_all_segment(){
	_allocator->lock_all();
}

inline bool should_mmap_file(size_t size, allocator_t& allocator, int mode){
	// allocator.report_incore_status();
	allocator.report_mem_usage();
	if (allocator.status.in_mem * 8 > allocator.config.total_memory * 7 ){
		TEST_PRINT("Warning: Falling back to file backed allocation\n");
		return true;
	}


	return mode == 1;
	// return size >= allocator.config.large_threshold + allocator.config.large_threshold / 2;
}

// // Test function
// inline bool never_mmap_file(size_t size, allocator_t& allocator){
// 	if (allocator.status.current > allocator.config.total_memory - MOVE_THRESHOLD){
// 		// bfprint(stdout, "Cannot allocate from memory, using file back\n", 46);
// 		return true;
// 	}

// 	return false;
// }

// #define should_mmap_file never_mmap_file

static int _enabled = 0;

void enable_mmap_hooks(){
	if (!_inited){
		_init();
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

void* dlmmap(size_t size, int mode){
	return _allocator->mmap_dispatch(NULL, size, mode);
}

int dlmunmap(void* ptr, size_t size){
	return _allocator->munmap_dispatch(ptr, size);
}

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

allocator_t::allocator_t() : malloc_map(new malloc_map_t()), _log_fd(NULL){
	pthread_mutex_init(&mutex, NULL);
}

allocator_t::allocator_t(FILE* log_fd) : malloc_map(new malloc_map_t()),
										  _log_fd(log_fd){
	pthread_mutex_destroy(&mutex);
}

#define ALLOCATOR_BUFFER_SIZE (256UL * 1024UL)
static unsigned char buffer[ALLOCATOR_BUFFER_SIZE];

void allocator_t::report_incore_status(){
	// Binary report format:
	// <address>, size: [bit-indicate-in-memory]

	logging_timer.start();

	if (_log_fd == NULL){
//		TEST_PRINT("binary report works with file log only\n");
		return;
	}

	for (auto it = malloc_map->begin(); it != malloc_map->end(); it++){
		auto meta = *(it->second);
		fprintf(_log_fd, "%p, %zu: ", meta.ptr, meta.size);

		for (size_t offset = 0; offset < meta.size; offset += pagesize * ALLOCATOR_BUFFER_SIZE){
			size_t mem_size = min(meta.size - offset, pagesize * ALLOCATOR_BUFFER_SIZE);
			int ret = mincore(meta.ptr, mem_size, &buffer[0]);
			if (ret < 0){
				TEST_PRINT("binary report failed reporting %p, error: %s\n", meta.ptr, strerror(errno));
				continue;
			}


			for (size_t i = 0; i < mem_size / pagesize; i+= 8){
				unsigned char u = ((buffer[i] & 0x1) << 7) |
							 ((buffer[i+1] & 0x1) << 6) |
							 ((buffer[i+2] & 0x1) << 5) |
							 ((buffer[i+3] & 0x1) << 4) |
							 ((buffer[i+4] & 0x1) << 3) |
							 ((buffer[i+5] & 0x1) << 2) |
							 ((buffer[i+6] & 0x1) << 1) |
							 ((buffer[i+7] & 0x1) << 0);
				fprintf(_log_fd, "%x", u);
			}
		}
		
		fprintf(_log_fd, "\n");
		fprintf(_log_fd, "\n");
	}

	logging_timer.stop();
}

void* allocator_t::mmap_dispatch(void* addr, size_t size, int mode){

	if (!_inited || !_enabled){
		return mmap_in_mem(addr, size);
	}

	size = (size + pagesize - 1) & ~(pagesize - 1);

	fileback_disabler();

	TEST_PRINT("mmaping: %p %zu\n", addr, size);

	void* ptr = NULL;
	auto meta = std::make_shared<meta_info>();

	if (!should_mmap_file(size, *this, mode)){
		meta->size = size;
		meta->path = NULL;

		ptr = mmap_in_mem(addr, size);

		if (ptr != NULL){
			this->status.small_alloc(size);
			meta->ptr = ptr;
			meta->detached = 1;
		}
	}

	if (ptr == NULL){
		// Fallback to on disk
		
		meta->size = size;
		meta->detached = 0;

		char* filename = (char*) malloc(33);
		generate_uuid_string(filename);

		char* path = (char*) malloc(FILE_HANDLE_PATH_MAX);
		snprintf(path, FILE_HANDLE_PATH_MAX, "%s/%s", this->config.folder.c_str(), filename);

		meta->path = path;

		mmap_handle handle = touch_file(this->config.folder.c_str(), filename, size, truncate_timer);
		meta->handle = handle;

		ptr = mmap_large(addr, this->config.in_memory_size, 
			max(size - this->config.in_memory_size, 0), handle, 0);

		if (ptr != NULL){
			mmap_init_timer.start();
			memset(ptr, 0, size);
			mmap_init_timer.stop();
		}

		meta->ptr = ptr;
		this->status.large_alloc(size);
		free(filename);

	}

	if (ptr == NULL){
		ERROR_PRINT("mmap failed\n");
		return NULL;
	}

	this->malloc_map->insert(malloc_map_t::value_type(ptr, meta));


	// TODO: thread safety
	char time_buffer[1024];
	time_t t = time(NULL);
	struct tm * p = localtime(&t);
	strftime(time_buffer, 1000, "%F %T", p);
	TEST_PRINT("mmap: %p %zu -> %p, on disk: %s, time: %s\n", addr, size, ptr, meta->path, time_buffer);

	return ptr;
}

// TODO: HANDLE IRREGULAR munmap
int allocator_t::munmap_dispatch(void* ptr, size_t size){

	if (!_inited){
		return munmap(ptr, size);
	}

	if (size == 0){
		return 0;
	}

	fileback_disabler();

	malloc_map_t::iterator it = this->malloc_map->find(ptr);
	int ret;

	if (it != this->malloc_map->end()){
		auto meta = *(it->second);
		if (meta.is_file_backed()){
			// free
			ret = munmap_large(ptr, meta.size);

			// clean up handle
			close(meta.handle.fd);

			// clean up file
			unlink(meta.path);
			free(meta.path);
			meta.path = NULL;

			this->status.large_free(meta.size);
		}else{
			ret = munmap(ptr, meta.size);

			this->status.small_free(meta.size);
		}

		this->malloc_map->erase(it);
	}else{
		ret = munmap(ptr, size);
	}

	TEST_PRINT("munmap: %p %zu -> %d\n", ptr, size, ret);

	return ret;
}

meta_info_ptr allocator_t::find_segment(void* ptr){
	malloc_map_t::iterator it = this->malloc_map->find(ptr);
	if (it == this->malloc_map->end()){
		// TODO hande system msync
		return meta_info_ptr(nullptr);
	}else{
		return it->second;
	}
}

static void report_handler(int sig, siginfo_t *si, void *unused){
	mutex_lock lock(&_allocator->mutex);

	unsigned char* ptr = static_cast<unsigned char*>(si->si_addr);
	unsigned char* page_ptr = reinterpret_cast<unsigned char*>(reinterpret_cast<size_t>(ptr) & ~(pagesize - 1));
	mprotect(page_ptr, pagesize, PROT_READ | PROT_WRITE);	

	GTEST_PRINT("Accessed address: %p, aligned: %p\n", si->si_addr, page_ptr);
	// bfprint_hex<void*>(stderr, si->si_addr);
	auto& malloc_map = *(_allocator->malloc_map);

	GTEST_PRINT("Before upper_bound\n");
	auto it = malloc_map.upper_bound(si->si_addr);
	GTEST_PRINT("After upper_bound\n");

	if (it == malloc_map.begin()){
		mprotect(page_ptr, pagesize, PROT_READ | PROT_WRITE);
		GTEST_PRINT("\tmmap chunk info: %p\n", static_cast<void*>(NULL));
		ERROR_PRINT("\tsegfault on none locked seg\n");
	}
	it--;

	GTEST_PRINT("Marking writable\n");

	auto& meta = *(it->second);
	mprotect(meta.ptr, meta.size, PROT_READ | PROT_WRITE);
	TEST_PRINT("\tmmap chunk info: %p, size: %lu\n", meta.ptr, meta.size);
	GTEST_PRINT("\tmmap chunk info: %p, size: %lu\n", meta.ptr, meta.size);
	fflush(stderr);
	// *ptr = *ptr;
	GTEST_PRINT("Accessed\n");
}

void allocator_t::set_watch_handler(){
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = report_handler;

	sigaction(SIGSEGV, &sa, NULL);
}

void allocator_t::lock_all(){
	mutex_lock lock(&mutex);
	for (auto it = malloc_map->begin(); it != malloc_map->end(); it++){
		auto& meta = *(it->second);
		if (meta.is_file_backed()){
			mprotect(meta.ptr, meta.size, PROT_NONE);
		}
	}

	set_watch_handler();
}

void allocator_t::msync_dispatch(void* ptr, size_t size, bool sync){
	assert(size % pagesize == 0);
	auto meta_ptr = find_segment(ptr);
	if (meta_ptr == nullptr){
		return;
	}
	auto& meta = *meta_ptr;
	if (!meta.detached){
		msync(ptr, size, sync ? (MS_SYNC) : (MS_ASYNC));
	}else{
		unsigned char* bytes = (unsigned char*) ptr;
		size_t page_num = size / pagesize;
		int fd = open(meta.path, O_WRONLY | O_DIRECT);
		lseek(fd, meta.handle.offset + (static_cast<unsigned char*>(ptr) - static_cast<unsigned char*>(meta.ptr)), SEEK_SET);
		if (fd < 0){
			ERROR_PRINT("Open failed, error: %s\n", strerror(errno));
		}
		for (size_t i = 0; i < page_num; i++){
			write(fd, &bytes[i * pagesize], pagesize);
		}
		close(fd);
	}
}

void allocator_t::detach_segment(void* ptr){
	auto meta_ptr = find_segment(ptr);
	if (meta_ptr == nullptr){
		return;
	}
	auto& meta = *meta_ptr;

	if (!meta.detached){
		msync_dispatch(meta.ptr, meta.size, true); // O_DIRECT writes
		mmap(meta.ptr, meta.size, PROT_READ | PROT_WRITE, 
				MAP_FIXED | MAP_SHARED, meta.handle.fd, meta.handle.offset);
	}
}

void allocator_t::attach_segment(void* ptr){
	auto meta_ptr = find_segment(ptr);
	if (meta_ptr == nullptr){
		return;
	}
	auto& meta = *meta_ptr;

	if (meta.detached){
		msync_dispatch(meta.ptr, meta.size, true);
		mmap(meta.ptr, meta.size, PROT_READ | PROT_WRITE, 
				MAP_FIXED | MAP_SHARED, meta.handle.fd, meta.handle.offset);
	}

}
