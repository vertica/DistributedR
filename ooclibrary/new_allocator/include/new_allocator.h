#ifndef __NEW_ALLOCATOR_H__
#define __NEW_ALLOCATOR_H__

#include <sys/types.h>

#include <string>
#include <iterator>
#include <map>
#include <list>
#include <vector>
#include <memory>
#include <unordered_map>
#include <thread>
#include <utility>

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/list.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>

#include "const.h"
#include "helper.h"
#include "meta_allocator.h"
#include "page_struct.h"
#include "ring_buffer.h"
#include "multi_ring_buffer.h"
#include "fast_lookup_table.h"

extern const size_t page_size;
extern size_t load_each_fault;

struct page_entry;

struct mmap_info;
struct abstract_mem_object_t;
struct mem_object_t;

struct shm_mmap_meta_t;
struct shm_object_meta_t;

struct shm_object_t;
struct object_pool_t;
struct page_cache_t;
struct mmap_arena;

// typedef mem_object_t* object_handle;
typedef abstract_mem_object_t* object_handle;
typedef mem_object_t* mem_object_handle;
typedef shm_object_t* shm_object_handle;
typedef mmap_info* mmap_handle;


/**
 * std container with custom handler
 */

typedef std::list<object_handle, container_allocator<object_handle> > object_list_t;
typedef object_list_t::iterator object_list_iterator;
typedef std::basic_string<char, std::char_traits<char>, container_allocator<char> > mem_safe_string;

/**
 * IO functions
 */

int read_object(mem_object_handle object);
int write_object(mem_object_handle object);


/**
 * structure represents virtual memory space
 */
struct vm_info_t {
	void* addr;
	size_t size;

	mmap_info* region;
	size_t offset;
};

/**
 * structure represents object pool location
 */
struct pool_info_t {
	object_list_t* list;
	object_list_iterator position;
	size_t epoch;
};

// template<typename lock_type_t>
// struct fd_info_t {
// 	lock_type_t lock;
// 	int fd;
// 	size_t offset;
// };

/**
 * 
 */
struct abstract_mem_object_t {
	enum mem_object_type {
		MEM,
		SHM
	};
	mem_object_type type;
	vm_info_t vm_info;
	pool_info_t pool_info;

	virtual ~abstract_mem_object_t();
	virtual int destroy() = 0;
};

/**
 * private memory chunk
 */
struct mem_object_t : public abstract_mem_object_t{
public:

	void* store;
	size_t version;

	enum mem_state_t {
		NEW,
		IN,
		IN_RD,
		OUT,
		DESTROYED
	};
	mem_state_t mem_state;

	// object_list_iterator _list_position;

	explicit mem_object_t(void* addr, size_t size, mmap_info* parent, off_t offset, object_list_t* pool);
	~mem_object_t();
	bool in_mem();
	int destroy();
};

// struct vm_location_t{
// 	void* _addr;
// 	size_t _size;
// };

/**
 * shared memory object meta, embedded in shm_mmap_meta_t
 * represents the shared loaded or non-loaded object
 * 
 * loaded data will be inside [name].[offset]
 */
class shm_object_meta_t {
public:
	enum shm_state_t {
		NEW,
		IN,
		IN_RD,
		OUT,
		DESTROYED
	};

	boost::interprocess::interprocess_mutex m_;

	volatile off_t region_offset_;

	volatile shm_state_t state_;
	volatile size_t ref_count_;

	volatile char in_mem_file_name_[PATH_MAX];
	volatile size_t offset_;	

	volatile size_t object_size_;

	shm_object_meta_t(shm_mmap_meta_t* region, size_t region_offset, size_t offset, size_t object_size);
	shm_mmap_meta_t* get_region() volatile;
	int load_to_shm() volatile;
	int dump_to_disk() volatile;
};

/**
 * On disk representation of mmap region meta data, in [name].meta
 */
struct shm_mmap_meta_t {
	size_t total_size;
	size_t object_size;

	bool destroyed;

	volatile char shm_name_[PATH_MAX];
	volatile char meta_file_name_[PATH_MAX];
	volatile char disk_file_name_[PATH_MAX];
	boost::interprocess::interprocess_mutex file_lock_;

	size_t object_count_;
	shm_object_meta_t object_array_[];

	shm_mmap_meta_t(
		const char* shm_name, 
		const char* meta_file_name,
		const char* disk_file_name,
		size_t object_count,
		size_t total_size, size_t object_size);

	static shm_mmap_meta_t* create(const char* shm_name, const char* disk_file_name, size_t total_size, size_t object_size);
	static shm_mmap_meta_t* attach(const char* shm_name);
	static int remove(const char* path);
};

/**
 * shared memory chunk
 */
struct shm_object_t : public abstract_mem_object_t {
	// volatile shm_mmap_meta_t* shared_mmap_meta;
	volatile shm_object_meta_t* shared_object_meta;

	enum shm_private_state_t{
		OUT,
		IN,
		DESTROYED
	};

	shm_private_state_t mem_state;

	explicit shm_object_t(void* addr, size_t size, 
		mmap_info* region, off_t offset,
		shm_object_meta_t* meta,
		object_list_t* pool);
	~shm_object_t();
	bool in_mem();
	bool in_shm();
	int destroy();
};

struct mmap_private_create_t {};
struct mmap_shared_create_t {};
struct mmap_shared_attach_t {};

static mmap_private_create_t mmap_private_create = mmap_private_create_t();
static mmap_shared_create_t mmap_shared_create = mmap_shared_create_t();
static mmap_shared_attach_t mmap_shared_attach = mmap_shared_attach_t();

/**
 * mmap region meta data
 */
struct mmap_info {
	mmap_arena* _arena;
	object_pool_t* _cache;

	std::vector<object_handle, container_allocator<object_handle> > _objects;
	mem_safe_string _path;
	void* _start_ptr;
	size_t _total_size;
	size_t _object_size;

	int fd;
	std::mutex fd_lock;

	bool _destroyed;
	bool _unlink_on_destroy;

	enum in_mem_preference_enum {
		NONE,
		META,
		ALL
	};

	in_mem_preference_enum _in_mem_preference;


	explicit mmap_info(mmap_private_create_t, mmap_arena* arena,
		const char* path, 
		size_t total_size, size_t object_size, 
		in_mem_preference_enum in_mem_preference);
	explicit mmap_info(mmap_shared_create_t, mmap_arena* arena, 
		const char* shm_name,
		const char* path, 
		size_t total_size, size_t object_size, 
		in_mem_preference_enum in_mem_preference);
	explicit mmap_info(mmap_shared_attach_t, mmap_arena* arena,
		const char* shm_name,
		in_mem_preference_enum in_mem_preference);
	~mmap_info();
	void munmap(void* ptr, size_t size);
	void destroy(bool sync);
	object_handle get_object(void* addr, size_t size);
};

class vm_manager_t {
	static inline size_t page_align(size_t s){return (s + page_size - 1) & ~(page_size - 1);}

public:

	static void* vm_align_mmap(size_t size);
	static int vm_munmap(void* ptr, size_t size);

	static int vm_mask(void* ptr, size_t size);
	static int vm_readonly_mask(void* ptr, size_t size);
	static int vm_unmask(void* ptr, size_t size);

	static int vm_clean_mmap(void* ptr, size_t size);
};

/**
 * virtual memory and physical memory manager
 */
struct allocator_t {
	struct reservation_t {
		bool valid;
		size_t size;
	};

	typedef reservation_t* reservation_handle;

	virtual size_t available() = 0;
	virtual size_t used() = 0;
	virtual size_t total() = 0;

	virtual reservation_handle try_reserve(size_t size) = 0;
	virtual reservation_handle try_reserve_shm(size_t size) = 0;
	virtual int mem_load_object(mem_object_handle object, reservation_handle rhandle) = 0;
	virtual int mem_evict_object(mem_object_handle object, bool sync, reservation_handle rhandle) = 0;
	virtual int mem_load_shm_object(shm_object_handle object, reservation_handle rhandle) = 0;
	virtual int mem_evict_shm_object(shm_object_handle object, bool sync, reservation_handle rhandle) = 0;
	virtual int refresh_object(object_handle object) = 0;
	virtual bool need_refresh_object(object_handle object) = 0;
};

/**
 * Single process allocator
 */
struct reservation_object_allocator_t : public allocator_t {

	size_t _available;
	size_t _allocated;

	size_t available();
	size_t used();
	size_t total();

	typedef reservation_t* reservation_handle;

	reservation_object_allocator_t(size_t total_size);

	reservation_handle try_reserve(size_t size);
	reservation_handle try_reserve_shm(size_t size);
	int mem_load_object(mem_object_handle object, reservation_handle rhandle);
	int mem_evict_object(mem_object_handle object, bool sync, reservation_handle rhandle);

	int mem_load_shm_object(shm_object_handle object, reservation_handle rhandle);
	int mem_evict_shm_object(shm_object_handle object, bool sync, reservation_handle rhandle);

	int refresh_object(object_handle object);
	bool need_refresh_object(object_handle object);
private:
	int alloc_memory_inplace(mem_object_handle, reservation_handle rhandle);
	int free_memory_inplace(mem_object_handle, reservation_handle rhandle);
};

/**
 * multi process allocator
 */
struct multi_reservation_object_allocator_t : public allocator_t {

	struct shared_memory_pool_t {
		boost::interprocess::interprocess_mutex m;
		volatile size_t head_epoch, tail_epoch;
		volatile size_t available;
		volatile size_t reserving, reserved;

		size_t shared_try_acquire(size_t size);
		int shared_release(size_t size);

		shared_memory_pool_t(size_t available_);

		static void init(const char* name, size_t available_);
		static shared_memory_pool_t* join(const char* name);
	};

	size_t _private_head_epoch;
	size_t _private_tail_epoch;

	size_t _shm_reserved;

	size_t _private_total;
	size_t _private_available;
	size_t _allocated;

	static const char* pool_object_name;
	shared_memory_pool_t* shm_pool;

	size_t available();
	size_t used();
	size_t total();

	typedef reservation_t* reservation_handle;

	multi_reservation_object_allocator_t(size_t total_size, const char* shm_name);

	reservation_handle try_reserve(size_t size);
	reservation_handle try_reserve_shm(size_t size);
	int try_clear_reserving();
	int mem_load_object(mem_object_handle object, reservation_handle rhandle);
	int mem_evict_object(mem_object_handle object, bool sync, reservation_handle rhandle);

	int mem_load_shm_object(shm_object_handle object, reservation_handle rhandle);
	int mem_evict_shm_object(shm_object_handle object, bool sync, reservation_handle rhandle);

	int refresh_object(object_handle object);
	bool need_refresh_object(object_handle object);
private:
	size_t next_head_epoch();
	int update_tail_epoch(size_t);
	size_t get_head_epoch();
	size_t get_tail_epoch();

	int combined_release(size_t);
	size_t private_acquire(size_t);
	size_t shared_acquire(size_t);

	int alloc_memory_inplace(mem_object_handle, reservation_handle rhandle);
	int free_memory_inplace(mem_object_handle, reservation_handle rhandle);

	int alloc_shm_inplace(volatile shm_object_meta_t*, reservation_handle);
};

// #define allocator_t reservation_object_allocator_t

/**
 * LRU object cache
 */
struct object_pool_t {
private:
	mmap_arena* _arena;

	// typedef std::list<object_handle> object_list_t;
	// typedef std::unordered_map<object_handle, list_iterator> object_it_map_t;

	allocator_t* _allocator;

	object_list_t object_list;
	object_list_t in_mem_object_list;
	// object_it_map_t object_map;
	
	struct pool_statistic_t {
		size_t _evict_count;
		size_t _evict_size_KB;

		void collect_evict(size_t size){
			_evict_count++;
			_evict_size_KB+= size;
		}
	};

	pool_statistic_t _statistic;

public:
	explicit object_pool_t(mmap_arena* arena, allocator_t* allocator);

	// object_list_t& get_list(object_handle object);
	object_list_t* get_list(bool in_mem);

	int evict_to(allocator_t::reservation_handle handle);

	int evict_object(object_handle object, bool sync, allocator_t::reservation_handle rhandle);
	int evict_mem_object(mem_object_handle object, bool sync, allocator_t::reservation_handle rhandle);
	int evict_shm_object(shm_object_handle object, bool sync, allocator_t::reservation_handle rhandle);
	int touch_object(object_handle object);
	int load_object(object_handle object);
	int load_mem_object(mem_object_handle object);
	int load_shm_object(shm_object_handle object);

	int clean_pool();

	// int resize(size_t new_size);
	// int reserve(size_t size);
	// int release(size_t size);
};


/**
 * page mask
 */
struct page_cache_t {

	mmap_arena* _arena;
	allocator_t* _allocator;

	// typedef std::list<page_entry> page_list_t;
	// typedef ring_buffer<page_entry> page_list_t;
	typedef multi_page_ring_buffer<container_allocator<page_entry> > page_list_t;
	typedef std::list<page_entry, container_allocator<page_entry> > force_in_mem_page_list_t;
	// typedef std::unordered_map<void*, page_list_t::iterator> page_it_map_t;
	// page_list_t page_list;
	// force_in_mem_page_list_t force_in_mem_page_list;

	// page_it_map_t page_map;

	size_t current_page_count;
	size_t total_page_count;

	explicit page_cache_t(mmap_arena* arena, size_t total_size);

	int load(void* addr);
	int load_object(object_handle object);

	object_handle get_object(void* addr);
};

/**
 * entrance of library
 */
struct mmap_arena {

	struct config{
		size_t object_size;
		size_t object_cache_size;
		size_t page_cache_size;
		unsigned char enable_meta_in_memory;

		bool use_shm;
		const char* shm_name;
	};

	typedef std::map<void*, mmap_handle, std::less<void*>, container_allocator<std::pair<void*, mmap_handle> > >\
		page_translate_table_t;

	const config _conf;

	fast_lookup_table page_table;
	range_lookup_unordered_map<ALIGNMENT> second_page_table;
	
	mem_safe_string _folder;

	std::shared_ptr<allocator_t> _allocator;
	std::shared_ptr<page_cache_t> _page_cache;
	std::shared_ptr<object_pool_t> _object_cache;

	std::mutex _global_mutex;

	explicit mmap_arena(const char* folder, config conf);
	~mmap_arena();

	mmap_handle mmap(void* addr, size_t size, int mode);
	mmap_handle shm_create(void* addr, const char* name, size_t size);
	mmap_handle shm_attach(void* addr, const char* name, size_t size);
	int partial_munmap(void* addr, size_t size);

	int reserve(size_t size);
	int release(size_t size);

	mmap_handle get_mmap_handle(void* addr, size_t size);
	object_handle get_object(void* addr, size_t size);
};

#endif