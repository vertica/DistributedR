// #include "common.h"
#include "new_allocator.h"

#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdint.h>
#include <memory.h>
#include <sys/mman.h>

#include "profiling.h"
#include "signal_blocker.h"

// using mem_object_t::mem_state_enum;
// #define NEW mem_object_t::mem_state_t::NEW
// #define IN mem_object_t::mem_state_t::IN
// #define IN_RD mem_object_t::mem_state_t::IN_RD
// #define OUT mem_object_t::mem_state_t::OUT
// #define DESTROYED mem_object_t::mem_state_t::DESTROYED

#define NONE mmap_info::in_mem_preference_enum::NONE
#define META mmap_info::in_mem_preference_enum::META
#define ALL mmap_info::in_mem_preference_enum::ALL

const size_t page_size = getpagesize();
static const size_t object_dump_page_num = 128;

size_t load_each_fault = MULTI_LOAD;

abstract_mem_object_t::~abstract_mem_object_t(){}

mem_object_t::mem_object_t(
	void* addr, 
	size_t size, 
	mmap_info* region, 
	off_t offset, 
	object_list_t* list)
	
	:version(1),
	mem_state(NEW){

	// printf("%zu\n", size);
	assert(size % page_size == 0);

	type = abstract_mem_object_t::mem_object_type::MEM;
	pool_info = pool_info_t{list, object_list_iterator(), 0},
	vm_info = vm_info_t{addr, size, region, offset};
}
mem_object_t::~mem_object_t(){}

bool mem_object_t::in_mem(){
	return mem_state == IN || mem_state == IN_RD;
}

int mem_object_t::destroy(){
	mem_state = DESTROYED;

	return 0;
}


shm_object_meta_t::shm_object_meta_t(shm_mmap_meta_t* region, size_t region_offset, size_t offset, size_t object_size)
	:region_offset_(region_offset),
	 state_(NEW),
	 ref_count_(0),
	 object_size_(object_size){
	assert(reinterpret_cast<const char*>(region) + region_offset_ == reinterpret_cast<const char*>(this));
	VOLATILE_SNPRINTF(in_mem_file_name_, PATH_MAX, "%s.data+%zu", region->shm_name_, offset);
	offset_ = offset;
}

shm_mmap_meta_t* shm_object_meta_t::get_region() volatile{
	return reinterpret_cast<shm_mmap_meta_t*>(reinterpret_cast<char*>(const_cast<shm_object_meta_t*>(this)) - region_offset_);
}

int shm_object_meta_t::load_to_shm() volatile{
	if (ref_count_ > 0){
		ref_count_++;
		return 1;
	}

	assert(state_ == NEW || state_ == OUT);
	int shm_fd = shm_open(const_cast<const char*>(in_mem_file_name_), O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
	if (shm_fd < 0){
		ERROR_PRINT("chunk exist\n");
		return -1;
	}
	ftruncate(shm_fd, object_size_);

	if (state_ == OUT){
		void* storage = mmap(nullptr, object_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
							shm_fd, 0);
		printf("STORAGE TEMP: %p %zu\n", storage, object_size_);
		close(shm_fd);

		auto region = get_region();
		boost::interprocess::scoped_lock<decltype(region->file_lock_)> lock(region->file_lock_);

		int fd = open(const_cast<const char*>(region->disk_file_name_), O_RDONLY | O_DIRECT);
		lseek(fd, this->offset_, SEEK_SET);
		read(fd, storage, object_size_);
		close(fd);
		munmap(storage, object_size_);
	}
	
	state_ = shm_state_t::IN;
	ref_count_++;
}

int shm_object_meta_t::dump_to_disk() volatile{
	ref_count_--;
	if (ref_count_ == 0){
		assert(state_ == IN || state_ == IN_RD);
		int shm_fd = shm_open(const_cast<const char*>(in_mem_file_name_), O_RDONLY, S_IRWXU);
		if (shm_fd < 0){
			ERROR_PRINT("chunk exist\n");
			return -1;
		}

		void* storage = mmap(nullptr, object_size_, PROT_READ, MAP_SHARED,
								shm_fd, 0);
		printf("STORAGE TEMP: %p %zu\n", storage, object_size_);
		close(shm_fd);

		auto region = get_region();

		boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(
			const_cast<boost::interprocess::interprocess_mutex&>(region->file_lock_));

		int fd = open(const_cast<char*>(region->disk_file_name_), O_RDONLY | O_DIRECT);
		lseek(fd, this->offset_, SEEK_SET);
		write(fd, storage, object_size_);
		close(fd);
		munmap(storage, object_size_);

		shm_unlink(const_cast<const char*>(in_mem_file_name_));
		printf("UNLINK %s\n", const_cast<const char*>(in_mem_file_name_));

		state_ = OUT;

		return 1;
	}
	return 0;
}

shm_mmap_meta_t::shm_mmap_meta_t(
	const char* shm_name, 
	const char* meta_file_name,
	const char* disk_file_name,
	size_t object_count,
	size_t total_size, size_t object_size){

	volatile_strncpy(this->shm_name_, shm_name, PATH_MAX);
	volatile_strncpy(this->meta_file_name_, meta_file_name, PATH_MAX);
	volatile_strncpy(this->disk_file_name_, disk_file_name, PATH_MAX);

	for (size_t i = 0; i < object_count; i++){
		new (&this->object_array_[i]) shm_object_meta_t(
			this, 
			reinterpret_cast<const char*>(&this->object_array_[i]) 
				- reinterpret_cast<const char*>(this),
			object_size * i,
			i != (object_count - 1 ) ? object_size : total_size - (object_count - 1) * object_size);
	}
}

shm_mmap_meta_t* shm_mmap_meta_t::create(
	const char* shm_name, 
	const char* disk_file_name, 
	size_t total_size, size_t object_size){

	size_t object_count = (total_size + object_size - 1) / object_size;
	// printf("%zu\n", sizeof(shm_mmap_meta_t));
	size_t this_size = sizeof(shm_mmap_meta_t) + sizeof(shm_object_meta_t) * object_count;
	char meta_file_name[PATH_MAX];
	VOLATILE_SNPRINTF(meta_file_name, PATH_MAX, "%s.meta", shm_name);


	int shm_fd = shm_open(const_cast<const char*>(meta_file_name), O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
	if (shm_fd < 0){
		if (errno == EEXIST){
			ERROR_PRINT("shm create %s: meta exist\n", shm_name);
		}else{
			ERROR_PRINT("shm create %s: errno %d\n", shm_name, errno);
		}
		return nullptr;
	}
	ftruncate(shm_fd, this_size);

	void* mem = mmap(nullptr, this_size, PROT_READ | PROT_WRITE, MAP_SHARED,
					shm_fd, 0);

	shm_mmap_meta_t* meta = new (mem) shm_mmap_meta_t(
		shm_name, meta_file_name, disk_file_name,
		object_count, total_size, object_size);
	return meta;
}
shm_mmap_meta_t* shm_mmap_meta_t::attach(const char* shm_name){
	char meta_file_name[256];
	VOLATILE_SNPRINTF(meta_file_name, PATH_MAX, "%s.meta",shm_name);


	int shm_fd = shm_open(const_cast<const char*>(meta_file_name), O_RDWR, S_IRWXU);
	if (shm_fd < 0){
		ERROR_PRINT("shm create %s: errno %d\n", shm_name, errno);
		return nullptr;
	}
	void* mem = mmap(nullptr, sizeof(shm_mmap_meta_t), PROT_READ | PROT_WRITE, MAP_SHARED,
					shm_fd, 0);
	size_t object_count = reinterpret_cast<shm_mmap_meta_t*>(mem)->object_count_;
	size_t this_size = sizeof(shm_mmap_meta_t) + object_count * sizeof(shm_object_meta_t);

	munmap(mem, sizeof(shm_mmap_meta_t));
	mem = mmap(nullptr, this_size, PROT_READ | PROT_WRITE, MAP_SHARED,
				shm_fd, 0);
	return reinterpret_cast<shm_mmap_meta_t*>(mem);
}
int shm_mmap_meta_t::remove(const char* path){
	// TODO
}



shm_object_t::shm_object_t(
	void* addr,
	size_t size,
	mmap_info* region,
	off_t offset,
	shm_object_meta_t* meta,
	object_list_t* list)
	{
	assert(size % page_size == 0);


	type = abstract_mem_object_t::mem_object_type::SHM;
	shared_object_meta = meta;

	pool_info = pool_info_t{list, object_list_iterator(), 0},
	vm_info = vm_info_t{addr, size, region, offset};
}

shm_object_t::~shm_object_t(){

}

bool shm_object_t::in_mem(){
	return mem_state == shm_private_state_t::IN;
}

bool shm_object_t::in_shm(){
	return shared_object_meta->state_ == shm_object_meta_t::IN;
}

int shm_object_t::destroy(){
	mem_state = DESTROYED;
	return 0;
}

mmap_info::mmap_info(mmap_private_create_t, mmap_arena* arena, const char* path, 
	size_t total_size, size_t object_size, 
	in_mem_preference_enum in_mem_preference)
	:_arena(arena), _path(path),
	 _object_size(object_size),
	 _total_size(total_size), _destroyed(false), 
	 _in_mem_preference(in_mem_preference),
	 _unlink_on_destroy(true){
	
	{
		timer_scope _timer(timers.truncate);
		touch_file(path);
		truncate_file(path, total_size);
		fd = open(_path.c_str(), O_RDWR | O_DIRECT);
	}

	// starting from _start_ptr, at least total_size bytes can be used
	_start_ptr = vm_manager_t::vm_align_mmap(total_size);
	unsigned char* cptr = static_cast<unsigned char*>(_start_ptr);

	// initialize object array
	size_t obj_count = 0;
	size_t last_object_size = 0;
	size_t offset = 0;

#ifdef NON_UNIFORM_META_SIZE
	if (in_mem_preference != NONE){
		obj_count = (_total_size + _object_size - 1) / _object_size;
		last_object_size = _total_size - (obj_count - 1) * _object_size;
	}else{
		obj_count = (_total_size - page_size + _object_size - 1) / _object_size + 1;
		last_object_size = _total_size - (obj_count - 2) * _object_size - page_size;
	}
#else
		obj_count = (_total_size + _object_size - 1) / _object_size;
		last_object_size = _total_size - (obj_count - 1) * _object_size;
#endif


	_objects.resize(obj_count);
	for (size_t i = 0; i < obj_count; i++){
		// PROBLEM: possible memory overhead
		object_handle mem = container_allocator<mem_object_t>().allocate(1);

#ifdef NON_UNIFORM_META_SIZE
		if (in_mem_preference != NONE && i == 0){
			// Speical page size object
			_objects[i] = new(mem) mem_object_t(cptr + offset, page_size, this, offset, 
				arena->_object_cache->get_list(true));
			offset += page_size;
		}else{
			_objects[i] = new(mem) mem_object_t(cptr + offset, 
				((i == obj_count - 1) ? last_object_size : _object_size), 
				this, offset, 
				arena->_object_cache->get_list(in_mem_preference == ALL));			
			offset += _object_size;
		}
#else
		_objects[i] = new(mem) mem_object_t(cptr + offset, 
			((i == obj_count - 1) ? last_object_size : _object_size), 
			this, offset, 
			arena->_object_cache->get_list(((in_mem_preference == META) && (i == 0)) || (in_mem_preference == ALL)));
		offset += _object_size;
#endif
	}
}

mmap_info::mmap_info(mmap_shared_create_t, mmap_arena* arena, 
	const char* shm_name,
	const char* path, 
	size_t total_size, size_t object_size, 
	in_mem_preference_enum in_mem_preference __attribute__((UNUSED)))
	:_arena(arena), _path(path),
	 _object_size(object_size),
	 _total_size(total_size), _destroyed(false), 
	 _in_mem_preference(in_mem_preference),
	 _unlink_on_destroy(false){
	
	{
		timer_scope _timer(timers.truncate);
		touch_file(path);
		truncate_file(path, total_size);
		fd = open(_path.c_str(), O_RDWR | O_DIRECT);
	}

	// starting from _start_ptr, at least total_size bytes can be used
	_start_ptr = vm_manager_t::vm_align_mmap(total_size);
	unsigned char* cptr = static_cast<unsigned char*>(_start_ptr);

	shm_mmap_meta_t* mmap_meta = shm_mmap_meta_t::create(shm_name, path, total_size, object_size);

	// initialize object array
	size_t obj_count = (total_size + object_size - 1) / object_size;
	size_t last_object_size = total_size - (obj_count - 1) * object_size;
	size_t offset = 0;

	_objects.resize(obj_count);

	for (size_t i = 0; i < obj_count; i++){
		// PROBLEM: possible memory overhead
		object_handle mem = container_allocator<shm_object_t>().allocate(1);

		_objects[i] = new (mem) shm_object_t(cptr + offset,
			((i == obj_count - 1) ? last_object_size : _object_size), 
			this, offset,
			&mmap_meta->object_array_[i],
			arena->_object_cache->get_list(((in_mem_preference == META) && (i == 0)) || (in_mem_preference == ALL))
			);
		offset += _object_size;
	}
}

mmap_info::mmap_info(mmap_shared_attach_t, mmap_arena* arena, 
	const char* shm_name,
	in_mem_preference_enum in_mem_preference __attribute__((UNUSED)))
	:_arena(arena), 
	 // _path(path),
	 // _object_size(object_size),
	 // _total_size(total_size), 
	 _destroyed(false), 
	 _in_mem_preference(in_mem_preference),
	 _unlink_on_destroy(false){

	shm_mmap_meta_t* mmap_meta = shm_mmap_meta_t::attach(shm_name);

	size_t total_size, object_size;

	_path = const_cast<const char*>(mmap_meta->disk_file_name_);
	_total_size = total_size = mmap_meta->total_size;
	_object_size = object_size = mmap_meta->object_size;

	fd = open(const_cast<const char*>(mmap_meta->disk_file_name_), O_RDWR | O_DIRECT);

	// starting from _start_ptr, at least total_size bytes can be used
	_start_ptr = vm_manager_t::vm_align_mmap(total_size);
	unsigned char* cptr = static_cast<unsigned char*>(_start_ptr);

	// initialize object array
	size_t obj_count = (total_size + object_size - 1) / object_size;
	size_t last_object_size = total_size - (obj_count - 1) * object_size;
	size_t offset = 0;

	_objects.resize(obj_count);

	for (size_t i = 0; i < obj_count; i++){
		// PROBLEM: possible memory overhead
		object_handle mem = container_allocator<shm_object_t>().allocate(1);

		_objects[i] = new (mem) shm_object_t(cptr + offset,
			((i == obj_count - 1) ? last_object_size : _object_size), 
			this, offset,
			&mmap_meta->object_array_[i],
			arena->_object_cache->get_list(((in_mem_preference == META) && (i == 0)) || (in_mem_preference == ALL))
			);
		offset += _object_size;
	}
}


mmap_info::~mmap_info(){
	// TODO: if cleaning _objects, should use container_allocator rather than direct delete

	close(fd);
}

void mmap_info::munmap(void* ptr, size_t size){
	// Handle partial munmap
	if (size == 0){
		return;
	}

	if (ptr == _start_ptr){
		destroy(false);
	}
}

void mmap_info::destroy(bool sync){
	for (auto it = _objects.begin(); it != _objects.end(); it++){
		if ((*it)->type == abstract_mem_object_t::MEM){
			auto object = dynamic_cast<mem_object_handle>(*it);
			if (object->in_mem()){
				_arena->_object_cache->evict_object(object, sync, nullptr);
			}
			object->destroy();
		}else{
			auto object = dynamic_cast<shm_object_handle>(*it);
			if (object->in_mem()){
				_arena->_object_cache->evict_object(object, sync, nullptr);
			}
			object->destroy();
		}
		
	}

	int ret = vm_manager_t::vm_munmap(_start_ptr, _total_size);

	// Destroy file
	if (_unlink_on_destroy){
		unlink(_path.c_str());
	}

	_destroyed = true;
}

object_handle mmap_info::get_object(void* addr, size_t size){
	// TODO: better multi size object support
#ifdef NON_UNIFORM_META_SIZE
	if (_in_mem_preference == META){
		if (addr == _start_ptr){
			return _objects[0];
		}
		
		size_t index = ((static_cast<char*>(addr) - static_cast<char*>(_start_ptr)) - page_size) / _object_size + 1;
		
		if (index >= _objects.size()){
			return object_handle(nullptr);
		}

		return _objects[index];
	}
#endif

	size_t index = (static_cast<char*>(addr) - static_cast<char*>(_start_ptr)) / _object_size;

	if (index >= _objects.size()){
		return object_handle(nullptr);
	}

	return _objects[index];
}

object_pool_t::object_pool_t(mmap_arena* arena, allocator_t* allocator)
	:_arena(arena) ,_allocator(allocator),
	 _statistic{}{

}

object_list_t* object_pool_t::get_list(bool in_mem){
	if (!in_mem){
		return &object_list;
	}else{
		return &in_mem_object_list;
	}
}

// int object_pool_t::reserve(size_t size){
// 	if (size > _allocator->_total_size){
// 		return -1;
// 	}

// 	return resize(_allocator->_total_size - size);
// }

// int object_pool_t::release(size_t size){
// 	return resize(_allocator->_total_size + size);
// }

// int object_pool_t::resize(size_t new_size){
// 	if (new_size >= _allocator->_total_size){
// 		_allocator->_total_size = new_size;
// 		return 1;
// 	}

// 	size_t diff = _allocator->_total_size - new_size;
// 	while (_allocator->available() < diff){
// 		int ret = this->evict();
// 		if (ret < 0){
// 			ERROR_PRINT("Cannot resize from %zu to %zu\n", _allocator->_total_size, new_size);
// 			return ret;
// 		}
// 	}

// 	_allocator->_total_size -= diff;
// 	return 0;
// }

int object_pool_t::evict_to(allocator_t::reservation_handle rhandle){
	
	if (object_list.empty()){
		return -1;
	}

	object_handle object;
	// object = object_list.back();

	if (!object_list.empty()){
		object = object_list.back();

		auto it = object_list.end();
		it--;
#ifdef USE_CLOCK_ALGORITHM
		for (size_t i = 0; i < CLOCK_ALGORITHM_MASK_NUM && it != object_list.begin(); i++){
			it--;
			vm_manager_t::vm_mask(*it);
		}
#endif
	}else if (!in_mem_object_list.empty()){
		object = in_mem_object_list.back();
	}else{
		return -1;
	}
	
	return evict_object(object, true, rhandle);
}

int object_pool_t::touch_object(object_handle object){

	if (object->type == abstract_mem_object_t::SHM){
		printf("Touched %s\n", dynamic_cast<shm_object_t*>(object)->shared_object_meta->in_mem_file_name_);
	}
	auto& list_it = object->pool_info.position;
	auto& list = *object->pool_info.list;

	if(list_it != object_list_iterator()){
		list.erase(list_it);
	}

	list.push_front(object);
	object->pool_info.position = list.begin();
	_allocator->refresh_object(object);

	return 0;
}

int object_pool_t::evict_mem_object(mem_object_handle object, bool sync, allocator_t::reservation_handle rhandle){

	auto& list = *object->pool_info.list;

	list.erase(object->pool_info.position);
	object->pool_info.position = object_list_iterator();

	_statistic.collect_evict(object->vm_info.size);
	_allocator->mem_evict_object(object, sync, rhandle);

	return 0;
}

int object_pool_t::evict_shm_object(shm_object_handle object, bool sync, allocator_t::reservation_handle rhandle){

	auto& list = *object->pool_info.list;

	list.erase(object->pool_info.position);
	object->pool_info.position = object_list_iterator();

	_statistic.collect_evict(object->vm_info.size);
	_allocator->mem_evict_shm_object(object, sync, rhandle);

}

int object_pool_t::evict_object(object_handle object, bool sync, allocator_t::reservation_handle rhandle){
	if (object->type == abstract_mem_object_t::mem_object_type::MEM){
		evict_mem_object(dynamic_cast<mem_object_handle>(object), sync, rhandle);
	}else{
		evict_shm_object(dynamic_cast<shm_object_handle>(object), sync, rhandle);
	}
}

int object_pool_t::clean_pool(){
	while (!object_list.empty() && _allocator->need_refresh_object(object_list.back())){
		SCOPE_TIMER(timers.shared_queue_eviction);
		evict_to(nullptr);
	}
	return 0;
}

int object_pool_t::load_mem_object(mem_object_handle object){

	SCOPE_TIMER(timers.load_object);
	TEST_PRINT("loading %s object at %p\n", (object->_force_in_mem ? "IN_MEM" : "ORDINARY"), object->_addr);

	if (object->mem_state == mem_object_t::mem_state_t::NEW || object->mem_state == mem_object_t::mem_state_t::OUT){
		// need load from disk
		// allocate memory
		auto rhandle = _allocator->try_reserve(object->vm_info.size);
		while (rhandle->size < object->vm_info.size && !object_list.empty()){
			evict_to(rhandle);
		}
		
		if (rhandle->size < object->vm_info.size && object_list.empty()){
			ERROR_PRINT("cannot find enough space for object %p at %p\n", object, object->vm_info.addr);
			ERROR_PRINT("allocated %zu, need %zu\n", rhandle->size, object->vm_info.size);
			return -1;
		}

		timers.load_object.pause_section(1, "eviction");
		
		_allocator->mem_load_object(object, rhandle);
		assert(!rhandle->valid);
	}else if (object->mem_state == mem_object_t::mem_state_t::IN_RD){
		_allocator->mem_load_object(object, nullptr);
	}else if (object->mem_state == mem_object_t::mem_state_t::IN){
	}

	// after the following statement, scope reservation is destructed


	timers.load_object.pause_section(6, "mem_load_object_end");

	touch_object(object);

	timers.load_object.pause_section(7, "list_operation");
	return 0;
}

int object_pool_t::load_shm_object(shm_object_handle object){

	SCOPE_TIMER(timers.load_object);
	TEST_PRINT("loading %s shm object at %p\n", (object->_force_in_mem ? "IN_MEM" : "ORDINARY"), object->_addr);

	// boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(object->meta->m_);

	if (object->mem_state == shm_object_t::OUT){
		if (!object->in_shm()){
			auto rhandle = _allocator->try_reserve_shm(object->vm_info.size);
			while (rhandle->size < object->vm_info.size && !object_list.empty()){
				evict_to(rhandle);
			}
			
			if (rhandle->size < object->vm_info.size && object_list.empty()){
				ERROR_PRINT("cannot find enough space of object %p at %p\n", object, object->vm_info.addr);
				ERROR_PRINT("allocated %zu, need %zu\n", rhandle->size, object->vm_info.size);
				return -1;
			}
			
			timers.load_object.pause_section(1, "eviction");
		
			_allocator->mem_load_shm_object(object, rhandle);
			assert(!rhandle->valid);			
		}else{
			_allocator->mem_load_shm_object(object, nullptr);
		}
		
	}

	if (object->mem_state == shm_object_t::OUT){

	// }else if (object->mem_state == shm_object_t::IN_RD){
	// 	ERROR_PRINT("not implemented: IN_RD\n");
	// 	_allocator->mem_load_shm_object(object, nullptr);
	}else if (object->mem_state == shm_object_t::IN){
	}

	// // after the following statement, scope reservation is destructed


	timers.load_object.pause_section(6, "mem_load_object_end");

	touch_object(object);

	timers.load_object.pause_section(7, "list_operation");
	return 0;
}

int object_pool_t::load_object(object_handle object){
	if (object->type == abstract_mem_object_t::mem_object_type::MEM){
		load_mem_object(dynamic_cast<mem_object_handle>(object));
	}else{
		load_shm_object(dynamic_cast<shm_object_handle>(object));
	}
}


mmap_arena::mmap_arena(const char* folder, config conf): \
	_conf(conf){

	// TODO: other allocator
	_folder = mem_safe_string(folder);

	if (!conf.use_shm){
		_allocator = std::allocate_shared<reservation_object_allocator_t>(container_allocator<reservation_object_allocator_t>(), 
			conf.object_cache_size);
	}else{
		_allocator = std::allocate_shared<multi_reservation_object_allocator_t>(container_allocator<multi_reservation_object_allocator_t>(), 
			conf.object_cache_size, conf.shm_name);
	}

	// _allocator = std::allocate_shared<reservation_object_allocator_t>(container_allocator<reservation_object_allocator_t>(), conf.object_cache_size);
	_object_cache = std::allocate_shared<object_pool_t>(container_allocator<object_pool_t>(), this, _allocator.get());
	_page_cache = std::allocate_shared<page_cache_t>(container_allocator<page_cache_t>(), this, conf.page_cache_size);

}
mmap_arena::~mmap_arena(){
	// TODO: add clean up
}

mmap_handle mmap_arena::mmap(void* addr, size_t size, int mode){
	// std::lock_guard<std::mutex> guard(_global_mutex);
	

	assert(addr == nullptr);
	timer_scope _timer(timers.mmap_total);

	// signal_blocker _block;

	// ALIGNMENT: size
	size = (size + page_size - 1) & ~(page_size - 1);

	char buf[PATH_MAX];
	generate_uuid_string(buf);
	mem_safe_string uuid = mem_safe_string(buf);
	mem_safe_string path = _folder + uuid;

	mmap_info* mem = container_allocator<mmap_info>().allocate(1);
	mmap_handle mhandle;

	if (mode == 0){ // small object, in mem alloc
		mhandle = new(mem) mmap_info(mmap_private_create, this, 
			path.c_str(), 
			size, _conf.object_size, 
			ALL);
		for (auto it = mhandle->_objects.begin(); it != mhandle->_objects.end(); it++){
			_object_cache->load_object(*it);
			_page_cache->load_object(*it);
		}
		second_page_table.insert(mhandle->_start_ptr, size, mhandle);
	}else{
		mhandle = new(mem) mmap_info(mmap_private_create, this, 
			path.c_str(), 
			size, _conf.object_size, 
			_conf.enable_meta_in_memory ? META : NONE);
		page_table.insert(mhandle->_start_ptr, size, mhandle);
	}

	TEST_PRINT("mmap: %p %zu -> %p\n", addr, size, mhandle->_start_ptr);
	return mhandle;
}

mmap_handle mmap_arena::shm_create(void* addr, const char* shm_name, size_t size){
	assert(addr == nullptr);
	timer_scope _timer(timers.mmap_total);

	// signal_blocker _block;

	// ALIGNMENT: size
	size = (size + page_size - 1) & ~(page_size - 1);

	char buf[PATH_MAX];
	generate_uuid_string(buf);
	mem_safe_string uuid = mem_safe_string(buf);
	mem_safe_string path = _folder + uuid;

	mmap_info* mem = container_allocator<mmap_info>().allocate(1);
	mmap_handle mhandle;

	mhandle = new(mem) mmap_info(mmap_shared_create, this, 
		shm_name,
		path.c_str(), 
		size, _conf.object_size,
		_conf.enable_meta_in_memory ? META : NONE);
	page_table.insert(mhandle->_start_ptr, size, mhandle);

	TEST_PRINT("mmap: %p %zu -> %p\n", addr, size, mhandle->_start_ptr);
	return mhandle;
}

mmap_handle mmap_arena::shm_attach(void* addr, const char* shm_name, size_t size){
		timer_scope _timer(timers.mmap_total);

	// signal_blocker _block;

	// ALIGNMENT: size
	size = (size + page_size - 1) & ~(page_size - 1);

	char buf[PATH_MAX];
	generate_uuid_string(buf);
	mem_safe_string uuid = mem_safe_string(buf);
	mem_safe_string path = _folder + uuid;

	mmap_info* mem = container_allocator<mmap_info>().allocate(1);
	mmap_handle mhandle;

	mhandle = new(mem) mmap_info(mmap_shared_attach, this, 
		shm_name,
		// size, _conf.object_size,
		_conf.enable_meta_in_memory ? META : NONE);
	page_table.insert(mhandle->_start_ptr, size, mhandle);

	TEST_PRINT("mmap: %p %zu -> %p\n", addr, size, mhandle->_start_ptr);
	return mhandle;
}

int mmap_arena::partial_munmap(void* addr, size_t size){
	// std::lock_guard<std::mutex> guard(_global_mutex);
	timer_scope _timer(timers.munmap_total);

	// signal_blocker _block;

	// TODO: consider overlapping multiple mmap
	auto mhandle = get_mmap_handle(addr, size);
	if (mhandle == nullptr){
		// TODO: error information?
		return -1;
	}
	mhandle->munmap(addr, size);

	// CLEAN PAGE TABLE
	
	if (mhandle->_destroyed){
		auto it = page_table.find(mhandle->_start_ptr);
		if (it != page_table.end()){
			page_table.erase(it);
			return 0;
		}

		auto sit = second_page_table.find(mhandle->_start_ptr);
		if (sit != second_page_table.end()){
			second_page_table.erase(sit);
			return 0;
		}

		return -1;

		// TODO: correctly clean up custom allocated memory

		// mhandle->~mmap_info();
		// container_allocator<mmap_info>().deallocate(mhandle, 1);
	}

	return 0;
}

mmap_handle mmap_arena::get_mmap_handle(void* addr, size_t size){

	SCOPE_TIMER(timers.get_mmap_handle);
	mmap_handle mhandle;

	auto it = page_table.find_contain(addr, size);
	if (it != page_table.end()){
		mhandle = it->second.handle;
	}else{
		auto sit = second_page_table.find_contain(addr, size);
		if (sit != second_page_table.end()){
			mhandle = sit->second.handle;
		}else{
			mhandle = mmap_handle(nullptr);
		}
	}

	return mhandle;
}

object_handle mmap_arena::get_object(void* addr, size_t size){
	
	auto mhandle = get_mmap_handle(addr, size);
	if (mhandle == nullptr){
		return nullptr;
	}

	return mhandle->get_object(addr, size);
}



// int mmap_arena::reserve(size_t size){
// 	_object_cache->reserve(size);
// }

// int mmap_arena::release(size_t size){
// 	_object_cache->release(size);
// }

page_cache_t::page_cache_t(mmap_arena* arena, size_t total_size):\
	_arena(arena),
	_allocator(arena->_allocator.get()){

	current_page_count = 0;
	total_page_count = total_size / page_size;
}

object_handle page_cache_t::get_object(void* addr){

	SCOPE_TIMER(timers.get_object_and_lock);

	auto object = _arena->get_object(addr, page_size);

	timers.get_object_and_lock.pause_section(0, "get object");

	if (object == nullptr){
		return nullptr;
	}

	timers.get_object_and_lock.pause_section(1, "load object");

	return object;
}

int page_cache_t::load_object(object_handle object){

	// TODO: ERROR if not cover
	_arena->_object_cache->load_object(object);

	// vm_manager_t::vm_unmask(object->_addr, object->_size);

	// if (ret < 0){
	// 	return ret;
	// }

	return 0;
}


int page_cache_t::load(void* addr){

	// TODO: may not this frequent
	_arena->_object_cache->clean_pool();

	// DEBUG
	SCOPE_TIMER(timers.page_load);

	addr = reinterpret_cast<void*>(reinterpret_cast<size_t>(addr) & ~(page_size - 1));

	auto object = get_object(addr);

	timers.page_load.pause_section(0, "get_object_and_lock");

	if (object == nullptr){
		ERROR_PRINT("cannot find object for page at %p\n", addr);
		return -1;
	}

	int ret = this->load_object(object);
	timers.page_load.pause_section(1, "touch object");

	if (ret < 0){
		return ret;
	}

	return 0;
}