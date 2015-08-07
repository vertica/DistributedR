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

// #define NEW mem_object_t::mem_state_t::NEW
// #define IN mem_object_t::mem_state_t::IN
// #define IN_RD mem_object_t::mem_state_t::IN_RD
// #define OUT mem_object_t::mem_state_t::OUT
// #define DESTROYED mem_object_t::mem_state_t::DESTROYED

using namespace boost::interprocess;

class _access_all{
	mem_object_t& object_;

public:
	_access_all(mem_object_t& object):object_(object){

		mprotect(object_.vm_info.addr, object_.vm_info.size, PROT_READ | PROT_WRITE);
	}

	~_access_all(){

		mprotect(object_.vm_info.addr, object_.vm_info.size, PROT_NONE);
	}
};

multi_reservation_object_allocator_t::shared_memory_pool_t::shared_memory_pool_t(size_t available_)
			:available(available_),
			 head_epoch(0),
			 tail_epoch(0){
}

size_t multi_reservation_object_allocator_t::shared_memory_pool_t::shared_try_acquire(size_t size){
	boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(this->m);

	size_t reserved = min(size, this->available);
	this->available -= reserved;
	return reserved;
}

int multi_reservation_object_allocator_t::shared_memory_pool_t::shared_release(size_t size){
	boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(this->m);

	this->available += size;
	return 0;
}

const char* multi_reservation_object_allocator_t::pool_object_name = "SHM_POOL";

void multi_reservation_object_allocator_t::shared_memory_pool_t::init(const char* name, size_t available_){
	boost::interprocess::shared_memory_object::remove(name);
	managed_shared_memory segment(create_only, name, 65536);
	shared_memory_pool_t* pool = segment.construct<multi_reservation_object_allocator_t::shared_memory_pool_t>
										 (pool_object_name)(available_);
	return;
}

multi_reservation_object_allocator_t::shared_memory_pool_t* 
multi_reservation_object_allocator_t::shared_memory_pool_t::join(const char* name){
	managed_shared_memory* segment = new managed_shared_memory(open_only, name);
	shared_memory_pool_t* pool = segment->find<multi_reservation_object_allocator_t::shared_memory_pool_t>(pool_object_name).first;
	return pool;
}

multi_reservation_object_allocator_t::multi_reservation_object_allocator_t(size_t total_size, const char* shm_name) 
	: _private_total(total_size),
	  _allocated(0), _private_available(total_size){

	  	shm_pool = multi_reservation_object_allocator_t::shared_memory_pool_t::join(shm_name);

	  }

int multi_reservation_object_allocator_t::update_tail_epoch(size_t epoch){
	// assert(this->_private_tail_epoch <= epoch);
	this->_private_tail_epoch = epoch;

	boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(shm_pool->m);

	shm_pool->tail_epoch = max(this->_private_tail_epoch, shm_pool->tail_epoch);
	return 0;
}

size_t multi_reservation_object_allocator_t::next_head_epoch(){
	// assert(this->_private_head_epoch <= epoch);
	// this->_private_head_epoch = epoch;

	boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(shm_pool->m);

	++shm_pool->head_epoch;
	this->_private_head_epoch = shm_pool->head_epoch;
	return this->_private_head_epoch;
}

size_t multi_reservation_object_allocator_t::get_head_epoch(){
	// assert(this->_private_head_epoch <= epoch);
	// this->_private_head_epoch = epoch;
	return shm_pool->head_epoch;
}

size_t multi_reservation_object_allocator_t::get_tail_epoch(){
	return shm_pool->tail_epoch;
}

int multi_reservation_object_allocator_t::combined_release(size_t size){
	if (size == 0){
		return 0;
	}

	_private_available += size;

	if (total() > _private_total){
		size_t diff = total() - _private_total;
		_private_available -= diff;
		return this->shm_pool->shared_release(diff);
	}
	return 0;
}

size_t multi_reservation_object_allocator_t::private_acquire(size_t size){
	size_t private_can_alloc = min(size, available());
	// _allocated += private_can_alloc;
	_private_available -= private_can_alloc;
	return private_can_alloc;
}

size_t multi_reservation_object_allocator_t::shared_acquire(size_t size){
	size_t shared_can_alloc = this->shm_pool->shared_try_acquire(size);
	return shared_can_alloc;
}

int multi_reservation_object_allocator_t::alloc_memory_inplace(mem_object_handle object, reservation_handle rhandle){
	if (rhandle == nullptr || rhandle->size < object->vm_info.size){
		ERROR_PRINT("alloc should be supplied with a large enough reservation handle\n");
	}

	rhandle->size -= object->vm_info.size;

	combined_release(rhandle->size);
	_allocated += object->vm_info.size;

	rhandle->valid = false;

	object->pool_info.epoch = next_head_epoch();

	// TODO: Add mmap here?

	return vm_manager_t::vm_unmask(object->vm_info.addr, object->vm_info.size);
}

int multi_reservation_object_allocator_t::alloc_shm_inplace(volatile shm_object_meta_t* shared_object_meta, reservation_handle rhandle){
	if (rhandle == nullptr || rhandle->size < shared_object_meta->object_size_){
		ERROR_PRINT("alloc should be supplied with a large enough reservation handle\n");
	}

	rhandle->size -= shared_object_meta->object_size_;
	combined_release(rhandle->size);
	_shm_reserved += shared_object_meta->object_size_;

	{
		boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(this->shm_pool->m);
		this->shm_pool->reserving += shared_object_meta->object_size_;
	}


	rhandle->valid = false;

	return 0;
}

int multi_reservation_object_allocator_t::try_clear_reserving(){
	if(this->_shm_reserved == 0){
		return 0;
	}
	boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(
		const_cast<boost::interprocess::interprocess_mutex&>(this->shm_pool->m));
	// boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> mlock(
	// 	const_cast<boost::interprocess::interprocess_mutex&>(this->m_));
	if (this->shm_pool->reserved > 0){
		size_t can_free = min(this->_shm_reserved, this->shm_pool->reserved);
		this->_shm_reserved -= can_free;
		this->_private_available += can_free;
		this->shm_pool->reserved -= can_free;
		this->shm_pool->reserving -= can_free;
		return 1;
	}
	return 0;
}

// int multi_reservation_object_allocator_t::alloc_shared_memory_inplace(shm_object_handle object, reservation_handle rhandle){
// 	scoped_lock lock(object->shm_meta->m);

// 	if (!object->shm_meta->loaded){
		
// 	}

// 	mmap(object->vm_info.addr, object->vm_info.size, 
// 		PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, 
// 		object->shm_meta->fd, object->shm_meta->offset);

// }

// int multi_reservation_object_allocator_t::free_shared_memory_inplace(shm_object_handle object, reservation_handle rhandle){
// 	scoped_lock lock(object->shm_meta->m);

// 	if (!object->shm_meta->loaded){
		
// 	}

// 	mmap(object->vm_info.addr, object->vm_info.size, 
// 		PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, 
// 		object->shm_meta->fd, object->shm_meta->offset);

// }

int multi_reservation_object_allocator_t::free_memory_inplace(mem_object_handle object, reservation_handle rhandle){
	_allocated -= object->vm_info.size;
	update_tail_epoch(object->pool_info.epoch);

	if (rhandle == nullptr){
		combined_release(object->vm_info.size);
	}else{
		rhandle->size += object->vm_info.size;
	}

	vm_manager_t::vm_mask(object->vm_info.addr, object->vm_info.size);

	madvise(object->vm_info.addr, object->vm_info.size, MADV_DONTNEED);
	
	return 0;
}

size_t multi_reservation_object_allocator_t::available(){
	return _private_available;
}

size_t multi_reservation_object_allocator_t::used(){
	return _allocated;
}

size_t multi_reservation_object_allocator_t::total(){
	return _private_available + _allocated;
}

multi_reservation_object_allocator_t::reservation_handle multi_reservation_object_allocator_t::try_reserve(size_t size){
	size_t can_alloc = 0;

	can_alloc += private_acquire(size);

	if (can_alloc < size){
		// try acquire from shared memory pool
		size_t shared_can_alloc = shared_acquire(size - can_alloc);
		can_alloc += shared_can_alloc;
	}

	return new reservation_t{true, can_alloc};
}

multi_reservation_object_allocator_t::reservation_handle multi_reservation_object_allocator_t::try_reserve_shm(size_t size){
	size_t can_alloc = 0;

	size_t shm_can_alloc = shared_acquire(size);
	can_alloc += shm_can_alloc;

	if (can_alloc < size){
		// try acquire from shared memory pool
		size_t private_can_alloc = private_acquire(size - can_alloc);
		can_alloc += private_can_alloc;
	}

	return new reservation_t{true, can_alloc};
}

int multi_reservation_object_allocator_t::mem_load_object(mem_object_handle object, reservation_handle rhandle){
	TEST_PRINT("allocing memory, required %zu, available: %zu\n", object->vm_info.size, available());

	timers.load_object.pause_section(3, "alloc memory");


	if (object->mem_state == mem_object_t::NEW || object->mem_state == mem_object_t::OUT){

		assert(rhandle != nullptr);

		if (rhandle->size < object->vm_info.size){
			ERROR_PRINT("reservation size %zu not enough for object %zu\n", rhandle->size, object->vm_info.size);
			return -1;
		}

		object->store = object->vm_info.addr;
		int ret = alloc_memory_inplace(object, rhandle);

		if (ret < 0){
			ERROR_PRINT("MEMORY NOT ENOUGH in allocator %p\n", this);
		}

		if (object->mem_state == mem_object_t::OUT){
			timer_scope _timer(timers.swap_read);
			// _access_all(object);
			read_object(object);
		}
		timers.load_object.pause_section(4, "read_object");

		object->mem_state = mem_object_t::IN;
		// make object available by default
		// int ret = mprotect(object->vm_info.addr, object->vm_info.size, PROT_NONE);
		timers.load_object.pause_section(5, "mprotect");
		if (ret < 0){
			TEST_PRINT("mprotect failed: %p %s\n", object, strerror(errno));
		}
		
		return 0;
	}else if (object->mem_state == mem_object_t::IN_RD){
		vm_manager_t::vm_unmask(object->vm_info.addr, object->vm_info.size);
		object->mem_state = mem_object_t::IN;
	}else if (object->mem_state == mem_object_t::IN){
		ERROR_PRINT("object %p already in memory, allocator %p\n", object, this);
	}else{
		ERROR_PRINT("object %p already destroyed, allocator %p\n", object, this);
	}

	return -1;
}

int multi_reservation_object_allocator_t::mem_evict_object(mem_object_handle object, bool sync, reservation_handle rhandle){

	if (sync && object->mem_state == mem_object_t::IN){
		_access_all _access(*object);
		write_object(object);
	}
	
	free_memory_inplace(object, rhandle);

	object->version++;
	object->mem_state = mem_object_t::OUT;
	object->store = nullptr;
	return 0;
}

int multi_reservation_object_allocator_t::mem_load_shm_object(shm_object_handle object, reservation_handle rhandle){
	assert(object->mem_state == shm_object_t::OUT);
	printf("%p %zu | ", rhandle, rhandle->size);
	if (rhandle != nullptr && !object->in_shm()){
		printf("Loading | ");
		if (rhandle->size < object->vm_info.size){
			ERROR_PRINT("reservation of size %zu not enough for %p with size %zu\n", 
				rhandle->size, object->vm_info.addr, object->vm_info.size);
			return -1;
		}

		alloc_shm_inplace(object->shared_object_meta, rhandle);

		object->shared_object_meta->load_to_shm();
	}

	int shm_fd = shm_open(const_cast<const char*>(object->shared_object_meta->in_mem_file_name_), O_RDWR, 0);
	if (shm_fd < 0){
		ERROR_PRINT("shm open failed\n");
		return -1;
	}

	int ret = munmap(object->vm_info.addr, object->vm_info.size);
	void* mem = mmap(object->vm_info.addr, object->vm_info.size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED,
						shm_fd, 0);

	close(shm_fd);

	if (mem != object->vm_info.addr){
		ERROR_PRINT("cannot mmap shm chunk to location %p\n", object->vm_info.addr);
		return -1;
	}

	object->mem_state = shm_object_t::IN;
	object->pool_info.epoch = next_head_epoch();

	printf("LOAD %s -> %p %zu\n", object->shared_object_meta->in_mem_file_name_, object->vm_info.addr, object->vm_info.size);
	return 0;
}

int multi_reservation_object_allocator_t::mem_evict_shm_object(shm_object_handle object, bool sync, reservation_handle rhandle){
	if (object->mem_state == shm_object_t::IN){
		boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(
			const_cast<boost::interprocess::interprocess_mutex&>(
				object->shared_object_meta->m_));

		int ret = object->shared_object_meta->dump_to_disk();
		if (ret == 1){
			// shm object freed
			if (rhandle != nullptr){
				rhandle->size += object->shared_object_meta->object_size_;
			}else{
				combined_release(object->shared_object_meta->object_size_);
			}
		}
		object->mem_state = shm_object_t::OUT;

		vm_manager_t::vm_clean_mmap(object->vm_info.addr, object->vm_info.size);
		vm_manager_t::vm_mask(object->vm_info.addr, object->vm_info.size);
		printf("EVICT %s %p %zu\n", object->shared_object_meta->in_mem_file_name_,
			object->vm_info.addr, object->vm_info.size);
	}
	return 0;
}


int multi_reservation_object_allocator_t::refresh_object(object_handle object){
	// size_t old_epoch = object->_epoch;
	// update_tail_epoch(object->_epoch);
	object->pool_info.epoch = next_head_epoch();
	// printf("%zu %zu\n", old_epoch, object->_epoch);
	return 0;
}

bool multi_reservation_object_allocator_t::need_refresh_object(object_handle object){
	if (total() <= _private_total){
		return false;
	}


	size_t global_tail = shm_pool->tail_epoch;
	size_t global_head = shm_pool->head_epoch;

	// printf("%zu %zu %zu %zu %zu\n", global_head, global_tail, _private_head_epoch, _private_tail_epoch, object->_epoch);

	if (global_tail > object->pool_info.epoch){
		return true;
	}
	return false;
}