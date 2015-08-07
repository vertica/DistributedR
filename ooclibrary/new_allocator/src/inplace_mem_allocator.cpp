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

#define NEW mem_object_t::mem_state_t::NEW
#define IN mem_object_t::mem_state_t::IN
#define IN_RD mem_object_t::mem_state_t::IN_RD
#define OUT mem_object_t::mem_state_t::OUT
#define DESTROYED mem_object_t::mem_state_t::DESTROYED

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

reservation_object_allocator_t::reservation_object_allocator_t(size_t total_size) 
	: _allocated(0), _available(total_size){}

int reservation_object_allocator_t::alloc_memory_inplace(mem_object_handle object, reservation_handle rhandle){
	if (rhandle == nullptr || rhandle->size < object->vm_info.size){
		ERROR_PRINT("alloc should be supplied with a large enough reservation handle\n");
	}

	rhandle->size -= object->vm_info.size;
	_allocated += object->vm_info.size;
	_available += rhandle->size;
	rhandle->valid = false;

	// TODO: Add mmap here?

	return vm_manager_t::vm_unmask(object->vm_info.addr, object->vm_info.size);
}

int reservation_object_allocator_t::free_memory_inplace(mem_object_handle object, reservation_handle rhandle){
	_allocated -= object->vm_info.size;
	if (rhandle == nullptr){
		_available += object->vm_info.size;
	}else{
		rhandle->size += object->vm_info.size;
	}

	vm_manager_t::vm_mask(object->vm_info.addr, object->vm_info.size);

	madvise(object->vm_info.addr, object->vm_info.size, MADV_DONTNEED);
	
	return 0;
}

size_t reservation_object_allocator_t::available(){
	return _available;
}

size_t reservation_object_allocator_t::used(){
	return _allocated;
}

size_t reservation_object_allocator_t::total(){
	return _available + _allocated;
}

reservation_object_allocator_t::reservation_handle reservation_object_allocator_t::try_reserve(size_t size){
	size_t can_alloc = min(size, available());
	// _allocated += can_alloc;
	_available -= can_alloc;
	return new reservation_t{true, can_alloc};
}

reservation_object_allocator_t::reservation_handle reservation_object_allocator_t::try_reserve_shm(size_t){
	ERROR_PRINT("not supported\n");
	return new reservation_t{false, 0};
}


int reservation_object_allocator_t::mem_load_object(mem_object_handle object, reservation_handle rhandle){
	TEST_PRINT("allocing memory, required %zu, available: %zu\n", object->vm_info.size, available());

	timers.load_object.pause_section(3, "alloc memory");


	if (object->mem_state == NEW || object->mem_state == OUT){
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

		if (object->mem_state == OUT){
			timer_scope _timer(timers.swap_read);
			// _access_all(object);
			read_object(object);
		}
		timers.load_object.pause_section(4, "read_object");

		// object->_mem_state = IN;
		// vm_manager_t::vm_unmask(object->_addr, object->_size);
		object->mem_state = IN_RD;
		vm_manager_t::vm_readonly_mask(object->vm_info.addr, object->vm_info.size);

		// make object available by default
		// int ret = mprotect(object->_addr, object->_size, PROT_NONE);
		timers.load_object.pause_section(5, "mprotect");
		if (ret < 0){
			TEST_PRINT("mprotect failed: %p %s\n", object, strerror(errno));
		}
		
		return 0;
	}else if (object->mem_state == IN_RD){
		vm_manager_t::vm_unmask(object->vm_info.addr, object->vm_info.size);
		object->mem_state = IN;
	}else if (object->mem_state == IN){
		ERROR_PRINT("object %p already in memory, allocator %p\n", object, this);
	}else{
		ERROR_PRINT("object %p already destroyed, allocator %p\n", object, this);
	}

	return -1;
}

int reservation_object_allocator_t::mem_evict_object(mem_object_handle object, bool sync, reservation_handle rhandle){

	if (sync && object->mem_state == IN){
		_access_all _access(*object);
		write_object(object);
	}else if (object->mem_state == IN_RD){
		timers.read_only_no_dump.hit();
	}
	
	free_memory_inplace(object, rhandle);

	object->version++;
	object->mem_state = OUT;
	object->store = nullptr;
	return 0;
	
}

int reservation_object_allocator_t::mem_load_shm_object(shm_object_handle, reservation_handle){
	ERROR_PRINT("shm is not supported in single process library\n");
	return -1;
}

int reservation_object_allocator_t::mem_evict_shm_object(shm_object_handle, bool, reservation_handle){
	ERROR_PRINT("shm is not supported in single process library\n");
	return -1;
}


int reservation_object_allocator_t::refresh_object(object_handle){
	return 0;
}

bool reservation_object_allocator_t::need_refresh_object(object_handle){
	return false;
}