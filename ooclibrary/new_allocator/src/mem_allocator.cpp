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

#define NEW mem_object::mem_state_enum::NEW
#define IN mem_object::mem_state_enum::IN
#define OUT mem_object::mem_state_enum::OUT
#define DESTROYED mem_object::mem_state_enum::DESTROYED

fixed_size_object_allocator_t::fixed_size_object_allocator_t(size_t total_size, size_t alloc_size) : _alloc_size(alloc_size), _total_size(total_size){}

void* fixed_size_object_allocator_t::mem_mmap_region(size_t size){
	size = (size + _alloc_size - 1) & ~(_alloc_size - 1);
	return mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
}

int fixed_size_object_allocator_t::mem_munmap_region(void* ptr, size_t size){
	size = (size + _alloc_size - 1) & ~(_alloc_size - 1);
	return munmap(ptr, size);
}

void* fixed_size_object_allocator_t::alloc_memory(size_t size){
	if (size <= _alloc_size && _alloc_size <= available()){
		count++;
		// TODO: change to aligned divide
		return mmap(NULL, _alloc_size,
						PROT_READ | PROT_WRITE,
						MAP_PRIVATE | MAP_ANON,
						-1,
						0);
	}else{
		return nullptr;
	}
}

int fixed_size_object_allocator_t::free_memory(void* ptr, size_t size){
	assert(size <= _alloc_size);
	count--;
	return munmap(ptr, _alloc_size);
}

int fixed_size_object_allocator_t::mem_load_object(object_handle object){
	TEST_PRINT("allocing memory, required %zu, available: %zu\n", object->_size, available());

	void* mem = alloc_memory(object->_size);

	if (mem == nullptr){
		ERROR_PRINT("MEMORY NOT ENOUGH in allocator %p\n", this);
	}

	if (object->_mem_state == NEW){
		object->_store = mem;

		// no need to IO

		object->_mem_state = IN;
		return 0;
	}else if (object->_mem_state == OUT){
		timer_scope _timer(timers.swap_read);
		object->_store = mem;

		read_object(object);

		object->_mem_state = IN;
		return 0;
	}else if (object->_mem_state == IN){
		ERROR_PRINT("object %p already in memory, allocator %p\n", object, this);
	}else{
		ERROR_PRINT("object %p already destroyed, allocator %p\n", object, this);
	}

	return -1;
}

int fixed_size_object_allocator_t::mem_evict_object(object_handle object, bool sync){
	// TODO: TUNING, should we evict all page when object is flushed? guess true

	for (size_t i = 0; i < object->_page_states.size(); i++){
		if (object->_page_states[i] != 0){
			page_entry page{static_cast<unsigned char*>(object->_addr) + page_size * i, object, object->_version};
			if (sync){
				mem_revoke_page(object, page);
			}else{
				mem_revoke_page_no_sync(object, page);
			}
		}
	}

	if (sync){
		write_object(object);
	}

	free_memory(object->_store, object->_size);

	object->_version++;
	object->_mem_state = OUT;
	object->_store = nullptr;
	return 0;
}

int fixed_size_object_allocator_t::mem_fill_page(object_handle object, page_entry page){
	
	if (page.version != object->_version){
		return -1;
	}

	timer_scope _scope(timers.page_object_movement_fill);

	int ret = mprotect(page.ptr, page_size, PROT_READ | PROT_WRITE);

	if (ret < 0){
		ERROR_PRINT("mprotect failed: %d\n", ret);
		return ret;
	}

	size_t offset = (static_cast<unsigned char*>(page.ptr) - static_cast<unsigned char*>(object->_addr));

	object->_page_states[offset / page_size] = 1;

	void* retp = memcpy(page.ptr, static_cast<unsigned char*>(object->_store) + offset, page_size);

	if (retp == nullptr){
		ERROR_PRINT("memcpy failed\n");
		return -1;
	}

	return 0;
}

int fixed_size_object_allocator_t::mem_fill_multi_page(object_handle object, page_entry page, size_t count){
	
	if (page.version != object->_version){
		return -1;
	}

	timer_scope _scope(timers.page_object_movement_fill, count);

	size_t load = min(count, 
		(reinterpret_cast<size_t>(object->_addr) + object->_size - reinterpret_cast<size_t>(page.ptr)) / page_size);

	int ret = mprotect(page.ptr, load * page_size, PROT_READ | PROT_WRITE);

	if (ret < 0){
		ERROR_PRINT("mprotect failed: %d\n", ret);
		return ret;
	}

	size_t offset = (static_cast<unsigned char*>(page.ptr) - static_cast<unsigned char*>(object->_addr));

	for (size_t i = 0; i < load; i++){
		object->_page_states[offset / page_size + i] = 1;		
	}

	void* retp = memcpy(page.ptr, static_cast<unsigned char*>(object->_store) + offset, load * page_size);

	if (retp == nullptr){
		ERROR_PRINT("memcpy failed\n");
		return -1;
	}

	return 0;

}

int fixed_size_object_allocator_t::mem_revoke_page(object_handle object, page_entry page){
	
	if (page.version != object->_version){
		return -1;
	}

	timer_scope _scope(timers.page_object_movement_revoke);

	size_t offset = (static_cast<unsigned char*>(page.ptr) - static_cast<unsigned char*>(object->_addr));

	if (object->_page_states[offset / page_size] == 2){
		return 1;
	}

	// Now object allocate directly in page location
	void* retp = memcpy(static_cast<unsigned char*>(object->_store) + offset, page.ptr, page_size);
	if (retp == nullptr){
		ERROR_PRINT("page not located at offset\n");
	}
	
	object->_page_states[offset / page_size] = 2;

	// RESOURCE: free page cache
	madvise(page.ptr, page_size, MADV_DONTNEED);
	mprotect(page.ptr, page_size, PROT_NONE);

	return 0;
}

int fixed_size_object_allocator_t::mem_revoke_page_no_sync(object_handle object, page_entry page){
	if (page.version != object->_version){
		return -1;
	}

	timer_scope _scope(timers.page_object_movement_object_dump);

	// RESOURCE: free mem, page cache
	madvise(page.ptr, page_size, MADV_DONTNEED);
	mprotect(page.ptr, page_size, PROT_NONE);

	return 0;
}

size_t fixed_size_object_allocator_t::available(){
	return _total_size - _alloc_size * count;
}

size_t fixed_size_object_allocator_t::used(){
	return _alloc_size * count;
}