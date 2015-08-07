#ifndef __MULTI_RING_BUFFER__
#define __MULTI_RING_BUFFER__

#include "ring_buffer.h"
#include "page_struct.h"


/**
 * Special ring buffer support multi-entry page
 *
 * Use the last bit of page.ptr to indicate whether is multi page entry
 */
template<typename _Alloc>
class multi_page_ring_buffer : public ring_buffer<page_entry, _Alloc>{

public:

	inline explicit multi_page_ring_buffer(size_t size):ring_buffer<page_entry, _Alloc>(size){}


	inline page_entry multi_pop_back(size_t& size){
		if (!this->empty()){
			auto& current_page = this->back();
			this->pop_back();

			if (reinterpret_cast<size_t>(current_page.ptr) & 0x1){
				reinterpret_cast<size_t&>(current_page.ptr) &= ~0x1;
				size = reinterpret_cast<size_t>(this->back().ptr);

				this->pop_back(size - 1);
			}else{
				size = 1;
			}
			// printf("Poped %p %zu\n", current_page.ptr, size);
			return current_page;
		}
		return page_entry{NULL, 0UL, 0UL};
	}
	inline void multi_push_front(page_entry t, size_t size){
		if (this->free() < size){
			return;
		}
		if (size == 1){
			this->push_front(t);
		}else{
			reinterpret_cast<size_t&>(t.ptr) |= 0x1;
			this->push_front(t);
			this->push_front(page_entry{reinterpret_cast<void*>(size), 0, 0}, size - 1);

			// printf("Pushed %p %zu\n", t.ptr, size);
		}
	}
};

#endif