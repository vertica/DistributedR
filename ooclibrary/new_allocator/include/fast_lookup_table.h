#ifndef __FAST_LOOKUP_TABLE__
#define __FAST_LOOKUP_TABLE__

#include "const.h"
#include "helper.h"
#include "meta_allocator.h"

struct mmap_info;
typedef mmap_info* mmap_handle;

#define simple_lookup_table fast_lookup_table

class simple_lookup_table {

	struct _Entry {
		mmap_handle handle;
	};

	typedef std::map<void*, _Entry, std::less<void*>, container_allocator<std::pair<void*, mmap_handle> > >\
		page_translate_table_t;
	page_translate_table_t page_table;

public:

	typedef page_translate_table_t::iterator iterator;



	inline void insert(void* addr, size_t range, mmap_handle mhandle){
		page_table.insert(page_translate_table_t::value_type(addr, _Entry{mhandle}));
	}
	
	inline iterator find(void* addr){
		return page_table.find(addr);
	}

	inline iterator find_contain(void* addr, size_t size){

		if (page_table.empty()){
			return page_table.end();
		}

		auto it = page_table.upper_bound(addr);
		if (it == page_table.begin()){
			return page_table.end();
		}
		it--;

		// TODO: range check
		// if (static_cast<char*>(mhandle->_start_ptr) + mhandle->_total_size < 
		// 	static_cast<char*>(addr) + size){
		// 		return end();
		// }


		return it;
	}
	inline iterator end(){
		return page_table.end();
	}
	inline void erase(iterator it){
		page_table.erase(it);
	}
};

template<size_t Alignment>
class range_lookup_unordered_map{
public: 

	struct _DataType {
		void* addr;
		size_t align_count;
		mmap_handle handle;

		_DataType(void* a, size_t ac, mmap_handle h):addr(a), align_count(ac), handle(h){}
	};

	typedef std::unordered_map<void*, _DataType, 
				std::hash<void*>, 
				std::equal_to<void*>, 
				container_allocator<std::pair<const void*, _DataType> > >
		data_map_t;

	typedef std::unordered_map<void*, void*, 
				std::hash<void*>, 
				std::equal_to<void*>, 
				container_allocator<std::pair<const void*, void*> > >
		head_map_t;

	typedef typename data_map_t::iterator iterator;

	head_map_t head_map;
	data_map_t data_map;


	inline void insert(void* addr, size_t range, mmap_handle mhandle){

		if (reinterpret_cast<size_t>(addr) % Alignment != 0){
			ERROR_PRINT("Non aligned address supplied: %p\n", addr);
		}

		size_t align_count = (range + Alignment - 1) / Alignment;
		data_map.insert(typename data_map_t::value_type(addr, _DataType(addr, align_count, mhandle)));
		for (size_t i = 0; i < align_count; i++){
			void* next = static_cast<void*>(static_cast<char*>(addr) + i * Alignment);
			head_map.insert(head_map_t::value_type(next, addr));
		}
	}
	inline iterator find(void* addr){
		return data_map.find(addr);
	}

	inline iterator find_contain(void* addr, size_t size){

		addr = reinterpret_cast<void*>(reinterpret_cast<size_t>(addr) & ~(Alignment - 1));

		auto head_it = head_map.find(addr);
		if (head_it == head_map.end()){
			ERROR_PRINT("Unable to find head for %p\n", addr);
			return data_map.end();
		}

		void* head = head_it->second;

		auto it = data_map.find(head);
		if (it == data_map.end()){
			ERROR_PRINT("Warning: cannot find head\n");
		}

		return it;
	}
	inline iterator end(){
		return data_map.end();
	}
	inline void erase(iterator it){

		for (size_t i = 0; i < it->second.align_count; i++){
			void* next = static_cast<void*>(static_cast<char*>(it->second.addr) + i * Alignment);
			auto head_it = head_map.find(next);
			if (head_it == head_map.end()){
				ERROR_PRINT("Unable to find head with %p\n", next);
				return;
			}
			head_map.erase(head_it);
		}
		data_map.erase(it);
	}
};

#endif
