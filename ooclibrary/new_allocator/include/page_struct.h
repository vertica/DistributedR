#ifndef __PAEG_STRUCT_H__
#define __PAEG_STRUCT_H__

struct mem_object;

struct page_entry {
	void* ptr;
	mem_object* object;
	size_t version;
	// page_entry(void* p);
};

#endif