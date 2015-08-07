#include "profiling.h"

timer_coll timers;

#undef TEST_PRINT
#define TEST_PRINT(...) fprintf(stderr, __VA_ARGS__)

void print_timer(const char* indent, const char* name, timer& t){
	if (!t.stopped()){
		t.stop();
		t.start();
	}
	TEST_PRINT("%s", indent);
	TEST_PRINT("%s:\t%zu hits, %zuus\n", name, t.count(), t.elapsed());
}

void print_timer(const char* indent, const char* name, timer_stub& t){
	TEST_PRINT("%s", indent);
	TEST_PRINT("%s:\tstub\n", name);
}

template<size_t _section>
void print_timer(const char* indent, const char* name, multi_timer<_section>& t){
	TEST_PRINT("%s", indent);
	TEST_PRINT("%s:\t%zu hits, %zuus\n", name, t.count(), t.elapsed());
	for (size_t i = 0; i < _section; i++){
		TEST_PRINT("%s", indent);
		TEST_PRINT("\t");
		if (t._section_name[i] != nullptr){
			TEST_PRINT("section %d, %s:\t%zuus\n", i, t._section_name[i], t._section_elapsed[i]);
		}else{
			TEST_PRINT("section %d:\t%zuus\n", i, t._section_elapsed[i]);
		}
	}
}

void print_timers(const char* indent){
	TEST_PRINT("timers:\n");

	PRINT_TIMER(indent, mmap_total);
	PRINT_TIMER(indent, munmap_total);
	PRINT_TIMER(indent, handler_total);

	TEST_PRINT("\n");

	PRINT_TIMER(indent, swap_read);
	PRINT_TIMER(indent, swap_write);

	TEST_PRINT("\n");

	PRINT_TIMER(indent, get_mmap_handle);
	PRINT_TIMER(indent, page_evict); // Should be zero
	PRINT_TIMER(indent, truncate);

	TEST_PRINT("\n");

	PRINT_TIMER(indent, page_object_movement_fill);
	PRINT_TIMER(indent, page_object_movement_revoke);
	PRINT_TIMER(indent, page_object_movement_object_dump);
	
	TEST_PRINT("\n");

	PRINT_TIMER(indent, object_mask);
	PRINT_TIMER(indent, object_unmask);
	
	TEST_PRINT("\n");
	
	PRINT_TIMER(indent, trace_log);

	TEST_PRINT("\n");
	
	PRINT_TIMER(indent, shared_queue_eviction);
	PRINT_TIMER(indent, read_only_no_dump);

	TEST_PRINT("\n");
}