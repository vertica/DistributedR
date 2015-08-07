#ifndef __PROFILING_H__
#define __PROFILING_H__
#include "timer.h"

#ifdef NO_PROFILING
#undef NO_PROFILING
#endif

struct timer_coll{
	timer mmap_total, munmap_total, handler_total;

	timer get_mmap_handle, page_evict, truncate, trace_log;

	timer swap_read, swap_write;

	timer page_object_movement_fill, page_object_movement_revoke, page_object_movement_object_dump;

	timer object_mask, object_unmask;

	timer shared_queue_eviction, read_only_no_dump;

	multi_timer<2> get_object_and_lock;
	multi_timer<5> page_load;
	multi_timer<8> load_object;


	// multi_timer<10> load_total;
	// timer page_evict;
	// multi_timer<10> load_object;
	// timer total, truncate;
	// timer swap_read, swap_write, find_object;
	// timer mmap_total, sigsegv_wrapper;
	// timer page_object_movement_fill;
	// timer page_object_movement_revoke;
	// timer page_object_movement_object_dump;
	// timer upper_bound;
	// timer trace_log;
	// timer_stub page_object_movement;
};

#define PRINT_TIMER(indent, name)  print_timer(indent , #name, timers.name)
#define SCOPE_TIMER(timer) 	_timer_scope< decltype(timer) > _scope(timer)


void print_timer(const char* indent, const char* name, timer& t);
void print_timer(const char* indent, const char* name, timer_stub& t);
void print_timers(const char* indent);

extern timer_coll timers;
#endif