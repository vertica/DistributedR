#include "symbol_finder.h"



DEF_CALL_LIB_FUNC(lock_all, lock_all_segment)
DEF_CALL_LIB_FUNC(enable_mmap, enable_mmap_hooks)
// DEF_CALL_LIB_FUNC(set_mem, setTotalMemory)

// void enable_mmap(){
// 	void (*enable_mmap_hooks)();
// 	enable_mmap_hooks = (void (*)()) dlsym(RTLD_DEFAULT, "enable_mmap_hooks");
// 	printf("%p\n", (void*) enable_mmap_hooks);

// 	if (enable_mmap_hooks != NULL){
// 		enable_mmap_hooks();
// 	}
// }

// void enable_mmap(){
// 	void (*enable_mmap_func)(); 
// 	enable_mmap_func = (void (*)()) dlsym(((void *) 0), "enable_mmap_hooks");
// 	printf("%p\n", (void* enable_mmap_func));
// 	if (enable_mmap_func != __null){ 
// 		enable_mmap_func();
// 	}
// }

void set_mem(size_t size){
	void (*set_mem_func)(size_t);
	set_mem_func = (void (*)(size_t)) dlsym(RTLD_DEFAULT, "setTotalMemory");
	printf("%p\n", (void*) set_mem_func);

	if (set_mem_func != NULL){
		set_mem_func(size);
	}
}