# a user space swapping library

## structure

.
├── GNUmakefile
├── README.md
├── TODO
├── external
│   ├── dlmalloc.c      // dlmalloc library, including custom header all_alloc.h
│   └── gprintf.c       // gprintf library, a printf function without usage of malloc
├── include
│   ├── all_alloc.h     // header file for dlmalloc.c to set options
│   ├── bitmap.h        // bitmap implementation
│   ├── common.h        // constants and helper functions
│   ├── const.h         // constants
│   ├── fast_lookup_table.h        // a hash table based lookup table supporting O(1) range
│   │                              // lookup for fixed-width entries
│   ├── gprintf.h                  // gprintf
│   ├── helper.h                   // file operation helper functions
│   ├── meta_allocator.h           // internal memory allocator, to avoid recursive call 
│   │                              // to exposed malloc
│   ├── multi_ring_buffer.h        // ring buffer support chunk push (size > 1)
│   ├── new_allocator.h            // * core logic
│   ├── page_struct.h              // page structure definition
│   ├── profiling.h                // profiling structure definition
│   ├── ring_buffer.h              // ring buffer implementation
│   ├── signal_blocker.h           // helper structure for blocking signal
│   ├── timer.h                    // timer implementation
│   ├── trace.h                    // tracer implementation
│   └── wrapper.h                  // wrapper function, expose initilization and runtime 
│                                  // access functions
├── src
│   ├── bitmap.cpp                 // bitmap implementation
│   ├── helper.cpp                 // helper functions implementation
│   ├── inplace_mem_allocator.cpp  // single process virtual memory allocator 
│   │                              // and memory resource manager
│   ├── mem_allocator.cpp          // deprecated copy based vm allocator and manager
│   ├── multi_reservation_object_allocator.cpp
│   │                              // multi process virtual memory allocator 
│   │                              // and memory resource manager
│   ├── new_allocator.cpp          // core logic
│   ├── object_io.cpp              // IO operations
│   ├── profiling.cpp              // profiling helper functions
│   ├── trace.cpp                  // tracer implementation
│   ├── vm_manager.cpp             // virtual memory management helper functions
│   └── wrapper.cpp                // wrapper functions implementation
└── test
    ├── init_shm.cpp               // helper program for initializing multiple process
    │                              // version library shm structure
    │
    │                              // profiling code
    ├── profile_shm.cpp
    ├── profile_signal.cpp
    ├── profile_timer.cpp
    │
    │                              // test code
    ├── test_all.cpp               // correctness test
    ├── test_basic.cpp             // random access
    ├── test_gprintf.c
    ├── test_meta_allocator.cpp
    ├── test_seq.cpp               // sequential access
    └── test_shm.cpp

## core logic

The library is exposed as allocation library providing malloc, free, realloc and calloc, via dlmalloc. These functions will be delegated to a custom version of mmap, which handles user-space-swap-enabled memory allocation. Code in dlmalloc.c & all_alloc.h

The custom mmap function will allocate only virtual memory space, not physical memory, when called, and return pointer to virtual memory space. When further accesses triggered, the data will be loaded. Code in wrapper.c

The core logic is in page fault handler part, with three layers structure as follow:

--------------------------------------------
| page mask (also called page cache)       |
--------------------------------------------
| object pool (also called object cache)   |
--------------------------------------------
| IO part                                  |
--------------------------------------------

Several key classes are:

1. abstract_mem_object_t, mem_object_t, shm_object_t: objects representing virtual memory. shm_object_t for shared memory objects.
2. mmap_info: objects representing single mmap region (similar to VMA entries)
3. vm_manager: helper for virtual memory management like mask (forbid all access), readonly mask (forbid write access), unmask (allow all access)
4. allocator_t: singleton object handling physical memory resource management
    - reservation_object_allocator_t: single process version
    - multi_reservation_object_allocator_t: multi process version, use a shared meta object through shm
5. object_pool_t: object pool, storing eviction order of objects
6. page_cache_t: page mask
7. mmap_arena: entrance singleton object
