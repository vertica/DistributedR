#ifndef __META_ALLOCATOR__
#define __META_ALLOCATOR__

#include <cstddef>
#include <memory>
#include <limits>

#include "common.h"

extern "C" {
    void* dlmalloc(size_t bytes);
    void dlfree(void* ptr);
}

#define in_mem_alloc dlmalloc
#define in_mem_free dlfree

/**
 * internal allocator, bypass swapping
 * wrapped as c++ allocator
 */

template<typename T>
class meta_allocator {

public:

    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef size_t size_type;
    typedef std::ptrdiff_t difference_type;

    //    convert an allocator<T> to allocator<U>
    template<typename U>
    struct rebind {
        typedef meta_allocator<U> other;
    };

    inline explicit meta_allocator() {}
    inline ~meta_allocator() {}
    inline meta_allocator(meta_allocator const&) {}

    template<typename U>
    inline meta_allocator(meta_allocator<U> const&) {}

    //    address
    inline pointer address(reference r) { return &r; }
    inline const_pointer address(const_reference r) { return &r; }

    //    memory allocation
    inline pointer allocate(size_type cnt, 
       typename std::allocator<void>::const_pointer = 0) { 
      return reinterpret_cast<pointer>(in_mem_alloc(cnt * sizeof (T))); 
    }

    inline void deallocate(pointer p, size_type) { 
        in_mem_free(p); 
    }

    //    size
    inline size_type max_size() const { 
        return std::numeric_limits<size_type>::max() / sizeof(T);
    }

    //    construction/destruction
    inline void construct(pointer p, const T& t) { new(p) T(t); }
    inline void destroy(pointer p) { p->~T(); }

    inline bool operator==(meta_allocator const&) const { return true; }
    inline bool operator!=(meta_allocator const& a) const { return !operator==(a); }
};    //    end of class meta_allocator 

#endif