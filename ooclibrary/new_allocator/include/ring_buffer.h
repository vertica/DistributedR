#ifndef __RING_BUFFER__
#define __RING_BUFFER__

#include <memory>

template <typename T, typename _Alloc = std::allocator<T> >
class ring_buffer{
	typedef typename std::vector<T, _Alloc> _storage_t;
	_storage_t _storage;
	size_t _head, _tail, _capacity;

public:
	inline explicit ring_buffer(size_t size):_storage(size), _head(0), _tail(0), _capacity(size){}
	inline bool empty(){
		return _head == _tail;
	}
	inline bool full(){
		return this->size() == _capacity;
	}
	inline size_t size(){
		return _head - _tail;
	}
	inline size_t capacity(){
		return _capacity;
	}
	inline size_t free(){
		return _capacity - size();
	}
	inline T& back(){
		return _storage[_tail];
	}
	inline void pop_back(size_t multi = 1){
		if (size() >= multi){
			_tail += multi;
			if (_tail >= _capacity){
				_tail -= _capacity;
				_head -= _capacity;
			}
		}
	}
	inline void push_front(T t, size_t multi = 1){
		if (free() < multi){
			return;
		}
		_storage[(_head % _storage.size())] = t;
		_head += multi;
	}
};

#endif