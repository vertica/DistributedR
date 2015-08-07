#ifndef __TIMER_H__
#define __TIMER_H__

#include "common.h"
#include <sys/time.h>
#include <array>


template<typename T>
class _timer_scope{
	T& _timer;
public:
	inline _timer_scope(T& timer):_timer(timer){
		_timer.hit();
		_timer.start();
	}

	inline _timer_scope(T& timer, size_t count):_timer(timer){
		_timer.hit(count);
		_timer.start();
	}

	inline ~_timer_scope(){
		_timer.stop();
	}
};

class timer {
public:

	struct timeval _t;
	size_t _elapsed;
	int _stopped;
	size_t _count;

	inline timer():_elapsed(0), _stopped(1), _count(0){reset();};
	inline void reset(){gettimeofday(&_t, NULL);}


	inline void start(){reset();_stopped = 0;}
	inline size_t stop(){
		struct timeval t;
		gettimeofday(&t, NULL);
		size_t diff = (t.tv_sec - _t.tv_sec) * 1000000UL + (t.tv_usec - _t.tv_usec);
		_t = t;
		_elapsed += diff;
		_stopped = 1;
		return diff;
	}
	inline int stopped(){return _stopped;}

	inline void hit(){_count++;}
	inline void hit(size_t add){_count += add;}
	inline size_t count(){return _count;}

	inline void print_time_passed(const char* name = nullptr){
		struct timeval t;
		gettimeofday(&t, NULL);
		if (t.tv_usec < _t.tv_usec){
			t.tv_sec--;
			t.tv_usec += 1000000UL;
		}

		if (name != nullptr){
			printf("timer %s: ", name);
		}

		printf("%zu seconds %.6zu microseconds\n", 
			t.tv_sec - _t.tv_sec, 
			t.tv_usec - _t.tv_usec);
	}
	inline size_t elapsed(){
		return _elapsed;
	}

	inline void clear(){
		_elapsed = 0;
	}

	inline void collect(){
		if (!_stopped){
			stop();
			start();
		}
	}
};

template<size_t _section>
class multi_timer {
public:

	struct timeval _t;
	size_t _elapsed;
	
	int _stopped;
	size_t _count;

	std::array<size_t, _section> _section_elapsed;
	std::array<const char*, _section> _section_name;

	inline multi_timer():_elapsed(0), _stopped(1), _count(0){
		// for (size_t i = 0; i < _section; i++){
		// 	_section_elapsed[i] = 0;
		// }
		memset(&_section_elapsed[0], 0, _section * sizeof(size_t));
		memset(&_section_name[0], 0, _section * sizeof(const char*));
		reset();
	}
	inline void reset(){gettimeofday(&_t, NULL);}

	inline size_t section(){return _section;}

	inline void start(){reset();_stopped = 0;}
	inline size_t stop(){
		struct timeval t;
		gettimeofday(&t, NULL);
		size_t diff = (t.tv_sec - _t.tv_sec) * 1000000UL + (t.tv_usec - _t.tv_usec);
		_t = t;
		_elapsed += diff;
		_stopped = 1;
		return diff;
	}
	inline size_t pause_section(size_t section){
		struct timeval t;
		gettimeofday(&t, NULL);
		size_t diff = (t.tv_sec - _t.tv_sec) * 1000000UL + (t.tv_usec - _t.tv_usec);
		_t = t;
		_elapsed += diff;
		_section_elapsed[section] += diff;
		return diff;
	}

	inline size_t pause_section(size_t section, const char* name){
		if (_section_name[section] == nullptr){
			_section_name[section] = name;
		}
		return pause_section(section);
	}
	inline int stopped(){return _stopped;}

	inline void hit(){_count++;}
	inline void hit(size_t add){_count += add;}
	inline size_t count(){return _count;}

	inline void print_time_passed(const char* name = nullptr){
		struct timeval t;
		gettimeofday(&t, NULL);
		if (t.tv_usec < _t.tv_usec){
			t.tv_sec--;
			t.tv_usec += 1000000UL;
		}

		if (name != nullptr){
			printf("timer %s: ", name);
		}

		printf("%zu seconds %.6zu microseconds\n", 
			t.tv_sec - _t.tv_sec, 
			t.tv_usec - _t.tv_usec);
	}
	inline size_t elapsed(){
		return _elapsed;
	}
};

class timer_stub {
public:

	inline void reset(){}


	inline void start(){}
	inline size_t stop(){return 0;}
	inline int stopped(){return 1;}

	inline void hit(){}
	inline size_t count(){return 0;}

	inline void print_time_passed(const char*){}
	inline size_t elapsed(){
		return 0;
	}
};

typedef _timer_scope<timer> timer_scope;

#define TIMER_PRINT_RESET() t.printTimePassed(); t.reset()

#endif