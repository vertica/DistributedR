#ifndef __TIMER_H__
#define __TIMER_H__

#include "common.h"
#include <sys/time.h>


class timer {
public:
	struct timeval _t;
	size_t _elapsed;
	inline timer():_elapsed(0){reset();};
	inline void reset(){gettimeofday(&_t, NULL);}
	inline void start(){reset();}
	inline size_t stop(){
		struct timeval t;
		gettimeofday(&t, NULL);
		size_t diff = (t.tv_sec - _t.tv_sec) * 1000000UL + (t.tv_usec - _t.tv_usec);
		_t = t;
		_elapsed += diff;
		return diff;
	}
	inline void printTimePassed(){
		struct timeval t;
		gettimeofday(&t, NULL);
		if (t.tv_usec < _t.tv_usec){
			t.tv_sec--;
			t.tv_usec += 1000000UL;
		}

		printf("%zu seconds %.6zu microseconds\n", 
			t.tv_sec - _t.tv_sec, 
			t.tv_usec - _t.tv_usec);
	}
	inline size_t elapsed(){
		return _elapsed;
	}
};

#define TIMER_PRINT_RESET() t.printTimePassed(); t.reset()

#endif