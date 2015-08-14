/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

 *This program is free software; you can redistribute it and/or modify
 *it under the terms of the GNU General Public License as published by
 *the Free Software Foundation; either version 2 of the License, or (at
 *your option) any later version.

 *This program is distributed in the hope that it will be useful, but
 *WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *General Public License for more details.  You should have received a
 *copy of the GNU General Public License along with this program; if
 *not, write to the Free Software Foundation, Inc., 59 Temple Place,
 *Suite 330, Boston, MA 02111-1307 USA
 ********************************************************************/

#ifndef _TIMER_H_
#define _TIMER_H_

#include <sys/time.h>
#include <inttypes.h>
#include <string.h>
#include <boost/thread/mutex.hpp>

namespace presto {

// need to define this in the main file for each executable!
extern uint64_t abs_start_time;
static inline uint64_t abs_time() {
  timeval now;
  gettimeofday(&now, NULL);
  return now.tv_usec + now.tv_sec * 1000000 -
    abs_start_time;
}

class Timer {
 public:
  Timer() {}

  void start() {
    try {
      timer_lock_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(10000));
      gettimeofday(&start_, NULL);
      timer_lock_.unlock();
    } catch(...) {return;}
  }

  uint64_t stop() {
    uint64_t passed_time = 0;    
    try {
      timer_lock_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(10000));
      gettimeofday(&end_, NULL);
      passed_time = end_.tv_usec + end_.tv_sec * 1000000 -
        start_.tv_usec - start_.tv_sec * 1000000;
      timer_lock_.unlock();
    } catch(...) {return passed_time;}    
    return passed_time;
  }

  uint64_t read() {
    uint64_t passed_time = 0;    
    try {
      timer_lock_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(10000));
      passed_time = end_.tv_usec + end_.tv_sec * 1000000 -
        start_.tv_usec - start_.tv_sec * 1000000;
      timer_lock_.unlock();
    } catch(...) {return passed_time;}    
    return passed_time;
  }

  uint64_t restart() {
    uint64_t ret = stop();
    start();
    return ret;
  }

  /*
  ** Get elapsed time since the start_ time without resetting start_ and end_ variable
  */
  uint64_t age() {
    timeval current;
    uint64_t passed_time = 0;    
    try {
      timer_lock_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(10000));
      gettimeofday(&current, NULL);
      passed_time = current.tv_usec + current.tv_sec * 1000000 -
        start_.tv_usec - start_.tv_sec * 1000000;
      timer_lock_.unlock();
    } catch(...) {return passed_time;}    
    return passed_time;
  }

 private:
  timeval start_, end_;
  boost::timed_mutex timer_lock_;
};

}  // namespace presto
#endif
