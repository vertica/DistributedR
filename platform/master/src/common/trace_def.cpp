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

#ifdef PERF_TRACE
#include <ztracer.hpp>
#include <boost/thread.hpp>
#include "dLogger.h"

namespace presto{
   bool trace_master = false;
   ZTracer::ZTraceRef master_trace;
   
   bool is_master = false;
  
   boost::thread_specific_ptr<bool> trace_worker;
   boost::thread_specific_ptr<ZTracer::ZTrace> worker_trace;
  
   bool trace_executor = false;
   ZTracer::ZTraceRef executor_trace;
}
#endif
