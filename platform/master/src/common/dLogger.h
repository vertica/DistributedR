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

#ifndef _DLOGGER_H_
#define _DLOGGER_H_

#include <boost/log/common.hpp>
#include <boost/log/formatters.hpp>
#include <boost/log/filters.hpp>

#include <boost/log/utility/init/to_file.hpp>
#include <boost/log/utility/init/to_console.hpp>
#include <boost/log/utility/init/common_attributes.hpp>

#include <boost/log/attributes/timer.hpp>
#include <boost/log/sources/logger.hpp>

#ifdef PERF_TRACE
#include <ztracer.hpp>
#include <boost/thread.hpp>
#endif

using namespace std;
using boost::shared_ptr;
//using namespace boost;

namespace logging = boost::log;
namespace src = boost::log::sources;
#ifdef BOOST_54
namespace expr = boost::log::expressions;
#endif
namespace keyword = boost::log::keywords;
namespace sinks = boost::log::sinks;
namespace attrs = boost::log::attributes;
namespace fmt = boost::log::formatters;
namespace flt = boost::log::filters;

namespace presto {

#ifdef PERF_TRACE   
  extern bool trace_master;
  
  extern ZTracer::ZTraceEndpointRef ztrace_insts; 

  extern bool is_master;
  
  extern ZTracer::ZTraceRef master_trace;
  
  extern boost::thread_specific_ptr<bool> trace_worker;
  extern boost::thread_specific_ptr<ZTracer::ZTrace> worker_trace;
  
  extern bool trace_executor;
  extern ZTracer::ZTraceRef executor_trace;
#endif  
  
typedef enum {
DEBUG, INFO, WARN, ERROR 
} severity_level;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(logger, src::severity_logger<severity_level>)

#ifdef BOOST_54
BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", severity_level)
BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "TimeStamp", boost::posix_time::ptime)
#endif

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (
    std::basic_ostream< CharT, TraitsT >& strm, severity_level lvl)
{
    static const char* const str[] =
    {
        "DEBUG",
        "INFO",
        "WARN",
        "ERROR"
    };
    if (static_cast< std::size_t >(lvl) < (sizeof(str) / sizeof(*str)))
        strm << str[lvl];
    else
        strm << static_cast< int >(lvl);
    return strm;
}

static void InitializeFileLogger(const char* logname){
    logging::init_log_to_file
    (
        keyword::file_name = logname,
        keyword::auto_flush=true,
        keyword::open_mode = (std::ios::out | std::ios::trunc),
        #ifdef BOOST_54
          keyword::format = expr::stream
              << expr::attr< boost::posix_time::ptime >("TimeStamp")
              << " [" << severity
              << "] " << expr::message
        #else
          keyword::format = fmt::format("%1% [%2%] %3%")
              % fmt::date_time("TimeStamp")
              % fmt::attr< severity_level >("Severity")
              % fmt::message()
        #endif
    );
    
    logging::add_common_attributes(); 
}

static void InitializeConsoleLogger(){
    logging::init_log_to_console
    (
        std::cout,
	std::cerr,    
        keyword::auto_flush=true,
        #ifdef BOOST_54
          keyword::format = expr::stream
              << expr::attr< boost::posix_time::ptime >("TimeStamp")
              << " [" << severity
              << "] " << expr::message
        #else
          keyword::format = fmt::format("%1% [%2%] %3%")
              % fmt::date_time("TimeStamp")
              % fmt::attr< severity_level >("Severity")
              % fmt::message()
        #endif
     );  
                                               
     logging::add_common_attributes(); 
}


static void stop_logging()
{
    boost::shared_ptr< logging::core > core = logging::core::get();
    core->flush();
    core->remove_all_sinks();
    core->reset_filter();
}

static void LoggerFilter(int loglevel) {
   severity_level sevlevel;

   if (loglevel==0) {
      sevlevel=ERROR;
   }
   else if (loglevel==1) {
      sevlevel=WARN;
   }
   else if (loglevel==2) {
      sevlevel=INFO;
   }
   else if (loglevel==3) {
      sevlevel=DEBUG;
   }
   else {
      sevlevel=INFO;
   }

   logging::core::get()->set_filter (   
         #ifdef BOOST_54
           severity >= sevlevel
         #else
           flt::attr< severity_level >("Severity") >= sevlevel
         #endif
   );
}

static void LOG_DEBUG(const char* msg, ...){
    char out_msg[1024];
    memset(out_msg, 0x00, sizeof(out_msg));
    va_list argptr;
    va_start(argptr, msg);
    vsprintf(out_msg, msg, argptr);
    va_end(argptr);
    
    #ifdef PERF_TRACE
    //is master 
    if(is_master){
        if(trace_master){
            master_trace->event(out_msg);
        }
    }else if(worker_trace.get()){ // is worker
        if(*(trace_worker.get())) {
            worker_trace->event(out_msg);
        }
        }
    else{ // is executor
        if(trace_executor){
            executor_trace->event(out_msg);
        }
    }
    
#endif

    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, DEBUG) << out_msg;    
}

static void LOG_DEBUG(string msg){
        
#ifdef PERF_TRACE
    //is master
    if(is_master){
        if(trace_master){
            master_trace->event(msg);
        }
    }else if(worker_trace.get()){ // is worker
        if(*(trace_worker.get())) {
            worker_trace->event(msg);
        }
        }
    else{ // is executor
        if(trace_executor){
            executor_trace->event(msg);
        }
    }
     
#endif  
    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, DEBUG) << msg;
}


static void LOG_INFO(const char* msg, ...){
    char out_msg[1024];
    memset(out_msg, 0x00, sizeof(out_msg));
    va_list argptr;
    va_start(argptr, msg);
    vsprintf(out_msg, msg, argptr);
    va_end(argptr);

#ifdef PERF_TRACE
    //is master 
    if(is_master){
        if(trace_master){
            master_trace->event(out_msg);
        }
    }else if(worker_trace.get()){ // is worker
        if(*(trace_worker.get())) {
            worker_trace->event(out_msg);
        }
        }
    else{ // is executor
        if(trace_executor){
            executor_trace->event(out_msg);
        }
    }
#endif 
    
    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, INFO) << out_msg;    
}

static void LOG_INFO(string msg){
    
#ifdef PERF_TRACE
    //is master
    if(is_master){
        if(trace_master){
            master_trace->event(msg);
        }
    }else if(worker_trace.get()){ // is worker
        if(*(trace_worker.get())) {
            worker_trace->event(msg);
        }
        }
    else{ // is executor
        if(trace_executor){
            executor_trace->event(msg);
        }
    }
     
#endif    
    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, INFO) << msg;
}

static void LOG_WARN(const char* msg, ...){
    char out_msg[1024];
    memset(out_msg, 0x00, sizeof(out_msg));
    va_list argptr;
    va_start(argptr, msg);
    vsprintf(out_msg, msg, argptr);
    va_end(argptr);

    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, WARN) << out_msg;    
}

static void LOG_WARN(string msg){
    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, WARN) << msg;
}

static void LOG_ERROR(const char* msg, ...){
    char out_msg[1024];
    memset(out_msg, 0x00, sizeof(out_msg));
    va_list argptr;
    va_start(argptr, msg);
    vsprintf(out_msg, msg, argptr);
    va_end(argptr);

    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, ERROR) << out_msg;    
}
 
static void LOG_ERROR(string msg){
    src::severity_logger<severity_level>& lg = logger::get();
    BOOST_LOG_SEV(lg, ERROR) << msg;
}


} //presto namespace

/*static boost::shared_ptr< sink_t > IDL(const char* filename) {
    typedef sinks::synchronous_sink< sinks::text_ostream_backend > sink_t;
    boost::shared_ptr< std::ostream > strm(new std::ofstream(filename));
    boost::shared_ptr< logging::core > core = logging::core::get();
   
    // Create a backend and initialize it with a stream
    boost::shared_ptr< sinks::text_ostream_backend > backend = boost::make_shared< sinks::text_ostream_backend >();
    //backend->add_stream(boost::shared_ptr< std::ostream >(&std::clog, logging::empty_deleter()));  // output to console
    //backend -> add_stream(strm);
    backend -> auto_flush(true);
    boost::shared_ptr< sink_t > sink(new sink_t(backend));
    sink->locked_backend()->add_stream(strm);
   
    sink->set_formatter
    (   
        expr::stream
            << expr::attr< boost::posix_time::ptime >("TimeStamp")
            << " [" << expr::attr< severity_level >("Severity")
            << "] "<< expr::message
    );

    core->add_sink(sink);
    logging::core::get()->add_global_attribute("TimeStamp", attrs::local_clock());

    return sink;
}

static void stop_logging(boost::shared_ptr< sink_t >& sink)
{
    boost::shared_ptr< logging::core > core = logging::core::get();
    core->remove_sink(dSink);
    dSink->stop();
    dSink->flush();
    dSink.reset();
}

int main(int, char*[])
{
    init();
    logging::add_common_attributes();

    LOG_INFO("An info macro msg");
    //using namespace logging::trivial;
    src::severity_logger<severity_level> lg=logger::get();

    BOOST_LOG_SEV(lg, DEBUG) << "A debug severity message";
    BOOST_LOG_SEV(lg, INFO) << "An informational severity message";
    BOOST_LOG_SEV(lg, WARN) << "A warning severity message";
    BOOST_LOG_SEV(lg, ERROR) << "An error severity message";

    return 0;
}*/

#endif
