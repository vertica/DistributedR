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

/**
 * Main class for Presto Worker. This class starts a Thrift server and
 * listens for commands.
 */

#ifndef _PRESTO_WORKER_
#define _PRESTO_WORKER_

#include <stdio.h>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <deque>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "shared.pb.h"
#include "common.h"
#include "dLogger.h"
#include "timer.h"

#include "WorkerInfo.h"
#include "MasterClient.h"

#include "ExecutorPool.h"
#include "ArrayStore.h"
#include "DataLoader.h"

#include "Observer.h"
#include <google/protobuf/message.h>
#include "RequestLogger.h"

#ifdef PERF_TRACE
#include <ztracer.hpp>
#endif

using namespace Rcpp;
using namespace zmq;
using namespace google::protobuf;

#define MAX_CACHE_SIZE_THRESHOLD 20*1024*1024*1024L  // 20 GB
#define NUM_CTX_THREADS 1
#define DEFAULT_EXECUTORS 1
#define DEFAULT_SHARED_MEMORY 6*1024*1024*1024L  // 6GB in bytes
#define DEFAULT_SPILL_DIR "/tmp"
#define DEFAULT_SPILL_SIZE (100LLU<<30)  // 100GB
#define MAX_NEW_TRANSFER_RETRY 2

namespace presto {

class DataLoader;

// This enum has to be sequential!!!!
// As we are counting the size using the last value
enum TASK_TYPE {
  EXECUTE = 0,
  IO,
  SEND,
  RECV,
  HELLO,
  MISC,
  VERTICALOAD,
  TASK_TYPE_NUM
};

#define NUM_THREADPOOLS (TASK_TYPE_NUM - EXECUTE)

#define EXEC_THREAD_NUM 0  // EXEC tasks. 0 means that use the maximum number of executors
#ifdef OOC_SCHEDULER
#define IO_THREAD_NUM 64  // IO-related tasks
#else
#define IO_THREAD_NUM 1
#endif
#define SEND_THREAD_NUM 4  // network sends
#define RECV_THREAD_NUM 4  // network receives
#define MISC_THREAD_NUM 2  // misc (composite creation, logging)
#define HELLO_THREAD_NUM 1
// The sequence of this variable has to comply with TASK_TYPE enum
static int NUM_THREADS[NUM_THREADPOOLS] = {EXEC_THREAD_NUM,
                                    IO_THREAD_NUM,
                                    SEND_THREAD_NUM,  
                                    RECV_THREAD_NUM,
                                    HELLO_THREAD_NUM,
                                    MISC_THREAD_NUM};

class PrestoWorker : public ISubject<google::protobuf::Message> {
 public:
  PrestoWorker(context_t* zmq_ctx,
               size_t shared_memory,
               int executors,
               const boost::unordered_map<std::string,
               ArrayStore*> &array_stores,
               int log_level,
               string master_ip, int master_port,
               int start_port, int end_port);
  ~PrestoWorker();
  void Run(string master_ip, int master_port, string worker_addr);

  void hello(ServerInfo master_location,
              ServerInfo worker_location,
              bool is_heartbeat, int attr_flag);

  void shutdown();

  void fetch(std::string name, ServerInfo location,
             size_t size, uint64_t id, uint64_t uid, std::string store);
  void newtransfer(std::string name, ServerInfo location,
                   size_t size, std::string store);
  void io(std::string array_name, std::string store_name,
          IORequest::Type type, uint64_t id, uint64_t uid);
  void clear(ClearRequest req);
  void newexecr(vector<std::string> func, vector<NewArg> args,
                vector<RawArg> raw_args,
                vector<CompositeArg> composite_args,
                uint64_t id, uint64_t uid, Response* res);
  void createcomposite(CreateCompositeRequest req);
  void verticaload(VerticaDLRequest req);

  WorkerInfo* getClient(const ServerInfo& location);

  void send_update_array(const std::string& darray_name);
  void HandleRequests(int type);
  void MonitorMaster();
  int GetExecutorID(pid_t pid) {
    return executorpool_->GetExecutorID(pid);
  }
  std::string GetHostIp() {
    return my_location_.name();
  }
  std::vector<pid_t> GetExecutorPids() {
    return executorpool_->GetExecutorPids();
  }

  ServerInfo SelfServerInfo() {
    return my_location_;
  }

  void CreateLoaderThread(uint64_t split_size, string split_prefix, uint64_t uid, uint64_t id);
  int GetStartPortRange() {return start_port_range_;}
  int GetEndPortRange(){return end_port_range_;}

  DataLoader* GetDataLoaderPtr() {
    return data_loader_;
  }

protected:
  WorkerRequest* CreateDfCcTask(CreateCompositeRequest& req);
  WorkerRequest* CreateListCcTask(CreateCompositeRequest& req);
  
 private:
  // keep Worker information given string:port information
  boost::unordered_map<std::string, boost::shared_ptr<WorkerInfo> > client_map;
  // a pointer to master process. We can send message using this
  boost::shared_ptr<MasterClient> master_;
  // ZMQ contenxt
  context_t* zmq_ctx_;

  ServerInfo my_location_;  // information about itself (hostname and port)
  ServerInfo master_location_;
  boost::timed_mutex client_map_mutex_;
  boost::timed_mutex exec_pool_del_mutex_;

  int num_executors_;  // total number of executors
  size_t shm_total_;   // total available shared memory size
  ExecutorPool *executorpool_;

  boost::unordered_map<std::string, ArrayStore*> array_stores_;
  boost::timed_mutex shmem_arrays_mutex_;
  // to keep all shared memory segments name
  boost::unordered_set<std::string> shmem_arrays_;

  struct compressed_array_t {
    boost::mutex *mutex;
    void *addr;  // NULL means no compression is needed
    size_t size;
  };
  boost::mutex compressed_arrays_mutex_;
  boost::unordered_map<std::string, compressed_array_t> compressed_arrays_;
  // a list of threads to handle different type of tasks
  boost::thread **request_threads_[NUM_THREADPOOLS];
  boost::thread *data_loader_thread_;
  // a list of mutexes to access request_queue_
  boost::mutex requests_queue_mutex_[NUM_THREADPOOLS];
  // a list of conditional variable to check if the queue is empty
  boost::condition_variable requests_queue_empty_[NUM_THREADPOOLS];
  // a queue that contains a list of worker request
  std::list<WorkerRequest*> requests_queue_[NUM_THREADPOOLS];
  bool running_;
  Timer master_last_contacted_;
  int start_port_range_;
  int end_port_range_;

  // profiling
  size_t total_bytes_fetched_;
  size_t total_bytes_sent_;
  uint64_t total_fetch_time_;
  uint64_t total_send_time_;
  uint64_t total_exec_time_;
  uint64_t total_cc_time_;
  struct worker_stats {
    size_t bytes_fetched;
    size_t bytes_sent;
    uint64_t fetch_time;
    uint64_t send_time;
    worker_stats() {
      bytes_fetched = bytes_sent = fetch_time = send_time = 0;
    }
  };
  boost::unordered_map<std::string, worker_stats> worker_stats_;
  boost::mutex worker_stat_mutex_;

  DataLoader* data_loader_;

  RequestLogger *mRequestLogger;

};

}  // namespace presto

#endif  // _PRESTO_WORKER_
