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

/* Class to handle a pool of executors (similar to a thread pool).
   Each executor is a separate process, incoming tasks are executed
   by the first available one. */

#ifndef __EXECUTOR_POOL_H__
#define __EXECUTOR_POOL_H__

#include <unistd.h>

#include <boost/unordered_map.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/unordered_set.hpp>

#include <cstdio>
#include <string>
#include <vector>

#include "shared.pb.h"
#include "interprocess_sync.h"
#include "MasterClient.h"
#include "dLogger.h"
#include "ArrayData.h"  // for struct Composite

namespace presto {

class PrestoWorker;

class ExecutorPool {
 public:
  ExecutorPool(PrestoWorker* worker_, int n_, ServerInfo *my_location_, MasterClient* master_,
               boost::timed_mutex *shmem_arrays_mutex,
               boost::unordered_set<std::string> *shmem_arrays, 
               const std::string &spill_dir, int log_level,
               string master_ip, int master_port);
  // Execute task on first available executor (blocks until execution is done!)
  void execute(std::vector<std::string> func,
               std::vector<NewArg> args, std::vector<RawArg> raw_args,
               std::vector<NewArg> composite_args, ::uint64_t id,
               ::uint64_t uid, Response* res);
  void execute(std::vector<std::string> func,
               std::vector<NewArg> args, std::vector<RawArg> raw_args,
               std::vector<NewArg> composite_args, ::uint64_t id, 
               ::uint64_t uid, Response* res, int executor_id, bool stage_updates);
  void clear(std::vector<std::string> splits, int executor_id);
  void persist(std::string split_name, int executor_id, ::uint64_t taskid);

  ~ExecutorPool();
  void InsertCompositeArray(std::string name, Composite* comp);
  MasterClient *master;
  int GetExecutorID(pid_t pid);
  std::vector<pid_t> GetExecutorPids() {
    return child_proc_ids_;
  }

  void reset_executors() {
    exec_index = 0; 
  }

  int GetExecutorInRndRobin();

 private:
  struct ExecutorData {
    FILE *send, *recv;
    bool ready;
    pid_t pid;
    boost::mutex executor_mutex;
    boost::condition_variable sync;
  };

  int num_executors;
  int exec_index;
  ServerInfo *my_location;
  std::string spill_dir_;

  //boost::unordered_map<int, ExecutorData*> executors;
  ExecutorData *executors;
  std::map<std::string, Composite*> composites_;
  boost::unordered_set<std::string> *shmem_arrays_;
  std::vector<pid_t> child_proc_ids_;

  boost::mutex comp_mutex;
  boost::mutex poolmutex;
  boost::mutex exec_mutex;
  boost::timed_mutex *shmem_arrays_mutex_;
  boost::interprocess::interprocess_semaphore *sema;

  PrestoWorker* worker;
};

}  // namespace presto

#endif
