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
 * Main class for Executor Scheduler
 */

#include <stdio.h>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "TaskScheduler.h"
#include "ExecutorPool.h"
#include "PrestoWorker.h"

using namespace boost;

namespace presto {

TaskScheduler::~TaskScheduler() {
  sync_persist.clear();
  parent_tasks.clear();
  
  boost::unordered_map<std::string, ExecSplit*>::iterator exec_it;
  for(exec_it = executor_splits.begin(); exec_it != executor_splits.end(); ++exec_it) {
    delete exec_it->second;
  }
  executor_splits.clear();

  boost::unordered_map<int, ExecStat*>::iterator stat_it; 
  for(stat_it = executor_stat.begin(); stat_it != executor_stat.end(); ++stat_it) {
    delete stat_it->second;
  }
  executor_stat.clear();

  boost::unordered_set<SplitUpdate*>::iterator update_it; 
  for(update_it = updated_splits.begin(); update_it != updated_splits.end(); ++update_it) {
    delete *update_it;
  }
  updated_splits.clear();
  LOG_INFO("Deleted Executor Scheduler");
}

void TaskScheduler::AddExecutor(int id) {
  ExecStat* stat = new ExecStat;
  stat->mem_used = 0;
  stat->persist_load = 0;
  stat->exec_load = 0;
  
  executor_stat[id] = stat;
}

void TaskScheduler::DeleteSplit(const std::string& splitname) {
  boost::unordered_set<int> executors;
  bool exists = false;

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  if(executor_splits.find(splitname) != executor_splits.end()) {
    executors = executor_splits[splitname]->executors;
    delete executor_splits[splitname];
    executor_splits.erase(splitname);
    exists = true;
  } else
    LOG_ERROR("Executor Scheduler: Split %s not found in executors for deletion", splitname.c_str());
  metalock.unlock();

  if (exists) {
    std::vector<std::string> splits;
    splits.push_back(splitname);

    for(boost::unordered_set<int>::iterator it = executors.begin(); it != executors.end(); ++it) {
       executorpool->clear(splits, *it);
    }
  }
  LOG_DEBUG("Executor Scheduler: Cleared split %s from executors", splitname.c_str()); 
}

/**************************************  AddParent Task *************************************/

int TaskScheduler::GetDeterministicExecutor(int32_t split_id) {
  int exec_idx = 0;
  std::pair<int, int> cluster_info = worker->GetClusterInfo(); 
  if(cluster_info.first == 0) {
    LOG_ERROR("Executor Scheduler: Error while getting deterministic executor - Number of workers is 0");
    return 0;
  }

  try {
    int local_split_id = floor(split_id/cluster_info.first);
    exec_idx = local_split_id%cluster_info.second;
  } catch(std::exception &ex) {
    LOG_ERROR("Executor Scheduler: Error while getting deterministic executor - %s", ex.what());
  }
  return exec_idx;
}


// Possibility to add policy
int64_t TaskScheduler::GetBestExecutor(const std::vector<NewArg>& args, uint64_t taskid) {
    int target_executor = -1;

    //Check if its 1st dobject initialization
    if(args.size() == 1) {
      int32_t split_id;
      string split_name;
      int32_t version;
      ParseVersionNumber(args[0].arrayname(), &version);
      ParseSplitName(args[0].arrayname(), &split_name, &split_id);

      if(version == 0) { // Dobject initialization foreach
        target_executor = GetDeterministicExecutor(split_id);
        LOG_DEBUG("Executor Scheduler: Task %zu - Dobject partition initialization assigned split %s to Executor Id %d", taskid, args[0].arrayname().c_str(), target_executor);
        return target_executor;
      }
    }

    // Get the executors of all splits.
    map<int, size_t> available;
    for(int32_t i = 0; i < args.size(); i++) {
       unique_lock<recursive_mutex> metalock(metadata_mutex);
       if(executor_splits.find(args[i].arrayname()) != executor_splits.end()) {
         ExecSplit *partition = executor_splits[args[i].arrayname()];
         boost::unordered_set<int> execs = partition->executors;

         for(boost::unordered_set<int>::iterator itr = execs.begin();
             itr != execs.end(); ++itr) {
           available[*itr] += partition->size;
          }
       }  //else dont do anything
       metalock.unlock();
    }

    if(available.size() > 0) {
       map<int, size_t>::iterator best = available.begin();
       if(args.size() > 1) {
         //get one with most splits (max split size)
         for (map<int, size_t>::iterator itr = available.begin();
            itr != available.end(); itr++) {
            if (best->second < itr->second) {
              best = itr;
            }
         }
      }
      target_executor = best->first;
    }

    if (target_executor == -1) {// No executor assigned yet. Choose one in round robin fashion
       target_executor = executorpool->GetExecutorInRndRobin();
       LOG_DEBUG("Executor Scheduler: Task %zu - Assigned to Executor Id %d in round robin", taskid, target_executor);
    } else
       LOG_DEBUG("Executor Scheduler: Task %zu - Assigned to Executor Id %d", taskid, target_executor);

    return target_executor;
}



int64_t TaskScheduler::AddParentTask(const std::vector<NewArg>& task_args, uint64_t parenttaskid, uint64_t taskid) {
  ::uint64_t executor_id = -1;
  unique_lock<mutex> parentlock(parent_mutex);
  if(parent_tasks.find(parenttaskid) != parent_tasks.end()) {
    executor_id = parent_tasks[parenttaskid];
    LOG_DEBUG("Executor Scheduler: Task %zu - Found parent task %zu. Assigned to Executor Id %d", taskid, parenttaskid, executor_id);
  } else {
    executor_id = GetBestExecutor(task_args, taskid);
    parent_tasks[parenttaskid] = executor_id;
    //LOG_DEBUG("Executor Scheduler: Task %zu - Assigned to Executor Id %d", taskid, executor_id);
  }
  parentlock.unlock();
  return executor_id;
}

/************************************** Validate Partitions ****************************************************************/

void TaskScheduler::PersistDone(uint64_t taskid, int executor_id) {
  unique_lock<recursive_mutex> metalock(metadata_mutex);
  executor_stat[executor_id]->persist_load--;
  metalock.unlock();
  sync_persist[taskid]->post();
}


bool TaskScheduler::IsSplitAvailable(const std::string& split_name, int executor_id) {
   bool onExec = true;

   if(executor_id != -1) {// Check for executor as well.
     unique_lock<recursive_mutex> metalock(metadata_mutex);
     if(executor_splits.find(split_name) != executor_splits.end()) {
       if(executor_splits[split_name]->executors.find(executor_id) == executor_splits[split_name]->executors.end())
         onExec = false;
     }   
     metalock.unlock();
   } else  // force persist
     onExec = false;

   return onExec;
}


bool TaskScheduler::IsBeingPersisted(const std::string& split_name) {
   bool ret;
   shmem_arrays_mutex->lock();
   ret = (shmem_arrays->find(split_name) != shmem_arrays->end());
   shmem_arrays_mutex->unlock();
   return ret;
}


int64_t TaskScheduler::ExecutorToPersistFrom(const std::string& split_name) {
   int executor_id = -1; 
   unique_lock<recursive_mutex> metalock(metadata_mutex);
   if(executor_splits.find(split_name) == executor_splits.end()) {
     LOG_ERROR("Executor Scheduler: Split %s does not exist in executors for persist", split_name.c_str());
     return 0;
   }

   ExecSplit* partition = executor_splits[split_name];
   boost::unordered_set<int>::iterator it = partition->executors.begin();
   executor_id = *it;
   for (it++; it != partition->executors.end(); it++) {
      if (executor_stat[executor_id]->persist_load > executor_stat[*it]->persist_load) {
        executor_id = *it;
       }   
   }   

   //Update metadata
   executor_stat[executor_id]->persist_load+=1;
   metalock.unlock();
   //LOG_DEBUG("Executor Scheduler: Executor Id %d chosen to persist split %s to Worker", executor_id, split_name.c_str());
   return executor_id;
}


//Check if all partitions are on the same executor.
//If not persist to Worker
int32_t TaskScheduler::ValidatePartitions(const std::vector<NewArg>& task_args, int executor_id, uint64_t taskid) {
   int num_persisted = 0;
   std::vector<std::string> all_partitions;

   for(int i=0; i<task_args.size(); i++) {
      if(task_args[i].arrayname() == "list_type...") {
        for(int j=0; j<task_args[i].list_arraynames_size(); j++)
           all_partitions.push_back(task_args[i].list_arraynames(j));
      } else
        all_partitions.push_back(task_args[i].arrayname());
   }

   boost::interprocess::interprocess_semaphore sema(0); 
   unique_lock<recursive_mutex> lock(persist_mutex);
   sync_persist[taskid] = &sema;
   lock.unlock();

   for(int i=0; i<all_partitions.size(); i++) {

      //bool persist = false;
      unique_lock<recursive_mutex> metalock(metadata_mutex);
      if(!IsSplitAvailable(all_partitions[i], executor_id)) {
        if (!IsBeingPersisted(all_partitions[i])) {
           //persist = true;
           int target_executor = ExecutorToPersistFrom(all_partitions[i]);
           LOG_DEBUG("Executor Scheduler: Task %zu - Split %s will be persisted from Executor Id %d", taskid, all_partitions[i].c_str(), target_executor);
           worker->prepare_persist(all_partitions[i], target_executor, taskid);
           num_persisted++;
        }
      } else
        LOG_DEBUG("Executor Scheduler: Task %zu - Split %s doesnt need to be persisted", taskid, all_partitions[i].c_str());

      metalock.unlock();
   }

   //Wait for persist tasks to complete
   for(int i=0; i< num_persisted; i++) {
     sync_persist[taskid]->wait();
   }

   LOG_DEBUG("Executor Scheduler: Task %zu - Dependent PERSIST tasks(#%d) complete", taskid, num_persisted);
   lock.lock();
   sync_persist.erase(taskid);
   lock.unlock();

   return num_persisted;
}

/**
  * Stage Updated partitions (for fault tolerance)
  *
  */
void TaskScheduler::StageUpdatedPartition(const std::string &name, size_t size, int executor_id, ::uint64_t exec_taskid) {
  SplitUpdate* partition = new SplitUpdate;

  partition->name = name;
  partition->executor = executor_id;
  partition->size = size;

  unique_lock<mutex> stagelock(stage_mutex);
  updated_splits.insert(partition);
  stagelock.unlock();
  LOG_DEBUG("EXECUTE TaskID %18zu - Split %s staged to Worker", exec_taskid, name.c_str());
}


/**
  * Merge Staged metadata to main metadata or clear old partitions.
  *
  */
void TaskScheduler::ForeachComplete(uint64_t id, uint64_t uid, bool status) {
  boost::unordered_map<int, std::vector<std::string>> clear_map;

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  if(status) {
    // if foreach is successful. make it live
    LOG_INFO("METADATAUPDATE Task               - Foreach is successful");

    unique_lock<mutex> stagelock(stage_mutex);
    boost::unordered_set<SplitUpdate*>::iterator it;
    for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
        SplitUpdate* split = *it;

       //Add to main metadata
       if(executor_splits.find(split->name) != executor_splits.end()) {
          ExecSplit *partition = executor_splits[split->name];
          partition->executors.insert(split->executor);
          executor_splits[split->name] = partition;
       } else {
         LOG_DEBUG("METADATAUPDATE Task               - Adding new split %s to Executor Id %d", split->name.c_str(), split->executor);
         ExecSplit *partition = new ExecSplit;
         partition->name = split->name;
         partition->size = split->size;
         partition->executors.insert(split->executor);
         executor_splits[split->name] = partition;
       }

       //TODO:Update executor size info

       // Populate clear_map for batch clean
       int32_t split_id;
       string darray_name;
       int32_t version;
       ParseVersionNumber(split->name, &version);
       ParseSplitName(split->name, &darray_name, &split_id);

      if(version > 1) {  // Version 0 does not exist.
        string parent = darray_name + "_" +
        int_to_string(split_id) + "_" +
        int_to_string(version - GC_DEFAULT_GEN);

        if (executor_splits.find(parent) == executor_splits.end())
          LOG_ERROR("METADATAUPDATE Task               - Previous split %s not found in executors", parent.c_str());
        else {
          ExecSplit* partition = executor_splits[parent];
          for(boost::unordered_set<int>::iterator i = partition->executors.begin();
              i != partition->executors.end(); i++) {
             LOG_INFO("METADATAUPDATE Task               - Previous split %s will be cleared from Executor Id %d", parent.c_str(), (*i));
             clear_map[*i].push_back(parent);
          }
        }

        SharedMemoryObject::remove(parent.c_str());
        //erase from metadata
        delete executor_splits[parent];
        executor_splits.erase(parent);
      }
    }
    stagelock.unlock();
  } else {
    // Discard changes and cleanup executors.
    LOG_INFO("METADATAUPDATE Task               - Foreach failed");
    // Populate clear_map for batch clean
    unique_lock<mutex> stagelock(stage_mutex);
    boost::unordered_set<SplitUpdate*>::iterator it;
    for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
       SplitUpdate* split = *it;
       LOG_INFO("METADATAUPDATE Task               - Newly created split %s will be cleared from Executor Id %d", split->name.c_str(), split->executor);
       clear_map[split->executor].push_back(split->name);
    }
    stagelock.unlock();
  }

  //Cleanup Staged metadata
  boost::unordered_set<SplitUpdate*>::iterator it;
  for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
    delete *it;
  }
  updated_splits.clear();
  metalock.unlock();

  MetadataUpdateReply req;
  req.set_id(id);
  req.set_uid(uid);
  req.mutable_location()->CopyFrom(worker->SelfServerInfo());
  //req.set_status(status);
  worker->getMaster()->MetadataUpdated(req);
  LOG_INFO("METADATAUPDATE Task               - Updated Worker Metadata. Sent notification to Master");

  for(boost::unordered_map<int, std::vector<std::string>>::iterator itr = clear_map.begin();
      itr != clear_map.end(); itr++) {
     executorpool->clear(itr->second, itr->first);
  }

  clear_map.clear();
  LOG_INFO("METADATAUPDATE Task               - Worker update complete");
}

}  // namespace presto
