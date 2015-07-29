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

void TaskScheduler::AddExecutor(int id) {
  ExecStat* stat = new ExecStat;
  stat->mem_used = 0;
  stat->persist_load = 0;
  stat->exec_load = 0;
  
  executor_stat[id] = stat;
}

void TaskScheduler::DeleteSplit(const std::string& splitname) {
  boost::unordered_set<int> executors;

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  if(executor_splits.find(splitname) != executor_splits.end()) {
    executors = executor_splits[splitname]->executors;
    delete executor_splits[splitname];
    executor_splits.erase(splitname);
  } else
    LOG_ERROR("DeleteSplit: Split %s not found in executors", splitname.c_str());
  metalock.unlock();

  std::vector<std::string> splits;
  splits.push_back(splitname);

  for(boost::unordered_set<int>::iterator it = executors.begin(); it != executors.end(); ++it) {
      executorpool->clear(splits, *it);
  }
}

int TaskScheduler::GetDeterministicExecutor(int32_t split_id) {
  std::pair<int, int> cluster_info = worker->GetClusterInfo(); 
  if(cluster_info.first == 0) {
    LOG_ERROR("GetDeterministicExecutor:Number of workers is 0!");
    return 0;
  }
  //LOG_INFO("GetDeterministicExecutor: #Worker(%d), split_id(%d), num_exec(%d)", cluster_info.first, split_id, cluster_info.second);
  int local_split_id = floor(split_id/cluster_info.first);
  int exec_idx = local_split_id%cluster_info.second;
  return exec_idx;
}


/**************************************  AddParent Task *************************************/

// Possibility to add policy
int64_t TaskScheduler::GetBestExecutor(const std::vector<NewArg>& args, int taskid) {
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
        LOG_INFO("GetBestExecutor(%d): Dobject initialization foreach: split_id(%d), executor(%d)", taskid, split_id, target_executor);
        return target_executor;
      }
    }
    // Get the executors of all splits.
    map<int, size_t> available;  //needs locks?
    //LOG_INFO("Arg size is %zu", args.size());

    for(int32_t i = 0; i < args.size(); i++) {
       unique_lock<recursive_mutex> metalock(metadata_mutex);
       if(executor_splits.find(args[i].arrayname()) != executor_splits.end()) {
         ExecSplit *partition = executor_splits[args[i].arrayname()];
         boost::unordered_set<int> execs = partition->executors;

         for(boost::unordered_set<int>::iterator itr = execs.begin();
             itr != execs.end(); ++itr) {
           available[*itr] += partition->size;
           LOG_INFO("GetBestExecutor(%d): Partition %s is available on executor %d", taskid, partition->name.c_str(), (*itr));
          }
       }  //else dont do anything
       metalock.unlock();
    }

    //LOG_INFO("GetBestWorker:: Map size is %zu", available.size());

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
       LOG_INFO("GetBestExecutor(%d): Still no executor assigned. Assigning in round robin", taskid);
       target_executor = executorpool->GetExecutorInRndRobin();
    }

    if(args.size() > 0) LOG_INFO("GetBestExecutor(%d): For split %s, chosen executor is %d", taskid, args[0].arrayname().c_str(), target_executor);
    else LOG_INFO("GetBestExecutor(%d): no args in this task, chosen executor is %d", taskid, target_executor);
    return target_executor;
}



int64_t TaskScheduler::AddParentTask(const std::vector<NewArg>& task_args, int64_t parenttaskid, int taskid) {
  ::uint64_t executor_id = -1;
  unique_lock<mutex> parentlock(parent_mutex);
  if(parent_tasks.find(parenttaskid) != parent_tasks.end()) {
    executor_id = parent_tasks[parenttaskid];
    LOG_INFO("AddParentTask(%d): Found parent taskid %u. This task should be executed on Executor %u", taskid, parenttaskid, executor_id);
  } else {
    executor_id = GetBestExecutor(task_args, taskid);
    LOG_INFO("AddParentTask(%d): Parenttask(%d) not found. Assigned Executor(%d)", taskid, parenttaskid, executor_id);
    parent_tasks[parenttaskid] = executor_id;
  }
  parentlock.unlock();
  return executor_id;
}

/************************************** Validate Partitions ****************************************************************/

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
   if(executor_splits.find(split_name) == executor_splits.end())
     LOG_ERROR("ExecutorToPersistFrom: Partition(%s) does not exist", split_name.c_str());

   ExecSplit* partition = executor_splits[split_name];
   boost::unordered_set<int>::iterator it = partition->executors.begin();
   executor_id = *it;
   //LOG_INFO("ExecutorToPersistFrom: Iteration %d", executor_id);
   for (it++; it != partition->executors.end(); it++) {
      if (executor_stat[executor_id]->persist_load > executor_stat[*it]->persist_load) {
        executor_id = *it;
       }   
   }   

   //Update metadata
   executor_stat[executor_id]->persist_load+=1;
   //LOG_INFO("ExecutorToPersistFrom: Partition(%s), executor(%d)", split_name.c_str()executor_id);
   metalock.unlock();
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

   for(int i=0; i<all_partitions.size(); i++) {
      LOG_INFO("Validating %s(%d)", all_partitions[i].c_str(), taskid);

      bool persist = false;
      unique_lock<recursive_mutex> metalock(metadata_mutex);
      if(!IsSplitAvailable(all_partitions[i], executor_id)) {
        LOG_INFO("ValidatePartitions(%d): Partition(%s) not available on executor(%d). Intra-worker persist needed.", taskid, all_partitions[i].c_str(), executor_id);
        if (!IsBeingPersisted(all_partitions[i])) {
           persist = true;
           int target_executor = ExecutorToPersistFrom(all_partitions[i]);
           LOG_INFO("ValidatePartitions(%d): Partition(%s) will be persisted from executor(%d)", taskid, all_partitions[i].c_str(), target_executor);
           worker->prepare_persist(all_partitions[i], target_executor, taskid);
           num_persisted++;
        }
      } else
        LOG_INFO("ValidatePartitions(%d): Partition(%s) doesnt need to be persisted", taskid, all_partitions[i].c_str());
      metalock.unlock();
      //if(!persist) { worker->PersistPost(taskid); }
   }

   return num_persisted;
}

/**
  * Stage Updated partitions (for fault tolerance)
  *
  */
void TaskScheduler::StageUpdatedPartition(const std::string &name, size_t size, int executor_id) {
  LOG_INFO("Stage partition(%s) updated_splitsDS size(%zu)", name.c_str(), updated_splits.size());
  SplitUpdate* partition = new SplitUpdate;

  partition->name = name;
  partition->executor = executor_id;
  partition->size = size;

  unique_lock<mutex> stagelock(stage_mutex);
  updated_splits.insert(partition);
  stagelock.unlock();
}


/**
  * Merge Staged metadata to main metadata or clear old partitions.
  *
  */
void TaskScheduler::foreachcomplete(bool status) {
  boost::unordered_map<int, std::vector<std::string>> clear_map;

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  if(status) {
    // if foreach is successful. make it live
    LOG_INFO("Foreach is successful");

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
         LOG_INFO("foreachcomplete: Creating new partition %s in executor %d", split->name.c_str(), split->executor);
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
       LOG_INFO("foreachcomplete: Parsed parent is %s %d %d", darray_name.c_str(), split_id, version);

      if(version>1) {  // Version 0 does not exist.
        string parent = darray_name + "_" +
        int_to_string(split_id) + "_" +
        int_to_string(version - GC_DEFAULT_GEN);

        if (executor_splits.find(parent) == executor_splits.end())
          LOG_ERROR("foreachcomplete: Parent Partition %s not found in Worker. Do nothing", parent.c_str());
        else {
          ExecSplit* partition = executor_splits[parent];
          for(boost::unordered_set<int>::iterator i = partition->executors.begin();
              i != partition->executors.end(); i++) {
             LOG_INFO("foreachcomplete: Need to clear parent %s from executor %d", parent.c_str(), (*i));
             clear_map[*i].push_back(parent);
          }
        }

        //erase from metadata
        delete executor_splits[parent];
        executor_splits.erase(parent);
      }
    }
    stagelock.unlock();
  }
  else {
    // Discard changes and cleanup executors.
    LOG_INFO("Foreach has failed. Rollback! %zu", updated_splits.size());
    // Populate clear_map for batch clean
    unique_lock<mutex> stagelock(stage_mutex);
    boost::unordered_set<SplitUpdate*>::iterator it;
    for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
       SplitUpdate* split = *it;
       LOG_INFO("foreachcomplete: Need to clear new split %s from executor %d", split->name.c_str(), split->executor);
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

  for(boost::unordered_map<int, std::vector<std::string>>::iterator itr = clear_map.begin();
      itr != clear_map.end(); itr++) {
     executorpool->clear(itr->second, itr->first);
  }

  clear_map.clear();
  LOG_INFO("Foreach cleanup complete");
}

}  // namespace presto
