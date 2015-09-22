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
#include <limits>

#include "TaskScheduler.h"
#include "ExecutorPool.h"
#include "PrestoWorker.h"

using namespace boost;

namespace presto {

/**
  * TaskScheduler destructor
  *
  **/
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
  LOG_INFO("Worker shutdown - Deleted Executor Scheduler");
}

/**
  * Initialize Executor statistics map
  *
  */
void TaskScheduler::AddExecutor(int id) {
  ExecStat* stat = new ExecStat;
  stat->mem_used = 0;
  stat->persist_load = 0;
  stat->exec_load = 0;
  
  executor_stat[id] = stat;
}

/**
  * Clears a split from Worker metadata
  * Sends CLEAR command to executors which contains the split
  *
  */
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
    LOG_ERROR("<Scheduler> Split %s not found in executors for deletion", splitname.c_str());
  metalock.unlock();

  if (exists) {
    std::vector<std::string> splits;
    splits.push_back(splitname);

    for(boost::unordered_set<int>::iterator it = executors.begin(); it != executors.end(); ++it) {
       executorpool->clear(splits, *it);
    }
  }
  LOG_DEBUG("<Scheduler> Cleared Split %s from executors", splitname.c_str()); 
}

/**
  * Helper function for AddParent()
  * Used only for dobject initialization foreach
  *
  * Calculate specific (rather than random) Executor Id for a split (or task containing split) 
  * based on its split id.
  * Allows deterministic allocation of splits to executors.
  *
  **/
int TaskScheduler::GetDeterministicExecutor(int32_t split_id) {
  int exec_idx = 0;
  std::pair<int, int> cluster_info = worker->GetClusterInfo(); 
  if(cluster_info.first == 0) {
    LOG_ERROR("<Scheduler> Error while getting deterministic executor - Number of workers is 0");
    return 0;
  }

  try {
    int local_split_id = floor(split_id/cluster_info.first);
    exec_idx = local_split_id%cluster_info.second;
  } catch(std::exception &ex) {
    LOG_ERROR("<Scheduler> Error while getting deterministic executor - %s", ex.what());
  }
  return exec_idx;
}


/**
  * Helper function for AddParent()
  * 
  * Evaluates and assigns best executor to a task based on input task arguments
  *
  **/
int TaskScheduler::GetBestExecutor(const std::vector<NewArg>& task_args, ::uint64_t taskid) {
    int target_executor = -1;

    // Extract all splits in the task
    std::vector<std::string> task_splits;
    for(int i=0; i<task_args.size(); i++) {
      if(task_args[i].arrayname() == "list_type...") {
        for(int j=0; j<task_args[i].list_arraynames_size(); j++)
           task_splits.push_back(task_args[i].list_arraynames(j));
      } else
        task_splits.push_back(task_args[i].arrayname());
    }

    // Check if this is for dobject initialization foreach/task
    if(task_args.size() == 1) {
      int32_t split_id;
      string split_name;
      int32_t version;
      ParseVersionNumber(task_splits[0], &version);
      ParseSplitName(task_splits[0], &split_name, &split_id);

      // It is Dobject initialization foreach
      // Calculate Deterministic Executor for the task
      if(version == 0) {
        target_executor = GetDeterministicExecutor(split_id);
        LOG_DEBUG("<Scheduler> TaskID %zu - Dobject partition initialization assigned Split %s to Executor Id %d", taskid, task_splits[0].c_str(), target_executor);
        return target_executor;
      }
    }

    // Get available Executors of all input splits in the task
    unique_lock<recursive_mutex> metalock(metadata_mutex, defer_lock);
    map<int, size_t> available;
    for(int32_t i = 0; i < task_splits.size(); i++) {
       metalock.lock();
       if(executor_splits.find(task_splits[i]) != executor_splits.end()) {
         ExecSplit *partition = executor_splits[task_splits[i]];
         boost::unordered_set<int> execs = partition->executors;

         for(boost::unordered_set<int>::iterator itr = execs.begin();
             itr != execs.end(); ++itr) {
           available[*itr] += partition->size;
          }
       }
       metalock.unlock();
    }

    // Memory efficient: Pick one with max cumulative split size
    /*if(available.size() > 0) {
       map<int, size_t>::iterator best = available.begin();
       if(task_splits.size() > 1) {
         for (map<int, size_t>::iterator itr = available.begin();
            itr != available.end(); itr++) {
            if (best->second < itr->second) {
              best = itr;
            }
         }
      }
      target_executor = best->first;
    }*/

    // Performance: Pick one with least execute load
    int min_load = std::numeric_limits<int>::max();
    if(available.size() > 0) {
      map<int, size_t>::iterator best = available.begin();

      metalock.lock();
      for (map<int, size_t>::iterator itr = available.begin(); 
           itr != available.end(); itr++) {
        if(executor_stat[itr->first]->exec_load == 0) {
          best = itr;
          break;
        } else {
          if(executor_stat[itr->first]->exec_load < min_load) {
            min_load = executor_stat[itr->first]->exec_load;
            best = itr;
          }
        }
      }
      metalock.unlock();

      target_executor = best->first;
    }

    // If no Executor assigned yet, choose one in round robin fashion
    if (target_executor == -1) {
       target_executor = executorpool->GetExecutorInRndRobin();
       LOG_DEBUG("<Scheduler> TaskID %zu - Assigned to Executor Id %d in round robin", taskid, target_executor);
    } else
       LOG_DEBUG("<Scheduler> TaskID %zu - Assigned to Executor Id %d", taskid, target_executor);

    metalock.lock();
    executor_stat[target_executor]->exec_load++;
    metalock.unlock();

    return target_executor;
}


/**
  * Called for tasks - NEWEXECR
  * 
  * Fetch or assign an executor to the task
  *
  **/
int TaskScheduler::AddParentTask(const std::vector<NewArg>& task_args, ::uint64_t parenttaskid, ::uint64_t taskid) {
  int executor_id = -1;
  unique_lock<mutex> parentlock(parent_mutex);
  if(parent_tasks.find(parenttaskid) != parent_tasks.end()) {
    executor_id = parent_tasks[parenttaskid];
    LOG_DEBUG("<Scheduler> TaskID %zu - Parent task %zu found. Assigned to Executor Id %d", taskid, parenttaskid, executor_id);
  } else {
    executor_id = GetBestExecutor(task_args, taskid);
    parent_tasks[parenttaskid] = executor_id;
  }
  parentlock.unlock();
  return executor_id;
}

/**
  * Helper function for ValidatePartitions()
  *
  * Function to update metadata and post waiting semaphores
  * once a split has been persisted successfully.
  *
  **/
void TaskScheduler::PersistDone(std::string splitname, int executor_id) {
  unique_lock<recursive_mutex> persistlock(metadata_mutex);
  shmem_arrays_mutex->lock();
  shmem_arrays->insert(splitname);
  shmem_arrays_mutex->unlock();

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  executor_stat[executor_id]->persist_load--;
  metalock.unlock();

  boost::unordered_set< ::uint64_t >::iterator itr = persist_tasks[splitname].begin();
  for(; itr != persist_tasks[splitname].end(); ++itr) {
    ::uint64_t taskid = *itr;
    sync_persist[taskid]->post();
  }
  persist_tasks.erase(splitname);
  persistlock.unlock();
}


/**
  * Helper function for ValidatePartitions()
  *
  * Check if a split is available either in Worker or Executor
  * Used for checking if Persist is needed for a split
  *
  **/
bool TaskScheduler::IsSplitAvailable(const std::string& split_name, int executor_id) {
   bool onExec = true;
   bool onWorker = true;

   shmem_arrays_mutex->lock();
   onWorker = (shmem_arrays->find(split_name) != shmem_arrays->end());
   shmem_arrays_mutex->unlock();

   // If executor_id is -1, force persist on Worker
   if(executor_id != -1) {
     unique_lock<recursive_mutex> metalock(metadata_mutex);
     if(executor_splits.find(split_name) != executor_splits.end()) {
       if(executor_splits[split_name]->executors.find(executor_id) == executor_splits[split_name]->executors.end())
         onExec = false;
     }   
     metalock.unlock();
   } else
     onExec = false;

   LOG_DEBUG("<Scheduler> Split %s(Executor Id: %d) - On Worker(%d), On Executor(%d)", split_name.c_str(), executor_id, onWorker, onExec);

   return (onWorker || onExec);
}


/**
  * Helper function for ValidatePartitions()
  *
  * Check if a PERSIST task has already been spawned for the split
  *
  **/
bool TaskScheduler::IsBeingPersisted(const std::string& split_name) {
   bool ret;
   unique_lock<recursive_mutex> lock(persist_mutex); 
   ret = (persist_tasks.find(split_name) != persist_tasks.end());
   lock.unlock();
   return ret;
}

/**
  *
  * Helper function for ValidatePartitions()
  *
  * Assign an executor from which a split should be persisted
  *
  **/
int TaskScheduler::ExecutorToPersistFrom(const std::string& split_name) {
   int executor_id = -1; 
   unique_lock<recursive_mutex> metalock(metadata_mutex);
   if(executor_splits.find(split_name) == executor_splits.end()) {
     LOG_ERROR("<Scheduler> Split %s does not exist in executors for persist", split_name.c_str());
     return 0;
   }

   ExecSplit* partition = executor_splits[split_name];
   boost::unordered_set<int>::iterator it = partition->executors.begin();
   executor_id = *it;
   // Assign an executor based on load
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


/**
  * Called for tasks - NEWEXEC, NEWTRANSFER, CREATECOMPOSITE
  *
  * Validate if all input split arguments are available
  * Launch PERSIST tasks if it needs to be persisted from another Executor on the Worker
  *
  **/
int32_t TaskScheduler::ValidatePartitions(const std::vector<NewArg>& task_args, int executor_id, ::uint64_t taskid) {
   int needs_persist = 0;
   std::vector<std::string> all_partitions;

   for(int i=0; i<task_args.size(); i++) {
      if(task_args[i].arrayname() == "list_type...") {
        for(int j=0; j<task_args[i].list_arraynames_size(); j++)
           all_partitions.push_back(task_args[i].list_arraynames(j));
      } else
        all_partitions.push_back(task_args[i].arrayname());
   }

   unique_lock<recursive_mutex> persistlock(persist_mutex, defer_lock);

   boost::unordered_set<std::string> processed;
   for(int i=0; i<all_partitions.size(); i++) {
      bool launch_persist = false;
      int target_executor = -1;
      unique_lock<recursive_mutex> metalock(metadata_mutex);
      if(!IsSplitAvailable(all_partitions[i], executor_id)) {
        if (!IsBeingPersisted(all_partitions[i])) {
           launch_persist = true;
           target_executor = ExecutorToPersistFrom(all_partitions[i]);
           LOG_DEBUG("<Scheduler> TaskID %zu - Split %s will be persisted from Executor Id %d", taskid, all_partitions[i].c_str(), target_executor);
        } else {
          LOG_DEBUG("<Scheduler> TaskID %zu - Split %s is being persisted", taskid, all_partitions[i].c_str());
        }

        // Update metadata and launch persists
        if(processed.count(all_partitions[i]) == 0) {
          persistlock.lock();
          if(sync_persist.count(taskid) == 0) {
            boost::interprocess::interprocess_semaphore sema(0);
            sync_persist[taskid] = &sema;
          }
          persist_tasks[all_partitions[i]].insert(taskid);
          persistlock.unlock();

          needs_persist++;
          processed.insert(all_partitions[i]);
        }

        if(launch_persist) worker->prepare_persist(all_partitions[i], target_executor, taskid);
      } else
        LOG_DEBUG("<Scheduler> TaskID %zu - Split %s is available", taskid, all_partitions[i].c_str());

      metalock.unlock();
   }

   for(int i=0; i< needs_persist; i++) {
     sync_persist[taskid]->wait();
   }

   LOG_DEBUG("<Scheduler> TaskID %zu - Dependent PERSIST tasks(#%d) complete", taskid, needs_persist);
   persistlock.lock();
   sync_persist.erase(taskid);
   persistlock.unlock();

   return needs_persist;
}

/**
  * Called after EXECUTE is complete.
  *
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
  * Called after EXECUTE is complete.
  *
  * Add Updated partitions (for Executes issued by dframe and dlist CC tasks)
  *
  */
void TaskScheduler::AddUpdatedPartition(const std::string &name, size_t size, int executor_id, ::uint64_t exec_taskid) {

  unique_lock<recursive_mutex> metalock(metadata_mutex);
  if(executor_splits.find(name) != executor_splits.end()) {
    ExecSplit *partition = executor_splits[name];
    partition->executors.insert(executor_id);
    executor_splits[name] = partition;
  } else {
    ExecSplit *partition = new ExecSplit;
    partition->name = name;
    partition->size = size;
    partition->executors.insert(executor_id);
    executor_splits[name] = partition;
  }
  metalock.unlock();

  LOG_DEBUG("EXECUTE TaskID %18zu - Split %s added to Worker", exec_taskid, name.c_str());
}


/**
  * Called for tasks - METADATAUPDATE
  *
  * If foreach is successful, merge Staged metadata to main Worker metadata and clear old splits
  * If foreach is unsuccessful, clear newly created splits
  *
  */
void TaskScheduler::ForeachComplete(::uint64_t id, ::uint64_t uid, bool status) {
  boost::unordered_map<int, std::vector<std::string>> clear_map;

  unique_lock<recursive_mutex> metalock(metadata_mutex);

  //Reset exec_stat
  for(boost::unordered_map<int, ExecStat*>::iterator itr = executor_stat.begin(); 
      itr != executor_stat.end(); ++itr) {
    itr->second->exec_load = 0;
  }
  
  if(status) {
    // Foreach is successful.
    LOG_INFO("METADATAUPDATE Task               - Foreach is successful");

    unique_lock<mutex> stagelock(stage_mutex);
    boost::unordered_set<SplitUpdate*>::iterator it;
    for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
        SplitUpdate* split = *it;

       //Add to Worker metadata
       if(executor_splits.find(split->name) != executor_splits.end()) {
          ExecSplit *partition = executor_splits[split->name];
          partition->executors.insert(split->executor);
          executor_splits[split->name] = partition;
       } else {
         LOG_DEBUG("METADATAUPDATE Task               - Adding new split %s(size: %zu) to Executor Id %d", split->name.c_str(), split->size, split->executor);
         ExecSplit *partition = new ExecSplit;
         partition->name = split->name;
         partition->size = split->size;
         partition->executors.insert(split->executor);
         executor_splits[split->name] = partition;
       }

       //TODO:Update executor size info

       // Prepare map for batched clear
       int32_t split_id;
       string darray_name;
       int32_t version;
       ParseVersionNumber(split->name, &version);
       ParseSplitName(split->name, &darray_name, &split_id);

      if(version > 1) {
        string parent = darray_name + "_" +
        int_to_string(split_id) + "_" +
        int_to_string(version - GC_DEFAULT_GEN);

        if (executor_splits.find(parent) == executor_splits.end())
          LOG_ERROR("METADATAUPDATE Task               - Previous split %s not found in executors", parent.c_str());
        else {
          ExecSplit* partition = executor_splits[parent];
          for(boost::unordered_set<int>::iterator i = partition->executors.begin();
              i != partition->executors.end(); i++) {
             LOG_DEBUG("METADATAUPDATE Task               - Previous split %s will be cleared from Executor Id %d", parent.c_str(), (*i));
             clear_map[*i].push_back(parent);
          }
        }

        SharedMemoryObject::remove(parent.c_str());
        
        /*shmem_arrays_mutex->lock();
        if(shmem_arrays->find(parent) != shmem_arrays.end())
          shmem_arrays->remove(parent);
        shmem_arrays_mutex->unlock();*/
        delete executor_splits[parent];
        executor_splits.erase(parent);
      }
    }
    stagelock.unlock();
  } else {
    // Foreach has failed
    LOG_INFO("METADATAUPDATE Task               - Foreach failed");
    
    // Prepare map for batched clear
    unique_lock<mutex> stagelock(stage_mutex);
    boost::unordered_set<SplitUpdate*>::iterator it;
    for(it = updated_splits.begin(); it != updated_splits.end(); ++it) {
       SplitUpdate* split = *it;
       LOG_DEBUG("METADATAUPDATE Task               - Newly created split %s will be cleared from Executor Id %d", split->name.c_str(), split->executor);
       clear_map[split->executor].push_back(split->name);
    }
    stagelock.unlock();
  }

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
  LOG_DEBUG("METADATAUPDATE Task               - Updated Worker Metadata. Sent notification to Master");

  for(boost::unordered_map<int, std::vector<std::string>>::iterator itr = clear_map.begin();
      itr != clear_map.end(); itr++) {
     executorpool->clear(itr->second, itr->first);
  }

  clear_map.clear();
  LOG_INFO("METADATAUPDATE Task               - Worker update complete");
}

}  // namespace presto
