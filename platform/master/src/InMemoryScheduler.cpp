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

#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <utility>

#include "master.pb.h"
#include "common.h"
#include "dLogger.h"
#include "Scheduler.h"
#include "TransferServer.h"
#include "DeserializeArray.h"
#include "PrestoException.h"

// Uncomment the following to schedule tasks
// to the worker that has the first split in
// the argument list (otherwise they are scheduled
// to the worker with the most bytes)
// #define HINTED_LOCATION 1

#define PROFILING_OUTPUT_NAME "scheduler_profiling"
// format: [time]: worker taskid tasktype status [elapsed] [misc info]

using namespace std;
using namespace boost;

namespace presto {
    

/** A task is done, and the InMemoryScheduler needs to perform further processing
 * @param taskid ID of a task that is completed
 * @param task a pointer of task information
 * @param type a type of a task to process
 */
void InMemoryScheduler::ChildDone(::uint64_t taskid, void *task, TaskType type) {
  unique_lock<recursive_mutex> lock(mutex_);
  if (type == EXEC) {  // task is done
    LOG_INFO("EXECUTE TaskID %14zu is complete.", taskid);
    tasks_.erase(taskid);  // delete TaskData corresponding to this taskid

    current_tasks_--;
    if (current_tasks_ == 0) {
      LOG_INFO("*** Foreach execution is complete.");
    }
#ifdef PROFILING
    fprintf(profiling_output_, "%8.3lf %15s %6zu EXEC  DONE %8.3lf\n",
            timer_.stop()/1e6,
            server_to_string(
                reinterpret_cast<ExecTask*>(task)->worker->server).c_str(),
            taskid, timers_[taskid].stop()/1e6);
    timers_.erase(taskid);

    current_tasks_--;
    if (current_tasks_ == 0) {
      fprintf(profiling_output_, "foreach done\n\n");
    }
#endif
  } else if (type == FETCH) {  // a fetch is done
    FetchTask *t = reinterpret_cast<FetchTask*>(task);
    LOG_DEBUG("FETCH TaskID %16zu of split size %7.2lfMB is complete.", taskid, t->split->size/static_cast<double>(1<<20));
#ifdef PROFILING
    {  // NOLINT
      FetchTask *t = reinterpret_cast<FetchTask*>(task);
      fprintf(profiling_output_,
              "%8.3lf %15s %6zu FETCH DONE %8.3lf %15s %7.2lfMB\n",
              timer_.stop()/1e6,
              server_to_string(t->to->server).c_str(),
              taskid, timers_[taskid].stop()/1e6,
              server_to_string(t->from->server).c_str(),
              t->split->size/static_cast<double>(1<<20));
    }
    timers_.erase(taskid);
#endif
    // get a list of tasks that are dependent to this Fetch task
    boost::unordered_set< ::uint64_t> dep_tasks = dependencies_[taskid];
    dependencies_.erase(taskid);
    lock.unlock();

    // handle
    for (boost::unordered_set< ::uint64_t>::iterator i = dep_tasks.begin();
         i != dep_tasks.end(); i++) {
      // a taskID that is dependent on this task
      ::uint64_t dep_task_id = *i;

      lock.lock();
      // Find a TaskData of dependent tasks
      if (contains_key(tasks_, dep_task_id)) {
        // decrease the number of dependencies as this task completes
        tasks_[dep_task_id].num_dependencies--;
        TaskData taskdata = tasks_[dep_task_id];

        // All other dependent tasks are done (all fetches done)
        if (taskdata.inited &&
            taskdata.num_dependencies == 0) {
          // launch task
          tasks_[dep_task_id].launched = true;
          lock.unlock();

          unique_lock<recursive_mutex> metalock(metadata_mutex);
          // Upon completion of a dependent task, Execute the task
          LOG_INFO("FETCH dependencies on the Foreach are resolved. Executing Foreach Task.");
          ::uint64_t id = Exec(taskdata.worker, taskdata.task);

#ifdef PROFILING
          fprintf(profiling_output_, "%8.3lf %15s %6zu EDEPS DONE %8.3lf\n",
                  timer_.stop()/1e6,
                  server_to_string(taskdata.worker->server).c_str(),
                  dep_task_id, timers_[dep_task_id].stop()/1e6);

          fprintf(profiling_output_, "%8.3lf %15s %6zu  FETCHDONE_EXEC STRT\n",
                  timer_.stop()/1e6,
                  server_to_string(taskdata.worker->server).c_str(),
                  id);

          timers_.erase(dep_task_id);
          timers_[id] = Timer();
          timers_[id].start();
#endif
          metalock.unlock();
        } else
          lock.unlock();
      } if (contains_key(cctasks_, dep_task_id)) {
        // One of a dependent task is create composite task
        // we and decrement the value
        cctasks_[dep_task_id].num_dependencies--;
        CCTaskData taskdata = cctasks_[dep_task_id];
        lock.unlock();

        if (taskdata.inited &&
            taskdata.num_dependencies == 0) {  // all fetches done
          // Start create composite task
          unique_lock<recursive_mutex> metalock(metadata_mutex);
          LOG_INFO("FETCH dependencies for Create Composite are resolved. Creating Composite Array.");
          ::uint64_t cc_id = CreateComposite(taskdata.worker,
                                           taskdata.name,
                                           *taskdata.arg);
          // copy over dependencies to new id.
          // We still need to keep a list that is dependent on the task
          dependencies_[cc_id] = dependencies_[dep_task_id];
          // delete old id and stuff
          cctask_ids_[make_pair(taskdata.worker, taskdata.name)] = cc_id;
          dependencies_.erase(dep_task_id);
          cctasks_.erase(dep_task_id);

#ifdef PROFILING
          fprintf(profiling_output_, "%8.3lf %15s %6zu CDEPS DONE %8.3lf\n",
                  timer_.stop()/1e6,
                  server_to_string(taskdata.worker->server).c_str(),
                  dep_task_id, timers_[dep_task_id].stop()/1e6);

          fprintf(profiling_output_, "%8.3lf %15s %6zu    CC STRT\n",
                  timer_.stop()/1e6,
                  server_to_string(taskdata.worker->server).c_str(),
                  cc_id);

          timers_.erase(dep_task_id);
          timers_[cc_id] = Timer();
          timers_[cc_id].start();
#endif
          metalock.unlock();
        }
      }
    }
  } else if (type == CREATECOMPOSITE) {  // cc done
    CCTask *t = reinterpret_cast<CCTask*>(task);
#ifdef PROFILING
    fprintf(profiling_output_, "%8.3lf %15s %6zu    CC DONE %8.3lf\n",
            timer_.stop()/1e6,
            server_to_string(t->worker->server).c_str(),
            taskid, timers_[taskid].stop()/1e6);
    timers_.erase(taskid);
#endif
    cctask_ids_.erase(make_pair(t->worker, t->name));
    // get a list of tasks that are dependent on this create composite task
    boost::unordered_set< ::uint64_t> dep_tasks = dependencies_[taskid];
    dependencies_.erase(taskid);
    lock.unlock();

    // handle
    for (boost::unordered_set< ::uint64_t>::iterator i = dep_tasks.begin();
         i != dep_tasks.end(); i++) {
      ::uint64_t dep_task_id = *i;  // a dependent taskID

      lock.lock();
      tasks_[dep_task_id].num_dependencies--;
      TaskData taskdata = tasks_[dep_task_id];

      // A dependent Execution task has all necessary splits to Execute.
      if (taskdata.inited &&
          taskdata.num_dependencies == 0) {
        // launch task
        tasks_[dep_task_id].launched = true;
        lock.unlock();

        unique_lock<recursive_mutex> metalock(metadata_mutex);
        LOG_INFO("Composite Array Created. All dependencies resolved. Executing Foreach Task.: %s", t->name.c_str());
        ::uint64_t id = Exec(taskdata.worker, taskdata.task);
#ifdef PROFILING
        fprintf(profiling_output_, "%8.3lf %15s %6zu CC_EDEPS DONE %8.3lf\n",
                timer_.stop()/1e6,
                server_to_string(taskdata.worker->server).c_str(),
                dep_task_id, timers_[dep_task_id].stop()/1e6);

        fprintf(profiling_output_, "%8.3lf %15s %6zu  CC_EXEC STRT\n",
                timer_.stop()/1e6,
                server_to_string(taskdata.worker->server).c_str(),
                id);

        timers_.erase(dep_task_id);
        timers_[id] = Timer();
        timers_[id].start();
#endif
        metalock.unlock();
      } else
        lock.unlock();
    }
  }
}

/**get a worker to fetch a split
 * Determine one with the least number of send tasks among workers with the splits
 * @return the best worker to fetch a split
 */
static Worker* best_worker_to_fetch_from(
    const boost::unordered_set<Worker*> &workers) {
  boost::unordered_set<Worker*>::iterator i = workers.begin();
  Worker *best = *i;
  for (i++; i != workers.end(); i++) {
    if (best->sendtasks.size() > (*i)->sendtasks.size()) {
      best = *i;
    }
  }

  return best;
}


/** Assign a ExecTask to a worker
 * @param tasks 
 * @param inputs
 * @return NULL
 */
void InMemoryScheduler::AddTask(const std::vector<TaskArg*> &tasks,
				const int scheduler_policy,
                                const std::vector<int> *inputs) {

  std::ostringstream funcstr;
  funcstr << "*** A new Foreach function received. " << 
  tasks.size() << " parallel EXECUTE tasks will be created and sent to Worker : \n" <<
  "function() ";

  for (int i = 0; i < tasks[0]->func_str.size(); i++) {
      if (i==tasks[0]->func_str.size()-1)
         funcstr << tasks[0]->func_str[i].c_str();
      else 
         funcstr << tasks[0]->func_str[i].c_str() << "\n";
  }
  
  LOG_INFO(funcstr.str());
#ifdef PROFILING
  fprintf(profiling_output_, "new foreach with %zu tasks\n", tasks.size());
  for (int i = 0;
       i < 5 && i < tasks[0]->func_str.size(); i++) {
    fprintf(profiling_output_, "%s\n", tasks[0]->func_str[i].c_str());
  }
  fprintf(profiling_output_, "...\n");
  unique_lock<recursive_mutex> lock(mutex_);
  current_tasks_ += tasks.size();
  lock.unlock();
#endif

  // The number of tasks = the number of splits
  int task_cnt = 0;
  current_tasks_ += tasks.size();
  map<Worker*, int> schedule_status;
  
  for (int i = 0; i < tasks.size(); i++) {
    task_cnt=i+1;
    // t contains all necessary information to execute a task
    TaskArg *t = tasks[i];
    ::uint64_t task_id = GetNewTaskID();

    unique_lock<recursive_mutex> lock(mutex_);
    tasks_[task_id] = TaskData();
    TaskData &taskdata = tasks_[task_id];
    lock.unlock();

    taskdata.task = t;
    taskdata.num_dependencies = 0;
    taskdata.launched = false;
    taskdata.inited = false;

    //  look for host that has most pieces in DRAM
    unique_lock<recursive_mutex> metalock(metadata_mutex, defer_lock);
    Worker *worker = NULL;

    //#ifndef HINTED_LOCATION
    if(scheduler_policy == 0){
    // mapping of available workers with the splits size that they have
    map<Worker*, size_t> available;
    // iterate over all splits necessary to perform this task
    int num_composite = 0, num_splits = 0;
    // check if the given task contains composite and/or splits
    for (int32_t i = 0; i < t->args.size(); i++) {
      const Arg &arg = t->args[i];
      if (arg.arrays_size() == 1) ++num_splits;
      else if (arg.arrays_size() > 1) ++num_composite;
    }

    // iterate over all splits necessary to perform this task
    for (int32_t i = 0; i < t->args.size(); i++) {
      const Arg &arg = t->args[i];
      // Iterate over all splits
      // the given split is a composite array and the task contains split
      // there are multiple tasks, do not consider the composite array location
      // it might be better to distribute task while creating a composite array
     
      //temporarily commented out because we want scheduling decision to take into account composite "list" type operations
     // if (arg.arrays_size() > 1 && num_splits > 0 && tasks.size() > 1) {
      //  continue;
     // }

      for (int32_t j = 0; j < arg.arrays_size(); j++) {
        metalock.lock();
        // Get a split pointer
        Split *split = splits[arg.arrays(j).name()];
        // workers with the split
        boost::unordered_set<Worker*> &locs = split->workers;
        for (boost::unordered_set<Worker*>::iterator i = locs.begin();
             i != locs.end(); i++) {
          available[*i] += split->size;
        }
        metalock.unlock();
      }
    }
    map<Worker*, size_t>::iterator best = available.begin();
    int min_load = 0xffffffff;
    // iterate over workers with splits to find the best one
    for (map<Worker*, size_t>::iterator i = available.begin();
         i != available.end(); i++) {
      // it has to have available space
      if (i->first->size > i->first->used) {
        // if a task contains a single split, respect even load distribution
        if (t->args.size() == 1) {
          // if a worker does not have a task assigned yet, assign to the worker
          if (schedule_status.count(i->first) == 0) {
            best = i;
            break;
          } else {
            // if worker already has assigned tasks, consider one with the minimal load
            if (schedule_status[i->first] < min_load) {
              min_load = schedule_status[i->first];
              best = i;
            }
          }
        } else {
        // if a task contains multiple splits, respect a worker with the most split size
          if (best->second < i->second) {
            best = i;
          }
        }
      }
    }
    if (best != available.end() && best->first->size > best->first->used) {
      // the best candidate has remaining memory space
      worker = best->first;
    } else {
      if (num_composite > 0 || num_splits > 0) {  // to check if a task contains splits
        // if the best host does not have enough memory
        // choose one with the most remaining.        
        worker = GetMostMemWorker();
      } else {
        // if this function does not use any splits, we do not have to
        // consider available shared memory size
        worker = GetRndAvailableWorker();
      }
    }
    }
    //#else
    else{
    LOG_DEBUG("InMemory Scheduler Policy: Send task to worker where first passed argument in located.");
    if (t->args.size() > 0) {
      // job is scheduled to the node that has the first split.
      worker = *splits[t->args[0].arrays(0).name()]->workers.begin();
    } else {
      // As there is no split information,
      // the job is scheduled to the first worker
      worker = workers.begin()->second;
    }
    }
    //#endif

    // if there is not available workers (memory-perspective, throw exception)
    if (worker == NULL) {
      tasks_.erase(task_id);
      throw PrestoWarningException
        ("no available workers found.\n check available memory"
        " by distributedR_status() or restart by distributedR_shutdown()");
    } else {
      if (schedule_status.count(worker) != 0) {
        schedule_status[worker] = schedule_status[worker] + 1;
      } else {
        schedule_status[worker] = 1;
      }
    }

#ifdef PROFILING
    fprintf(profiling_output_, "%8.3lf %15s %6zu  EXEC CREA\n",
            timer_.stop()/1e6,
            server_to_string(worker->server).c_str(),
            task_id);
    timers_[task_id] = Timer();
    timers_[task_id].start();
#endif
    // assign a worker to this task
    taskdata.worker = worker;

    // move rest of pieces to best host
    Response ret; 
    for (int32_t i = 0; i < t->args.size(); i++) {
      const Arg &arg = t->args[i];
      //if it's a list, for now treat it as a regular split arg
      if (arg.arrays_size() == 1 || arg.is_list()) {  // split or list of splits
          for(int32_t j = 0; j < arg.arrays_size(); j++){
            const Array &array = arg.arrays(j);
            unique_lock<recursive_mutex> lock(mutex_);
            // we need the metadata lock because we don't want a splits location
            // to change while we are registering the dependencies etc.
            metalock.lock();

            Split *split = splits[array.name()];
            if (!IsSplitOnWorker(split, worker)) {
              ::uint64_t beingfetched = IsSplitBeingFetched(split, worker);
              if (beingfetched == 0) {  // it is not being fetched
                Worker *from = best_worker_to_fetch_from(split->workers);
                // create a fetch task to prepare a task
                //LOG_DEBUG("Task number %d - Task argument %d - Split %s is not on Worker %15s. It is will be fetched from Worker %15s", 
                //          task_cnt, i+1, split, server_to_string(worker->server).c_str(), server_to_string(from->server).c_str());
                ::uint64_t fetch_task_id = Fetch(
                    worker,
                    from,
                    split);
    #ifdef PROFILING
                fprintf(profiling_output_,
                        "%8.3lf %15s %6zu FETCH STRT %15s %7.2lfMB\n",
                        timer_.stop()/1e6,
                        server_to_string(worker->server).c_str(),
                        fetch_task_id,
                        server_to_string(from->server).c_str(),
                        split->size/static_cast<double>(1<<20));
                timers_[fetch_task_id] = Timer();
                timers_[fetch_task_id].start();
    #endif

                // update dependency to execute a task after fetch is done
                dependencies_[fetch_task_id].insert(task_id);
                taskdata.num_dependencies++;
                lock.unlock();
              } else {
                // Increment num dependencies
                // if the dependencies_ information does not include this task
                // on this fetch
                if (dependencies_[beingfetched].count(task_id) == 0) {
                  taskdata.num_dependencies++;
                  dependencies_[beingfetched].insert(task_id);
                }
                lock.unlock();
              }
              if (task_cnt==1)
                 LOG_INFO("Foreach argument '%s' has %d FETCH dependencies. Resolving dependencies.", arg.name().c_str(), taskdata.num_dependencies);
            }
            metalock.unlock();
          }
      } else {  // composite
        string name = CompositeName(arg);

        if (task_cnt==1) 
            LOG_INFO("Foreach argument '%s' (Internal name: %s) is a Composite array. Composite Array will be created before execution.", arg.name().c_str(), name.c_str());

        lock.lock();
        if (!contains_key(splits, name) ||
            !contains_key(splits[name]->workers, worker)) {
          // composite is not present
          // composite is present, but the worker does not have that split
          // increment the dependencies to this composite
          taskdata.num_dependencies++;

          pair<Worker*, string> key = make_pair(worker, name);
          if (contains_key(cctask_ids_, key)) {
            // already being created on this worker
            // we just need to register the dependency
            // to avoid duplicate inserting.
            if (dependencies_[cctask_ids_[key]].count(task_id) == 0) {
              dependencies_[cctask_ids_[key]].insert(task_id);
            } else {
              // if already inserted decrement it
              taskdata.num_dependencies--;
            }
          } else {
            // garbage collect previous version
            string splitname = name.substr(0, name.find('.'));
            if (contains_key(current_composite, splitname) &&
                current_composite[splitname] != name) {
              DeleteSplit(splits[current_composite[splitname]]);
              // fprintf(stderr, "GCing old composite %s\n",
              //         current_composite[splitname].c_str());
            }
            current_composite[splitname] = name;

            // we need to create the composite
            ::uint64_t cc_id = GetNewTaskID();
#ifdef PROFILING
            fprintf(profiling_output_, "%8.3lf %15s %6zu    CC CREA\n",
                    timer_.stop()/1e6,
                    server_to_string(worker->server).c_str(),
                    cc_id);
            timers_[cc_id] = Timer();
            timers_[cc_id].start();
#endif
            // State that the task_id is dependent on this cc_id task
            dependencies_[cc_id].insert(task_id);
            cctasks_[cc_id] = CCTaskData();
            cctask_ids_[key] = cc_id;
            CCTaskData &cc = cctasks_[cc_id];
            cc.name = name;
            cc.num_dependencies = 0;
            cc.worker = worker;
            cc.inited = false;
            cc.arg = &arg;
            for (int32_t j = 0; j < arg.arrays_size(); j++) {
              unique_lock<recursive_mutex> lock(mutex_);
              // we need the metadata lock because
              // we don't want a splits location
              // to change while we are registering
              // the dependencies etc.
              metalock.lock();

              Split *split = splits[arg.arrays(j).name()];
              if (!IsSplitOnWorker(split, worker)) {
                // if a worker does not have a required split
                cc.num_dependencies++;

                ::uint64_t beingfetched = IsSplitBeingFetched(split, worker);
                if (beingfetched == 0) {
                  // if this is not being fetched, fetch it first
                  Worker *from = best_worker_to_fetch_from(split->workers);
                  
                  ::uint64_t fetch_task_id = Fetch(
                      worker,
                      from,
                      split);
                  //LOG_DEBUG("Split %s of Composite Array creation is not on Worker %15s. It is will be fetched from Worker %15s", split, server_to_string(worker->server).c_str(), server_to_string(from->server).c_str());
#ifdef PROFILING
                  fprintf(profiling_output_,
                          "%8.3lf %15s %6zu CC_FETCH STRT %15s %7.2lfMB\n",
                          timer_.stop()/1e6,
                          server_to_string(worker->server).c_str(),
                          fetch_task_id,
                          server_to_string(from->server).c_str(),
                          split->size/static_cast<double>(1<<20));
                  timers_[fetch_task_id] = Timer();
                  timers_[fetch_task_id].start();
#endif
                  dependencies_[fetch_task_id].insert(cc_id);
                  lock.unlock();
                } else {
                  // if the split is being fetched,
                  // add cc_id to get notification
                  dependencies_[beingfetched].insert(cc_id);
                  lock.unlock();
                }
              }
              metalock.unlock();
            }
            //LOG_INFO("Task number %d - Composite Array %s has %d CREATE COMPOSITE Dependency. Resolving dependencies.", task_cnt, name.c_str(), cc.num_dependencies);
            cc.inited = true;

            if (cc.num_dependencies == 0) {  // all fetches done
              // create composite
	      LOG_INFO("Composite Array '%s' - FETCH dependencies resolved. Creating Composite Array.", arg.name().c_str());
              unique_lock<recursive_mutex> metalock(metadata_mutex);
              ::uint64_t cc_id2 = CreateComposite(cc.worker,
                                                cc.name,
                                                *cc.arg);
#ifdef PROFILING
              fprintf(profiling_output_, "%8.3lf %15s %6zu CDEPS DONE %8.3lf\n",
                      timer_.stop()/1e6,
                      server_to_string(worker->server).c_str(),
                      cc_id, timers_[cc_id].stop()/1e6);

              fprintf(profiling_output_, "%8.3lf %15s %6zu    CC STRT\n",
                      timer_.stop()/1e6,
                      server_to_string(cc.worker->server).c_str(),
                      cc_id2);

              timers_.erase(cc_id);
              timers_[cc_id2] = Timer();
              timers_[cc_id2].start();
#endif

              // copy over dependencies to new id
              dependencies_[cc_id2] = dependencies_[cc_id];
              // delete old id and stuff
              dependencies_.erase(cc_id);
              cctask_ids_[key] = cc_id2;
              metalock.unlock();
            }
          }
        }
        lock.unlock();
      }
    }
    // We are ready to start a task
    taskdata.inited = true;

    lock.lock();
    // All necessary split is prepared
    if (taskdata.num_dependencies == 0 && taskdata.launched == false) {
      if (task_cnt==1)
	 LOG_INFO("Foreach arguments has no dependencies. Sending task to Worker for execution");
      ::uint64_t id = Exec(worker, t);
#ifdef PROFILING
      fprintf(profiling_output_, "%8.3lf %15s %6zu EDEPS DONE %8.3lf\n",
              timer_.stop()/1e6,
              server_to_string(taskdata.worker->server).c_str(),
              task_id, timers_[task_id].stop()/1e6);

      fprintf(profiling_output_, "%8.3lf %15s %6zu  ADDTASK_EXEC STRT\n",
              timer_.stop()/1e6,
              server_to_string(taskdata.worker->server).c_str(),
              id);

      timers_.erase(task_id);
      timers_[id] = Timer();
      timers_[id].start();
#endif
    } else {
    }
    lock.unlock();
  }
}

/** InMemoryScheduler constructor
 * @param hostname the host name of the master
 */
InMemoryScheduler::InMemoryScheduler(const std::string &hostname, PrestoMaster* presto_master)
    : Scheduler(hostname, presto_master),
      current_tasks_(0) {
  timer_.start();
#ifdef PROFILING
  fprintf(stderr, "writing profiling info to %s\n", PROFILING_OUTPUT_NAME);
  profiling_output_ = fopen(PROFILING_OUTPUT_NAME, "w");
#endif
}

/** InMemoryScheduler destructor
 */
InMemoryScheduler::~InMemoryScheduler() {
  tasks_.clear();
  dependencies_.clear();
  timers_.clear();
  cctasks_.clear();
  cctask_ids_.clear();
  current_composite.clear();
#ifdef PROFILING
  fclose(profiling_output_);
#endif
}

}  // namespace presto
