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

#include "common.h"
#include "Scheduler.h"

#define TRY_SCHEDULE_TASKS_MAX 3

using namespace std;
using namespace boost;

// #undef PROFILING

#define PROFILING_OUTPUT_NAME "scheduler_profiling"
// format: [time]: worker taskid tasktype status [elapsed] [misc info]

#ifdef OOC_LOGGING
#define OOC_LOG(n, a...) fprintf(ooc_log_, n, ## a)
#else
#define OOC_LOG(n, a...)
#endif

#define INIT_TIMING(name)                       \
  static int name ## i_ = 0;                    \
  static double name ## t_ = 0;
#define DONE_TIMING(name)                                               \
  fprintf(profiling_output_,                                            \
          #name " timing: %lfs total, %lf per call\n",                  \
          name ## t_, name ## t_ / name ## i_);                         \
  name ## i_ = name ## t_ = 0;
#define TIMING_START(name)                      \
  Timer name ## t__;                            \
  name ## t__.start();
#define TIMING_END(name)                        \
  name ## t_ += name ## t__.stop() / 1e6;       \
  name ## i_++;

INIT_TIMING(scheduletask)
INIT_TIMING(tryexectasks)
INIT_TIMING(tryscheduletasks)
INIT_TIMING(unlocksplits)
INIT_TIMING(tryloadsplits)
INIT_TIMING(execdone)
INIT_TIMING(childdone)
INIT_TIMING(issplitbeingacquired)
INIT_TIMING(scheduleacquire)
INIT_TIMING(st1)
INIT_TIMING(st2)
INIT_TIMING(st3)
INIT_TIMING(st4)
INIT_TIMING(st5)
INIT_TIMING(st6)
INIT_TIMING(st7)
INIT_TIMING(ls1)
INIT_TIMING(ls2)
INIT_TIMING(flushunlockedsplits)
INIT_TIMING(flushunlockedsplits1)
INIT_TIMING(flushunlockedsplits2)

#define OOC_LOG_NAME "ooc_log"

namespace presto {

bool skip_send = false;

OOCScheduler::OOCScheduler(const std::string &hostname, PrestoMaster* presto_master)
    : Scheduler(hostname, presto_master) {
  timer_.start();
#ifdef PROFILING
  fprintf(stderr, "writing profiling info to %s\n", PROFILING_OUTPUT_NAME);
  profiling_output_ = fopen(PROFILING_OUTPUT_NAME, "w");
#endif
#ifdef OOC_LOGGING
  fprintf(stderr, "writing ooc info to %s\n", OOC_LOG_NAME);
  ooc_log_ = fopen(OOC_LOG_NAME, "w");
#endif

  exec_time = new double[20];
}

// --- LOGIC ---

void OOCScheduler::ChildDone(uint64_t taskid, void *task, TaskType type) {
  TIMING_START(childdone)

      // fprintf(stderr, "done type is %d\n", static_cast<int>(type));
      Worker *worker = NULL;
  Split *split = NULL;
  if (type == LOAD || type == FETCH || type == EXEC ||
      type == MOVETODRAM || type == MOVETOSTORE) {
    switch (type) {
      case LOAD:
      case MOVETODRAM:
        worker = reinterpret_cast<LoadTask*>(task)->store->worker;
        split = reinterpret_cast<LoadTask*>(task)->split;
        break;
      case MOVETOSTORE:
        worker = reinterpret_cast<SaveTask*>(task)->store->worker;
        split = reinterpret_cast<SaveTask*>(task)->split;
        break;
      case FETCH:
        worker = reinterpret_cast<FetchTask*>(task)->to;
        split = reinterpret_cast<FetchTask*>(task)->split;
        break;
      case EXEC:
        worker = reinterpret_cast<ExecTask*>(task)->worker;
        break;
    }
  }

  unique_lock<recursive_mutex> lock(mutex_);
  if (type == EXEC) {  // task is done
    TIMING_START(execdone)
        UnLockSplits(&tasks_[taskid]);
    tasks_.erase(taskid);

    LOG("task done %zu, trying to schedule new tasks\n", taskid);
    TryScheduleTasks(worker);
    LOG("task done %zu, trying to load splits\n", taskid);
    TryLoadSplits(worker);
    LOG("task done %zu, trying to execute scheduled tasks\n", taskid);
    TryExecTasks(worker);

#ifdef PROFILING
    fprintf(profiling_output_, "%8.5lf %6zu EXEC  DONE %8.5lf\n",
            abs_time()/1e6,
            taskid, timers_[taskid].stop()/1e6);
    timers_.erase(taskid);

    if (tasks_.empty() &&
        waiting_tasks_[worker].empty() &&
        waiting_tasks_exec_[worker].empty()) {
      fprintf(profiling_output_, "foreach done\n");
      fprintf(profiling_output_,
              "  scheduler time: %lf\n",
              total_scheduler_time_);
      fprintf(profiling_output_,
              "  child scheduler time: %lf\n",
              total_child_scheduler_time_);
      fprintf(profiling_output_, "  setup time: %lf\n", setup_time);
      for (int i = 0; i <= worker->executors+1; i++) {
        fprintf(profiling_output_, "  exec time %d: %lf\n", i, exec_time[i]);
      }
      total_scheduler_time_ = total_child_scheduler_time_ = 0;
      DONE_TIMING(childdone);
      DONE_TIMING(execdone);
      DONE_TIMING(scheduletask);
      DONE_TIMING(tryexectasks);
      DONE_TIMING(tryscheduletasks);
      DONE_TIMING(unlocksplits);
      DONE_TIMING(tryloadsplits);
      DONE_TIMING(issplitbeingacquired);
      DONE_TIMING(scheduleacquire);
      DONE_TIMING(st1);
      DONE_TIMING(st2);
      DONE_TIMING(st3);
      DONE_TIMING(st4);
      DONE_TIMING(st5);
      DONE_TIMING(st6);
      DONE_TIMING(st7);
      DONE_TIMING(ls1);
      DONE_TIMING(ls2);
      DONE_TIMING(flushunlockedsplits);
      DONE_TIMING(flushunlockedsplits1);
      DONE_TIMING(flushunlockedsplits2);
    }
#endif

    if (worker->exectasks.size() <= worker->executors) {
      exec_time[worker->exectasks.size()+1] += current_op_timer.stop()/1e6;
      current_op_timer.start();
    }

    lock.unlock();
    TIMING_END(execdone)
        } else if (type == FETCH || type == LOAD) {
    // a corresponding fetch/load is done
    unordered_set<uint64_t> dep_tasks = dependencies_[make_pair(worker, split)];
    dependencies_.erase(make_pair(worker, split));

    if (contains_key(locked_on_[split], worker)) {
      locked_loaded_size_[worker] += split->size;
    }


    if (type == FETCH) {
      FetchTask *t = reinterpret_cast<FetchTask*>(task);
      UnLockSplit(t->split, t->from);

      TryScheduleTasks(t->from);
      TryLoadSplits(t->from);
      if (!waiting_splits_[t->from].empty()) {
        FlushUnlockedSplit(t->from);
      }
    } else {
#ifdef PROFILING
      fprintf(profiling_output_, "%8.5lf %6zu LOAD  DONE %8.5lf\n",
              abs_time()/1e6,
              taskid, timers_[taskid].stop()/1e6);
      timers_.erase(taskid);
#endif
    }
    lock.unlock();

    for (unordered_set<uint64_t>::iterator i = dep_tasks.begin();
         i != dep_tasks.end(); i++) {
      uint64_t dep_task_id = *i;

      lock.lock();
      if (contains_key(tasks_, dep_task_id)) {
        tasks_[dep_task_id].num_dependencies--;
        TaskData taskdata = tasks_[dep_task_id];
        lock.unlock();

        if (taskdata.inited &&
            taskdata.num_dependencies == 0) {  // all fetches done
          // launch task
          lock.lock();
#ifdef PROFILING
          fprintf(profiling_output_, "%8.5lf %6zu EXDEP DONE %8.5lf\n",
                  abs_time()/1e6,
                  dep_task_id, timers_[dep_task_id].stop()/1e6);
          timers_.erase(dep_task_id);
#endif
          waiting_tasks_exec_[taskdata.worker].push_back(dep_task_id);
          LOG("all dependencies of task %zu satisfied, "
              "trying to execute\n", dep_task_id);
          TryExecTasks(taskdata.worker);
          // uint64_t new_task_id = Exec(taskdata.worker, taskdata.task);
          // tasks_[new_task_id] = tasks_[dep_task_id];
          // tasks_.erase(dep_task_id);
          lock.unlock();
        } else {
        }
      } else if (contains_key(cctasks_, dep_task_id)) {
        cctasks_[dep_task_id].num_dependencies--;
        CCTaskData taskdata = cctasks_[dep_task_id];
        lock.unlock();

        if (taskdata.inited &&
            taskdata.num_dependencies == 0) {  // all fetches done
          // create composite
          unique_lock<recursive_mutex> metalock(metadata_mutex);
          uint64_t cc_id = CreateComposite(taskdata.worker,
                                           taskdata.name,
                                           *taskdata.arg);
          // copy over dependencies to new id
          dependencies_cc[cc_id] = dependencies_cc[dep_task_id];
          // delete old id and stuff
          dependencies_cc.erase(dep_task_id);
          cctasks_.erase(dep_task_id);

          LOG("all dependencies of composite %zu satisfied, "
              "creating with new id %zu\n", dep_task_id, cc_id);

#ifdef PROFILING
          fprintf(profiling_output_, "%8.5lf %6zu CCDEP DONE %8.5lf\n",
                  abs_time()/1e6,
                  dep_task_id, timers_[dep_task_id].stop()/1e6);
          timers_.erase(dep_task_id);

          fprintf(profiling_output_, "%8.5lf %6zu CC   STRT\n",
                  abs_time()/1e6,
                  cc_id);
          timers_[cc_id] = Timer();
          timers_[cc_id].start();
#endif
        }
      }
    }
  } else if (type == CREATECOMPOSITE) {
#ifdef PROFILING
    fprintf(profiling_output_, "%8.5lf %6zu CC    DONE %8.5lf\n",
            abs_time()/1e6,
            taskid, timers_[taskid].stop()/1e6);
    timers_.erase(taskid);
#endif
    CCTask *t = reinterpret_cast<CCTask*>(task);
    cctask_ids_.erase(make_pair(t->worker, t->name));

    unordered_set<uint64_t> dep_tasks = dependencies_cc[taskid];
    dependencies_cc.erase(taskid);
    lock.unlock();

    // handle
    for (unordered_set<uint64_t>::iterator i = dep_tasks.begin();
         i != dep_tasks.end(); i++) {
      uint64_t dep_task_id = *i;

      lock.lock();
      tasks_[dep_task_id].num_dependencies--;
      TaskData taskdata = tasks_[dep_task_id];
      lock.unlock();

      if (taskdata.inited &&
          taskdata.num_dependencies == 0) {  // all deps done
        // launch task
        lock.lock();

#ifdef PROFILING
        fprintf(profiling_output_, "%8.5lf %6zu EXDEP DONE %8.5lf\n",
                abs_time()/1e6,
                dep_task_id, timers_[dep_task_id].stop()/1e6);
        timers_.erase(dep_task_id);
#endif

        LOG("all dependencies of task %zu satisfied, "
            "trying to execute\n", dep_task_id);

        waiting_tasks_exec_[taskdata.worker].push_back(dep_task_id);
        TryExecTasks(taskdata.worker);
        // uint64_t new_task_id = Exec(taskdata.worker, taskdata.task);
        // tasks_[new_task_id] = tasks_[dep_task_id];
        // tasks_.erase(dep_task_id);
        lock.unlock();

        LOG("all dependencies of %d satisfied, waiting to exec\n",
            static_cast<int>(dep_task_id));
      }
    }
  } else if (type == MOVETOSTORE) {
#ifdef PROFILING
    fprintf(profiling_output_, "%8.5lf %6zu SAVE  DONE\n",
            abs_time()/1e6,
            taskid);
#endif
    LOG("move to store %zu done, trying to load splits\n", taskid);
    TryLoadSplits(worker);
    lock.unlock();
  } else {
    lock.unlock();
  }

  lock.lock();

  if (!waiting_splits_[worker].empty()) {
    LOG("there are waiting splits, trying to flush some unlocked\n");
    FlushUnlockedSplit(worker);
    LOG("there are waiting splits, trying to load some\n");
    TryLoadSplits(worker);
  }
  lock.unlock();

  TIMING_END(childdone)
      }

void OOCScheduler::AddTask(const vector<TaskArg*> &tasks,
			   const int scheduler_policy,
                           const vector<int> *inputs) {
  unique_lock<mutex> single_threading_lock(single_threading_mutex);

  setup_time = 0;
  for (int i = 0; i < workers.begin()->second->executors+1; i++) {
    exec_time[i] = 0;
  }

  LOG("new foreach with %zu tasks\n", tasks.size());
  for (int i = 0;
       i < 5 && i < tasks[0]->func_str.size(); i++) {
    LOG("%s\n", tasks[0]->func_str[i].c_str());
  }
  LOG("...\n");

  ostringstream msg;
  msg << "new foreach with " << tasks.size() << " tasks" << endl;
  for (int i = 0;
       i < 5 && i < tasks[0]->func_str.size(); i++) {
    msg << tasks[0]->func_str[i].c_str() << endl;
  }
  msg << "..." << endl;
  WorkerLog(workers.begin()->second, msg.str());

  // if (msg.str().find("io") != string::npos) {
  //   skip_send = true;
  //   LOG("starting skip send!\n");
  // }


#ifdef PROFILING
  fprintf(profiling_output_, "new foreach with %zu tasks\n", tasks.size());
  for (int i = 0;
       i < 5 && i < tasks[0]->func_str.size(); i++) {
    fprintf(profiling_output_, "%s\n", tasks[0]->func_str[i].c_str());
  }
  fprintf(profiling_output_, "...\n");
#endif

#ifdef OOC_LOGGING
  OOC_LOG("new foreach with %zu tasks\n", tasks.size());
  for (int i = 0;
       i < 5 && i < tasks[0]->func_str.size(); i++) {
    OOC_LOG("%s\n", tasks[0]->func_str[i].c_str());
  }
  OOC_LOG("...\n");
#endif

  unordered_set<Worker*> modified_workers;

  size_t total_size = 0;
  size_t unique_size = 0;
  size_t total_size_after_skip = 0;
  size_t unique_size_after_skip = 0;
  unordered_set<Split*> used_splits;
  unordered_map<Split*, vector<int> > split_uses;
  vector<int> skipped;
  current_op_timer.start();

  Timer timer;
  timer.start();

  for (int ti = 0; ti < tasks.size(); ti++) {
    TaskArg *t = tasks[ti];

#ifdef OOC_LOGGING
    size_t s = 0;
    for (int j = 0; j < t->args.size(); j++) {
      const Arg &arg = t->args[j];
      for (int k = 0; k < arg.arrays_size(); k++) {
        Split *split = splits[arg.arrays(k).name()];
        s += split->size;
        total_size += split->size;
        if (!contains_key(used_splits, split)) {
          used_splits.insert(split);
          unique_size += split->size;
        }
      }
    }
#endif

    total_size_after_skip = total_size;
    unique_size_after_skip = unique_size;

    bool empty = false;
    for (int j = 0; j < inputs->size(); j++) {
      const Arg &arg = t->args[inputs->at(j)];
      for (int k = 0; k < arg.arrays_size(); k++) {
        Split *split = splits[arg.arrays(k).name()];
        if (split->empty) {
          empty = true;
          t->sema->post();
          LOG("skipping task %d (empty split %s)\n", ti, split->name.c_str());
          break;
        }
      }
      if (empty)
        break;
    }

    if (empty) {
      skipped.push_back(ti);
      continue;
    }
    uint64_t task_id = GetNewTaskID();

    unique_lock<recursive_mutex> lock(mutex_);
    tasks_[task_id] = TaskData();
    TaskData &taskdata = tasks_[task_id];
    lock.unlock();

    taskdata.task = t;
    taskdata.num_dependencies = 0;
    taskdata.inited = false;

#ifdef PROFILING
    fprintf(profiling_output_, "%8.5lf %6zu EXEC  CREA\n",
            abs_time()/1e6, task_id);

    timers_[task_id] = Timer();
    timers_[task_id].start();
#endif

    Worker *worker;
    if (workers.size() > 1) {
      //  look for host that has most pieces in DRAM
      unique_lock<recursive_mutex> metalock(metadata_mutex, defer_lock);
      map<Worker*, size_t> available;
      for (int32_t i = 0; i < t->args.size(); i++) {
        const Arg &arg = t->args[i];
        for (int32_t j = 0; j < arg.arrays_size(); j++) {
          metalock.lock();
          Split *split = splits[arg.arrays(j).name()];
          unordered_set<Worker*> &locs = split->workers;
          for (unordered_set<Worker*>::iterator i = locs.begin();
               i != locs.end(); i++) {
            available[*i] += split->size;
          }
          metalock.unlock();
        }
      }

      map<Worker*, size_t>::iterator best = available.begin();
      for (map<Worker*, size_t>::iterator i = available.begin();
           i != available.end(); i++) {
        if (best->second < i->second) {
          best = i;
        }
      }
      // TODO(erik): somehow take arraystores into account

      // register task for best worker
      if (best != available.end()) {
        worker = best->first;
      } else {
        worker = workers.begin()->second;
      }
    } else {
      worker = workers.begin()->second;
    }
    taskdata.worker = worker;

    lock.lock();
    LOG("created task %d\n", static_cast<int>(task_id));
    waiting_tasks_[worker].push_back(task_id);
    modified_workers.insert(worker);

    lock.unlock();
  }

  for (unordered_set<Worker*>::iterator i = modified_workers.begin();
       i != modified_workers.end(); i++) {
    Worker *worker = *i;
    LOG("trying to schedule task after task creation\n");
    TryScheduleTasks(worker);
    LOG("trying to load splits after task creation\n");
    TryLoadSplits(worker);
    if (!waiting_splits_[worker].empty()) {
      LOG("flushing a split after task creation"
          "because some are waiting to be loaded\n");
      FlushUnlockedSplit(worker);
    }
  }

  setup_time = current_op_timer.stop()/1e6;
  current_op_timer.start();

#ifdef OOC_LOGGING
  for (int i = 0; i < skipped.size(); i++) {
    TaskArg *t = tasks[i];
    for (int j = 0; j < t->args.size(); j++) {
      const Arg &arg = t->args[j];
      for (int k = 0; k < arg.arrays_size(); k++) {
        Split *split = splits[arg.arrays(k).name()];
        total_size_after_skip -= split->size;
        if (contains_key(used_splits, split)) {
          used_splits.erase(split);
          unique_size_after_skip -= split->size;
        }
      }
    }
  }

  OOC_LOG("Total size: %zuMB\n"
          "Unique size: %zuMB\n",
          total_size>>20, unique_size>>20);
  if (total_size != total_size_after_skip) {
    OOC_LOG("Total size after skipping: %zuMB\n"
            "Unique size after skipping: %zuMB\n",
            total_size_after_skip>>20,
            unique_size_after_skip>>20);
  }
#endif

#ifdef PROFILING
  fprintf(profiling_output_, "addtask time: %lfs\n", timer.stop()/1e6);
#endif

  // lock.lock();
  // // check if we have enough memory for all the splits
  // if (TaskFits(t, worker)) {  // all splits fit
  //   ScheduleTask(&taskdata, worker, task_id);

  //   if (taskdata.num_dependencies == 0) {
  //     uint64_t new_task_id = Exec(worker, t);
  //     tasks_[new_task_id] = tasks_[task_id];
  //     tasks_.erase(task_id);
  //   }
  //   lock.unlock();
  // } else {  // doesn't fit, execute later
  //   LOG("task %llu doesn't fit, executing later!!\n", task_id);
  //   waiting_tasks_[worker].insert(task_id);
  //   lock.unlock();
  // }
}


// --- HELPERS ---

// ooc

void OOCScheduler::ScheduleTask(TaskData *td,
                                Worker *worker,
                                uint64_t task_id) {
  TIMING_START(scheduletask)

      LOG("task %d being scheduled on %s\n",
          static_cast<int>(task_id),
          server_to_string(worker->server).c_str());

  TaskData &taskdata = *td;
  TaskArg *t = td->task;

  unique_lock<recursive_mutex> lock(mutex_);

  TIMING_START(st4);
  // create split for composite here so we can lock it
  // NOTE(erik): it will be locked with size 0 so locked metric
  // will be inaccurate!! could approximate with sum of split sizes
  for (int32_t i = 0; i < t->args.size(); i++) {
    const Arg &arg = t->args[i];
    if (arg.arrays_size() > 1) {
      string name = CompositeName(arg);

      if (!contains_key(splits, name)) {
        Split *sp = new Split;
        sp->name = name;
        sp->size = 0;
        splits[sp->name] = sp;
      }
    }
  }
  TIMING_END(st4);

  TIMING_START(st5);
  LockSplits(&taskdata);
  lock.unlock();
  TIMING_END(st5);

  // get missing splits
  TIMING_START(st6);
  Response ret;
  for (int32_t i = 0; i < t->args.size(); i++) {
    const Arg &arg = t->args[i];
    if (arg.arrays_size() == 1) {
      TIMING_START(st1);
      const Array &array = arg.arrays(0);

      // we need the metadata lock because we don't want a splits location
      // to change while we are registering the dependencies etc.
      unique_lock<recursive_mutex> metalock(metadata_mutex);

      Split *split = splits[array.name()];
      TIMING_END(st1);
      TIMING_START(st2);
      if (!IsSplitOnWorker(split, worker)) {
        // check if we already registered a dependency
        // on this split for the current task
        pair<Worker*, Split*> key = make_pair(worker, split);
        if (!contains_key(dependencies_, key) ||
            !contains_key(dependencies_[key], task_id)) {
          taskdata.num_dependencies++;
        }

        TIMING_START(issplitbeingacquired);
        uint64_t beingacquired = IsSplitBeingAcquired(split, worker);
        TIMING_END(issplitbeingacquired);
        if (beingacquired == 0) {  // we need to get the split
          TIMING_START(scheduleacquire);
          // uint64_t acquire_task_id = GetSplit(split, worker);
          if (!contains_key(waiting_splits_set_[worker], split)) {
            waiting_splits_[worker].push_back(split);
            waiting_splits_set_[worker].insert(split);
          }
          TIMING_END(scheduleacquire);
        } else {  // the split is on its way
        }
        TIMING_END(st2);
        TIMING_START(st3);
        dependencies_[make_pair(worker, split)].insert(task_id);
        TIMING_END(st3);
      }
      metalock.unlock();
    } else {  // composite
      string name = CompositeName(arg);
      lock.lock();

      if (!contains_key(splits, name) ||
          !contains_key(splits[name]->workers, worker)) {
        // composite is not present
        taskdata.num_dependencies++;

        pair<Worker*, string> key = make_pair(worker, name);
        if (contains_key(cctask_ids_, key)) {
          // already being created on this worker
          // we just need to register the dependency
          dependencies_cc[cctask_ids_[key]].insert(task_id);
        } else {
          // garbage collect previous version
          string splitname = name.substr(0, name.find('.'));
          if (contains_key(current_composite, splitname) &&
              current_composite[splitname] != name) {
            DeleteSplit(splits[current_composite[splitname]]);
          }
          current_composite[splitname] = name;

          // we need to create the composite
          uint64_t cc_id = GetNewTaskID();
          dependencies_cc[cc_id].insert(task_id);
          cctasks_[cc_id] = CCTaskData();
          cctask_ids_[key] = cc_id;
          CCTaskData &cc = cctasks_[cc_id];
          cc.name = name;
          cc.num_dependencies = 0;
          cc.worker = worker;
          cc.inited = false;
          cc.arg = &arg;

#ifdef PROFILING
          fprintf(profiling_output_, "%8.5lf %6zu CC    CREA\n",
                  abs_time()/1e6,
                  cc_id);
          timers_[cc_id] = Timer();
          timers_[cc_id].start();
#endif

          for (int32_t j = 0; j < arg.arrays_size(); j++) {
            unique_lock<recursive_mutex> lock(mutex_);
            // we need the metadata lock because
            // we don't want a splits location
            // to change while we are registering
            // the dependencies etc.
            unique_lock<recursive_mutex> metalock(metadata_mutex);

            Split *split = splits[arg.arrays(j).name()];
            if (!IsSplitOnWorker(split, worker)) {
              cc.num_dependencies++;

              uint64_t beingacquired = IsSplitBeingAcquired(split, worker);
              if (beingacquired == 0) {  // we need to get the split
                // uint64_t acquire_task_id = GetSplit(split, worker);
                if (!contains_key(waiting_splits_set_[worker], split)) {
                  waiting_splits_[worker].push_back(split);
                  waiting_splits_set_[worker].insert(split);
                }
              } else {  // the split is on its way
              }
              dependencies_[make_pair(worker, split)].insert(cc_id);
            }
            metalock.unlock();
          }
          cc.inited = true;

          if (cc.num_dependencies == 0) {  // all fetches done
            // create composite
            unique_lock<recursive_mutex> metalock(metadata_mutex);
            uint64_t cc_id2 = CreateComposite(cc.worker,
                                              cc.name,
                                              *cc.arg);

            // copy over dependencies to new id
            dependencies_cc[cc_id2] = dependencies_cc[cc_id];
            // delete old id and stuff
            dependencies_cc.erase(cc_id);
            cctask_ids_[key] = cc_id2;
            metalock.unlock();

#ifdef PROFILING
            fprintf(profiling_output_, "%8.5lf %6zu CCDEP DONE %8.5lf\n",
                    abs_time()/1e6,
                    cc_id, timers_[cc_id].stop()/1e6);
            timers_.erase(cc_id);

            fprintf(profiling_output_, "%8.5lf %6zu CC   STRT\n",
                    abs_time()/1e6,
                    cc_id2);
            timers_[cc_id2] = Timer();
            timers_[cc_id2].start();
#endif
          }
        }
      }
      lock.unlock();
    }
    // for (int32_t j = 0; j < arg.arrays_size(); j++) {
    //   lock.lock();

    //   // we need the metadata lock because we don't want a splits location
    //   // to change while we are registering the dependencies etc.
    //   unique_lock<recursive_mutex> metalock(metadata_mutex);

    //   Split *split = splits[arg.arrays(j).name()];
    //   if (!IsSplitOnWorker(split, worker)) {
    //     // check if we already registered a dependency
    //     // on this split for the current task
    //     pair<Worker*, Split*> key = make_pair(worker, split);
    //     if (!contains_key(dependencies_, key) ||
    //         !contains_key(dependencies_[key], task_id)) {
    //       taskdata.num_dependencies++;
    //     }

    //     uint64_t beingacquired = IsSplitBeingAcquired(split, worker);
    //     if (beingacquired == 0) {  // we need to get the split
    //       // uint64_t acquire_task_id = GetSplit(split, worker);
    //       if (!contains_key(waiting_splits_set_[worker], split)) {
    //         waiting_splits_[worker].push_back(split);
    //         waiting_splits_set_[worker].insert(split);
    //       }
    //     } else {  // the split is on its way
    //     }
    //     dependencies_[make_pair(worker, split)].insert(task_id);
    //   }
    //   metalock.unlock();
    //   lock.unlock();
    // }
  }

  TIMING_END(st6);
  taskdata.inited = true;

  TIMING_START(st7);
  lock.lock();
  if (taskdata.num_dependencies == 0) {
    // uint64_t new_task_id = Exec(worker, t);
    // tasks_[new_task_id] = tasks_[task_id];
    // tasks_.erase(task_id);
    waiting_tasks_exec_[worker].push_back(task_id);
    TryExecTasks(worker);
  }
  lock.unlock();
  TIMING_END(st7);

  TIMING_END(scheduletask)
      }

void OOCScheduler::TryExecTasks(Worker *worker) {
  TIMING_START(tryexectasks)

      unique_lock<recursive_mutex> lock(metadata_mutex);
  unique_lock<recursive_mutex> lock2(mutex_);
  while (!waiting_tasks_exec_[worker].empty()) {
    uint64_t id = waiting_tasks_exec_[worker].front();
    Worker *worker = tasks_[id].worker;
    // TODO(erik): good multiplier here?
    if (worker->exectasks.size() < 2*worker->executors) {
      uint64_t new_task_id = Exec(worker, tasks_[id].task);
      LOG("execing task %d as %d\n",
          static_cast<int>(id),
          static_cast<int>(new_task_id));
      tasks_[new_task_id] = tasks_[id];
      tasks_.erase(id);
      waiting_tasks_exec_[worker].pop_front();

      if (worker->exectasks.size() <= worker->executors + 1) {
        // we were in iowait until now
        exec_time[worker->exectasks.size()-1] += current_op_timer.stop()/1e6;
        current_op_timer.start();
      }

#ifdef PROFILING
      fprintf(profiling_output_, "%8.5lf %6zu EXEC  STRT\n",
              abs_time()/1e6,
              new_task_id);
      timers_[new_task_id] = Timer();
      timers_[new_task_id].start();
#endif
    } else {
      break;
    }
  }
  lock2.unlock();
  lock.unlock();

  TIMING_END(tryexectasks)
      }

void OOCScheduler::LockSplit(Split *split, Worker *worker) {
  unique_lock<recursive_mutex> lock(metadata_mutex);

  locked_splits_[worker][split]++;
  if (locked_splits_[worker][split] == 1) {
    locked_size_[worker] += split->size;
    locked_on_[split].insert(worker);
    if (contains_key(split->workers, worker)) {
      locked_loaded_size_[worker] += split->size;
    }
  }
}

void OOCScheduler::LockSplits(TaskData *td) {
  TIMING_START(ls1)
      unique_lock<recursive_mutex> lock(metadata_mutex, defer_lock);

  vector<Arg> &args = td->task->args;
  Worker *worker = td->worker;
  size_t prev_locked_size = locked_size_[worker];
  for (int i = 0; i < args.size(); i++) {
    for (int j = 0; j < args[i].arrays_size(); j++) {
      lock.lock();
      Split *split = splits[args[i].arrays(j).name()];
      lock.unlock();
      locked_splits_[worker][split]++;
      if (locked_splits_[worker][split] == 1) {
        locked_size_[worker] += split->size;
        locked_on_[split].insert(worker);
        if (contains_key(split->workers, worker)) {
          locked_loaded_size_[worker] += split->size;
        }
      }
    }
    if (args[i].arrays_size() > 1) {
      lock.lock();
      Split *split = splits[CompositeName(args[i])];
      lock.unlock();
      locked_splits_[worker][split]++;
      if (locked_splits_[worker][split] == 1) {
        locked_size_[worker] += split->size;
        locked_on_[split].insert(worker);
        if (contains_key(split->workers, worker)) {
          locked_loaded_size_[worker] += split->size;
        }
      }
    }
  }

  TIMING_END(ls1)
      TIMING_START(ls2)
      size_t newly_locked_size = locked_size_[worker] - prev_locked_size;
  if (ExpectedMemUsage(worker) >= worker->size) {
    LOG("flushing splits because we locked too much (%zuMB) "
        "(unlocked: %zuMB, locked&present: %zuMB, locked: %zuMB)\n",
        newly_locked_size>>20,
        (worker->used - locked_loaded_size_[worker])>>20,
        locked_loaded_size_[worker]>>20,
        (locked_size_[worker] - locked_loaded_size_[worker])>>20);
    FlushUnlockedSplits(worker,
                        ExpectedMemUsage(worker) - worker->size +
                        100 * (1LL<<20));
  }
  TIMING_END(ls2)
      }

void OOCScheduler::UnLockSplit(Split *split, Worker *worker) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  locked_splits_[worker][split]--;

  if (locked_splits_[worker][split] == 0) {
    locked_splits_[worker].erase(split);
    locked_size_[worker] -= split->size;
    if (contains_key(split->workers, worker)) {
      locked_loaded_size_[worker] -= split->size;
    }

    string &name = split->name;
    int32_t split_id;
    string darray_name;
    int32_t version;
    ParseVersionNumber(name, &version);
    ParseSplitName(name, &darray_name, &split_id);

    locked_on_[split].erase(worker);
    // garbage collect parent if it is version 1
    // i.e. empty darray (NOTE: in some corner cases
    // this is incorrect, e.g. pagerank vector!)
    // TODO(erik): make this correct
    // if (version == 1) {
    //   LOG("deleting initial version %s\n", split->name.c_str());
    //   DeleteSplit(split);
    // }
  }
}

void OOCScheduler::UnLockSplits(TaskData *td) {
  TIMING_START(unlocksplits)

      unique_lock<recursive_mutex> lock(metadata_mutex, defer_lock);

  vector<Arg> &args = td->task->args;
  Worker *worker = td->worker;
  for (int i = 0; i < args.size(); i++) {
    for (int j = 0; j < args[i].arrays_size(); j++) {
      lock.lock();
      Split *split = splits[args[i].arrays(j).name()];
      // lock.unlock();
      // locked_splits_[worker][split]--;

      // if (locked_splits_[worker][split] == 0) {
      //   locked_splits_[worker].erase(split);
      //   locked_size_[worker] -= split->size;
      // }
      UnLockSplit(split, worker);
      lock.unlock();
    }
    if (args[i].arrays_size() > 1) {
      lock.lock();
      Split *split = splits[CompositeName(args[i])];
      UnLockSplit(split, worker);
      lock.unlock();
    }
  }
  TIMING_END(unlocksplits)
      }

bool OOCScheduler::IsLocked(Split *split, Worker *worker) {
  return contains_key(locked_splits_[worker], split);
}

size_t OOCScheduler::ExtraSpaceNeeded(TaskArg *task, Worker *worker) {
  unique_lock<recursive_mutex> lock(metadata_mutex, defer_lock);

  size_t ret = 0;
  vector<Arg> &args = task->args;
  for (int i = 0; i < args.size(); i++) {
    for (int j = 0; j < args[i].arrays_size(); j++) {
      lock.lock();
      Split *split = splits[args[i].arrays(j).name()];
      lock.unlock();

      if (!IsLocked(split, worker)) {
        ret += split->size;
      }
    }
  }

  return ret;
}

bool OOCScheduler::TaskFits(TaskArg *task, Worker *worker) {
  return locked_size_[worker] + ExtraSpaceNeeded(task, worker) <= worker->size;
}

size_t OOCScheduler::ExpectedMemUsage(Worker *worker) {
  return worker->used + locked_size_[worker] - locked_loaded_size_[worker];
  // size_t ret = 0;

  // // splits in dram
  // for (unordered_set<Split*>::iterator i = worker->splits_dram.begin();
  //      i != worker->splits_dram.end(); i++) {  // BIGFOR
  //   ret += (*i)->size;
  // }

  // // locked splits
  // for (unordered_map<Split*, int>::iterator i =
  //          locked_splits_[worker].begin();
  //      i != locked_splits_[worker].end(); i++) {  // BIGFOR
  //   if (!contains_key(worker->splits_dram, i->first)) {
  //     ret += i->first->size;
  //   }
  // }

  // return ret;
}

void OOCScheduler::FlushUnlockedSplits(Worker *worker, size_t flush_size) {
  TIMING_START(flushunlockedsplits)
      LOG("flushing at least %zuMB unlocked splits!\n", flush_size >> 20);
  unique_lock<recursive_mutex> lock(metadata_mutex);
  unordered_set<Split*> splits_to_flush;
  if (flush_size == 0) {
    flush_size = worker->size;
  }
  size_t size = 0;
  TIMING_START(flushunlockedsplits1)
      for (unordered_set<Split*>::iterator i = worker->splits_dram.begin();
           i != worker->splits_dram.end(); i++) {  // BIGFOR!
        // TODO(erik): this doesnt flush composites
        if (!contains_key(locked_splits_[worker], *i) &&
            (*i)->size > 0 && (*i)->name.find('.') == string::npos) {
          splits_to_flush.insert(*i);
          size += (*i)->size;
          if (size >= flush_size) {
            break;
          }
        }
      }
  TIMING_END(flushunlockedsplits1)

      LOG("flushing %zuMB unlocked splits (target was %zuMB)\n",
          size>>20, flush_size>>20);

  TIMING_START(flushunlockedsplits2)
      for (unordered_set<Split*>::iterator i = splits_to_flush.begin();
           i != splits_to_flush.end(); i++) {
        LOG("flushing split %s\n", (*i)->name.c_str());
        ArrayStore *store = worker->arraystores.begin()->second;
        if (!contains_key(store->splits, *i)) {
          uint64_t id = MoveToStore(*i, store);
          // #ifdef PROFILING
          //     fprintf(profiling_output_, "%8.5lf %6zu MOVE  STRT\n",
          //     abs_time()/1e6,
          //     id);
          //     timers_[id] = Timer();
          //     timers_[id].start();
          // #endif

        } else {
          Delete(*i, worker);
        }
      }
  TIMING_END(flushunlockedsplits2)
      lock.unlock();
  TIMING_END(flushunlockedsplits)
      }

void OOCScheduler::FlushUnlockedSplit(Worker *worker) {
  LOG("flushing a single split!\n");
  unique_lock<recursive_mutex> lock(metadata_mutex);
  for (unordered_set<Split*>::iterator i = worker->splits_dram.begin();
       i != worker->splits_dram.end(); i++) {  // BIGFOR (?)
    // TODO(erik): this doesnt flush composites
    if (!contains_key(locked_splits_[worker], *i) &&
        (*i)->size > 0 && (*i)->name.find('.') == string::npos) {
      LOG("flushing split %s\n", (*i)->name.c_str());
      ArrayStore *store = worker->arraystores.begin()->second;
      if (!contains_key(store->splits, *i)) {
        MoveToStore(*i, store);
      } else {
        Delete(*i, worker);
      }
      return;
    }
  }
  lock.unlock();
}

uint64_t OOCScheduler::GetSplit(Split *split, Worker *worker) {
  LOG("getting split %s\n", split->name.c_str());
  unique_lock<recursive_mutex> lock(metadata_mutex);
  uint64_t acquire_task_id = 0;
  if (!split->workers.empty()) {  // we can fetch
    acquire_task_id = Fetch(worker,
                            *split->workers.begin(),
                            split);
  } else {  // we need to load
    // TODO(erik): handle case when it's in
    // another worker's arraystore!!!

    for (unordered_map<string, ArrayStore*>::iterator i =
             worker->arraystores.begin();
         i != worker->arraystores.end(); i++) {
      if (contains_key(i->second->splits, split)) {
        acquire_task_id = Load(split, i->second);

#ifdef PROFILING
        fprintf(profiling_output_, "%8.5lf %6zu LOAD  STRT\n",
                abs_time()/1e6,
                acquire_task_id);
        timers_[acquire_task_id] = Timer();
        timers_[acquire_task_id].start();
#endif
        break;
      }
    }
  }

  if (acquire_task_id == 0) {
    LOG("can't get split %s, nowhere to be found. in flight?\n",
        split->name.c_str());
  }
  lock.unlock();
  return acquire_task_id;
}

void OOCScheduler::TryScheduleTasks(Worker *worker) {
  TIMING_START(tryscheduletasks)
      unique_lock<recursive_mutex> lock(metadata_mutex);
  unique_lock<recursive_mutex> mylock(mutex_);
  list<uint64_t>::iterator i;
  int checked = 0;  // TODO(erik): make this nicer
  for (i = waiting_tasks_[worker].begin();
       i != waiting_tasks_[worker].end(); i++) {  // BIGFOR (??)
    if (TaskFits(tasks_[*i].task, worker)) {
      LOG("waiting task %d fits, scheduling!\n", static_cast<int>(*i));
      ScheduleTask(&tasks_[*i], worker, *i);
      i = waiting_tasks_[worker].erase(i);
      i--;
    } else {
      LOG("waiting task %d still doesn't fit\n", static_cast<int>(*i));
      if (checked++ > TRY_SCHEDULE_TASKS_MAX) {
        break;
      }
      // break;  // break here if we want FIFO task scheduling
    }
  }
  mylock.unlock();
  lock.unlock();

  TIMING_END(tryscheduletasks)
      }

// bool OOCScheduler::TryLoadSplit(Worker *worker) {
//   if (waiting_splits_.find(worker) != waiting_splits_.end()) {
//     list<Split*> &waiting = waiting_splits_[worker];
//     for (list<Split*>::iterator i = waiting.begin();
//          i != waiting.end(); i++) {
//       if (worker->used + (*i)->size <= worker->size) {
//         GetSplit(*i, worker);
//         return true;
//       }
//     }
//   }

//   return false;
// }

bool OOCScheduler::TryLoadSplits(Worker *worker) {
  TIMING_START(tryloadsplits)

      LOG("trying to load splits\n");
  unique_lock<recursive_mutex> lock(metadata_mutex);
  bool ret = false;
  if (contains_key(waiting_splits_,  worker)) {
    size_t load_size = 0;

    list<Split*> &waiting = waiting_splits_[worker];
    for (list<Split*>::iterator i = waiting.begin();
         i != waiting.end(); i++) {  // BIGFOR
      if (worker->used + load_size + (*i)->size <= worker->size) {
        // the following check is necessary because
        // getsplit can fail if the split is currently
        // in a move, in flight between 2 places
        if (GetSplit(*i, worker) != 0) {
          load_size += (*i)->size;
          waiting_splits_set_[worker].erase(*i);
          i = waiting.erase(i);
          i--;
          ret = true;
        }
      } else {  // break here if we want FIFO split loading
        // break;
      }
    }
  }
  lock.unlock();

  TIMING_END(tryloadsplits)

      return ret;
}

void OOCScheduler::Flush(Worker *worker, size_t flush_size) {
  LOG("flushing splits because of base scheduler order!\n");
  FlushUnlockedSplits(worker, flush_size);
}
}
