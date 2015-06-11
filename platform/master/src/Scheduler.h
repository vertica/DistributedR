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

// Scheduler classes that handle scheduling/execution of all Presto
// tasks and I/O.

#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

#include <map>
#include <list>
#include <string>
#include <vector>
#include <set>

#include "shared.pb.h"
#include "worker.pb.h"
#include "master.pb.h"

#include "WorkerInfo.h"
#include "dLogger.h"
#include "PrestoMaster.h"

#include "timer.h"
#include "DeserializeArray.h"

#define DRAM_AS_ARRAYSTORE NULL

// #undef SCHEDULER_LOGGING

#ifdef SCHEDULER_LOGGING
#define LOG(n, a...) fprintf(stderr, "%6.5lf " n , abs_time()/1e6, ## a )
#else
#define LOG(n, a...)
#endif

namespace presto {

class PrestoMaster;
class ArrayStoreData;
struct ExecTask;
struct FetchTask;
struct SaveTask;
struct LoadTask;
struct Worker;
struct Split;
struct ArrayStore;

// the kinds of tasks workers can execute
enum TaskType {
  EXEC = 1,
  FETCH,
  SAVE,
  LOAD,
  MOVETODRAM,
  MOVETOSTORE,
  CREATECOMPOSITE,
  NONE
};

// determines the generation of darray to keep
#define GC_DEFAULT_GEN 1
// determines when to perform garbage collection
// when shm is over 50% full, GC happens
#define PRESTO_GC_THRESHOLD 0.5

/** @struct TaskArg
  * @brief This struct contains information (argument) needed to perform an Exec task.
  * @var TaskArg::func_str
  * A function that will be executed expressed in string
  * @var TaskArg::args
  * Arguments to express split information
  * @var TaskArg::raw_args
  * Arguements that are not related to splits
  * @var TaskArg::sema 
  * a semaphore to notify the completion of this task. Upon completion, this is posted
  * @var TaskArg::t
  * a timer to keep track of execution time of this task
  */
struct TaskArg {
  std::vector<std::string> func_str;
  std::vector<Arg> args;
  std::vector<RawArg> raw_args;
  boost::interprocess::interprocess_semaphore *sema;
  Timer t;
};

// an exec task that is currently being executed
struct ExecTask {
  Worker* worker;  // worker executing the task
  boost::unordered_set<Split*> splits;  // all splits used by the task

  TaskArg *args;  // task arguments
  uint64_t id;
};

// a create composite task that is currently being executed
struct CCTask {
  Worker* worker;  // worker executing the task
  // boost::unordered_set<Split*> splits;  // all splits used by the task
  std::string name;  // name of the composite

  uint64_t id;
};

// a save task (copying a split to an arraystore)
// that is currently being executed
struct SaveTask {
  Split *split;  // the split being saved
  ArrayStore *store;  // the array store it is being saved to

  uint64_t id;
};

// a load task (copying a split from an arraystore)
// that is currently being executed
struct LoadTask {
  Split *split;  // the split being loaded
  ArrayStore *store;  // the array store it is loaded from

  uint64_t id;
};

// a fetch task (copying a split from an other worker)
// that is currently being executed
struct FetchTask {
  Split *split;  // the split being fetched
  Worker *from, *to;  // the participating workers
  uint64_t id;  // task id of this fetch task
};

// clear split from arraystore or worker memory
// might need to register the cleared split at some other place
// after the clear is done, struct contains this info
struct ClearTask {
  SaveTask *save;
  LoadTask *load;
  int type;

  uint64_t id;
};

// a split
struct Split {
  boost::unordered_set<Worker*> workers;  // workers that have the split in their DRAM
  boost::unordered_set<ArrayStore*> arraystores;  // array stores that have the split
  /* boost::unordered_set<ExecTask*> exectasks; */
  /* boost::unordered_set<FetchTask*> fetchtasks; */
  /* boost::unordered_set<SaveTask*> savetasks; */
  /* boost::unordered_set<LoadTask*> loadtasks; */

  std::string name;
  size_t size;
  bool empty;
};

// an array store (for storing arrays on disk)
struct ArrayStore {
  Worker *worker;  // worker where the array store is
  boost::unordered_set<Split*> splits;  // splits stored
  boost::unordered_set<SaveTask*> savetasks;  // save tasks being served
  boost::unordered_set<LoadTask*> loadtasks;  // load tasks being served

  size_t size;  // total size
  size_t used;  // currently used
  string name;
};

// a worker
struct Worker {
  boost::unordered_map<std::string, ArrayStore*> arraystores;  // array stores
  boost::unordered_set<ExecTask*> exectasks;  // exec tasks being executed
  boost::unordered_set<CCTask*> cctasks;  // create composite tasks being executed
  boost::unordered_set<Split*> splits_dram;  // splits in DRAM
  boost::unordered_set<FetchTask*> fetchtasks;  // fetch tasks being executed
  boost::unordered_set<FetchTask*> sendtasks;  // fetch responses being executed
  boost::unordered_set<SaveTask*> savetasks;  // save tasks being executed
  boost::unordered_set<LoadTask*> loadtasks;  // load tasks being executed

  ServerInfo server;
  // DRAM size (not actual, but according to worker config)
  // allocated for Presto instance
  size_t size;
  // amount of DRAM currently used for storing splits by presto instance
  size_t used;
  // total (real) memory size in the worker. Initialized at the HelloReply.
  size_t mem_total_worker;
  // actual used memory size in a worker.
  // Initialized at HelloReply and TaskDoneRequest.
  size_t mem_used_worker;
  int executors;  // number of executors on worker
  WorkerInfo *workerinfo;
  Timer last_contacted;
};

struct ForeachStatus {
  int num_tasks;
  bool is_error;
  std::ostringstream error_stream;
};

// Abstract Scheduler class that keeps track of metadata
// and provides functions for dispatching tasks.
// Child classes only need to implement the scheduling
// decision making and then call the functions here to do stuff.
class Scheduler {
 public:
  explicit Scheduler(const std::string &hostname, PrestoMaster* presto_master)
      : current_task_id_(1),
      hostname_(hostname),
      presto_master_(presto_master) {
        scheduler_time_.start();
        taskdones_.clear();
        foreach_status_ = NULL;

        total_scheduler_time_ = 0;
        total_child_scheduler_time_ = 0;
  }

  virtual ~Scheduler();

  // --- scheduling logic ---

  // register new tasks
  void AddTask(TaskArg *t);
  virtual void AddTask(const std::vector<TaskArg*> &tasks,
		       const int scheduler_policy,
                       const std::vector<int> *inputs = NULL) = 0;

  // handle a task done notification (task and type describe the task
  // that was completed): child version (scheduling decisions)!
  virtual void ChildDone(uint64_t taskid, void *task, TaskType type) = 0;

  // shared memory is full, force a flush on worker
  virtual void Flush(Worker *worker, size_t size) {}

  // handle a task done notification: parent version! (bookkeeping)
  bool Done(TaskDoneRequest* req);


  // --- bookkeeping ---

  size_t GetSplitSize(const string& split);

  // get location where a split is stored
  string GetSplitLocation(const string &split_name);

  // create a new split (corresponding data is normally the result of an update)
  void AddSplit(const std::string &name, size_t size,
                const std::string &store, const std::string &w,
                bool empties=false);

  // register a worker
  void AddWorker(WorkerInfo *wi, size_t shared_memory,
                 int executors, std::vector<ArrayStoreData> array_stores);

  // --- misc. functionality ---

  // get split as R object
  SEXP GetSplitToMaster(const std::string &name);

  // create name of a composite from its splits
  static std::string CompositeName(const Arg &arg);
  static std::string CompositeName(vector<string>& array_names);
  void DeleteSplit(const string& split_name);

  boost::unordered_map<std::string, Worker*>& GetWorkerInfo() {
    return workers;
  }

  boost::unordered_map<std::string, Split*>& GetSplitInfo() {
    return splits;
  }

  void UpdateWorkerMem(string worker, size_t total, size_t used);

  void RefreshWorker(string worker);

  // print stats of an array or split
  void PrintArrayStats(const vector<string> &split_names);
  bool IsValidWorker(string name_port_key) {
    return (workers.count(name_port_key) > 0);
  }

  // -- data loader --
  
  void InitiateDataLoader(WorkerInfo* wi, uint64_t split_size, string split_prefix);
  void FetchLoaderStatus(WorkerInfo* wi, std::vector<std::string>vnodes);
  void StopDataLoader(WorkerInfo* wi);

  // -- fault tolerance --

  std::pair<bool, bool> UpdateTaskResult(TaskDoneRequest* req, bool validated);
  void PurgeUpdates(TaskDoneRequest* req);
  bool IsExecTask(uint64_t taskid);
  void SetForeachError(bool value);

  void TaskErrorMsg(const char* error_msg, std::string node = "") {
    foreach_status_->error_stream.str(std::string()); 
    if(node.empty())
      foreach_status_->error_stream << " in Master: " << error_msg 
      <<"\nCheck your code."; 
    else
      foreach_status_->error_stream << " in Worker " << node << ": " << error_msg
      <<"\nCheck your code.";  
  }

  void ResetForeach() {
    boost::unordered_map<uint64_t, TaskDoneRequest*>::iterator i = taskdones_.begin();
    for(; i != taskdones_.end(); ++i) {
      delete i->second;
    }
    taskdones_.clear();
    if(foreach_status_ != NULL) {
      delete foreach_status_;
      foreach_status_ = NULL;
    }
  }

  void InitializeForeach(ForeachStatus* foreach_status) {
    ResetForeach();
    foreach_status_ = new ForeachStatus;
    foreach_status_ = foreach_status;
  }

  // Current implementation is basedon assumption that 
  // atmost 1 foreach() will run at time
  ForeachStatus *foreach_status_;
  boost::unordered_map<uint64_t, TaskDoneRequest*> taskdones_;

 protected:
  // get new unique task id
  uint64_t GetNewTaskID();

  // execute an exec task on a worker
  uint64_t Exec(Worker *worker, TaskArg *task);

  // createa a composite array from splits on a worker
  uint64_t CreateComposite(Worker *worker,
                           const std::string &name,
                           const Arg &arg);

  // execute a fetch task on a worker
  uint64_t Fetch(Worker *to, Worker *from, Split *split);

  // execute save/load/move to/from array store on a worker
  uint64_t Save(Split *split, ArrayStore *arraystore);
  uint64_t Load(Split *split, ArrayStore *arraystore);
  uint64_t MoveToStore(Split *split, ArrayStore *arraystore);
  uint64_t MoveToDRAM(Split *split, ArrayStore *arraystore);

  // delete from array store or DRAM on a worker
  // NOTE: these are assumed to be instantenous,
  // so there is no task id or done event
  uint64_t Delete(Split *split, ArrayStore *store,
                  bool metadata_already_erased = false);
  uint64_t Delete(Split *split, Worker *worker,
                  bool metadata_already_erased = false);

  // send a log msg to the worker
  void WorkerLog(Worker *worker, const std::string &msg);

  // check whether a split is in the DRAM of a worker
  bool IsSplitOnWorker(Split *split, Worker *worker);

  // check whether a split is being fetched by a worker
  uint64_t IsSplitBeingFetched(Split *split, Worker *worker);

  // check whether a split is being loaded by a worker
  uint64_t IsSplitBeingLoaded(Split *split, Worker *worker);

  // check whether a split is being fetched or loaded by a worker
  uint64_t IsSplitBeingAcquired(Split *split, Worker *worker);

  // completely delete a split (from all workers, arraystores, bookkeeping)
  void DeleteSplit(Split *split);

  
  Worker* GetRndAvailableWorker();

  Worker* GetMostMemWorker();

  // map of splitsnames to splits
  boost::unordered_map<std::string, Split*> splits;

  // map of hostname:port to worker
  boost::unordered_map<std::string, Worker*> workers;

  // mutex to protect all scheduler metadata
  boost::recursive_mutex metadata_mutex;

  // ooc stuff
  boost::unordered_map<Split*, boost::unordered_set<Worker*> > locked_on_;
  boost::unordered_map<Worker*, boost::unordered_map<Split*, int> > locked_splits_;
  boost::unordered_map<Worker*, size_t> locked_size_;
  boost::unordered_map<Worker*, size_t> locked_loaded_size_;

  // timer
  Timer scheduler_time_;
  Timer total_scheduler_timer_;
  double total_scheduler_time_;
  double total_child_scheduler_time_;

  boost::mutex single_threading_mutex;

 private:
  // do simple garbage collection on worker
  void GarbageCollect(Worker *worker, int dgree);

  uint64_t current_task_id_;
  boost::mutex current_task_id_mutex_;


  // map of task ids to tasks
  boost::unordered_map<uint64_t, FetchTask*> fetchtasks;
  // a mapping of taskID to ExecTask pointer
  boost::unordered_map<uint64_t, ExecTask*> exectasks;
  boost::unordered_map<uint64_t, CCTask*> cctasks;
  boost::unordered_map<uint64_t, SaveTask*> savetasks;
  boost::unordered_map<uint64_t, LoadTask*> loadtasks;
  // NOTE(erik): only cleartasks coming from a move are put in this
  boost::unordered_map<uint64_t, ClearTask*> cleartasks;

  // set of move task ids (these are the copy task ids,
  // so we need to issue a delete when the copy is done)
  boost::unordered_set<uint64_t> movetasks;

  // is the split currently being moved?
  boost::unordered_map<Split*, int> split_moving_;

  // -- data loader --
  map<uint64_t, WorkerInfo*> dataloadereq;
  map<uint64_t, WorkerInfo*> fetchresultreq;
  std::string hostname_;

  PrestoMaster* presto_master_;
};

class InMemoryScheduler : public Scheduler {
 public:
  explicit InMemoryScheduler(const std::string &hostname, PrestoMaster* presto_master);
  ~InMemoryScheduler();
  virtual void AddTask(const std::vector<TaskArg*> &tasks,
		       const int scheduler_policy = 0,
                       const std::vector<int> *inputs = NULL);
  virtual void ChildDone(uint64_t taskid, void *task, TaskType type);

 private:
  /** @struct TaskData
    * @brief This keeps information about an execution task.
    * @var TaskData::num_dependencies
    * num_dependencies contains a number of other tasks where this task is dependent
    * After all other dependent tasks complete, this task starts execution
    * @var TaskData::worker
    * A worker that is executing this task
    * @var TaskData::inited
    * Indicates if the initialization of this task is complete
    * @var TaskData::task
    * Contains information of arguments to perform this task
    */
  struct TaskData {
    int num_dependencies;
    Worker *worker;
    bool inited;
    bool launched;
    TaskArg *task;
  };

  /** @struct CCTaskData
    * @brief Contains information about CreateComposite tasks
    * @var CCTaskData::name
    * a name of composite array that will be created from this CCtask
    * @var CCTaskData::num_dependencies
    * the number of dependent tasks for this task. Upon completion of all other tasks, this task starts.
    * @var CCTaskData::worker
    * a worker that will execute this task
    * @var CCTaskData::inited
    * A flag to indicate if this task is initiated
    * @var CCTaskData::arg
    * a list of arguments that is needed to perform this task
    */
  struct CCTaskData {
    string name;
    int num_dependencies;
    Worker *worker;
    bool inited;
    const Arg *arg;
  };

  boost::recursive_mutex mutex_;
  // maping of taskID to TaskData for execution
  boost::unordered_map<uint64_t, TaskData> tasks_;
  // mapping of task ID to a list of other taskIDs
  // that is dependent on the key taskID
  // This is generally used when an Exec task initiatres Fetch tasks
  boost::unordered_map<uint64_t, boost::unordered_set<uint64_t> > dependencies_;
  // mapping of taskID to timer
  boost::unordered_map<uint64_t, Timer> timers_;

  /* boost::unordered_map<uint64_t, boost::unordered_set<uint64_t> > cc_dependencies_; */
  // mapping of taskID to create composite task data
  boost::unordered_map<uint64_t, CCTaskData> cctasks_;
  boost::unordered_map<pair<Worker*, std::string>, uint64_t> cctask_ids_;

  // current composite (if any) of a split
  // darrayname with corresponding composite array
  boost::unordered_map<string, string> current_composite;

  Timer timer_;
  FILE *profiling_output_;
  // To keep track of remaining tasks in a foreach (to show progress)
  int current_tasks_;
};

class OOCScheduler : public Scheduler {
 public:
  explicit OOCScheduler(const std::string &hostname, PrestoMaster* presto_master);
  virtual void AddTask(const std::vector<TaskArg*> &tasks,
		       const int scheduler_policy = 0,
                       const std::vector<int> *addtask = NULL);
  virtual void ChildDone(uint64_t taskid, void *task, TaskType type);
  virtual void Flush(Worker *worker, size_t size = 0);

 private:
  struct TaskData {
    int num_dependencies;
    Worker *worker;
    bool inited;
    TaskArg *task;
  };
  struct CCTaskData {
    string name;
    int num_dependencies;
    Worker *worker;
    bool inited;
    const Arg *arg;
  };

  boost::recursive_mutex mutex_;
  boost::unordered_map<uint64_t, TaskData> tasks_;
  boost::unordered_map<pair<Worker*, Split*>, boost::unordered_set<uint64_t> > dependencies_;
  boost::unordered_map<uint64_t, boost::unordered_set<uint64_t> > dependencies_cc;

  // composites
  boost::unordered_map<uint64_t, CCTaskData> cctasks_;
  boost::unordered_map<pair<Worker*, std::string>, uint64_t> cctask_ids_;
  // current composite (if any) of a split
  boost::unordered_map<string, string> current_composite;

  // ooc
  void LockSplit(Split *split, Worker *worker);
  void UnLockSplit(Split *split, Worker *worker);
  void LockSplits(TaskData *td);
  void UnLockSplits(TaskData *td);
  bool IsLocked(Split *split, Worker *worker);
  size_t ExtraSpaceNeeded(TaskArg *task, Worker *worker);
  bool TaskFits(TaskArg *task, Worker *worker);
  void ScheduleTask(TaskData *td, Worker *worker, uint64_t task_id);
  size_t ExpectedMemUsage(Worker *worker);
  void FlushUnlockedSplits(Worker *worker, size_t flush_size = 0);
  void FlushUnlockedSplit(Worker *worker);
  uint64_t GetSplit(Split *split, Worker *worker);
  /* void TryScheduleTask(Worker *worker); */
  bool TryLoadSplits(Worker *worker);
  void TryScheduleTasks(Worker *worker);
  /* bool TryLoadSplit(Worker *worker); */
  void TryExecTasks(Worker *worker);

  //  boost::unordered_map<Worker*, uint64_t> scheduled_tasks_;
  boost::unordered_map<Worker*, list<uint64_t> > waiting_tasks_;
  boost::unordered_map<Worker*, list<Split*> > waiting_splits_;
  boost::unordered_map<Worker*, boost::unordered_set<Split*> > waiting_splits_set_;
  boost::unordered_map<Worker*, list<uint64_t> > waiting_tasks_exec_;
  // what workers the splits are locked on


  boost::unordered_map<uint64_t, Timer> timers_;
  Timer timer_;
  FILE *profiling_output_;
  FILE *ooc_log_;

  // profiling
  double setup_time;
  Timer current_op_timer;
  double *exec_time;
};

}  // namespace presto

#endif
