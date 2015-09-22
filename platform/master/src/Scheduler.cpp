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
#include <boost/thread.hpp>
#include <Rcpp.h>

#include "master.pb.h"
#include "common.h"
#include "Scheduler.h"
#include "TransferServer.h"
#include "DeserializeArray.h"
#include "PrestoException.h"

using namespace std;
using namespace boost;

namespace presto {

extern bool skip_send;

// helper function so we don't have to deal with
// setting up requests in the scheduler code
static Scheduler *sch = NULL;

/** After receiving a EXEC task, it sends the task to a target worker
 * @param wi a worker information that will execute this task
 * @param t Argument about task (TaskArg description in Scheduler.h)
 * @param id id of a task - generally 0
 * @param uid Id of this task
 * @return NULL
 */
static void dispatch_task(WorkerInfo *wi, TaskArg *t,
                          ::uint64_t id, ::uint64_t uid,
                          ::uint64_t parentid) {

  NewExecuteRRequest req;
  req.set_id(id);
  req.set_uid(uid);
  req.set_parenttaskid(parentid);
  for (int i = 0; i < t->func_str.size(); i++)
    req.add_func(t->func_str[i]);  // add function string
  for (int i = 0; i < t->args.size(); i++) {
    if (t->args[i].arrays_size() == 1 || t->args[i].is_list()) {  // add split arg
      NewArg arg;
      arg.set_varname(t->args[i].name());
      //need to add list-splits, and overwrite the name of the array (so logic in executorpool and executor knows how to handle these)
      if(t->args[i].is_list()){
          for(int j = 0; j < t->args[i].arrays_size(); j ++){
            arg.add_list_arraynames(t->args[i].arrays(j).name());
          }
          arg.set_arrayname("list_type...");
      }else{
          arg.set_arrayname(t->args[i].arrays(0).name());
      }
      req.add_args()->CopyFrom(arg);
    } else {  // add composite args
      NewArg arg;
      arg.set_varname(t->args[i].name());
      arg.set_arrayname(Scheduler::CompositeName(t->args[i]));
      req.add_composite_args()->CopyFrom(arg);
    }
  }
  // add arguments other than split and composite args
  for (int i = 0; i < t->raw_args.size(); i++)
    req.add_raw_args()->CopyFrom(t->raw_args[i]);

  wi->NewExecuteR(req);
  LOG_INFO("EXECUTE TaskID %14d - Sent to Worker %s", static_cast<int>(uid), wi->hostname().c_str());
}

/** Send a Fetch task to a target worker.
 * @param to a worker that will receive a split
 * @param from a worker that will send a split
 * @param name a name of a split that will be sent
 * @param size the size of a split that will be sent
 * @param id id of this task (generally 0)
 * @param uid id of this fetch task.
 * @param parentid parent id of this task
 * @return NULL
 */
static void dispatch_fetch(WorkerInfo *to, const ServerInfo &from,
                           const string &name, size_t size, 
                           ::uint64_t id, ::uint64_t uid) {
  FetchRequest req;
  req.set_name(name);
  req.mutable_location()->CopyFrom(from);
  req.set_size(size);
  req.set_id(id);
  req.set_uid(uid);

  to->Fetch(req);
  LOG_DEBUG("FETCH TaskID %16d - Sent to Worker %s", static_cast<int>(uid), to->hostname().c_str());
}

/** Sends a IO task to a target worker
 * @param wi a worker that will execute this task
 * @param array_name a name of splits that will perform this IO task
 * @param store_name a name of external storage related to this IO task
 * @param type a tpye of this task (SAVE or LOAD)
 * @param id ID of this task (generally 0)
 * @param uid the real ID of this task
 * @return NULL
 */
static void dispatch_io(WorkerInfo *wi, const string &array_name,
                        const string &store_name, IORequest::Type type,
                        ::uint64_t id, ::uint64_t uid) {
  IORequest req;
  req.set_array_name(array_name);
  req.set_store_name(store_name);
  req.set_type(type);
  req.set_id(id);
  req.set_uid(uid);
  wi->IO(req);
  LOG_INFO("IO TaskID %19d - Sent to Worker %s", static_cast<int>(uid), wi->hostname().c_str());
}

/** Send a create composite task to a target worker
 * @param wi a worker information that will execute this CreateComposite task
 * @param name a name of Composite array that will be created
 * @param name arg argument that contains information about splits that will build a composite array
 * @param id an ID of this task (Generally 0)
 * @param uid a real ID of this task
 * @return NULL
 */
static void dispatch_createcomposite(WorkerInfo *wi,
                                     const string &name,
                                     const Arg &carg,
                                     const std::vector<Arg>* task_args,
                                     ::uint64_t id,
                                     ::uint64_t uid,
                                     ::uint64_t parentid) {
  CreateCompositeRequest req;
  req.set_name(name);
  for (int j = 0; j < carg.arrays_size(); j++) {
    NewArg arg;
    arg.set_varname(carg.name());
    arg.set_arrayname(carg.arrays(j).name());
    req.add_cargs()->CopyFrom(arg);
    //req.add_arraynames(carg.arrays(j).name());
    req.add_offsets()->CopyFrom(carg.offsets(j));
  }

  req.mutable_dims()->CopyFrom(carg.dim());

  if(task_args !=NULL && DATASTORE == RINSTANCE) {
    for (int i = 0; i < task_args->size(); i++) {
      if ((*task_args)[i].arrays_size() == 1 || (*task_args)[i].is_list()) {
        NewArg arg;
        arg.set_varname((*task_args)[i].name());
        if((*task_args)[i].is_list()){
            for(int j = 0; j < (*task_args)[i].arrays_size(); j ++){
              arg.add_list_arraynames((*task_args)[i].arrays(j).name());
            }
            arg.set_arrayname("list_type...");
        }else{
            arg.set_arrayname((*task_args)[i].arrays(0).name());
        }
        req.add_task_args()->CopyFrom(arg);
      }
    }
  }

  req.set_id(id);
  req.set_uid(uid);
  req.set_parenttaskid(parentid);
  wi->CreateComposite(req);
  LOG_INFO("CREATECOMPOSITE Create TaskID %6d - Sent to Worker %s", static_cast<int>(uid), wi->hostname().c_str());
}

/** Send Start Data-Loader request to Worker
 * @param wi a worker information that should start DataLoader
 * @param split_size approximate split sizes to be created
 * @return NULL
 */
void Scheduler::InitiateDataLoader(WorkerInfo* wi,
                                   ::uint64_t split_size,
                                   string split_prefix) {

  ::int64_t taskid = GetNewTaskID();
  unique_lock<recursive_mutex> lock(metadata_mutex);
  dataloadereq[taskid]=wi;
  lock.unlock();

  VerticaDLRequest req;
  req.set_type(VerticaDLRequest::START);
  req.set_split_size(split_size);
  req.set_split_name(split_prefix);
  req.set_id(000);
  req.set_uid(taskid);
  wi->VerticaLoad(req);

  LOG_INFO("VerticaDL::START TaskID %11d - Sent to Worker %s", taskid, wi->hostname().c_str());
}

/** Fetch Data-Loader results after UDx has completed
 * @param wi a worker information from which Data-Loader results should be fetched
 * @param vnodename UDx result required for validation of data loaded
 * @return NULL
 */
void Scheduler::FetchLoaderStatus(WorkerInfo* wi,
                         std::vector<std::string> vnodenames) {

   ::int64_t taskid = GetNewTaskID();
   unique_lock<recursive_mutex> lock(metadata_mutex);
   fetchresultreq[taskid] = wi;
   lock.unlock();

   VerticaDLRequest req;
   req.set_type(VerticaDLRequest::FETCH);
   req.set_id(000);
   req.set_uid(taskid);

   for(int i = 0; i<vnodenames.size(); i++) {
     req.add_query_result(vnodenames[i]);
   }

   wi->VerticaLoad(req);
   LOG_INFO("VerticaDL::FETCH TaskID %11d - Sent to Worker %s", static_cast<int>(taskid), wi->hostname().c_str());
}

/** Send Stop Dataloader request to Worker
 * @param wi a worker information that should stop Data-Loader
 * @return NULL
 */
void Scheduler::StopDataLoader(WorkerInfo* wi) {

  ::int64_t taskid = GetNewTaskID();
  unique_lock<recursive_mutex> lock(metadata_mutex);
  dataloadereq[taskid]=wi;
  lock.unlock();

  VerticaDLRequest req;
  req.set_type(VerticaDLRequest::STOP);
  req.set_id(000);
  req.set_uid(taskid);
  wi->VerticaLoad(req);

  LOG_INFO("VerticaDL::STOP TaskID %12d - Sent to Worker %s", taskid, wi->hostname().c_str());
}

bool Scheduler::IsExecTask(::uint64_t taskid) {
  bool isExec = false;
  unique_lock<recursive_mutex> lock(metadata_mutex);
  isExec = (exectasks.find(taskid) != exectasks.end());
  lock.unlock();
  return isExec;
}

/* Updates foreach execution status from each TaskDoneRequest and buffers TaskDoneRequests
 * for processing once all tasks are executed in worker.
 */

std::pair<bool, bool> Scheduler::UpdateTaskResult(TaskDoneRequest* req, bool validated) {
  unique_lock<mutex> single_threading_lock(single_threading_mutex);

  ::uint64_t taskid = req->uid();
  void *task;

  foreach_status_->num_tasks--;
  if (req->has_task_result() && req->task_result() != TASK_SUCCEED) {
    foreach_status_->is_error = true;
    TaskErrorMsg(req->task_message().c_str(), server_to_string(req->location()));
  } else if (!validated)
    foreach_status_->is_error = true;

  taskdones_[taskid] = req;
  std::pair<bool, bool> result = std::make_pair((foreach_status_->num_tasks==0), foreach_status_->is_error);

  unique_lock<recursive_mutex> lock(metadata_mutex);
  ExecTask *exectask = exectasks[taskid];
  task = reinterpret_cast<void*>(exectask);
  lock.unlock();

  reinterpret_cast<ExecTask*>(task)->args->sema->post();
  return result;
}

/*Set the error flag associated with this foreach status. (TODO) Not thread safe*/
void Scheduler::SetForeachError(bool value){
  foreach_status_->is_error = value;
}

/** Removes invalid splits created on worker in case of errored foreach.
 */
void Scheduler::PurgeUpdates(TaskDoneRequest* req) {
  // Do nothing if storage layer is RINSTANCE.
  // Will be taken care by fault tolerance mechanism in worker itself.
  if(DATASTORE == WORKER) {
    for(int i=0; i<req->update_names_size(); i++) {
       Split *split = new Split;
       split->name=req->update_names(i);
       Worker *worker = workers[server_to_string(req->location())];
       split->size=req->update_sizes(i);
       Delete(split, worker, true, true);
       delete split;
    }
  }
}

/** This sends status of the foreach to all workers.
  * Workers can start updating thier medatata including clearing old partitions
  */

void Scheduler::UpdateWorkerMetadata(bool success) {
  boost::unordered_map<std::string, Worker*>::iterator wit;
  for(wit=workers.begin(); wit != workers.end(); ++wit) {
    WorkerInfo* wi = wit->second->workerinfo;
    ::int64_t taskid = GetNewTaskID();
    MetadataUpdateRequest req;
    req.set_status(success);
    req.set_id(000);
    req.set_uid(taskid);

    wi->MetadataUpdate(req);
    LOG_INFO("METADATAUPDATE TaskID %7d - Sent to Worker %s", taskid, wi->hostname().c_str());
  }
}

/** This handles task-done. It generally updates worker information after performing a task.
 * It updates split information after a task (if needed)
 * @param req information about a task done
 */
bool Scheduler::Done(TaskDoneRequest* req) {
  bool success = true;
  unique_lock<mutex> single_threading_lock(single_threading_mutex);

  if (sch == NULL)
    sch = this;

  total_scheduler_timer_.start();
  ::uint64_t taskid = req->uid();  // an ID of this task
  void *task;
  TaskType type;
  Worker *worker = NULL;

  unique_lock<recursive_mutex> lock(metadata_mutex);

  if (exectasks.find(taskid) != exectasks.end()) {
    // find a task in the exectask map, and this is an Exec task done message
    // xxfprintf(stderr, "task done with id %d\n", int(taskid));
    LOG_DEBUG("EXECUTE TaskID %14d - Received TASKDONE from Worker", static_cast<int>(taskid));
    ExecTask *exectask = exectasks[taskid];

    worker = exectask->worker;  // get a worker that executes this task
    worker->exectasks.erase(exectask);  // clear worker's exectask queue
    exectasks.erase(taskid);  // remove from a task queue
    // update splits that result from this Task
    /*for (int i = 0; i < req.update_names_size(); i++) {
      AddSplit(req.update_names(i),
               req.update_sizes(i),
               req.update_stores(i),
               server_to_string(req.location()));
      splits[req.update_names(i)]->empty = req.update_empties(i);
    }*/
    // a pointer of this task for further processing in ChildDone
    task = reinterpret_cast<void*>(exectask);
    type = EXEC;
  } else if (fetchtasks.find(taskid) != fetchtasks.end()) {
    // This is a fetch task done message
    LOG_DEBUG("FETCH TaskID %16d - Received TASKDONE from Worker", static_cast<int>(taskid));
    FetchTask *fetchtask = fetchtasks[taskid];
    if (fetchtask == NULL) {
      throw PrestoWarningException
        ("Fetch task is NULL. Restart session using distributedR_shutdown()");
    }
    //(TODO) Need to cleanly handle cases when fetch task did not succeed. Currently just prints error message.
    if (req->has_task_result() && req->task_result() != TASK_SUCCEED) {
      LOG_ERROR("FETCH TaskID %16d - Encountered TASKERROR from Worker.", static_cast<int>(taskid));
      success = false;
    }

    // update a split's worker information by adding newly received hostname
    fetchtask->split->workers.insert(fetchtask->to);
    // a worker that receives a split, and update the worker's information
    worker = fetchtask->to;
    worker->splits_dram.insert(fetchtask->split);
    worker->fetchtasks.erase(fetchtask);
    worker->sendtasks.erase(fetchtask);
    worker->used += fetchtask->split->size;

    fetchtasks.erase(taskid);

    task = reinterpret_cast<void*>(fetchtask);
    type = FETCH;
  } else if (contains_key(cctasks, taskid)) {
//    LOG_DEBUG("CREATECOMPOSITE Done TaskID %6d - Received TASKDONE from Worker", static_cast<int>(taskid));
    // This is a create composite task
    CCTask *cctask = cctasks[taskid];
    worker = cctask->worker;
    LOG_DEBUG("CREATECOMPOSITE Done TaskID %6d - Received TASKDONE name - %s from %s", static_cast<int>(taskid), cctask->name.c_str(), worker->server.name().c_str());
    // create split for composite
    // TODO(erik): use AddSplit !!!
    Split *split;
    if (!contains_key(splits, cctask->name)) {
      // If this split is not already present. create a new split
      // NOTE(erik): this branch should not fire in OOCScheduler,
      // because we already added the split before!
      split = new Split;
      split->name = cctask->name;
      split->size = req->update_sizes_size() > 0 ? req->update_sizes(0) : 0;
      split->empty = req->update_empties_size() > 0 ? req->update_empties(0) : true;
//      splits[split->name] = split;
    } else {
      // This split is already in the split (OOCScheduler)
      split = splits[cctask->name];
      if (split->size != req->update_sizes(0)) {
        for (boost::unordered_set<Worker*>::iterator i = locked_on_[split].begin();
             i != locked_on_[split].end(); i++) {
          locked_size_[*i] -= split->size;
          locked_size_[*i] += req->update_sizes(0);
        }
        split->size = req->update_sizes_size() > 0 ? req->update_sizes(0) : 0;
        split->empty = req->update_empties_size() > 0 ? req->update_empties(0) : true;
      }
    }
    split->workers.insert(worker);  // update worker that has this split
    splits[split->name] = split;
    worker->splits_dram.insert(split);
    worker->used += split->size;

    // remove this task information
    worker->cctasks.erase(cctask);
    cctasks.erase(taskid);

    task = reinterpret_cast<void*>(cctask);
    type = CREATECOMPOSITE;
  } else if (loadtasks.find(taskid) != loadtasks.end()) {
    // This is a LOAD task - OOCScheduler
    LOG_DEBUG("LOAD TaskID %17d - Received TASKDONE from Worker", static_cast<int>(taskid));
    LoadTask *loadtask = loadtasks[taskid];
    Split *split = loadtask->split;
    worker = loadtask->store->worker;

    // delete from source if this was a move
    if (movetasks.find(taskid) != movetasks.end()) {
      ::uint64_t clear_id = Delete(split, loadtask->store, true);

      ClearTask *ct = new ClearTask;
      ct->load = loadtask;
      ct->type = LOAD;
      cleartasks[clear_id] = ct;

      movetasks.erase(taskid);

      type = NONE;
    } else {
      split->workers.insert(worker);
      worker->splits_dram.insert(split);
      worker->used += split->size;

      split_moving_[split]--;
      type = LOAD;
    }

    loadtask->store->loadtasks.erase(loadtask);
    loadtasks.erase(taskid);

    task = reinterpret_cast<void*>(loadtask);
  } else if (savetasks.find(taskid) != savetasks.end()) {
    LOG_DEBUG("SAVE TaskID %17d - Received TASKDONE from Worker", static_cast<int>(taskid));
    // This is a savetask - OOC Scheduler
    SaveTask *savetask = savetasks[taskid];
    Split *split = savetask->split;
    ArrayStore *store = savetask->store;
    worker = store->worker;

    // delete from source if this was a move
    if (movetasks.find(taskid) != movetasks.end()) {
      ::uint64_t clear_id = Delete(split, store->worker, true);

      ClearTask *ct = new ClearTask;
      ct->save = savetask;
      ct->type = SAVE;
      cleartasks[clear_id] = ct;
      movetasks.erase(taskid);

      type = NONE;
    } else {
      split->arraystores.insert(store);
      store->splits.insert(split);
      store->used += split->size;

      split_moving_[split]--;
      type = SAVE;
    }

    savetask->store->savetasks.erase(savetask);
    savetasks.erase(taskid);

    task = reinterpret_cast<void*>(savetask);
  } else if (contains_key(cleartasks, taskid)) {
    LOG_DEBUG("CLEAR TaskID %16d - Received TASKDONE from Worker", static_cast<int>(taskid));
    // This is a task to clear a split from a worker
    // This part is called when a Clear task is called from LOAD or SAVE task.
    // In general, Worker does not send a response from a Clear task
    ClearTask *ct = cleartasks[taskid];
    cleartasks.erase(taskid);

    if (ct->type == LOAD) {
      Split *split = ct->load->split;
      worker = ct->load->store->worker;
      split->workers.insert(worker);
      worker->splits_dram.insert(split);
      worker->used += split->size;

      split_moving_[split]--;
      task = reinterpret_cast<void*>(ct->load);
      type = MOVETODRAM;
    } else {
      Split *split = ct->save->split;
      ArrayStore *store = ct->save->store;
      worker = store->worker;
      split->arraystores.insert(store);
      store->splits.insert(split);
      store->used += split->size;

      split_moving_[split]--;
      task = reinterpret_cast<void*>(ct->save);
      type = MOVETOSTORE;
    }

    delete ct;
  } else if(dataloadereq.find(taskid) != dataloadereq.end()) {
    LOG_DEBUG("VerticaDL::START TaskID %5d - Received TASKDONE from Worker", static_cast<int>(taskid));
    // Data-Loader started
    std::string hostname = req->location().name();
    int loader_port = req->loader_port();
    WorkerInfo* workerinfo = dataloadereq[taskid];

    presto_master_->GetDataLoader()->HandlePortAssignment(dataloadereq[taskid], loader_port);
    type = NONE;
  } else if (fetchresultreq.find(taskid) != fetchresultreq.end()) {
    LOG_DEBUG("VerticaDL::FETCH TaskID %5d - Received TASKDONE from Worker", static_cast<int>(taskid));
    // Data-Loader results fetched
    presto_master_->GetDataLoader()->WorkerLoaderComplete(fetchresultreq[taskid], req->npartitions(),
                                                          req->task_result(), req->task_message());
    type = NONE;
  }

  // check if memory of worker becomes PRESTO_GC_THRESHOLD full.
  // If it is, perform GarbageCollection
  if (type == EXEC || type == FETCH || type == LOAD || type == MOVETODRAM ||
      type == CREATECOMPOSITE) {
    double consumed = (double)worker->used / (double)worker->size; // NOLINT

    if (consumed > PRESTO_GC_THRESHOLD) {
      LOG_WARN("Worker %s memory consumption(%1.6f) exceeded PRESTO_GC_THRESHOLD(%1.3f). Removing old versions of Split.",
            server_to_string(worker->server).c_str(), consumed, PRESTO_GC_THRESHOLD);
      GarbageCollect(worker, GC_DEFAULT_GEN);
    }
  }

  lock.unlock();

  if (type != NONE) {
    // We need to process this TaskDone message further based on Scheduler type
    // double begin = scheduler_time_.stop();
    Timer t;
    t.start();
    ChildDone(taskid, task, type);
    total_child_scheduler_time_ += t.stop() / 1e6;
    // total_child_scheduler_time_ += (scheduler_time_.stop() - begin) / 1e6;
  }

  switch (type) {
  case EXEC:
    reinterpret_cast<ExecTask*>(task)->args->sema->post();
    delete reinterpret_cast<ExecTask*>(task)->args;
    delete reinterpret_cast<ExecTask*>(task);
    break;
  case FETCH:
    delete reinterpret_cast<FetchTask*>(task);
    break;
  case SAVE:
  case MOVETOSTORE:
    delete reinterpret_cast<SaveTask*>(task);
    break;
  case LOAD:
  case MOVETODRAM:
    delete reinterpret_cast<LoadTask*>(task);
    break;
  case CREATECOMPOSITE:
    delete reinterpret_cast<CCTask*>(task);
    break;
  default:
    break;
  }
  if (worker != NULL) {
    worker->mem_used_worker =
        req->mem_used()>0 ?
        req->mem_used()
        : worker->mem_used_worker;
    worker->last_contacted.restart();
  }

  total_scheduler_time_ += total_scheduler_timer_.stop() / 1e6;
  return success;
}

void Scheduler::AddTask(TaskArg *t) {
  vector<TaskArg*> tasks(1);
  tasks[0] = t;
  AddTask(tasks, 0);
}

void Scheduler::GarbageCollect(Worker *worker, int degree) {
  // delete splits which are at least input degree generations old
  boost::unordered_set<Split*> splits_to_delete;

  unique_lock<recursive_mutex> lock(metadata_mutex);
  for (boost::unordered_set<Split*>::iterator i = worker->splits_dram.begin();
       i != worker->splits_dram.end(); i++) {
    int32_t split_id;
    string darray_name;
    int32_t version;
    ParseVersionNumber((*i)->name, &version);
    ParseSplitName((*i)->name, &darray_name, &split_id);
    string grandchild = darray_name + "_" +
        int_to_string(split_id) + "_" +
        int_to_string(version+degree);

    if (splits.find(grandchild) != splits.end()) {
      splits_to_delete.insert(*i);
    }
  }
  for (boost::unordered_set<Split*>::iterator i = splits_to_delete.begin();
       i != splits_to_delete.end(); i++) {
    LOG("garbage collecting %s\n", (*i)->name.c_str());
    LOG_DEBUG("Garbage Collecting %s", (*i)->name.c_str());
    DeleteSplit(*i);
  }
  lock.unlock();
}

::uint64_t Scheduler::GetNewTaskID() {
  unique_lock<mutex> lock(current_task_id_mutex_);
  current_task_id_++;
  if (current_task_id_ == 0)
    current_task_id_ = 1;
  ::uint64_t ret = current_task_id_;
  lock.unlock();
  return ret;
}

/** It updates an split information after Exec task completion
 * @param name a split name that is updated
 * @param size the size of this split
 * @param store the location in the external storage of this split
 * @param w a name of worker that will store this split
 */
void Scheduler::AddSplit(const string &name, size_t size,
                         const string &store, const string &w,
                         bool empties) {
  LOG("adding split %s to %s\n", name.c_str(), w.c_str());
  LOG_DEBUG("Adding Split %s to %s", name.c_str(), w.c_str());
  Split *split = new Split;
  unique_lock<recursive_mutex> lock(metadata_mutex);
  if (workers.count(w) == 0) {
    ostringstream msg;
    msg << "AddSplit: cannot find a worker to add \"" << w << "\"";
    throw PrestoWarningException(msg.str());
  }
  Worker *worker = workers[w];  // a worker pointer that keeps this split

  if (store.empty()) {  // this split is kept in memory
    split->workers.insert(worker);  // update split location
    // empty splits are always considered to be in memory
    // through special case handling
    if (size > 0) {
      // add the split to the worker's info and update size
      worker->splits_dram.insert(split);
      worker->used += size;
    }
  } else {
    LOG("split was created in %s!!!\n", store.c_str());
    ArrayStore *st = worker->arraystores[store];
    split->arraystores.insert(st);
    st->splits.insert(split);
    st->used += size;
  }
  split->name = name;
  split->size = size;

  // If a worker size is over the limit.
  if (worker->used >= worker->size) {
    LOG_INFO("%zu/%zuMB used on Worker %s, flushing!", worker->used>>20, worker->size>>20, server_to_string(worker->server).c_str());
    //LOG_DEBUG("%zu/%zuMB used on Worker %s, flushing!", server_to_string(worker->server).c_str(), worker->used>>20, worker->size>>20);
    Flush(worker, worker->used - worker->size + 100 * (1LL<<20));
  }
  // update the split information
  splits[name] = split;
  splits[name]->empty = empties;

  // garbage collect that is older than GC_DEFAULT_GEN
  int32_t split_id;
  string darray_name;
  int32_t version;
  ParseVersionNumber(name, &version);
  ParseSplitName(name, &darray_name, &split_id);
  string grandparent = darray_name + "_" +
      int_to_string(split_id) + "_" +
      int_to_string(version - GC_DEFAULT_GEN);

  if (splits.find(grandparent) != splits.end()) {
    LOG_DEBUG("AddSplit: garbage collecting %s", grandparent.c_str());
    if(DATASTORE == WORKER) {
      DeleteSplit(splits[grandparent]);
    } else {
      DeleteSplit(splits[grandparent], worker);
    }
  }

  lock.unlock();
}

void Scheduler::DeleteSplit(const string& split_name) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  if (splits.find(split_name) != splits.end()) {
    LOG_INFO("DeleteSplit: garbage collecting split %s", split_name.c_str());
    DeleteSplit(splits[split_name]);
  } else {
    LOG_WARN("DeleteSplit: garbage collection failed for split %s", split_name.c_str());
  }
  lock.unlock();
}
/** Execute a function by dispatching a task to a worker
 * @param worker a worker information that will execute a given Exec task
 * @param taskarg Arguments that are needed to perform this task
 * @return an ID of this task
 */
::uint64_t Scheduler::Exec(Worker *worker, TaskArg *taskarg, ::uint64_t parentid) {
  if (worker->workerinfo->IsRunning() == false) {
    forward_exception_to_r(PrestoWarningException("a scheduled node is not running"));
//    throw PrestoWarningException("a scheduled node is not running");
  }
  ::uint64_t id = GetNewTaskID();
  LOG_DEBUG("EXECUTE TaskID %14d - Initializing", static_cast<int>(id));

  ExecTask *exectask = new ExecTask;
  exectask->worker = worker;
  exectask->id = id;
  exectask->args = taskarg;

  for (int32_t i = 0; i < taskarg->args.size(); i++) {
    const Arg &arg = taskarg->args[i];
    for (int32_t j = 0; j < arg.arrays_size(); j++) {
      exectask->splits.insert(splits[arg.arrays(j).name()]);
    }
  }

  unique_lock<recursive_mutex> lock(metadata_mutex);
  exectasks[id] = exectask;
  worker->exectasks.insert(exectask);
  lock.unlock();

  dispatch_task(worker->workerinfo, taskarg, 000, id, parentid);
  return id;
}

::uint64_t Scheduler::CreateComposite(Worker *worker,
                                    const std::string &name,
                                    const Arg &arg,
                                    const std::vector<Arg> *task_args,
                                    ::uint64_t parentid) {
  ::uint64_t id = GetNewTaskID();
  LOG_DEBUG("CREATECOMPOSITE Task %6d Initializing", static_cast<int>(id));

  CCTask *cctask = new CCTask;
  cctask->worker = worker;
  cctask->id = id;
  cctask->name = name;
  // TODO(erik): cctask->splits: fill up or remove if not needed


  unique_lock<recursive_mutex> lock(metadata_mutex);
  cctasks[id] = cctask;
  worker->cctasks.insert(cctask);
  lock.unlock();

  dispatch_createcomposite(worker->workerinfo, name, arg, task_args, 000, id, parentid);

  return id;
}

::uint64_t Scheduler::Fetch(Worker *to, Worker *from, Split *split) {
  WorkerInfo *w = to->workerinfo;
  ServerInfo s = from->server;
  ::uint64_t id = GetNewTaskID();

  LOG_INFO("FETCH TaskID %16d - Will Fetch Split %s from Worker %s to Worker %s",
      static_cast<int>(id), split->name.c_str(),
      server_to_string(from->server).c_str(),
      server_to_string(to->server).c_str());

  FetchTask *fetchtask = new FetchTask;
  fetchtask->split = split;
  fetchtask->from = from;
  fetchtask->to = to;
  fetchtask->id = id;

  unique_lock<recursive_mutex> lock(metadata_mutex);
  fetchtasks[id] = fetchtask;
  to->fetchtasks.insert(fetchtask);
  from->sendtasks.insert(fetchtask);
  lock.unlock();

  dispatch_fetch(w, s, split->name, split->size, 000, id);
  return id;
}

::uint64_t Scheduler::Save(Split *split, ArrayStore *arraystore) {
  ::uint64_t id = GetNewTaskID();

  LOG_DEBUG("save task %d: %s to %s on %s\n",
      static_cast<int>(id), split->name.c_str(),
      arraystore->name.c_str(),
      server_to_string(arraystore->worker->server).c_str());

  Worker *worker = arraystore->worker;
  WorkerInfo *wi = worker->workerinfo;

  SaveTask *savetask = new SaveTask;
  savetask->split = split;
  savetask->store = arraystore;
  savetask->id = id;

  unique_lock<recursive_mutex> lock(metadata_mutex);
  savetasks[id] = savetask;
  worker->savetasks.insert(savetask);

  split_moving_[split]++;
  lock.unlock();

  dispatch_io(wi, split->name, arraystore->name, IORequest::SAVE, 000, id);
  return id;
}

::uint64_t Scheduler::Load(Split *split, ArrayStore *arraystore) {
  ::uint64_t id = GetNewTaskID();

  LOG("load task %d: %s from %s on %s\n",
      static_cast<int>(id), split->name.c_str(),
      arraystore->name.c_str(),
      server_to_string(arraystore->worker->server).c_str());

  Worker *worker = arraystore->worker;
  WorkerInfo *wi = worker->workerinfo;

  LoadTask *loadtask = new LoadTask;
  loadtask->split = split;
  loadtask->store = arraystore;
  loadtask->id = id;

  unique_lock<recursive_mutex> lock(metadata_mutex);
  loadtasks[id] = loadtask;
  worker->loadtasks.insert(loadtask);

  split_moving_[split]++;
  lock.unlock();

  dispatch_io(wi, split->name, arraystore->name, IORequest::LOAD, 000, id);
  return id;
}

::uint64_t Scheduler::MoveToStore(Split *split, ArrayStore *arraystore) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  split->workers.erase(arraystore->worker);
  arraystore->worker->splits_dram.erase(split);
  arraystore->worker->used -= split->size;

  ::uint64_t id = Save(split, arraystore);

  LOG("move task %d: %s to %s on %s\n",
      static_cast<int>(id), split->name.c_str(),
      arraystore->name.c_str(),
      server_to_string(arraystore->worker->server).c_str());

  movetasks.insert(id);
  lock.unlock();

  return id;
}

::uint64_t Scheduler::MoveToDRAM(Split *split, ArrayStore *arraystore) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  split->arraystores.erase(arraystore);
  arraystore->splits.erase(split);
  arraystore->used -= split->size;

  ::uint64_t id = Load(split, arraystore);

  LOG("move task %d: %s from %s on %s\n",
      static_cast<int>(id), split->name.c_str(),
      arraystore->name.c_str(),
      server_to_string(arraystore->worker->server).c_str());

  movetasks.insert(id);
  lock.unlock();

  return id;
}

::uint64_t Scheduler::Delete(Split *split, ArrayStore *store,
                           bool metadata_already_erased) {
  ::uint64_t id = GetNewTaskID();
  LOG("delete from store task %zu: %s from %s on %s\n",
      id,
      split->name.c_str(), store->name.c_str(),
      server_to_string(store->worker->server).c_str());

  unique_lock<recursive_mutex> lock(metadata_mutex);
  if (!metadata_already_erased) {
    split->arraystores.erase(store);
    store->splits.erase(split);
    store->used -= split->size;  // changed! on Dec 6
  }
  lock.unlock();

  ClearRequest req;
  req.set_name(split->name);
  req.set_store(store->name);
  req.set_uid(id);
  req.set_id(000);
  store->worker->workerinfo->Clear(req);

  return id;
}

::uint64_t Scheduler::Delete(Split *split, Worker *worker,
                           bool delete_in_worker,
                           bool metadata_already_erased) {
  LOG("delete from mem task %zu: %s on %s\n",
      id,
      split->name.c_str(),
      server_to_string(worker->server).c_str());

  unique_lock<recursive_mutex> lock(metadata_mutex);
  if (!metadata_already_erased) {
    split->workers.erase(worker);
    worker->splits_dram.erase(split);
    worker->used -= split->size;  // changed! on Dec 6
  }
  lock.unlock();

  ::uint64_t id = 0;
  if(delete_in_worker) {
    id = GetNewTaskID();
    ClearRequest req;
    req.set_name(split->name);
    req.set_uid(id);
    req.set_id(000);
    worker->workerinfo->Clear(req);
  }

  return id;
}

/** check if a split exists on a given worker
 * @param split a split object to check if it were on a given worker
 * @param worker a worker to check the split availability
 * @return boolean to check if an input split is on the given worker
 */
bool Scheduler::IsSplitOnWorker(Split *split, Worker *worker) {
  bool ret;
  unique_lock<recursive_mutex> lock(metadata_mutex);
  ret = (split->size == 0 || contains_key(worker->splits_dram, split));
  lock.unlock();
  return ret;
}

/** check if a split is being fetched.
  * @param split a split object to check
  * @param to check if the input split is fetched to this worker
  * @return  if a split is being fetched, the task id is returned. Otherwise, 0 is returned.
  */
::uint64_t Scheduler::IsSplitBeingFetched(Split *split, Worker *worker) {
  // TODO(erik): speed this up with special map
  ::uint64_t ret = 0;
  unique_lock<recursive_mutex> lock(metadata_mutex);
  for (boost::unordered_map< ::uint64_t, FetchTask*>::iterator i = fetchtasks.begin();
       i != fetchtasks.end(); i++) {
    if (i->second->split == split && i->second->to == worker) {
      ret = i->second->id;
      break;
    }
  }
  lock.unlock();
  return ret;
}

::uint64_t Scheduler::IsSplitBeingLoaded(Split *split, Worker *worker) {
  // TODO(erik): speed this up with special map
  ::uint64_t ret = 0;
  unique_lock<recursive_mutex> lock(metadata_mutex);
  for (boost::unordered_map< ::uint64_t, LoadTask*>::iterator i = loadtasks.begin();
       i != loadtasks.end(); i++) {
    if (i->second->split == split && i->second->store->worker == worker) {
      ret = i->second->id;
      break;
    }
  }
  lock.unlock();
  return ret;
}

::uint64_t Scheduler::IsSplitBeingAcquired(Split *split, Worker *worker) {
  // TODO(erik): this arbitrarily favors fetches over loads
  ::uint64_t id = IsSplitBeingFetched(split, worker);
  if (id != 0) {
    return id;
  }

  id = IsSplitBeingLoaded(split, worker);
  if (id != 0) {
    return id;
  }

  return 0;
}

void Scheduler::DeleteSplit(Split *split, Worker* current_worker) {
  if (split == NULL) {
    LOG_ERROR("DeleteSplit: input split is null");
    throw PrestoWarningException("DeleteSplit: input split is null");
  }
  unique_lock<recursive_mutex> lock(metadata_mutex);
  if (contains_key(split_moving_, split) && split_moving_[split] > 0) {
    LOG("didn't delete split %s because it is in flight\n",
        split->name.c_str());
    return;
  }

  splits.erase(split->name);
  lock.unlock();
  // NOTE(erik): we would need to keep the lock here while
  // we access the split, but the assumption is that since
  // we are garbage collecting this, noone is touching it
  // at this point

  boost::unordered_set<Worker*> workers = split->workers;
  for (boost::unordered_set<Worker*>::iterator i = workers.begin();
       i != workers.end(); i++) {

    if(DATASTORE == WORKER)
      Delete(split, *i);
    else {
      // Delete_in_worker should be false only for the workers which updated the split.
      // In that scenario, clear will be taken care by the fault-tolerance mechanism of the worker
      bool delete_in_worker = true;
      if(current_worker != NULL) {
        delete_in_worker = (*i == current_worker) ? false : true;
      }

      Delete(split, *i, delete_in_worker);
    }

  }

  boost::unordered_set<ArrayStore*> arraystores = split->arraystores;
  for (boost::unordered_set<ArrayStore*>::iterator i = arraystores.begin();
       i != arraystores.end(); i++) {
    Delete(split, *i);
  }

  delete split;
}

void Scheduler::AddWorker(
    WorkerInfo *wi, size_t shared_memory,
    int executors, vector<ArrayStoreData> array_stores) {
  string name = wi->hostname()+":"+int_to_string(wi->port());
  if (workers.count(name) != 0) {
    LOG_WARN("Already registered worker (%s) tries to reconnect", name.c_str());
    return;
  }
  Worker *worker = new Worker;
  worker->server.set_presto_port(wi->port());
  worker->server.set_name(wi->hostname());
  worker->size = shared_memory;
  worker->used = 0;
  worker->executors = executors;
  worker->workerinfo = wi;
  worker->last_contacted.start();

  workers[name] = worker;

  for (int i = 0; i < array_stores.size(); i++) {
    ArrayStore *array_store = new ArrayStore;
    array_store->worker = worker;
    array_store->size = array_stores[i].size();
    array_store->used = 0;
    array_store->name = array_stores[i].name();
    worker->arraystores[array_store->name] = array_store;
  }
}

size_t Scheduler::GetSplitSize(const string& split_name) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  Split *split = splits[split_name];
  lock.unlock();
  return split->size;
}

string Scheduler::GetSplitLocation(const string &split_name) {
  unique_lock<recursive_mutex> lock(metadata_mutex);
  Split *split = splits[split_name];
  WorkerInfo *wi = (*split->workers.begin())->workerinfo;
  lock.unlock();
  return wi->hostname()+":"+int_to_string(wi->port());
}


SEXP Scheduler::GetSplitToMaster(const string &name) {
  // TODO(erik): handle case when split is in a store
  unique_lock<recursive_mutex> lock(metadata_mutex);
  Split *split = splits[name];
  WorkerInfo *wi = (*split->workers.begin())->workerinfo;
  lock.unlock();

  std::string store;
  pair<int, int> port_range = presto_master_->GetMasterPortRange(); 

  TransferServer tw(WORKER, RINSTANCE, port_range.first, port_range.second);
  pair<void*, ::int64_t> ret = tw.transfer_blob(name, wi, hostname_, store);

  SEXP data = Deserialize(ret.first, ret.second);
  return data;
}

/** Generate a composite darray name.
 *  composite naming scheme - darrayname.c_0_versionhash
 *  @param arg Arguments that contain split information to create a composite array
 *  @return a composite array name with the given input
 **/
string Scheduler::CompositeName(const Arg &arg) {
  if (arg.arrays_size() == 0)
    throw PrestoWarningException("Composite Array name create failed");
  vector<string> split_names;
  for (int i = 0; i < arg.arrays_size(); i++) {
    split_names.push_back(arg.arrays(i).name());
  }

  return CompositeName(split_names);
}

string Scheduler::CompositeName(vector<string>& array_names) {
  if (array_names.size() == 0)
    throw PrestoWarningException("Composite Array name create failed");
  string split_name;
  int32_t split_id;
  ParseSplitName(array_names[0], &split_name, &split_id);
  ::uint64_t comp_hash = 0;
  for (int i = 0; i < array_names.size(); ++i) {
    int32_t version;
    ParseVersionNumber(array_names[i], &version);
    comp_hash = (comp_hash << 1) ^ version;
  }

  return split_name + ".c_0_" + int_to_string(comp_hash);
}

void Scheduler::UpdateWorkerMem(string worker, size_t total, size_t used) {
  if (workers.count(worker) == 0) {
    fprintf(stderr,
            "UpdateWorkerMemory: no worker (%s) found\n",
            worker.c_str());
    return;
  }
  Worker* w = workers[worker];
  if (NULL == w) {
    fprintf(stderr,
            "UpdateWorkerMemory: worker (%s) is NULL\n",
            worker.c_str());
    return;
  }
  w->mem_total_worker = total != 0 ? total : w->mem_total_worker;
  w->mem_used_worker = used != 0 ? used : w->mem_used_worker;
  return;
}

void Scheduler::PrintArrayStats(const vector<string> &split_names) {
  vector<pair<size_t, Split*> > s;
  size_t sum = 0;
  for (int i = 0; i < split_names.size(); i++) {
    s.push_back(make_pair(splits[split_names[i]]->size,
                          splits[split_names[i]]));
    sum += s.back().first;
  }


  FILE *dst = stderr;
  fprintf(dst, "Array stats for %s\n", split_names[0].c_str());
  if (s.size() > 1) {
    sort(s.begin(), s.end());

    fprintf(dst, "Biggest splits:\n");
    for (int i = 0; i < min(5, static_cast<int>(s.size())); i++) {
      int j = s.size()-1-i;
      fprintf(dst, "  %s %zuMB\n", s[j].second->name.c_str(), s[j].first>>20);
    }

    fprintf(dst, "Smallest splits:\n");
    for (int i = 0; i < min(5, static_cast<int>(s.size())); i++) {
      fprintf(dst, "  %s %zuMB\n", s[i].second->name.c_str(), s[i].first>>20);
    }

    fprintf(dst, "Total  size: %zuMB\n", sum>>20);
    fprintf(dst, "Avg    size: %zuMB\n", (sum/s.size())>>20);
    fprintf(dst, "Median size: %zuMB\n", s[s.size()/2].first>>20);
  } else {
    fprintf(dst, "  %s %zuMB\n",
            s.back().second->name.c_str(),
            s.back().first>>20);
  }
}

void Scheduler::RefreshWorker(string worker) {
  if (workers.count(worker) == 0) {
    fprintf(stderr,
            "RefreshWorker: no worker (%s) found\n",
            worker.c_str());
    return;
  }
  Worker* w = workers[worker];
  if (NULL == w) {
    fprintf(stderr,
            "RefreshWorker: worker (%s) is NULL\n",
            worker.c_str());
    return;
  }
  w->last_contacted.restart();
}

void Scheduler::WorkerLog(Worker *worker, const std::string &msg) {
  LogRequest req;
  req.set_msg(msg);
  worker->workerinfo->Log(req);
}

Worker* Scheduler::GetMostMemWorker() {
  boost::unordered_map<std::string, Worker*>::iterator wit;
  size_t max_remaining = 0;
  Worker* worker = NULL;
  for (wit = workers.begin(); wit != workers.end(); ++wit) {
    // choose one with the most reamining memory worker to schedule job
    if ((wit->second->size) > (wit->second->used) &&
      ((wit->second->size - wit->second->used) > max_remaining)) {
      worker = wit->second;
      max_remaining = wit->second->size - wit->second->used;
    }
  }
  if (worker == NULL) {
    throw PrestoWarningException("Could not find a worker with enough memory to run the job. Add more nodes or increase memory capacity on worker nodes.");
  }
  return worker;
}

Worker* Scheduler::GetNextAvailableWorker() {
  boost::unordered_map<std::string, Worker*>::iterator wit;
  vector<Worker*> worker_vector;
  for(wit=workers.begin(); wit != workers.end(); ++wit) {
    if (wit->second->size > wit->second->used) {
      worker_vector.push_back(wit->second);
    }
  }
  if (worker_vector.size() == 0) {
    throw PrestoWarningException("GetNextAvailableWorker: no available worker");
  }
  current_Worker = (current_Worker+1)%worker_vector.size();
  return worker_vector[current_Worker];
}

Scheduler::~Scheduler() {
  boost::unordered_map< ::uint64_t, ExecTask*>::iterator exec_it = exectasks.begin();
  for(; exec_it != exectasks.end(); ++exec_it) {
    try {
      exec_it->second->args->sema->post();
      exec_it->second->args->sema->post();
    } catch(...){}
    delete exec_it->second->args;
    delete exec_it->second;
  }

  boost::unordered_map< ::uint64_t, FetchTask*>::iterator fetch_it = fetchtasks.begin();
  for(; fetch_it != fetchtasks.end(); ++fetch_it) {
    delete fetch_it->second;
  }

  boost::unordered_map< ::uint64_t, SaveTask*>::iterator save_it = savetasks.begin();
  for(; save_it != savetasks.end(); ++save_it) {
    delete save_it->second;
  }

  boost::unordered_map< ::uint64_t, LoadTask*>::iterator load_it = loadtasks.begin();
  for(; load_it != loadtasks.end(); ++load_it) {
    delete load_it->second;
  }

  boost::unordered_map< ::uint64_t, CCTask*>::iterator cc_it = cctasks.begin();
  for(; cc_it != cctasks.end(); ++cc_it) {
    delete cc_it->second;
  }

  boost::unordered_map<std::string, Split*>::iterator split_it = splits.begin();
  for(; split_it != splits.end(); ++split_it) {
    split_it->second->workers.clear();
    split_it->second->arraystores.clear();
    delete split_it->second;
  }

  boost::unordered_map<std::string, Worker*>::iterator worker_it = workers.begin();
  for(; worker_it != workers.end(); ++worker_it) {
    worker_it->second->exectasks.clear();
    worker_it->second->cctasks.clear();
    worker_it->second->splits_dram.clear();
    worker_it->second->fetchtasks.clear();
    worker_it->second->sendtasks.clear();
    worker_it->second->savetasks.clear();
    worker_it->second->loadtasks.clear();
    boost::unordered_map<std::string, ArrayStore*>::iterator as_it = worker_it->second->arraystores.begin();
    for(; as_it != worker_it->second->arraystores.end(); ++as_it) {
      delete as_it->second;
    }
    delete worker_it->second;
  }
  exectasks.clear();
  fetchtasks.clear();
  savetasks.clear();
  loadtasks.clear();
  cctasks.clear();
  splits.clear();

  if(foreach_status_ != NULL) {
    delete foreach_status_;
    foreach_status_ = NULL;
  }

  LOG("Total scheduler time: %lfs\n", total_scheduler_time_);
  LOG("Total child scheduler time: %lfs\n", total_child_scheduler_time_);
}

}  // namespace presto
