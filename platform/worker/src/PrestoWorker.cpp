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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdint.h>
#include <netinet/tcp.h>
#include <set>

#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/algorithm/string.hpp>


#include <Rinterface.h>

#include <string>
#include <vector>
#include <dirent.h>
#include <fstream>
#include <algorithm>

#include "atomicio.h"
#include "PrestoWorker.h"
#include "TransferServer.h"
#include "SharedMemory.h"
#include "ArrayData.h"

#include "timer.h"

#include "RequestLogger.h"

using namespace std;
using namespace boost;

namespace presto {
// to enable clean worker shutdown (handliing shared memory)
static PrestoWorker* worker_ptr = NULL;
static MasterClient* cmd_to_master = NULL;
// to guarantee the kill command is performed once
static boost::timed_mutex kill_lock;
// to prevent shutdown called multiple times
static bool shutdown_called = false;
extern int shared_memory_sem;
::uint64_t abs_start_time;

// for benchmarking
static bool skip_io = false;
static int iobusy = 0;

/** Send a WORKERABORT message to the master.
 * Upon receiving, the master will shutdown the session.
 * @param msg a message that will be sent to the master
 * @return NULL
 */
static void SendAbortMsg(const char* msg) {
  if (NULL != cmd_to_master) {
    LOG_DEBUG("Worker %s sending WORKERABORT message to Master : %s", getenv("HOSTNAME"), msg);
    WorkerAbortRequest war;
    war.mutable_location()->CopyFrom(worker_ptr->SelfServerInfo());
    war.set_reason(msg);
    cmd_to_master->WorkerAborting(war);
  } else {
    //fprintf(stderr, "%8.3lf : cmd_to_master is null.. why??\n", abs_time()/1e6);
  }
}

/** SIGINT handler. It send a ABORT message to the master for clean shutdown. 
 * @param sig the signal numberthat we got
 * @return NULL
 */
static void WorkerSigintHandler(int sig) {
  LOG_INFO("SIGINT Handler caught.");
  signal(SIGCHLD, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  try {
    bool ret = kill_lock.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == false) {
      return;
    }
    // if abort message is already sent or shutdown message
    // is from master, ignore!
    if (shutdown_called == true) {
      kill_lock.unlock();
      LOG_DEBUG("SIGINT Handler - Shutdown is already called.");
      return;
    }
    shutdown_called = true;
    kill_lock.unlock();
  } catch(...) {return;}

  LOG_INFO("Sending Abort message to Master session. - User ends Worker session");
  SendAbortMsg("a user ends a worker session");
  // having some time for the packet to be delivered. needs better method!!
  sleep(1);
  if (NULL != worker_ptr) {
    worker_ptr->shutdown();
  }
  kill(0, SIGTERM);
  exit(0);
}

/** SIGCHLD handler. This will be called when one of child process terminates.
 * Send worker abourt message to the master
 * @param sig a signal that we got
 * @return NULL
 */
static void WorkerSigchldHandler(int sig) {
  // disable SIGPIPE signal as it can happen during Presto shutdown
  signal(SIGCHLD, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  int status;
  pid_t pid;
  pid = wait(&status);

  int executor_id = worker_ptr->GetExecutorID(pid);
  string hostname = worker_ptr->GetHostIp();

  bool is_dataloader_running = false;
  DataLoader* dataloaderptr = worker_ptr->GetDataLoaderPtr();
  if (dataloaderptr != NULL) 
    is_dataloader_running = !(dataloaderptr->IsTransferComplete());

  if(!is_dataloader_running)
    LOG_INFO("SIGCHLD signal called from ExecutorID %d (child process:%d) with status %d", executor_id, pid, status);
  else 
    LOG_INFO("SIGCHLD signal called during Vertica Connector execution.");

  /*fprintf(stderr,
          "%8.3lf : sigchld signal called from child process (%d) with status: %d\n",
          abs_time()/1e6, pid, status);*/
  // this is the only case where sigchld is called
  // and child process is running. refer to man(wait)!!
  if (WIFCONTINUED(status) == true) {  
    LOG_INFO("Child process has resumed. Ignore SIGCHLD signal");
    printf("a child process is resumed. ignore it\n");
    return;
  }
  try {
    bool ret = kill_lock.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == false) {
      return;
    }
    // if abort message is already sent or shutdown message
    // is from master, ignore!
    if (shutdown_called == true) {
      LOG_DEBUG("SIGCHLD Handler - Shutdown is already called.");
      kill_lock.unlock();
      return;
    }
    shutdown_called = true;
    kill_lock.unlock();
  } catch(...) {return;}
  
  ostringstream msg;
  if (!is_dataloader_running) {
    msg << hostname << " is exiting as an executor ID (" << executor_id << ") gets killed"
        << endl << "check a log file in " << hostname << " - /tmp/R_executor_*_" << executor_id <<".log";
    msg << check_out_of_memory(worker_ptr->GetExecutorPids());
  } else
    msg << "<DataLoader> "<< hostname << " is exiting as it is out of memory to load data from Vertica.";

  LOG_ERROR(msg.str().c_str());
  SendAbortMsg(msg.str().c_str());
  // having some time for the packet to be delivered.
  // needs better method!!
  sleep(1);
  if (NULL != worker_ptr) {
    worker_ptr->shutdown();
  }
  exit(0);
}

/** This checks if there is a presto-worker process that is already running
 *  *
 *   */
static set<int> check_if_worker_running() {
  string presto_worker_bin ("R-worker-bin");
  DIR* dir;
  struct dirent* ent;
  char* endptr;
  char buf[512];
  pid_t cpid = getpid();  
  set<int> distributedR_ids;
  if (!(dir = opendir("/proc"))) {
    return distributedR_ids;
  }
  while((ent = readdir(dir)) != NULL) {
    long lpid = strtol(ent->d_name, &endptr, 10);
    if (*endptr != '\0') {
      continue;
    }

    /* try to open the cmdline file */
    snprintf(buf, sizeof(buf), "/proc/%ld/cmdline", lpid);
    FILE* fp = fopen(buf, "r");
    if (fp) {
      memset(buf, 0x00, sizeof(buf));
      if (fgets(buf, sizeof(buf), fp) != NULL) {
        /* check the first token in the file, the program name */
        char* first = strtok(buf, " ");
        if (first == NULL) {
          fclose(fp);
          continue;
        }
        string cmd_string(first);
        if (cmd_string.find(presto_worker_bin) != std::string::npos && lpid != cpid) {
          rewind(fp);
          int rSize = fread(buf, 1, sizeof(buf), fp);
          for(int i = 0; i < rSize; ++i) {
            if (buf[i] == '\0') buf[i] = ' ';
          }
          buf[rSize-1] = '\0';
          string cmd_with_args = string(buf);
          std::vector<std::string> elems;
          boost::split(elems, cmd_with_args, boost::is_any_of(" "));
          int node_id = 0;
          for(int i = 0; i < elems.size() - 1; ++i) {
            if (elems[i].compare("-a") == 0) {
              for (int j = 0; j < elems[i+1].size(); ++j) {
                char x = elems[i+1].at(j);
                node_id += int(x);
              }
            }
            if (elems[i].compare("-b") == 0) {
              node_id += atoi(elems[i+1].c_str());
            }
          }
          distributedR_ids.insert(node_id);
        }
      }
      fclose(fp);
    }
  }
  closedir(dir);  
  return distributedR_ids;
}


/* Read splits from remote workers and keep them in the shared memory region
* @param name the name of a split to fetch from a remote worker
* @param location information about a worer from where we will fetch a split
* @param size the size of a split to fetch
* @param id id of a task
* @param uid uid of a task
* @param store a location to keep the split (not memory)
* @return NULL
*/
void PrestoWorker::fetch(string name, ServerInfo location,
                         size_t size, ::uint64_t id,
                         ::uint64_t uid,
                         string store) {
  master_last_contacted_.start();    
  TaskDoneRequest req;
  try {
    req.set_task_result(TASK_SUCCEED);
    req.set_task_message("Succeed");
    SharedMemoryObject shm(
      boost::interprocess::open_or_create, name.c_str(),
      boost::interprocess::read_write);  // create shared memory region to write
    shm.truncate(size);  // allocate region
    boost::interprocess::mapped_region region(shm,
      boost::interprocess::read_write);
    void *addr = region.get_address();  // address where the data will be written

    shmem_arrays_mutex_.lock();
    shmem_arrays_.insert(name);
    shmem_arrays_mutex_.unlock();

    Timer t;
    t.start();

    TransferServer tw(start_port_range_, end_port_range_);
    // fetch data from remote worker and write it to shared memory region
    tw.transfer_blob(addr, name, size, getClient(location),
                     my_location_.name(), store);
    ArrayData *ad = ParseShm(name);  // read the memory segment
    ad->Decompress();  // decompress it if it is compressed
    delete ad;

    t.stop();
    LOG_DEBUG("FETCH TaskID %20zu - Split %10s (size %zu) fetched to Worker %15s.", 
                    uid, name.c_str(), size, server_to_string(location).c_str());

    total_bytes_fetched_ += size;
    total_fetch_time_ += t.read();
    worker_stat_mutex_.lock();
    worker_stats_[location.name().c_str()].bytes_fetched += size;
    worker_stats_[location.name().c_str()].fetch_time += t.read();
    worker_stat_mutex_.unlock();
  } catch(std::exception& ex) {
    req.set_task_result(TASK_EXCEPTION);
    ostringstream msg;
    msg << "Fetch error from " << server_to_string(my_location_) 
      << check_out_of_memory(executorpool_->GetExecutorPids()) << endl << FILE_DESCRIPTOR_ERR_MSG;
    LOG_ERROR("FETCH TaskID %20zu error : %s", uid, msg.str().c_str());
    req.set_task_message(msg.str());
  }
  req.set_id(id);
  req.set_uid(uid);
  req.mutable_location()->CopyFrom(my_location_);
  master_->TaskDone(req);  // send task done message
  LOG_DEBUG("FETCH TaskID %20zu - Sent TASKDONE message to Master", uid);
  
}

/** Handle fetch request from remote workers. This reads splits from shared memory and send them to a requester
 * @param name name of a split that is requested
 * @param location the reuqester information (hostname and port)
 * @param size size of the requested split
 * @param store the location of store (external storage except memory)
 * @return NULL
 */
void PrestoWorker::newtransfer(string name, ServerInfo location,
                               size_t size, string store) {
  // open connection to a requester
  int32_t sockfd = presto::connect(location.name(), location.presto_port());
  if (sockfd < 0) {
    ostringstream msg;
    msg << "connect failed for transfer to " << location.name()
      << ":" << location.presto_port();
    LOG_ERROR("NEWTRANSFER Task               - Connection to Worker %s:%d failed for Split transfer", location.name().c_str(), location.presto_port());
    close(sockfd);
    throw PrestoWarningException(msg.str());
    return;
  }

  Timer t;
  t.start();
  if (store.empty()) {
    size_t bytes_written = 0;
    // addr is NULL means no compression is needed
    // Read from shared memory region using name and get pointer
    SharedMemoryObject shm(
      boost::interprocess::open_or_create, name.c_str(),
      boost::interprocess::read_write);
    boost::interprocess::mapped_region region(
      shm, boost::interprocess::read_write);
    void* addr = region.get_address();

    // TODO(shivaram): Test if this works correctly and add this
    // for compressed arrays as well
    // Write data to the socket
    bytes_written = atomicio(vwrite, sockfd, addr, size);
    int32_t new_transfer_retry = 0;
    while (bytes_written != size &&
           new_transfer_retry < MAX_NEW_TRANSFER_RETRY) {
      new_transfer_retry++;
      LOG_ERROR("NEWTRANSFER Task - Atomicio failed for %s size %ld bytes_written %ld. Retry %d", 
                strerror(errno), size, bytes_written, new_transfer_retry);
      int32_t sockfd = presto::connect(location.name(),
                                       location.presto_port());
      if (sockfd < 0) {
        ostringstream msg;
        msg << "connect failed for transfer to " << location.name()
          << ":" << location.presto_port();
        LOG_ERROR("NEWTRANSFER Task               - Connection failed for Transfer to %s:%d", location.name().c_str(), location.presto_port());
        close(sockfd);
        throw PrestoWarningException(msg.str());
        return;
      }
      bytes_written = atomicio(vwrite, sockfd, addr, size);
    }
  }

  close(sockfd);

  t.stop();
  // update stats at worker
  LOG_DEBUG("NEWTRANSFER Task               - Split %10s(size %zu) transferred to Worker %s", 
                  name.c_str(), size, server_to_string(location).c_str());
  total_bytes_sent_ += size;
  total_send_time_ += t.read();
  worker_stat_mutex_.lock();
  worker_stats_[location.name().c_str()].bytes_sent += size;
  worker_stats_[location.name().c_str()].send_time += t.read();
  worker_stat_mutex_.unlock();
}

/** To save split in to storage (not memory). It is needed in case memory is insufficient
 * @param array_name a name of split to spill
 * @param store_name name of a split in an external storage
 * @param type Type of IO reuqest (SAVE, LOAD)
 * @param id id of the task
 * @param uid uid of a task
 * @return NULL
 */
void PrestoWorker::io(string array_name, string store_name,
                      IORequest::Type type, ::uint64_t id,
                      ::uint64_t uid) {
  ArrayStore *store = array_stores_[store_name];
  if (skip_io)
    goto end;

  if (type == IORequest::SAVE) {
    fprintf(stderr, "%8.5lf: %zu: saving %s\n",
            abs_time()/1e6, uid, array_name.c_str());
    store->Save(array_name);
  } else {
    fprintf(stderr, "%8.5lf: %zu: loading %s\n",
            abs_time()/1e6, uid, array_name.c_str());
    store->Load(array_name);
  }

 end:
  // send TaskDone message
  TaskDoneRequest req;
  req.set_id(id);
  req.set_uid(uid);
  req.mutable_location()->CopyFrom(my_location_);
  master_->TaskDone(req);
}

/** Remove splits from storage (either memory or external storage)
 * @param req contains information about clear request (split name, ...)
 * @return NULL
 */
void PrestoWorker::clear(ClearRequest req) {
  if (skip_io)
    goto end;

  if (req.has_store()) {
#ifndef USE_MMAP_AS_SHMEM
    /*fprintf(stderr, "%8.3lf : %zu: clearing %s from store\n",
            abs_time()/1e6, req.id(), req.name().c_str());*/
    LOG_DEBUG("CLEAR Task                     - Clearing %s from Array Store", req.name().c_str());
    array_stores_[req.store()]->Delete(req.name());
#endif
  } else {
    LOG_DEBUG("CLEAR Task                     - Clearing %s from Shared memory", req.name().c_str());
    SharedMemoryObject::remove(req.name().c_str());
  }

 end:
  // Send task done message
/*  
  TaskDoneRequest done;
  done.set_id(req.id());
  done.set_uid(req.uid());
  done.mutable_location()->CopyFrom(my_location_);
  master_->TaskDone(done);
*/  
  LOG_DEBUG("CLEAR Task                     - Sent TASKDONE message to Master.");
}

/* Create and send Execute Task to Worker to create 
 * Composite list by concatenating all splits of the dlist
 */
WorkerRequest* PrestoWorker::CreateListCcTask(CreateCompositeRequest& req){
  WorkerRequest* wrk_req = new WorkerRequest;
  wrk_req->Clear();

  wrk_req->set_type(WorkerRequest::NEWEXECR);
  NewExecuteRRequest exec_req;
  std::stringstream function_str;
  exec_req.set_id(req.id());
  exec_req.set_uid(req.uid());

  function_str << "composite_list <- c(";
  
  for(int i = 0; i<req.arraynames_size(); i++) {
    if(i == req.arraynames_size()-1)
      function_str << "split_"<< i;
    else
    function_str << "split_"<< i << ",";
  }
  function_str << ")\nupdate(composite_list)";
  exec_req.add_func(function_str.str());

  for (int i = 0; i < req.arraynames_size(); i++) {
    NewArg arg;
    std::stringstream sname_stream;
    sname_stream << "split_" << i;
    arg.set_varname(sname_stream.str());
    arg.set_arrayname(req.arraynames(i));
    exec_req.add_args()->CopyFrom(arg);
  }

  NewArg out_arg;
  out_arg.set_varname("composite_list");
  out_arg.set_arrayname(req.name());
  exec_req.add_args()->CopyFrom(out_arg);

  wrk_req->mutable_newexecr()->CopyFrom(exec_req);

  return wrk_req;
}

/** As composite array creation of DataFrame is more complex than that of dense/sparse array,
 * We create a Exec Task to build a DataFrame composite array.
 * Currently, we use R's internal function (serialize/unserialize/cbind/rbind) to build it
 * @param req Create composite array request 
 * @return using the input request, we build a EXEC task as a WorkerRequest, and it can be queued in exec_tasks_
 */
WorkerRequest* PrestoWorker::CreateDfCcTask(CreateCompositeRequest& req){
  vector<pair<int, int> > offsets;
  pair<int, int> dims = make_pair(req.dims().val(0), req.dims().val(1));
  for (int i = 0; i < req.arraynames_size(); i++) {
    offsets.push_back(make_pair(req.offsets(i).val(0),
                                req.offsets(i).val(1)));
  }

  WorkerRequest* wrk_req = new WorkerRequest;
  wrk_req->Clear();  

  wrk_req->set_type(WorkerRequest::NEWEXECR);
  NewExecuteRRequest exec_req;
  std::stringstream function_str;
  exec_req.set_id(req.id());
  exec_req.set_uid(req.uid());
  std::pair<int, int> block_dim (0,0);

  int num_r_split = offsets.size()>0 ? 1:0; //At least one split is present.
  int num_c_split = num_r_split; 
  int last_rval=0, last_cval=0;

  //Calculate the number of blocks on the row side and column side
  //TODO (iR): Currently assumes that the blocks comform by sizes (i.e., can be stiched together without errors). 
  //E.g. each row of block should have columns of the same sizes.
  for(int i=0;i<offsets.size();++i){
    if(offsets[i].first != last_rval){
      num_r_split++;
      last_rval = offsets[i].first;
    }
    //For columns only the splits in the first row block has to be checked
    if((num_r_split ==1) && (offsets[i].second != last_cval)){
      num_c_split++;
      last_cval = offsets[i].second;
    }
  }

  if (num_r_split>1 && num_c_split>1){
    function_str << "row_combined <- list()\n";
    for (int i = 0; i < num_c_split; ++i) {
      function_str << "row_combined[[" << (i+1) << "]] <- rbind(";
      for (int j = 0; j < num_r_split; ++j) {
        if (j != 0) function_str << ", ";
        function_str << "split_" << (j * num_c_split + i);
      }
      function_str << ")\n";
    }

    function_str << "out_data_frame <- cbind(";
    for (int i = 0; i < num_c_split; ++i) {
      if (i != 0) function_str << ", ";
      function_str << "row_combined[[" << (i+1) << "]]";
    }
    function_str << ")\n";
    function_str << "update(out_data_frame)";
    // 2-D partitioned!! Most complex one
  } 
  else if (num_c_split>1 && num_r_split <=1) {
    function_str << "out_data_frame <- cbind(";
    for (int i = 0; i < num_c_split; ++i) {
      if (i != 0) function_str << ", ";
      function_str << "split_" << i;
    }
    function_str << ")\n";
    function_str << "update(out_data_frame)";
    // Column partitioned we can simple build data frame on it
  } else {
    // Row partitioned or single split - rbind
    function_str << "out_data_frame <- rbind(";
    for (int i = 0; i < num_r_split; ++i) {
      if (i != 0) function_str << ", ";
      function_str << "split_" << i;
    }
    function_str << ")\n";
    function_str << "update(out_data_frame)";
  }
  exec_req.add_func(function_str.str());  // add function string
  // matching between R variable name and Presto darray name
  // Before executor runs this, it will load the shared darray into the R variable
  for (int i = 0; i < req.arraynames_size(); i++) {
    NewArg arg;
    std::stringstream sname_stream;
    sname_stream << "split_" << i;
    arg.set_varname(sname_stream.str());
    arg.set_arrayname(req.arraynames(i));
    exec_req.add_args()->CopyFrom(arg);
  }
  
  NewArg out_arg;
  out_arg.set_varname("out_data_frame");
  out_arg.set_arrayname(req.name());
  exec_req.add_args()->CopyFrom(out_arg);

  wrk_req->mutable_newexecr()->CopyFrom(exec_req);

  return wrk_req;
}

/** create a composite array from a request of a master node. 
 * This funtion allocates a region on shared memory corresponding to the input matrix size
 * @param req create composite array request (Split information that will be embedded into composite array
 * @return NULL
 */
void PrestoWorker::createcomposite(CreateCompositeRequest req) {
  Timer t;
  t.start();
  Composite *composite = NULL;  
  TaskDoneRequest done;  
  ARRAYTYPE arr_type = EMPTY;  
  size_t size = 0;
  try {
    // in case of task failure, the code is overwritten in the try/catch block
    done.set_task_result(TASK_SUCCEED);
    composite = new Composite;
    executorpool_->InsertCompositeArray(req.name(), composite);
    arr_type = GetClassType(req.arraynames(0));
  } catch(std::exception& ex) {
    ostringstream msg;
    msg << "CreateComposite preparation Error: " << ex.what()
        << endl << FILE_DESCRIPTOR_ERR_MSG;
    LOG_ERROR("CREATECOMPOSITE TaskID %10zu - %s", req.id(), msg.str().c_str());
    done.set_task_result(TASK_EXCEPTION);
    done.set_task_message(msg.str());    
    done.set_id(req.id());
    done.set_uid(req.uid());
    done.add_update_sizes(size);
    done.mutable_location()->CopyFrom(my_location_);
    master_->TaskDone(done);
    t.stop();
    total_cc_time_ += t.read();    
    return;
  }
  // if the input is data_frame create a Exec task and use Executors
  // Otherwise, use worker to create a composite
  if (arr_type == DATA_FRAME) {
    // deal with the data frame!!
    vector<pair<std::int64_t, std::int64_t> > offsets;
    for (int i = 0; i<req.arraynames_size(); i++) {
       ArrayData *ad = ParseShm(req.arraynames(i));
       offsets.push_back(make_pair(req.offsets(i).val(0), req.offsets(i).val(1)));
       composite->offsets.push_back(offsets.back());
       composite->dims.push_back(ad->GetDims());
       composite->splitnames.push_back(req.arraynames(i));
       delete ad;
    }    
    composite->dobjecttype = DFRAME;
 
    WorkerRequest* wrk_req = CreateDfCcTask(req);
    unique_lock<mutex> lock(requests_queue_mutex_[EXECUTE]);
    bool notify = requests_queue_[EXECUTE].empty();
    // keep a request at the appropriate queue
    requests_queue_[EXECUTE].push_back(wrk_req);
    if (notify) {
      // Wake up request processing thread
      requests_queue_empty_[EXECUTE].notify_all();
    }

    lock.unlock();
  } else if (arr_type == LIST) {
    //Create Composite Array for lists
    vector<pair<std::int64_t, std::int64_t> > offsets;
    for (int i = 0; i<req.arraynames_size(); i++) {
       ArrayData* ad = ParseShm(req.arraynames(i));
       offsets.push_back(make_pair(req.offsets(i).val(0), req.offsets(i).val(1)));
       composite->offsets.push_back(offsets.back());
       composite->dims.push_back(ad->GetDims());
       composite->splitnames.push_back(req.arraynames(i));
       delete ad;
    }
    composite->dobjecttype = DLIST;

    WorkerRequest* wrk_req = CreateListCcTask(req);
    unique_lock<mutex> lock(requests_queue_mutex_[EXECUTE]);
    bool notify = requests_queue_[EXECUTE].empty();
    requests_queue_[EXECUTE].push_back(wrk_req);
    lock.unlock();

    if (notify)
       requests_queue_empty_[EXECUTE].notify_all();

  } else {
    try {
      vector<ArrayData*> splits;
      vector<pair<std::int64_t, std::int64_t> > offsets;
      pair<std::int64_t, std::int64_t> dims;
      for (int i = 0; i < req.arraynames_size(); i++) {
        splits.push_back(ParseShm(req.arraynames(i)));
        offsets.push_back(make_pair(req.offsets(i).val(0),
                                    req.offsets(i).val(1)));
        composite->offsets.push_back(offsets.back());
        composite->dims.push_back(splits.back()->GetDims());
        composite->splitnames.push_back(req.arraynames(i));
      }
      if (arr_type == DENSE) {
        composite->dobjecttype = DARRAY_DENSE;
      } else if (arr_type == SPARSE || arr_type == SPARSE_TRIPLET){
        composite->dobjecttype = DARRAY_SPARSE;
      }
      dims = make_pair(req.dims().val(0), req.dims().val(1));
      size = CreateComposite(req.name(), offsets, splits, dims, arr_type);
      for (int i = 0; i < splits.size(); i++) {
        delete splits[i];
      }
    } catch (std::exception& ex) {
      ostringstream msg;
      msg << "CreateComposite Error: " << ex.what()
          << endl << FILE_DESCRIPTOR_ERR_MSG;
      LOG_ERROR("CREATECOMPOSITE TaskID %10zu - %s", req.id(), msg.str().c_str());
      done.set_task_result(TASK_EXCEPTION);
      done.set_task_message(msg.str());    
    }
    done.set_id(req.id());
    done.set_uid(req.uid());
    done.add_update_sizes(size);
    done.mutable_location()->CopyFrom(my_location_);
    master_->TaskDone(done);   
    LOG_INFO("CREATECOMPOSITE TaskID %10zu - Sent TASKDONE message to Master", req.uid());
  }
  t.stop();

  total_cc_time_ += t.read();
    // Send task done message
}

/** PrestoWorker constructor
 * @param zmq_ctx a context information of ZMQ library
 * @param shared_memory the quota of shared memory for this worker - This is shared by all executors (unit in Bytes)
 * @param executors a number of executors to deploy
 * @param array_stores mapping of name of splits to ArrayStore (not in memory)
 * @return PrestoWorker object
 */
PrestoWorker::PrestoWorker(
    context_t* zmq_ctx,
    size_t shared_memory,
    int executors,
    const boost::unordered_map<string, ArrayStore*> &array_stores,
    int log_level,
    string master_ip, int master_port,
    int start_port, int end_port)
    : zmq_ctx_(zmq_ctx),
      shm_total_(shared_memory),
      num_executors_(executors),
      array_stores_(array_stores),
      start_port_range_(start_port),
      end_port_range_(end_port),
      running_(true) {
  my_location_.Clear();
  int log_level_=log_level;
  if (array_stores_.empty()) {
    array_stores_["/tmp"] = new FSArrayStore(DEFAULT_SPILL_SIZE,
                                             DEFAULT_SPILL_DIR);
  }
  LOG_INFO("Creating Executors in Worker");
  executorpool_ = new ExecutorPool(num_executors_,
                                   &my_location_,
                                   master_.get(),
                                   &shmem_arrays_mutex_,
                                   &shmem_arrays_,
                                   array_stores_.begin()->first,
                                   log_level_,
                                   master_ip, master_port);
  // Create multiple HandleRequest threads
  for (int i = 0; i < NUM_THREADPOOLS; i++) {
    NUM_THREADS[i] = NUM_THREADS[i] <= 0 ? num_executors_ : NUM_THREADS[i];
    request_threads_[i] = new thread*[NUM_THREADS[i]];
    for (int j = 0; j < NUM_THREADS[i]; j++) {
      request_threads_[i][j] = new thread(bind(&PrestoWorker::HandleRequests,
                                               this, i));
    }
  }

  data_loader_thread_ = NULL;
  data_loader_ = NULL;
  LOG_INFO("Created HandleRequest threads to listen requests from Master");
  // initialize stats related information
  total_bytes_fetched_ = total_bytes_sent_ =
      total_fetch_time_ = total_send_time_ =
      total_exec_time_ = total_cc_time_ = 0;

  mRequestLogger = new RequestLogger("MasterRequestLogger");
}

/** PrestoWorker destructor
 * This deletes all shared memory segments
 */
PrestoWorker::~PrestoWorker() {
  try {
    bool ret = exec_pool_del_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      if (executorpool_!= NULL) {
        delete executorpool_;
        executorpool_ = NULL;
      }
      exec_pool_del_mutex_.unlock();
    }
  } catch(...) {}

  try {
    bool ret = shmem_arrays_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      for (boost::unordered_set<string>::iterator i = shmem_arrays_.begin();
        i != shmem_arrays_.end(); i++) {
        if (i->c_str() != NULL) {
          LOG_INFO("Removing Shared memory object: %s", i->c_str());
          SharedMemoryObject::remove(i->c_str());
        }
      }
      shmem_arrays_.clear();
      shmem_arrays_mutex_.unlock();
    }
  } catch(...) {}
  if (shared_memory_sem > 0) {
    close(shared_memory_sem);
    shared_memory_sem = -1;
  }
  set<int> worker_set = check_if_worker_running();
  if (worker_set.size() == 0) {
    std::remove(get_shm_size_check_name().c_str());
  }
  
  // Remove array stores
  for (boost::unordered_map<string, ArrayStore*>::iterator i =
           array_stores_.begin();
       i != array_stores_.end();
       i++) {
    delete i->second;
  }
  array_stores_.clear();

  try {
    bool ret = client_map_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      boost::unordered_map<std::string, boost::shared_ptr<WorkerInfo> >::iterator itr =
          client_map.begin();
      for (; itr != client_map.end(); ++itr) {
        itr->second->Close();
      }
      client_map.clear();
      client_map_mutex_.unlock();
    }
  } catch(...) {}

  master_->Close();
  cmd_to_master = NULL;
  master_.reset();
  ShutdownProtobufLibrary();

  if (mRequestLogger) {
      delete mRequestLogger;
  }
}

/** A handler to process request from master or other workers. This function is called from Run function as it fills an appropriate task queue
 * @param type a type of the request. This is to differentiate a class of requests (
 * @return NULL
 */
void PrestoWorker::HandleRequests(int type) {
  vector<string> func;  // string expression of function
  vector<Arg> args;  // a class that wraps all arguments (split/raw/composite)
  vector<RawArg> raw_args;  // arguments not related to splits
  vector< ::int64_t> offset_dims;  // offset dimentsions

  vector<NewArg> new_args;  // argument related to shplits
  vector<NewArg> composite_args;  // argument about composite array

  mutex &requests_queue_mutex_ = this->requests_queue_mutex_[type];
  list<WorkerRequest*> &requests_queue_ = this->requests_queue_[type];
  condition_variable &requests_queue_empty_ = this->requests_queue_empty_[type];
  // Response worker_resp;
  try{
    while (true) {
      boost::this_thread::interruption_point();
      unique_lock<mutex> lock(requests_queue_mutex_);
      while (requests_queue_.empty()) {
        // This will be fired
        // when a queue is empty and becomes filled
        requests_queue_empty_.wait(lock);

        if (!running_) {
          lock.unlock();
          return;
        }
      }
      if (!running_) {
        lock.unlock();
        return;
      }
      // read request from a queue
      if (requests_queue_.size() == 0) {
        LOG_WARN("A worker task thread tries to fetch a task from an empty queue");
        lock.unlock();
        continue;
      }
      WorkerRequest &worker_req = *requests_queue_.front();
      requests_queue_.pop_front();
      if (type == IO)
        iobusy++;

      int ib = iobusy;
      int qs = requests_queue_.size();
      lock.unlock();      
      boost::this_thread::interruption_point();
      if(master_ == NULL && worker_req.type() != WorkerRequest::HELLO) {
        LOG_WARN("Worker has not established a connection to Master yet. Only HELLO Handshaking requests are accepted. All other requests are ignored.");
	delete &worker_req;
        continue;
      }
      // process
      LOG_INFO("New Request from Master of type %s received", WorkerRequest::Type_Name(worker_req.type()).c_str());

      if (worker_req.type() == WorkerRequest::IO)
        LOG_INFO("New IO Task received : %s processing (%d busy, %d queue)", worker_req.io().array_name().c_str(), ib, qs);

      Response worker_resp;      
      boost::this_thread::interruption_point();
      switch (worker_req.type()) {
        case WorkerRequest::HELLO:
          {
            LOG_DEBUG("New HELLO Request received from Worker");
            hello(worker_req.hello().master_location(),
                  worker_req.hello().worker_location(),
                  worker_req.hello().is_heartbeat(),
                  worker_req.hello().reply_attr_flag());
          }
          break;
        case WorkerRequest::FETCH:
          {
            LOG_DEBUG("New FETCH TaskID %16zu - Received from Master", worker_req.fetch().uid());
            string store;
            if (worker_req.fetch().has_store()) {
              store = worker_req.fetch().store();
            }
            fetch(worker_req.fetch().name(),
                  worker_req.fetch().location(),
                  worker_req.fetch().size(),
                  worker_req.fetch().id(),
                  worker_req.fetch().uid(),
                  store);
          }
          break;
        case WorkerRequest::NEWTRANSFER:
          {
            LOG_DEBUG("New NEWTRANSFER Task           - Received from Worker %s at Port %d", worker_req.fetch().location().name().c_str(), worker_req.fetch().location().presto_port());
            string store;
            if (worker_req.fetch().has_store()) {
              store = worker_req.fetch().store();
            }
            newtransfer(worker_req.fetch().name(),
                        worker_req.fetch().location(),
                        worker_req.fetch().size(),
                        store);
          }
          break;
        case WorkerRequest::CREATECOMPOSITE:
          {
            LOG_INFO("New CREATECOMPOSITE TaskID %6zu - Received from Master", worker_req.createcomposite().uid());
            createcomposite(worker_req.createcomposite());
          }
          break;
        case WorkerRequest::VERTICALOAD:
          {
            LOG_INFO("New VERICALOAD Task ID %zu - Received from Master", worker_req.verticaload().uid());
            verticaload(worker_req.verticaload());
          }
          break;
        case WorkerRequest::IO:
          {
            io(worker_req.io().array_name(),
               worker_req.io().store_name(),
               worker_req.io().type(),
               worker_req.io().id(),
               worker_req.io().uid());
          }
          break;
        case WorkerRequest::CLEAR:
          {
            LOG_DEBUG("New CLEAR Task                 - Received from Master");
            clear(worker_req.clear());
          }
          break;
        case WorkerRequest::NEWEXECR:
          LOG_INFO("New EXECUTE TaskID %14zu - Received from Master", worker_req.newexecr().uid());
          func.clear();
          new_args.clear();
          raw_args.clear();
          composite_args.clear();

          get_vector_from_repeated_field
              <RepeatedPtrField<string>::const_iterator,
               string>(worker_req.newexecr().func().begin(),
                       worker_req.newexecr().func_size(), &func);

          get_vector_from_repeated_field
              <RepeatedPtrField<NewArg>::const_iterator,
               NewArg>(worker_req.newexecr().args().begin(),
                       worker_req.newexecr().args_size(), &new_args);
          
          get_vector_from_repeated_field
              <RepeatedPtrField<RawArg>::const_iterator,
               RawArg>(worker_req.newexecr().raw_args().begin(),
                       worker_req.newexecr().raw_args_size(), &raw_args);
          get_vector_from_repeated_field<
            RepeatedPtrField<NewArg>::const_iterator, NewArg>(
                worker_req.newexecr().composite_args().begin(),
                worker_req.newexecr().composite_args_size(), &composite_args);


          {
            executorpool_->execute(func, new_args, raw_args, composite_args,
                                   worker_req.newexecr().id(),
                                   worker_req.newexecr().uid(),
                                   &worker_resp);
          }
          break;
        case WorkerRequest::LOG:
          fprintf(stderr, "%s\n", worker_req.log().msg().c_str());
          break;
        default:
          LOG_ERROR("Invalid Request!");
          //fprintf(stderr, "%8.3lf : Invalid request to Run2!\n", abs_time()/1e6);
      }
      if (type == IO) {
        lock.lock();
        iobusy--;
        int ib = iobusy;
        int qs = requests_queue_.size();
        lock.unlock(); 
        LOG_INFO("IO Task - %s proc done (%d busy, %d queue)", worker_req.io().array_name().c_str(), ib, qs);
      }
      delete &worker_req;

      if (!running_)
        return;
    }
  }catch(boost::thread_interrupted const& ) {
    return;
  }
}

/** Run worker process. This function receives tasks from a master or other workers and fill a task to appropriate task queue
 * @param port a port number of worker process
 * @return NULL
 */
void PrestoWorker::Run(string master_addr, int master_port, string worker_addr) {
  int port_num = -1;
  srand(time(NULL) + getpid());
  
  socket_t* sock;
  try { 
    sock = CreateBindedSocket(start_port_range_, end_port_range_, zmq_ctx_, &port_num);
  } catch (...) {
    LOG_ERROR("PrestoWorker Socket bind error. Check port number: %d\n", port_num);
    delete this;
    exit(0);
  }
  
  if (master_addr.size() != 0 && master_port > 0) {
    LOG_INFO("Creating a connection for handshake with master %s:%d", master_addr.c_str(), master_port);
    ServerInfo ms, ws;
    ms.set_name(master_addr);
    ms.set_presto_port(master_port);
    ws.set_name(worker_addr);
    ws.set_presto_port(port_num);
    hello(ms, ws, false, 0xffffffff);
  } else {
    LOG_INFO("Master IP:port info is not given. Wait for a master to initiate the handshake");
  }

  WorkerRequest *worker_req;
  while (true) {
    message_t request;
    try {
      // receive request. The request can be sent from master or other workers
      sock->recv(&request);
    } catch (zmq::error_t err) {
      ostringstream msg;
      msg << "Run error: "<< err.what();
      LOG_ERROR(msg.str());
      continue;
    }

    worker_req = new WorkerRequest;
    worker_req->Clear();
    worker_req->ParseFromArray(request.data(), request.size());

    //notify the request to all the observers
    Notify(*worker_req);

    int type = MISC;
    //LOG_INFO("PrestoWorker::Run getting a message type: %s", WorkerRequest::Type_Name(worker_req.type()).c_str());
    switch (worker_req->type()) {
      case WorkerRequest::NEWEXECR:        
        master_last_contacted_.start();                      
        type = EXECUTE;  // function execution
        break;
      case WorkerRequest::IO:        
        master_last_contacted_.start();                      
        int iob, qs;
        {
          unique_lock<mutex> l(requests_queue_mutex_[IO]);
          iob = iobusy;
          qs = requests_queue_[IO].size();
        }
        LOG_INFO("IO Task - %s received (%d busy, %d queue)", worker_req->io().array_name().c_str(), iob, qs);
        type = IO;  // related IO - SAVE/LOAD from/to external storage
        break;
      case WorkerRequest::FETCH:
        // This worker needs to fetch (or receive) from other workers        
        // fetch is recevied only from a master
        master_last_contacted_.start(); 
        type = RECV;
        break;
      case WorkerRequest::NEWTRANSFER:
        // new_transfer is sent by other worker
        type = SEND;  // This worker has to send to other workers
        break;

      case WorkerRequest::HELLO:        
        master_last_contacted_.start();                      
        type = HELLO;
        break;
      case WorkerRequest::CREATECOMPOSITE:
      case WorkerRequest::CLEAR:
      case WorkerRequest::LOG:        
        master_last_contacted_.start();                      
        type = MISC;
        break;
      case WorkerRequest::VERTICALOAD:
        type = VERTICALOAD;
        break;
      case WorkerRequest::SHUTDOWN:
        if (master_ == NULL) {
          LOG_WARN("A shutdown message is received when a connection to a master is not established. Ignore it");
          break;
        }
        try {
          LOG_INFO("Received Shutdown message from Master. Shutting down.");
          bool ret = kill_lock.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
          if(ret  == false) {
            delete sock;
            return;
          }
          // if abort message is already sent or shutdown message
          // is from master, ignore!
          if (shutdown_called == true) {
            kill_lock.unlock();
            delete sock;
            return;
          }
          shutdown_called = true;
          kill_lock.unlock();
        } catch(...) {
          delete sock;
          return;
        }
        shutdown();  // TODO(erik): is this necessary?
        delete sock;
        return;
    }

    unique_lock<mutex> lock(requests_queue_mutex_[type]);
    bool notify = requests_queue_[type].empty();
    // keep a request at the appropriate queue
    requests_queue_[type].push_back(worker_req);
    if (notify) {
      // Wake up request processing thread
      requests_queue_empty_[type].notify_all();
    }
    lock.unlock();
  }
  delete sock;
}

/** get WorkerInfo from a given host name and port information
 * @param location a hostname and port information which we want to get WorkerInfo
 * @return WorkerInfo* class with the corresponding hostname port information
 */
WorkerInfo* PrestoWorker::getClient(const ServerInfo& location) {
  string key = location.name() + int_to_string(location.presto_port());
  WorkerInfo* wc = NULL;
  client_map_mutex_.lock();
  boost::unordered_map<std::string, boost::shared_ptr<WorkerInfo> >::iterator itr =
      client_map.find(key);
  // if the worker information is not there yet, we create the information.
  if (itr == client_map.end()) {
    boost::shared_ptr<WorkerInfo> wi(new WorkerInfo(location.name(),
                                             location.presto_port(), zmq_ctx_));
    client_map.insert(make_pair(key, wi));
    LOG_INFO("Opened connection to %s:%d", location.name().c_str(), location.presto_port());
    itr = client_map.find(key);
    wc = itr->second.get();
  } else {
    wc  = itr->second.get();
  }
  client_map_mutex_.unlock();
  return wc;
}

/** This function handles hello message from the master. It register the master information, and it sends reply back to the master.
 * @param master_location the hostname and port number of master node
 * @param worker_location the hostname and port number of itself
 * @abs_time the time when the session is started
 * @return NULL
 */
void PrestoWorker::hello(ServerInfo master_location,
                       ServerInfo worker_location,
                         bool is_heartbeat, int reply_flag) {
  // To check if this is the first hello message for initial setup
  if (is_heartbeat == false) {
    if (master_ != NULL) {
      LOG_WARN("Worker has another connection with the Master. Ignoring HELLO Handshaking request.");
      return;
    }
    // master_ pointer is accessed to send message to the master node
    master_.reset(new MasterClient(master_location.name(),
                                   master_location.presto_port(), zmq_ctx_));


    FILE* f2 = fopen("/tmp/r_master_request_tracer", "r");
    if(f2) {
        //register observers
        master_->Subscribe(mRequestLogger);
        fclose(f2);
    }



    LOG_INFO("Worker opened connection to Master at %s:%d", master_location.name().c_str(), master_location.presto_port());
    // enable to send message to master dirctly from executor pool
    executorpool_->master = master_.get();
    cmd_to_master = master_.get();
    // set its own infromation
    my_location_.CopyFrom(worker_location);
    master_location_.CopyFrom(master_location);
    // When a master is registered, start a master monitor
    // to check if a master is alive
    thread thr(boost::bind(&PrestoWorker::MonitorMaster, this));
    thr.detach();
    master_last_contacted_.start();    
  }
  // prepare for a reply
  if (master_ == NULL) {
    LOG_WARN("No Master is registered with the Worker. HELLO message will be ignored.");
    return;
  }
  master_last_contacted_.start();
  HelloReplyRequest helloreply;
  helloreply.set_is_heartbeat(is_heartbeat);
  //std::string hrr_str[1024];
  helloreply.mutable_location()->CopyFrom(worker_location);
  LOG_DEBUG("Sending reply with worker info: %s %d", worker_location.name().c_str(), worker_location.presto_port());
  if (reply_flag & (1 << SHARED_MEM_QUOTA)) {
    helloreply.set_shared_memory(shm_total_);
  }
  if (reply_flag & (1 << NUM_EXECUTOR)) {
    helloreply.set_executors(num_executors_);
  }
  if (reply_flag & (1 << ARRAY_STORES)) {
    for (boost::unordered_map<string, ArrayStore*>::iterator i =
             array_stores_.begin();
         i != array_stores_.end();
         i++) {
      ArrayStoreData array_store;
      array_store.set_size(i->second->GetSize());
      array_store.set_name(i->first);
      array_store.mutable_location()->CopyFrom(worker_location);
      helloreply.add_array_stores()->CopyFrom(array_store);
    }
  }
  if (reply_flag & (1 << SYS_MEM_TOTAL)) {
    helloreply.set_mem_total(get_total_memory());
  }
  if (reply_flag & (1 << SYS_MEM_USED)) {
    helloreply.set_mem_used(get_used_memory());
  }
  master_->HelloReply(helloreply);  // send reply message

  if (is_heartbeat== false) {
    LOG_INFO("HELLO Handshaking reply sent to Master. Master %s:%d registered with Worker", master_location.name().c_str(), master_location.presto_port());
  }
}

/** Prepare to shutdown the worker process
 * Closes connection to other workers and master
 */
void PrestoWorker::shutdown() {
  signal(SIGCHLD, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  LOG_INFO("Worker Shutdown triggered.");
  fprintf(stderr, "%8.3lf : stop\n", abs_time()/1e6);
  running_ = false;
  // print stats
  
  LOG_DEBUG("Total MB fetched: %7.2lf MB\nTotal fetch time: %7.2lf s\nTotal MB   sent: %7.2lf MB\nTotal send time: %7.2lf s\nTotal cc time: %7.2lf s", 
             total_bytes_fetched_/static_cast<double>(1<<20), total_fetch_time_/1e6, total_bytes_sent_/static_cast<double>(1<<20), total_send_time_/1e6, 
             total_cc_time_/1e6);

  /*fprintf(stderr, "total MB fetched: %7.2lf MB\n",
          total_bytes_fetched_/static_cast<double>(1<<20));
  fprintf(stderr, "total fetch time: %7.2lf s\n",
          total_fetch_time_/1e6);
  fprintf(stderr, "total MB   sent: %7.2lf MB\n",
          total_bytes_sent_/static_cast<double>(1<<20));
  fprintf(stderr, "total send time: %7.2lf s\n",
          total_send_time_/1e6);
  fprintf(stderr, "total cc time: %7.2lf s\n",
          total_cc_time_/1e6);*/
  worker_stat_mutex_.lock();
  for (boost::unordered_map<string, worker_stats>::iterator i = worker_stats_.begin();
       i != worker_stats_.end(); i++) {

    LOG_DEBUG("Worker %s:\n    MB fetched: %7.2lfMB\n    fetch time: %7.2lf\n    MB   sent: %7.2lfMB\n    send time: %7.2lf",
              i->first.c_str(), i->second.bytes_fetched/static_cast<double>(1<<20), i->second.fetch_time/1e6, 
              i->second.bytes_sent/static_cast<double>(1<<20), i->second.send_time/1e6);
    /*fprintf(stderr, "worker %s:\n", i->first.c_str());
    fprintf(stderr, "    MB fetched: %7.2lfMB\n",
            i->second.bytes_fetched/static_cast<double>(1<<20));
    fprintf(stderr, "    fetch time: %7.2lf\n",
            i->second.fetch_time/1e6);
    fprintf(stderr, "    MB   sent: %7.2lfMB\n",
            i->second.bytes_sent/static_cast<double>(1<<20));
    fprintf(stderr, "    send time: %7.2lf\n",
            i->second.send_time/1e6);*/
  }
  worker_stat_mutex_.unlock();
  //fprintf(stderr, "%8.3lf : PrestoWorker shutdown - joining threads\n", abs_time()/1e6);
  
  if(data_loader_thread_ != NULL) {
    data_loader_thread_->interrupt();
    //data_loader_thread_->try_join_for(boost::chrono::seconds(5));
    data_loader_thread_ = NULL;
    delete data_loader_thread_;
    LOG_INFO("Killed Data Loader thread");
  }

  if(data_loader_ != NULL) {
    delete data_loader_;
    data_loader_ = NULL;
    LOG_INFO("Killed Data Loader");
  }
    
  LOG_DEBUG("PrestoWorker shutdown - joining threads");
  for (int i = 0; i < NUM_THREADPOOLS; i++) {
    if (request_threads_[i] == NULL) continue;
    for (int j = 0; j < NUM_THREADS[i]; j++) {
      try{
        if (request_threads_[i][j] == NULL) continue;
        if (request_threads_[i][j]->joinable() == false) continue;
        request_threads_[i][j]->interrupt();
        LOG_DEBUG("PrestoWorker shutdown - joining threads for %d:%d", i, j);
        // to avoid deadlock 
        if(request_threads_[i][j]->get_id() == boost::this_thread::get_id()) continue;
        request_threads_[i][j]->try_join_for(boost::chrono::seconds(5));
        delete request_threads_[i][j];
        request_threads_[i][j] = NULL;
      } catch(...) {
        continue;
      }
    }
    delete request_threads_[i];
    request_threads_[i] = NULL;
  }
  LOG_INFO("Worker shutdown - destroying executorpool");
  try {
    bool ret = exec_pool_del_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      if (executorpool_!= NULL) {
        delete executorpool_;
        executorpool_ = NULL;
      }
      exec_pool_del_mutex_.unlock();
    }
  } catch(...) {}
  LOG_INFO("Worker shutdown - Removing shared memory segments");
  try {
    bool ret = shmem_arrays_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      for (boost::unordered_set<string>::iterator i = shmem_arrays_.begin();
        i != shmem_arrays_.end(); i++) {
        if (i->c_str() != NULL) {
          LOG_DEBUG("Removing shared memory object: %s", i->c_str());
          SharedMemoryObject::remove(i->c_str());
        }
      }
      shmem_arrays_.clear();
      shmem_arrays_mutex_.unlock();
    }
  } catch(...) {}
  LOG_INFO("Worker shutdown - Removing sem lock : %d", shared_memory_sem);
  if (shared_memory_sem > 0) {
    close(shared_memory_sem);
    shared_memory_sem = -1;
  }
  set<int> worker_set = check_if_worker_running();
  if (worker_set.size() == 0) {
    std::remove(get_shm_size_check_name().c_str());
  }
  
  // Remove array stores
  for (boost::unordered_map<string, ArrayStore*>::iterator i = array_stores_.begin();
      i != array_stores_.end(); i++) {
    delete i->second;
  }
  array_stores_.clear();
  // NOTE: We also need to close sockets in all other worker threads 
  LOG_INFO("Worker shutdown - Closing connection to other workers");
  try {
    bool ret = client_map_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == true) {
      boost::unordered_map<std::string, boost::shared_ptr<WorkerInfo> >::iterator itr =
          client_map.begin();
      for (; itr != client_map.end(); ++itr) {
        itr->second->Close();
      }
      client_map.clear();
      client_map_mutex_.unlock();
    }
  } catch(...) {}
  master_->Close();
  cmd_to_master = NULL;
  master_.reset();
  LOG_INFO("Worker Shutdown complete.");
}

void PrestoWorker::MonitorMaster() {
  while (true) {
    // sleep for WORKER_HEARTBEAT_PERIOD to check if Master is alive
    // If a master did not send a hello message in WORKER_DEAD_THRESHOLD
    // a worker shutdown
    boost::this_thread::sleep
    (boost::posix_time::seconds(WORKER_HEARTBEAT_PERIOD));
    ::uint64_t elapsed_time = master_last_contacted_.age()/1e6;
    if(elapsed_time > (WORKER_DEAD_THRESHOLD)) {
      LOG_INFO("Master node is detected to be down. Shutdown worker : elapsed time since last heartbeat: %lld", elapsed_time);
      try {
        bool ret = kill_lock.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
        if(ret  == false) {
          return;
        }
        // if abort message is already sent or shutdown message
        // is from master, ignore!
        if (shutdown_called == true) {
          kill_lock.unlock();
          return;
        }
        shutdown_called = true;
        kill_lock.unlock();
      } catch(...) {return;}
      shutdown();
      exit(0);
    }
    //LOG_INFO("Master Alive.");
  }
}

void PrestoWorker::verticaload(VerticaDLRequest verticaload) {
  if(verticaload.type() == VerticaDLRequest::START) {
    CreateLoaderThread(verticaload.split_size(),
                       verticaload.split_name(),
                       verticaload.uid(), 
                       verticaload.id());
  }
  else if (verticaload.type() == VerticaDLRequest::FETCH) {
    std::vector<std::string> qry_res;
    qry_res.clear();  
    get_vector_from_repeated_field
              <RepeatedPtrField<string>::const_iterator,
               string>(verticaload.query_result().begin(),
                       verticaload.query_result_size(), &qry_res);
    std::pair<DLStatus, ::uint64_t> worker_result = data_loader_->SendResult(qry_res);

    LOG_INFO("<DataLoader> Number of partitions generated is %d", worker_result.second);

    TaskDoneRequest reply;
    reply.set_id(verticaload.id());
    reply.set_uid(verticaload.uid());
    reply.set_npartitions(worker_result.second);
    reply.set_task_result(worker_result.first.transfer_success_);
    reply.set_task_message(worker_result.first.transfer_error_msg_);
    reply.mutable_location()->CopyFrom(my_location_);

    master_->TaskDone(reply);

  } else if (verticaload.type() == VerticaDLRequest::STOP) {

    if(data_loader_thread_!=NULL){ 
      data_loader_thread_->interrupt();
      data_loader_thread_ = NULL;
      delete data_loader_thread_; 
      LOG_INFO("<DataLoader> Deleted Data loader thread");
    }

    if(data_loader_ != NULL) {
      delete data_loader_;
      data_loader_ = NULL;
      LOG_INFO("<DataLoader> Killed Data Loader");
    }
  }
}

void PrestoWorker::CreateLoaderThread(::uint64_t split_size, string split_prefix, 
                                      ::uint64_t uid, ::uint64_t id) {
  int port_number = -1;
  int32_t clientfd;
  try {
    clientfd = CreateBindedSocket(start_port_range_, end_port_range_, &port_number);
  } catch (...) {
    LOG_ERROR("<DataLoader> Error creating socket %d", clientfd);
    port_number = -1;
    // leeky: isn't it better to return here if there's an error
  }

  std::pair<int32_t, int32_t> bind_info = std::make_pair(clientfd, port_number);
  data_loader_ = new DataLoader(this, port_number, clientfd, split_size, split_prefix);
  data_loader_thread_ = new thread(bind(&DataLoader::Run, data_loader_));  

  if(port_number == -1)
    LOG_ERROR("<DataLoader> Could not open a port for loading data from Vertica.");
  else   
    LOG_INFO("<DataLoader> Worker started Data Loader. Listening socket at port#:%d for connection from Vertica", port_number);

  TaskDoneRequest reply;
  reply.set_id(id);
  reply.set_uid(uid);
  reply.set_loader_port(port_number);
  reply.mutable_location()->CopyFrom(my_location_);
  master_->TaskDone(reply);
}

/** Parse XML configuration file of worker information. This contains port number, memory size, the number of executors.
 * @param config a path where the configuration file exists
 * @param port where the port value will be written
 * @param shared_memory the size of memory to be allocated in the shared memory region will be written to this argument
 * @param executors the number of executors that will be launched from this worker will be written to this argument
 * @array_stores store pointer
 * @return NULL
 */
static void ParseXMLConfig(const string &config,
                           int32_t &port,
                           unsigned long long &shared_memory,
                           int &executors,
                           boost::unordered_map<string, ArrayStore*> *array_stores) {
  property_tree::ptree pt;
  try {
    read_xml(config, pt, boost::property_tree::xml_parser::no_comments);
  } catch(...) {
      throw PrestoShutdownException("<jorgem>Error parsing XML.\n");
  }
  size_t conf_mem, conf_num_exec;

  property_tree::ptree worker_conf = pt.get_child("WorkerConfig");

  try {
    string worker_start_port = worker_conf.get_child("StartPortRange").data();
    presto::strip_string(worker_start_port);
    port = atoi(worker_start_port.c_str());
  } catch (property_tree::ptree_bad_path &) {
  }

  try {
    string worker_executors = worker_conf.get_child("Executors").data();
    presto::strip_string(worker_executors);
    conf_num_exec = atoi(worker_executors.c_str());
    executors = conf_num_exec > 0 ? conf_num_exec : executors;
  } catch (property_tree::ptree_bad_path &) {
  }

  try {
    string worker_sharedmemory = worker_conf.get_child("SharedMemory").data();
    presto::strip_string(worker_sharedmemory);
    conf_mem =
        atol(worker_sharedmemory.c_str()) *
        (1LL<<20);    
    shared_memory = conf_mem > 0 ? conf_mem : shared_memory;
  } catch (property_tree::ptree_bad_path &) {
  }

  try {
    property_tree::ptree storage_conf = worker_conf.get_child("Storage");
    for (property_tree::ptree::iterator storage_it = storage_conf.begin();
         storage_it != storage_conf.end();
         storage_it++) {
      property_tree::ptree device_conf = storage_it->second;
      string name = device_conf.get_child("Name").data();
      presto::strip_string(name);
      string worker_size =  device_conf.get_child("Size").data();
      presto::strip_string(worker_size);
      size_t size = atol(
          worker_size.c_str()) *
          (1LLU<<20);
#ifndef USE_MMAP_AS_SHMEM
      (*array_stores)[name] = new FSArrayStore(size, name);
#else
      (*array_stores)[name] = new MMapArrayStore(size);
#endif
    }
  } catch (property_tree::ptree_bad_path &) {
  }
}

}  // namespace presto

/** Determine optimal # of executors automatically.
 * It get the number of cores and the number of executors will be num_cores 
 * @return optimal number of executors
 */
static int auto_executors() {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  return num_cores > 0 ? (num_cores) : DEFAULT_EXECUTORS;
}

/** Determine optimal shared mem size automatically by considering the SHM mounted size and system configuration
 * The unit is in BYTE
 * @return optimal shared memory size
 */
static unsigned long long auto_shared_memory() {
  unsigned long long shm_size = presto::get_free_shm_size();
  if(shm_size == 0) {
    // if statvfs fails, read the total memory size
    struct sysinfo mem_info;
    sysinfo(&mem_info);
    shm_size = mem_info.totalram * mem_info.mem_unit;
  }
  // the unit of shmall is in pages
  unsigned long long shmall = 0;
  FILE *f = fopen(SHMALL_SYS_FILE, "r");
  if (f != NULL) {
    if (fscanf(f, "%llu", &shmall) != 1) {
        shmall = 0;
    }    
    fclose(f);
  }
  shmall *= getpagesize();
// By default, do not consider SHMALL configuration value
// as they have to dealt at the level of boost.interprocess library
#ifndef CONSIDER_SHMALL_CONF
  shmall = shm_size;
#endif
  // return the minimum value of shm mounted size and shm system configuration in MB
  unsigned long long optimal_size = DEFAULT_SHARED_MEMORY;
  if(shm_size > 0 && shmall > 0) {
    // /dev/shm size and shmall are valid - choose smaller one
    optimal_size = min(shm_size, shmall);
  } else if (shm_size > 0 && shmall == 0) {
    // only /dev/shm size is valid
    optimal_size = shm_size;
  } else if (shm_size == 0 && shmall > 0) {
    // only shmall size if valid
    optimal_size = shmall;
  }
  return (unsigned long long)(optimal_size * MAX_PRESTO_SHM_FRACTION);
}

/** remove remaining shared memory segments from previous sessions
* ALL PRESTO SHM SEGMENT NAME STARTS WITH presto-shm-uid (refer get_presto_shm_prefix()
* @return NULL
*/
static void clean_garbage_presto_shm(set<int> distributedR_ids) {
  DIR *dp = opendir(SHM_FOLDER);
  struct dirent *ep;
  if (dp != NULL) {
    string shm_prefix = PRESTO_SHM_PREFIX;
    size_t sps = shm_prefix.size();

    string loader_file_prefix = LOADER_SHM_PREFIX;
    size_t lps = loader_file_prefix.size();

    while (ep = readdir(dp)) {  // NOLINT
      if (ep != NULL) {
        string fname = ep->d_name;
        if (fname.compare(0, sps, shm_prefix) == 0 || fname.compare(0, lps, loader_file_prefix) == 0) {
          std::vector<std::string> elems;
          // parse the shared memory segment. The format is
          // R-shm-session ID with master addr/port-dobject name_split id_version
          boost::split(elems, fname, boost::is_any_of("-_"));
          // if the session ID is not an active one, remove it
          if (distributedR_ids.count(atoi(elems[2].c_str())) == 0) {
            boost::interprocess::shared_memory_object::remove(fname.c_str());
          }
        }
      }
    }
    (void)closedir(dp);
  }
}

using namespace presto;
using namespace std;

int main(int argc, char **argv) {
  // check if PrestoWorker is running under a same user.
  // if it is, we terminate
  set<int> distRIds = check_if_worker_running();
  clean_garbage_presto_shm(distRIds);
  presto::has_R_instance = false;

  srand(time(NULL) + getpid());

  int32_t start_port = 50000;
  int32_t end_port = 50100;
  unsigned long long shared_memory = auto_shared_memory();
  int executors = auto_executors();
  int log_level = 3;
  std::string master_addr, worker_addr;
  int master_port= -1;
  boost::unordered_map<string, presto::ArrayStore*> array_stores;

/*
  // Read the configuration file if it exists
  if (argc > 1) {
    try {
      presto::ParseXMLConfig(argv[1], start_port,
                             shared_memory, executors,
                             &array_stores);
    } catch(...) {
    }
  }
*/
  // handling CTRL-C message
  signal(SIGINT, presto::WorkerSigintHandler);
  // handling child process signal
  signal(SIGCHLD, presto::WorkerSigchldHandler);
  int c;
  // if the configuration is delivered from command line option
  // p: port number of a worker - if this is 0, a worker randomly selects one
  // e: number of executors
  // m: the quota of shared memory
  // l: log level (0:error, 1: warning, 2: info, 3: debug)
  // a: address of master node. If this value is given, a worker initiates connection to a master.
  // b: the port of master node
  // w: worker address identifed from user input
  //     Otherwise, a master initiates connection to a worker.
  while ((c = getopt (argc, argv, "p:e:m:l:a:b:w:q:v:")) != -1)
    switch (c) {
      case 'p':
        start_port = atoi(optarg);
        break;
      case 'q':
        end_port = atoi(optarg);
        break;
      case 'e':
        executors = atoi(optarg) > 0 ? atoi(optarg) : executors;
        break;
      case 'm':
        {
	  unsigned long long optarg_memory= atol(optarg) * (1LL<<20);
	  if(optarg_memory >0){
	    unsigned long long total_memory= get_total_memory();	    
	    shared_memory = optarg_memory > total_memory ? total_memory : optarg_memory;
	  }
	}
        break;
      case 'l':
        log_level = atoi(optarg) > 3 ? log_level : atoi(optarg);
        break;
      case 'a':
        master_addr = std::string(optarg);
        break;
      case 'b':
        master_port = atoi(optarg);
        break;
      case 'w':
        worker_addr = std::string(optarg);
        break;
      default:
        continue;
    }
  
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  if(worker_addr.size() == 0) {
    worker_addr = string(hostname);
  }
  char workerLogname[1024];
  sprintf(workerLogname, "/tmp/R_worker_%s_%s.%d.log", getenv("USER"), master_addr.c_str(), master_port);
  InitializeFileLogger(workerLogname);  
  LoggerFilter(log_level);
  if (master_addr.size() <= 0 || master_port <= 0) {
    LOG_ERROR("Invalid master address and port number - %s:%d", master_addr.c_str(), master_port);
    return 0;
  }  
  LOG_INFO("Starting worker.");

  //sleep for N secs if that file exists
  FILE* f = fopen("/tmp/r_worker_startup_sleep_secs", "r");
  if(f) {
    char buffer[10];
    size_t nread = fread(buffer, 1, 10, f);
    int secs = atoi(buffer);
    if(secs > 0 and secs < 120) {
      LOG_INFO("Sleeping for %d secs, pid: %d\n", secs, getpid());
      sleep(secs);
    }
  }

  vector<boost::shared_ptr<presto::PrestoWorker> > worker_handles;

  context_t zmq_ctx(NUM_CTX_THREADS);

  try {
    presto::PrestoWorker *worker =
        new presto::PrestoWorker(&zmq_ctx,
                                 shared_memory,
                                 executors,
                                 array_stores,
                                 log_level,
                                 master_addr, master_port,
                                 start_port, end_port);

    LOG_INFO("Worker %s:(%d~%d) with %d executors and %llu Shared Memory. Master - %s:%d", 
      hostname, start_port, end_port, executors, shared_memory, master_addr.c_str(), master_port);
    presto::worker_ptr = worker;  // to enable clean worker shutdown


    FILE* f2 = fopen("/tmp/r_worker_request_tracer", "r");
    presto::RequestLogger requestLogger("WorkerRequestLogger");
    if(f2) {
        //register observers
        worker->Subscribe(&requestLogger);
        fclose(f2);
    }

    worker->Run(master_addr, master_port, worker_addr);
    delete worker;
  } catch (zmq::error_t err) {
    ostringstream msg;
    msg << "Proto ZMQ error " << err.what();
    LOG_ERROR(msg.str());
  }

  printf("exiting\n");
  return 0;
}
