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

#include <string>
#include <vector>
#include <unistd.h>
#include <boost/thread.hpp>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <stdint.h>

#include "common.h"
#include "UpdateUtils.h"
#include "timer.h"
#include "PrestoWorker.h"
#include "ExecutorPool.h"

#define MAX_LOG_NAME_SIZE 200

using namespace boost;

namespace presto {

// TODO(erik): master is not actually inited at this point; nicest solution is
// probably to move executorpool creation into hello in workerclient.cpp
/** A class that handles multiple executors. This class stands in-between worker and executors to send/receive tasks
 * @param n the number of executors to launch (each executor is a distinct R process)
 * @param my_location a pointer of information about itself (worker) (name and port number)
 * @param master a handler to send a message/command to master
 * @param shmem_arrays_mutex a mutex to access shared memory object
 * @param spill_dir a directory name that will be used when shared memory is full
 * @return Executor pool class object
 */
ExecutorPool::ExecutorPool(PrestoWorker* pw_, int n_, ServerInfo *my_location_,
                           MasterClient* master_,
                           timed_mutex *shmem_arrays_mutex,
                           boost::unordered_set<string> *shmem_arrays,
                           const string &spill_dir,
                           int log_level,
                           string master_ip, int master_port)
    : worker(pw_), num_executors(n_), my_location(my_location_), master(master_),
      shmem_arrays_mutex_(shmem_arrays_mutex),
      shmem_arrays_(shmem_arrays), spill_dir_(spill_dir) {
  
  int unique_worker_id = static_cast<int>(getpid());  // determine worker pid
  exec_index = 0;

  sema = new boost::interprocess::interprocess_semaphore(num_executors);
  
  for (int i = 0; i < num_executors; i++) {

    ExecutorData* executor = new ExecutorData;

    child_proc_ids_.push_back(0);  // initialize the child process ID
    int sendpipe[2];  // to send commands to executor
    int recvpipe[2];  // to receive result from executor

    int ret;
    ret = pipe(sendpipe);
    if (ret != 0) {
      ostringstream msg;
      msg << "ExecutorPool Constructor: error creating send pipe for executor "<< i << " : " << strerror(ret);
      LOG_ERROR(msg.str());
      continue;  // leeky we should notify this to master
    }
    ret = pipe(recvpipe);
    if (ret != 0) {
      ostringstream msg;
      msg << "ExecutorPool Constructor: error creating recv pipe for executor "<< i << " : " << strerror(ret);
      LOG_ERROR(msg.str());
      continue;  // leeky we should notify this to master
    }

    pid_t pid = fork();
    if (pid == 0) {
      char logname[MAX_LOG_NAME_SIZE];
      int open_flag = O_RDWR | O_CREAT;
#ifdef UNIQUE_EXECUTOR_LOG_NAMES
      sprintf(logname, "/tmp/R_executor_%s_%s.%d_%d_%d.log", getenv("USER"), master_ip.c_str(), master_port, i+1, unique_worker_id);
#else
      sprintf(logname, "/tmp/R_executor_%s_%s.%d_%d.log", getenv("USER"), master_ip.c_str(), master_port, i+1);
      open_flag |= O_TRUNC;
#endif
      int fd = open(logname, open_flag, S_IRUSR | S_IWUSR);
      if (fd < 0) {
        LOG_ERROR("Failed to open Executor logfile: %s\n", logname);
      } else {
        dup2(fd, 2);  // make stderr go to log
        dup2(fd, 1);  // make stdout go to log
        close(fd);  // fd no longer needed - the dup'ed handles are sufficient
      }
      dup2(sendpipe[0], 0);  // executor receives on stdin
      close(sendpipe[0]);
      close(sendpipe[1]);
      close(recvpipe[0]);

      // execl("/usr/bin/valgrind", "/usr/bin/valgrind",
      //       "./R-executor-bin",
      //       executors[i].sync_name.c_str(),
      //       int_to_string(recvpipe[1]).c_str(),
      //       spill_dir_.c_str(),
      //       reinterpret_cast<char*>(NULL));
      string fname = "./R-executor-bin";
      ifstream file(fname.c_str());
      if (!file) {
        fname = "./bin/R-executor-bin";
      }
      char log_level_[10];
      sprintf(log_level_, "%d", log_level);
      execl(fname.c_str(), fname.c_str(),
            int_to_string(recvpipe[1]).c_str(),
            spill_dir_.c_str(), logname,
            log_level_,
            reinterpret_cast<char*>(NULL));
    } else {
      // keep child process id to kill child process upon exit
      child_proc_ids_[i] = pid;
      executor->process_id = pid;
      executor->send = fdopen(sendpipe[1], "w");  // prepare for pipes
      executor->recv = fdopen(recvpipe[0], "r");
      close(sendpipe[0]);
      close(recvpipe[1]);
      if (executor->send != NULL && executor->recv != NULL) {
        executor->ready = true;
      }
      executor->id = i+1;

      executors[i+1] = executor;
      LOG_INFO("Created new Executor with ID %d Process ID %jd", i+1,(::intmax_t)pid);
    }
  }

  // wait until executors start waiting for tasks
  /*for (volatile int i = 0; i < n; i++) {
    fprintf(stderr, "executor %d done\n", i);
  }*/
}

/** Executor pool destructor
 *
 */
ExecutorPool::~ExecutorPool() {
  for (int i = 0; i < child_proc_ids_.size(); ++i) {
    kill(child_proc_ids_[i], SIGKILL);  // upon exit, kill all child processes
  }

  int child_status = 1, loop_count = 5;
  LOG_DEBUG("Executorpool destructor: Waiting for child processes to join");
  do {
    pid_t done = waitpid(-1, &child_status, WNOHANG);
    if (done == -1 && errno == ECHILD) {
      LOG_DEBUG("ExecutorPool desctructor: All child processes are terminated");
      break;    
    }
    sleep(1);
  } while (--loop_count > 0); 

  LOG_DEBUG("Executorpool destructor: Closing pipe descriptor");


  /*boost::unordered_map<std::string, Split*>::iterator sit = local_splits.begin();
  for(; sit != local_splits.end(); ++sit) {  
     sit->second->executors.clear();
     delete sit->second;
  }*/
  
  boost::unordered_map<int, ExecutorData*>::iterator eit = executors.begin();
  for(; eit != executors.end(); ++eit) {
     if (eit->second->send != NULL) {
        fclose(eit->second->send);
        eit->second->send = NULL;
     }
     if (eit->second->recv != NULL) {
        fclose(eit->second->recv);
        eit->second->recv = NULL;
     }
     eit->second->clear_tasks.clear();
     eit->second->send_tasks.clear();
     eit->second->load_tasks.clear();
     eit->second->exec_tasks.clear();
     delete eit->second;
  }
 
  LOG_DEBUG("Executorpool destructor: Closing semaphores");
  if (sema != NULL) {
    delete sema;
    sema = NULL;
  }
  LOG_DEBUG("Executorpool destructor: Removing shared memory segments");
  for (map<string, Composite*>::iterator i = composites_.begin();
       i != composites_.begin(); i++) {
    delete i->second;
  }
  composites_.clear();
}


int ExecutorPool::GetExecutorInRndRobin() {
    unique_lock<mutex> lock(exec_mutex);
    exec_index++;
    return exec_index%num_executors;
    lock.unlock();
}

/** executes a task from a worker
 * @param func the string of function to execute
 * @param args arguments about splits
 * @param raw_args argument not-related to splits or composite array
 * @param composite_args arguments related to composite arrays
 * @param id ID of a task (are we really using it??) leeky
 * @param uid real task id from a master
 * @param res  response... are we using it?? leeky
 * @return NULL
 */
void ExecutorPool::execute(std::vector<std::string> func,
                           std::vector<NewArg> args,
                           std::vector<RawArg> raw_args,
                           std::vector<NewArg> composite_args,
                           ::uint64_t id, ::uint64_t uid, 
                           Response* res, int executor_id ) {
  LOG_DEBUG("EXECUTE TaskID %18zu - Waiting for an Available Executor", uid);
  
  //Timer timer;
  //timer.start();
  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  int target_executor_id = -1;

  if(executor_id < 0) {   // Run on next available executor
     for (int i = 0; i < num_executors; ++i) {
        if (executors[exec_index]->ready == false)
           exec_index = GetExecutorInRndRobin();//(exec_index + 1) % num_executors;
        else {
           target_executor_id=exec_index;
        }
     }
  } else {
     target_executor_id=executor_id;
     // Wait till the executor is ready
     while(executors[target_executor_id]->ready==false) {}
  }
  
  LOG_DEBUG("EXECUTE TaskID %18zu - Will be executed at Executor Id %d", uid, target_executor_id);
  //timer.start();
  // set an executor not ready to prevent further scheduling
  executors[target_executor_id]->ready = false;
  //exec_index = (exec_index + 1) % num_executors;
  lock.unlock();

  // write arguments about splits
  // split argument format
  // number_of_splits\n
  // split_name_in_presto variable_name_in_R\n
  // repeati_until_number_of_splits!

  ExecutorData* executor = executors[target_executor_id];

  LOG_INFO("Sending cmd for EXECUTE to the executor");

  fprintf(executor->send, "%d\n", 1);

  if (args.size() > 0)
     LOG_DEBUG("EXECUTE TaskID %18zu - Sending dobject Arguments to Executor.", uid);
      
  fprintf(executor->send, "%zu\n", args.size());
  for (int j = 0; j < args.size(); j++) {
     std::string arrayname(args[j].arrayname().c_str());
          
     fprintf(executor->send, "%s %s\n",
            arrayname.c_str(),
            args[j].varname().c_str());

     //send over split array names separately if list_type
     if(arrayname == "list_type..."){
        //send number of splits in this list_type arg
        fprintf(executor->send, "%d\n",args[j].list_arraynames_size());
        //send split names
        for(int k = 0; k < args[j].list_arraynames_size(); k++){
           fprintf(executor->send, "%s\n", args[j].list_arraynames(k).c_str());
        }
     }
        
     /*shmem_arrays_mutex_->lock();
        
     //Add Splits to executors
     //if it's a list-type arg, don't add the arrayname to shmem_arrays. instead, add all of the splits associated with it
     if(arrayname=="list_type..."){
        for(int l = 0; l < args[j].list_arraynames_size(); l++){
           shmem_arrays_->insert(args[j].list_arraynames(l));
        }
     }
     else {
        shmem_arrays_->insert(args[j].arrayname());
     }

     shmem_arrays_mutex_->unlock();*/
  }
      
  fflush(executor->send);

  // write raw arguments (not splits or composite arrays)
  // raw argument format
  // number_of_raw_arguments \n
  // name_of_argument_in_R size_of_the_value: value_of_the_variable\n
  // repeat_until_number_of_raw_args
  if (raw_args.size() > 0)
     LOG_DEBUG("EXECUTE TaskID %18zu - Sending Raw Arguments to Executor.", uid);
      
  fprintf(executor->send, "%zu\n", raw_args.size());
  for (int j = 0; j < raw_args.size(); j++) {
     // if the raw variable is embedded in the protobuf message value field
     if (raw_args[j].has_value() == true) {
        fprintf(executor->send, "%s %zu:", raw_args[j].name().c_str(), raw_args[j].value().size());
        for (int k = 0; k < raw_args[j].value().size(); k++)
              fprintf(executor->send, "%c", raw_args[j].value()[k]);
     } else if (raw_args[j].fetch_need() == true) {
        // we need to fetch the raw variable from a master
        fprintf(executor->send, "%s 0:", raw_args[j].name().c_str());
        fprintf(executor->send, "%s %d %s %zu", raw_args[j].server_addr().c_str(), raw_args[j].server_port(),
                                                raw_args[j].fetch_id().c_str(), raw_args[j].data_size());
     }
     fprintf(executor->send, "\n");
  }
  fflush(executor->send);

  // write array arguments
  // composite array argument format
  // number_of_composite_arguments \n
  // variable_name_in_R name_of_composite_array_in_presto
  // number_of_splits_in_the_composite_array:
  // split_name x_offset_of_split_from_00 y_offset_of_split_from_00
  // x_dim_of_split y_dim_of_split
  // repeat till the number of splits
  if (composite_args.size() > 0)
     LOG_DEBUG("EXECUTE TaskID %18zu - Sending Composite Arguments to Executor.", uid);
       
  fprintf(executor->send, "%zu\n", composite_args.size());      
  for (int j = 0; j < composite_args.size(); j++) {
     // the composite array is already created in the worker
     Composite *composite = composites_[composite_args[j].arrayname()];
     fprintf(executor->send, "%s %s %d %d ",
             composite_args[j].varname().c_str(),
             composite_args[j].arrayname().c_str(),
             composite->dobjecttype,
             static_cast<int>(composite->offsets.size())
            );
     for (int k = 0; k < composite->offsets.size(); k++) {
        fprintf(executor->send, " %s %zu %zu %zu %zu ",
                composite->splitnames[k].c_str(),
                composite->offsets[k].first,
                composite->offsets[k].second,
                composite->dims[k].first,
                composite->dims[k].second);

        // keep track of shared memory segment
        // later we need to clean them up
        /*shmem_arrays_mutex_->lock();
        shmem_arrays_->insert(composite_args[j].arrayname());
        shmem_arrays_mutex_->unlock();*/
     }                
     fprintf(executor->send, "\n");
  }
  fflush(executor->send);  // send the arguments to an executor


  // write function
  // in func, each line is separate strings and we have to get the size first
  size_t fun_str_length = 0;
  for (int j = 0; j < func.size(); j++) {
     fun_str_length += func[j].size();
     ++fun_str_length; // get space for \n
  }
  // write function length followed by string
  LOG_DEBUG("EXECUTE TaskID %18zu - Sending Function body to Executor.", uid);
  fprintf(executor->send, "%zu\n", fun_str_length);
  for (int j = 0; j < func.size(); j++) {
     fprintf(executor->send, "%s\n", func[j].c_str());
  }
  // write end
  fflush(executor->send);
  LOG_INFO("EXECUTE TaskID %18zu - Executing Function sent to Executor Id %d", uid, target_executor_id);

  // wait until executor is ready
  // boost::interprocess::scoped_lock<
  //   boost::interprocess::interprocess_mutex> executor_lock(
  //     executors[i].iss->executor_mutex);
  // executor_lock.unlock();
  // // notify executor to start
  // executors[i].iss->executor_condition.notify_one();

  //timer.start();

  TaskDoneRequest req;

  bool first = true;
  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors
     // const string &name = executors[i].update_vector->at(k).name();
     char cname[100];  // to keep a resultant split name
     size_t size;
     size_t rdim, cdim;
     int empty;
     memset(task_msg, 0x00, sizeof(task_msg));
     // This function blocks as it is waiting for fscanf from executor.
     int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                   &empty, &rdim, &cdim, task_msg);
     if (ret != 5) {
         // we are using size field to indicate the task result
         req.set_task_result(TASK_EXCEPTION);
         ostringstream msg;
         msg << "Error from worker " << server_to_string(*my_location) 
             << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
         req.set_task_message(msg.str());
         LOG_ERROR(msg.str());
         break;
     }

     //timer.start();
        
     // & means the task is complete
     // After a task is done, all update() variables are processed first.
     // After all updates are propagated, the task completes.
     if (strncmp(cname, "&", 100) == 0) {
        // we are using size field to indicate the task result
        LOG_INFO("EXECUTE TaskID %18zu - Task Execution complete.", uid);
        req.set_task_result(size);
        req.set_task_message(string(task_msg));
      	if (size==TASK_EXCEPTION){
      	   ostringstream msg;
           msg << "TASK_EXCEPTION : TaskID "<< uid << " execution failed at Executor " << target_executor_id << " with message: " << task_msg;
           LOG_ERROR(msg.str());  
	}
        break;
     } 

     string name(cname);  // the name of a split after task completion
     shmem_arrays_mutex_->lock();
     //shmem_arrays_->insert(name);  // insert the new split name
     worker->StageUpdatedPartition(name, size, target_executor_id);
     shmem_arrays_mutex_->unlock();

     req.add_update_names(name);
     req.add_update_sizes(size);
     req.add_update_empties(empty);
     req.add_row_dim(rdim);
     req.add_col_dim(cdim);
     if (size < INMEM_UPDATE_SIZE_LIMIT) {
        req.add_update_stores(string());
     } else {
        req.add_update_stores(spill_dir_);
     }
  }

  executors[target_executor_id]->ready = true;
  sema->post();
  req.set_id(id);
  req.set_uid(uid);
  req.mutable_location()->CopyFrom(*my_location);
  LOG_DEBUG("EXECUTE TaskID %18zu - Updated variables read.", uid);
  master->TaskDone(req);  // send a task done message to master
  LOG_INFO("EXECUTE TaskID %18zu - Sent TASKDONE message to Master", uid);
}


void ExecutorPool::clear(std::vector<std::string> splits, int executor_id) {

  LOG_INFO("New Request of type CLEAR to be sent to Executor");

  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  // Wait till the executor is ready
  do{
  } while(executors[executor_id]->ready==false);

  executors[executor_id]->ready = false;
  lock.unlock();

  ExecutorData* executor = executors[executor_id];

  LOG_INFO("Sending cmd for CLEAR to the executor: Size(%zu)", splits.size());

  fprintf(executor->send, "%d\n", 2);

  fprintf(executor->send, "%zu\n", splits.size());
  for (int i = 0; i < splits.size(); i++) {
     //std::string splitname(splits[i].c_str());
     LOG_INFO("ExecutorPool: Sending partition %s to executor %d", splits[i].c_str(), executor_id);
     fprintf(executor->send, "%s\n", splits[i].c_str());
  }
  fflush(executor->send);

  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors 
    /*//char cname[100];  // to keep a resultant split name
    int success;
    int32_t ret = ParseClearLine(executor->recv, &success);
    LOG_INFO("Ret is %d", ret);
    if(ret != 1) {
      LOG_ERROR("Error clearning partitions");
    }
    break;*/
    char cname[100];  // to keep a resultant split name
    size_t size;
    size_t rdim, cdim;
    int empty;
    memset(task_msg, 0x00, sizeof(task_msg));
    // This function blocks as it is waiting for fscanf from executor.
    int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                  &empty, &rdim, &cdim, task_msg);
    //LOG_INFO("ret is %d", ret);
    if (ret != 5) {
        // we are using size field to indicate the task result
        //req.set_task_result(TASK_EXCEPTION);
        ostringstream msg;
        msg << "Error from worker " << server_to_string(*my_location) 
            << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
        //req.set_task_message(msg.str());
        LOG_ERROR(msg.str());
        break;
    }
      
    // & means the task is complete
    // After a task is done, all update() variables are processed first.
    // After all updates are propagated, the task completes.
    if (strncmp(cname, "&", 100) == 0) {
       // we are using size field to indicate the task result
       LOG_INFO("Clear Task complete.");
       //req.set_task_result(size);
       //req.set_task_message(string(task_msg));
       if (size==TASK_EXCEPTION){
          ostringstream msg;
          msg << "TASK_EXCEPTION : Clear task execution failed at Executor " << executor_id << " with message: " << task_msg;
          LOG_ERROR(msg.str());  
       }
       break;
    }   

  }

  executors[executor_id]->ready = true;
  sema->post();
}


void ExecutorPool::fetch(std::string split_name, int32_t serverfd, int port_number, int executor_id) {

  LOG_INFO("New Request of type FETCH to be sent to Executor");

  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  // Wait till the executor is ready
  do{ 
  } while(executors[executor_id]->ready==false);

  executors[executor_id]->ready = false;
  lock.unlock();

  ExecutorData* executor = executors[executor_id];

  LOG_INFO("Sending cmd for FETCH to the executor for partition name %s", split_name.c_str());

  fprintf(executor->send, "%d\n", 3); 
  fprintf(executor->send, "%s %d %d\n", split_name.c_str(), serverfd, port_number);
  fflush(executor->send);

  LOG_INFO("Sent splitnames");
  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors 
    /*//char cname[100];  // to keep a resultant split name
    int success;
    int32_t ret = ParseClearLine(executor->recv, &success);
    LOG_INFO("Ret is %d", ret);
    if(ret != 1) {
      LOG_ERROR("Error clearning partitions");
    }
    break;*/
    char cname[100];  // to keep a resultant split name
    size_t size;
    size_t rdim, cdim;
    int empty;
    memset(task_msg, 0x00, sizeof(task_msg));
    // This function blocks as it is waiting for fscanf from executor.
    int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                  &empty, &rdim, &cdim, task_msg);
    LOG_INFO("ret is %d", ret);
    if (ret != 5) {
        // we are using size field to indicate the task result
        //req.set_task_result(TASK_EXCEPTION);
        ostringstream msg;
        msg << "Error from worker " << server_to_string(*my_location)
            << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
        //req.set_task_message(msg.str());
        LOG_ERROR(msg.str());
        break;
    }

    // & means the task is complete
    // After a task is done, all update() variables are processed first.
    // After all updates are propagated, the task completes.
    if (strncmp(cname, "&", 100) == 0) {
       // we are using size field to indicate the task result
       LOG_INFO("Clear Task complete.");
       //req.set_task_result(size);
       //req.set_task_message(string(task_msg));
       if (size==TASK_EXCEPTION){
          ostringstream msg;
          msg << "TASK_EXCEPTION : Clear task execution failed at Executor " << executor_id << " with message: " << task_msg;
          LOG_ERROR(msg.str());
       }
       break;
    }

  }

  executors[executor_id]->ready = true;
  sema->post();
}

void ExecutorPool::newtransfer(std::string split_name, std::string server_name, int port_number, int executor_id) {

  LOG_INFO("New Request of type NEWTRANSFER to be sent to Executor");

  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  // Wait till the executor is ready
  do{
  } while(executors[executor_id]->ready==false);

  executors[executor_id]->ready = false;
  lock.unlock();

  ExecutorData* executor = executors[executor_id];

  LOG_INFO("Sending cmd for NEWTRANSFER to the executor for partition name %s", split_name.c_str());

  fprintf(executor->send, "%d\n", 4);
  fprintf(executor->send, "%s %s %d\n", split_name.c_str(), server_name.c_str(), port_number);
  fflush(executor->send);

  LOG_INFO("Sent splitname");
  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors 
    /*//char cname[100];  // to keep a resultant split name
    int success;
    int32_t ret = ParseClearLine(executor->recv, &success);
    LOG_INFO("Ret is %d", ret);
    if(ret != 1) {
      LOG_ERROR("Error clearning partitions");
    }
    break;*/
    char cname[100];  // to keep a resultant split name
    size_t size;
    size_t rdim, cdim;
    int empty;
    memset(task_msg, 0x00, sizeof(task_msg));
    // This function blocks as it is waiting for fscanf from executor.
    int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                  &empty, &rdim, &cdim, task_msg);
    LOG_INFO("ret is %d", ret);
    if (ret != 5) {
        // we are using size field to indicate the task result
        //req.set_task_result(TASK_EXCEPTION);
        ostringstream msg;
        msg << "Error from worker " << server_to_string(*my_location)
            << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
        //req.set_task_message(msg.str());
        LOG_ERROR(msg.str());
        break;
    }

    // & means the task is complete
    // After a task is done, all update() variables are processed first.
    // After all updates are propagated, the task completes.
    if (strncmp(cname, "&", 100) == 0) {
       // we are using size field to indicate the task result
       LOG_INFO("NewTransfer Task complete.");
       //req.set_task_result(size);
       //req.set_task_message(string(task_msg));
       if (size==TASK_EXCEPTION){
          ostringstream msg;
          msg << "TASK_EXCEPTION : NewTransfer task execution failed at Executor " << executor_id << " with message: " << task_msg;
          LOG_ERROR(msg.str());
       }
       break;
    }

  }

  executors[executor_id]->ready = true;
  sema->post();
}


void ExecutorPool::persist_to_worker(std::string split_name, int executor_id) {

  LOG_INFO("New Request of type PERSIST_TO_WORKER to be sent to Executor");

  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  // Wait till the executor is ready
  do{ 
  } while(executors[executor_id]->ready==false);

  executors[executor_id]->ready = false;
  lock.unlock();

  ExecutorData* executor = executors[executor_id];

  LOG_INFO("Sending cmd for PERSIST_TO_WORKER to the executor %d for partition name %s", executor_id, split_name.c_str());

  fprintf(executor->send, "%d\n", 5); 
  fprintf(executor->send, "%s\n", split_name.c_str());
  fflush(executor->send);

  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors 
    /*//char cname[100];  // to keep a resultant split name
    int success;
    int32_t ret = ParseClearLine(executor->recv, &success);
    LOG_INFO("Ret is %d", ret);
    if(ret != 1) {
      LOG_ERROR("Error clearning partitions");
    }
    break;*/
    char cname[100];  // to keep a resultant split name
    size_t size;
    size_t rdim, cdim;
    int empty;
    memset(task_msg, 0x00, sizeof(task_msg));
    // This function blocks as it is waiting for fscanf from executor.
    int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                  &empty, &rdim, &cdim, task_msg);
    LOG_INFO("ret is %d", ret);
    if (ret != 5) {
        // we are using size field to indicate the task result
        //req.set_task_result(TASK_EXCEPTION);
        ostringstream msg;
        msg << "Error from worker " << server_to_string(*my_location)
            << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
        //req.set_task_message(msg.str());
        LOG_ERROR(msg.str());
        break;
    }

    // & means the task is complete
    // After a task is done, all update() variables are processed first.
    // After all updates are propagated, the task completes.
    if (strncmp(cname, "&", 100) == 0) {
       // we are using size field to indicate the task result
       LOG_INFO("Persist_to_disk Task complete.");
       //req.set_task_result(size);
       //req.set_task_message(string(task_msg));
       if (size==TASK_EXCEPTION){
          ostringstream msg;
          msg << "TASK_EXCEPTION : Clear task execution failed at Executor " << executor_id << " with message: " << task_msg;
          LOG_ERROR(msg.str());
       }
       break;
    }

  }

  executors[executor_id]->ready = true;
  sema->post();
}


void ExecutorPool::shutdown(int executor_id) {

  LOG_INFO("New Request of type EXECUTOR to be sent to Executor");

  sema->wait();  // the semaphore is posted when a task is done

  Timer total_worker_exec;
  total_worker_exec.start();

  unique_lock<mutex> lock(poolmutex);
  // Wait till the executor is ready
  do{
  } while(executors[executor_id]->ready==false);

  executors[executor_id]->ready = false;
  lock.unlock();

  ExecutorData* executor = executors[executor_id];

  LOG_INFO("Sending cmd for SHUTDOWN to the executor for partition name");

  fprintf(executor->send, "%d\n", 6);
  fflush(executor->send);

  char task_msg[EXCEPTION_MSG_SIZE];
  while (true) {   // waiting for a result from executors 
    /*//char cname[100];  // to keep a resultant split name
    int success;
    int32_t ret = ParseClearLine(executor->recv, &success);
    LOG_INFO("Ret is %d", ret);
    if(ret != 1) {
      LOG_ERROR("Error clearning partitions");
    }
    break;*/
    char cname[100];  // to keep a resultant split name
    size_t size;
    size_t rdim, cdim;
    int empty;
    memset(task_msg, 0x00, sizeof(task_msg));
    // This function blocks as it is waiting for fscanf from executor.
    int32_t ret = ParseUpdateLine(executor->recv, cname, &size,
                                  &empty, &rdim, &cdim, task_msg);
    LOG_INFO("ret is %d", ret);
    if (ret != 5) {
        // we are using size field to indicate the task result
        //req.set_task_result(TASK_EXCEPTION);
        ostringstream msg;
        msg << "Error from worker " << server_to_string(*my_location)
            << check_out_of_memory(child_proc_ids_) << endl << "Failed to parse function execution result from executor";
        //req.set_task_message(msg.str());
        LOG_ERROR(msg.str());
        break;
    }

    // & means the task is complete
    // After a task is done, all update() variables are processed first.
    // After all updates are propagated, the task completes.
    if (strncmp(cname, "&", 100) == 0) {
       // we are using size field to indicate the task result
       LOG_INFO("Clear Task complete.");
       //req.set_task_result(size);
       //req.set_task_message(string(task_msg));
       if (size==TASK_EXCEPTION){
          ostringstream msg;
          msg << "TASK_EXCEPTION : Clear task execution failed at Executor " << executor_id << " with message: " << task_msg;
          LOG_ERROR(msg.str());
       }
       break;
    }

  }
  executors[executor_id]->ready = true;
  sema->post();
}

void ExecutorPool::InsertCompositeArray(std::string name, Composite* comp) {
  unique_lock<mutex> lock(comp_mutex);
  composites_[name] = comp;
  lock.unlock();
}

int ExecutorPool::GetExecutorID(pid_t pid) {
  for(int i = 0; i < child_proc_ids_.size(); ++i) {
    if(child_proc_ids_[i] == pid) {
      return i;
    }
  }
  return 0;
}

}  // namespace presto
