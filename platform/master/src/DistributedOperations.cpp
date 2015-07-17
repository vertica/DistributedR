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
 * Functions to perform operations on Distributed Arrays
 */

#include <time.h>
#include <boost/thread/locks.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <zmq.hpp>

#include <string>
#include <vector>
#include <signal.h>

#include <Rcpp.h>

#include "DistributedObjectMap.h"
#include "DistributedOperations.h"
#include "DistributedObject.h"
#include "Scheduler.h"
#include "PrestoMaster.h"
#include "atomicio.h"

#ifdef PERF_TRACE
#include <ztracer.hpp>
#endif

using namespace std;
using namespace Rcpp;
using namespace boost;

#define MAX_RAW_MSG_SIZE 62914560  // allow upto 60MB to raw message (due to protobuf msg size limit)
#define SERVER_SHUTDOWN_MSG "-1"

namespace presto {
    
#ifdef PERF_TRACE
    extern ZTracer::ZTraceEndpointRef ztrace_inst;
#endif
    
// keeping original R SIGINT handler
extern sighandler_t r_sigint_handler;
  
// a function that is customized for Presto SIGINT handler
extern "C" void m_sigint_handler(int sig);

/** Get darray handler with a given R-environment variable
 * @param xp R environment variable
 * @return DistributedArrayHandle pointer
 */
Rcpp::XPtr<DistributedObject> inline get_dobject(SEXP xp) {
  Rcpp::Environment env(xp);
  Rcpp::XPtr<DistributedObject> ptr1(env.get(".pointer"));
  return ptr1;
}

/** Get scheduler object with a given R-environment variable
 * @param xp R environment variable
 * @return Scheduler pointer
 */
Scheduler* get_scheduler(SEXP xp) {
  Rcpp::Environment env(xp);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  return ptr1->GetScheduler();
}

pair<int, int> get_master_port_range(SEXP xp) {
  Rcpp::Environment env(xp);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  return ptr1->GetMasterPortRange();
}
void SetServerThread(SEXP xp, boost::thread* thr_ptr) {
  Rcpp::Environment env(xp);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  ptr1->SetLargeChunkServerThread(thr_ptr);
  return;
}
/** Get a materialized split with the given input
 * @param presto_master_exp presto master object
 * @param split_ a split object that will be returned
 * @return materialized split object
 */
RcppExport SEXP DistributedObject_Get(SEXP presto_master_exp,
                                     SEXP split_) {
  BEGIN_RCPP
  Rcpp::S4 split(split_);
  Rcpp::IntegerVector split_ids(split.slot("split_ids"));
  string splitname;
  if (split_ids.size() <= 0)
    throw PrestoWarningException("DistributedObject_Get split id error");  
  Rcpp::XPtr<DistributedObject> d = get_dobject(split.slot("dobject_ptr"));
  if (split_ids.size() == 1) {  // single split
    int32_t split_id = split_ids[0];
    splitname = d->GetSplitFromId(split_id)->name();
  } else {  // composite
    vector<string> split_names;
    for (int i = 0; i < split_ids.size(); ++i) {
      split_names.push_back(d->GetSplitFromId(split_ids[i])->name());
    }
    splitname = get_scheduler(presto_master_exp)->CompositeName(split_names);
  }

  return get_scheduler(presto_master_exp)->GetSplitToMaster(splitname);
  END_RCPP
}

/** A thread to be responsible for sending Raw data to a client
 * @param raw_data_cache keeps the mapping of key and RawVector
 * @port_number this variable is called by reference to let a master thread to know the server is ready
 */
void RawDataTransferThread (map<string, Rcpp::RawVector>* raw_data_cache,
                                    pair<int, int> m_port_range,
                                           volatile int* port_number) {
  if (raw_data_cache == NULL) {
    fprintf(stderr, "RawDataTransferThread input raw data is NULL\n");
    return;
  }
  try{
    int n_port;
    int32_t serverfd;
    try {
      serverfd = CreateBindedSocket(m_port_range.first, m_port_range.second, &n_port);
    } catch (...) {
      LOG_ERROR("RawDataTransferThread - fail to open/bind a socket\n");
      *port_number = -1;
      return;
    }
    LOG_DEBUG("RawDataTransferThread socket bind complete. Attached to port: %d", n_port);
    *port_number = n_port;
    int32_t new_fd = -1;
    struct sockaddr_in their_addr;
    socklen_t sin_size = sizeof(their_addr);
    static int32_t ret = 0;
    ret = listen(serverfd, SOMAXCONN);
    if (ret < 0) {
      fprintf(stderr, "RawDataTransferThread - listen failure\n");
      close(serverfd);
      return ;
    }
    // now we are ready to accept connection from workers
    while(true) {
      boost::this_thread::interruption_point();
      new_fd = accept(serverfd, (struct sockaddr *) &their_addr, &sin_size);
      if (new_fd < 0) {
        fprintf(stderr, "RawDataTransferThread - accept new_fd less than zero\n");
        break;
      }
      uint32_t sz, size_bytes_read = 0, recv_t = 0;
      char recv_client_msg[256];
      memset(recv_client_msg, 0x00, 256);
      int rbytes = read(new_fd, recv_client_msg, sizeof(recv_client_msg)-1);
      if (strncmp(recv_client_msg, SERVER_SHUTDOWN_MSG, strlen(SERVER_SHUTDOWN_MSG)) == 0) {
        close(new_fd);
        break;
      }
      if (raw_data_cache->count(string(recv_client_msg)) == 0) {
        fprintf(stderr, "DistributedExecOperation: a client requested invalid dataset: %s cur map size: %lu\n", recv_client_msg, raw_data_cache->size());
        close(new_fd);
        break;
      }
      boost::this_thread::interruption_point();
      Rcpp::RawVector send_data = (*raw_data_cache)[string(recv_client_msg)];
      char* reply = (char*) malloc(send_data.size());
    // memcpy for RawVector does not work and we have to copy byte by byte
      for (size_t i = 0; i < send_data.size(); ++i) {
        reply[i] = send_data[i];
      }
      size_t bsend = atomicio(vwrite, new_fd, reply, send_data.size());  
      close(new_fd);
      free(reply);
      if (bsend != send_data.size()) {
        fprintf(stderr, "DistributedExecOperation: fail to send to a remote worker\n");
        break;
      }
    }
    raw_data_cache->clear();
    delete raw_data_cache;
    raw_data_cache = NULL;
    close(serverfd);
  } catch (boost::thread_interrupted const&) {}
  return;
}


/** Execute a function in the Presto
 * @param presto_master_exp a presto master object
 * @param func_body_exp function body that will be called in the executor
 * @param num_calls_exp number of tasks to be created
 * @param arg_names_exp a list of argument names that are related to splits
 * @param split_names_exp a list of split names that will be used in the execution
 * @param arg_vals_exp the value of split input argument
 * @param raw_names_exp a list of argument names that are not related to splits
 * @param raw_vals_exp the value of input raw arguments (not splits)
 * @param wait_exp sets the execution of this task as sync (true) /async (false)
 * @param scheduler_policy_exp type of scheduler policy. Default = 0, i.e. minimize data movement
 * @param inputs_exp inputs for test
 * @param progress_sexp a boolean flag to indicate if the progress bar will be presented (true: display, false: no display)
 * @return execution result
 */
RcppExport SEXP DistributedObject_ExecR(SEXP presto_master_exp,
                                       SEXP func_body_exp,
                                       SEXP num_calls_exp,
                                       SEXP arg_names_exp,
                                       SEXP split_names_exp,
                                       SEXP arg_vals_exp,
                                       SEXP list_args_exp,
                                       SEXP raw_names_exp,
                                       SEXP raw_vals_exp,
                                       SEXP wait_exp,
				       SEXP scheduler_policy_exp,
				       SEXP inputs_sexp,
                                       SEXP progress_sexp,
                                       SEXP trace_sexp) {
    
//Initialize a new trace every time a new task begins execution
#ifdef PERF_TRACE
  bool trace = Rcpp::as<bool>(trace_sexp);
  if(trace){
    master_trace = ZTracer::create_ZTrace("Executing a new task", ztrace_inst);
    trace_master = true;
  } else {
    trace_master = false;
    struct blkin_trace_info info;
    info.trace_id = 1337;
    info.span_id = 1234;
    info.parent_span_id = 4321;
    master_trace = ZTracer::create_ZTrace("dummy", ztrace_inst, &info, false);
  } 
  is_master = true;
#endif
    
  BEGIN_RCPP;
  // ignore sigint during scheduling time
  signal(SIGINT, SIG_IGN);
  bool wait = as<bool>(wait_exp);
  int calls = Rcpp::as<int>(num_calls_exp);
  bool progress = Rcpp::as<bool>(progress_sexp);
  Rcpp::Environment env(presto_master_exp);
  Rcpp::XPtr<PrestoMaster> pm(env.get(".pointer"));

  if (calls == 0) {
    signal(SIGINT, r_sigint_handler);
    return Rcpp::wrap(false);
  }

  Rcpp::CharacterVector func_body(func_body_exp);

  Rcpp::List arg_names_vec(arg_names_exp);
  Rcpp::List raw_names_vec(raw_names_exp);
  Rcpp::List split_names_vec(split_names_exp);
  Rcpp::List arg_vals_vec(arg_vals_exp);
  Rcpp::List raw_vals_vec(raw_vals_exp);
  Rcpp::List list_args_vec(list_args_exp);

  boost::interprocess::interprocess_semaphore sema(0);

  vector<TaskArg*> tasks;
  vector<int> inputs(Rf_length(inputs_sexp));
    memcpy(&inputs[0], INTEGER(inputs_sexp), inputs.size() * sizeof(inputs[0]));
  int scheduler_policy = Rcpp::as<int>(scheduler_policy_exp);

  Timer t;
  double misct = 0, argst = 0, rawargst = 0;
  double args1 = 0, args2 = 0, args3 = 0;
  map<string, Rcpp::RawVector>*  server_cache = NULL;
  boost::thread* server_thread = NULL;
  volatile int server_port = 0;
  
#ifdef PERF_TRACE
      vector<string> func_str = vector<string>(func_body.begin(), func_body.end());
      std::string func_print = "";
      
      const int max_lines = 5;
      
      for(int i=0; i!=func_str.size();++i){
          func_print = func_print + " " + func_str[i];
          if(i >= max_lines) break;
      }
      
      char message[1000];
      sprintf(message,"starting task with %d calls, function body starting with: %s", calls, func_print.c_str());
      LOG_INFO(message);
#endif
  
  for (int32_t i = 0; i < calls; ++i) {
      
    size_t raw_msg_size = 0;  // to keep track of cumulated raw message size per each task
    t.start();
    TaskArg &task = *(new TaskArg);
    // semaphore to monitor task completion
    task.sema = &sema;
    vector<Arg> &func_args = task.args;
    Arg arg;
    vector<RawArg> &func_raw_args = task.raw_args;
    RawArg raw_arg;
    Tuple offset;
    vector<string> &func_str = task.func_str;
    func_str = vector<string>(func_body.begin(), func_body.end());

    misct += t.stop()/1e6;

    func_args.clear();

    t.start();
    Timer ti;
    for (int32_t k = 0; k < arg_names_vec.size(); ++k) {
      ti.start();
      arg.clear_arrays();
      arg.clear_offsets();
      
      //mark as a list-type argument
      bool is_list = false;
      for(int32_t j = 0; j < list_args_vec.size(); j ++){
          if(as<string>(list_args_vec[j]) == as<string>(arg_names_vec[k])){
              is_list = true;
              break;
          }
      }

      arg.set_is_list(is_list);
      arg.set_name(as<string>(arg_names_vec[k]));

      Rcpp::List splits = arg_vals_vec[k];
      Rcpp::IntegerVector split_ids(
          Rcpp::as<SEXP>(splits[i % splits.size()]));

      args1 += ti.restart()/1e6;
      DistributedObject *d = pm->GetDistributedObjectMap()->GetDistributedObject(Rcpp::as<string>(split_names_vec[k]));
      if (d == NULL) {
        ostringstream msg;
        msg << "dobject \""
            << Rcpp::as<string>(split_names_vec[k]) << "\" is not found";
        throw PrestoWarningException(msg.str());
      }
//      DistributedObject *d =
//          new DistributedObjectHandle(pm,
//                                     Rcpp::as<string>(split_names_vec[k]),
//                                     -1);
      args2 += ti.restart()/1e6;
      for (int32_t j = 0; j < split_ids.size(); ++j) {
        int32_t split_id = split_ids[j];
        arg.add_arrays()->CopyFrom(*d->GetSplitFromPos(split_id));

        offset.Clear();
        const vector< ::int64_t> &off_vec = d->GetBoundary(split_id);
        for (int32_t i = 0; i < off_vec.size(); ++i) {
          offset.add_val(off_vec[i]);
        }
        arg.add_offsets()->CopyFrom(offset);
      }

      args3 += ti.restart()/1e6;

      if (arg.arrays_size() > 1 && !is_list) {  // composite
        std::vector< ::int64_t> dim = d->GetDimensions();
        offset.Clear();
        for (int32_t i = 0; i < dim.size(); ++i) {
          offset.add_val(dim[i]);
        }
        arg.mutable_dim()->CopyFrom(offset);
      }
      func_args.push_back(arg);

//      delete d;
    }
    argst += t.restart()/1e6;
    for (int32_t k = 0; k < raw_names_vec.size(); ++k) {
      raw_arg.clear_name();
      raw_arg.clear_value();
      string raw_val_name(as<string>(raw_names_vec[k]));
      raw_arg.set_name(raw_val_name);

      Rcpp::List raw_vals_list = raw_vals_vec[k];
      Rcpp::RawVector raw_value = raw_vals_list[i % raw_vals_list.size()];
      raw_msg_size += raw_value.size();
      // if the current raw message size is larger than threshold, use external socket to transfer
      if (raw_msg_size > MAX_RAW_MSG_SIZE) {
        raw_msg_size -= raw_value.size();  // subtract from the raw_msg_size as we do not use protobuf
        raw_arg.set_fetch_need(true);
        ostringstream strm;
        strm << raw_val_name << i;
        string fetch_id = strm.str();
        raw_arg.set_fetch_id(fetch_id);
        // server_cache contains the mapping of data_id to RawVector pointer
        if (server_cache == NULL) {
          // this server_cache object will be deleted in the server thread
          // it is to avoid leakage in case error happens
          server_cache = new map<string, Rcpp::RawVector>();
          if (server_cache == NULL) {
            forward_exception_to_r(PrestoWarningException
              ("Fail to allocate server data to rawvector map"));
          }
        }
        // here, we don't need a lock as the read will happen after all writes are done
        server_cache->insert(pair<string, Rcpp::RawVector>(fetch_id, raw_value));
        // if server_thread is not running, start one
        if (server_thread == NULL) {
          server_port = 0;
          pair<int, int> port_range = get_master_port_range(presto_master_exp);
          server_thread = new boost::thread
            (RawDataTransferThread, server_cache, port_range, &server_port);
          // polling until the server port gets assigned
          // the server_port variable is called by reference and updated after the server becomes ready
          while (server_port == 0){}
          if (server_port < 0) {
            server_thread->interrupt();
            server_thread->try_join_for(boost::chrono::seconds(5));
            delete server_thread;
            forward_exception_to_r(PrestoWarningException
              ("DistributedExecOperation failed to find an open Port"));
          }
          SetServerThread(presto_master_exp, server_thread);
        }
        raw_arg.set_server_addr(pm->GetMasterAddr());
        raw_arg.set_server_port(server_port);
        raw_arg.set_data_size(raw_value.size());
      } else {
        // send value through protobuf message
        raw_arg.set_fetch_need(false);
        for (int32_t j = 0; j < raw_value.size(); ++j) {
          raw_arg.mutable_value()->push_back(raw_value[j]);
        }
      }
      func_raw_args.push_back(raw_arg);
    }
    rawargst += t.stop()/1e6;
    
    tasks.push_back(&task);
    
  }

  ForeachStatus &foreach = *(new ForeachStatus);
  foreach.num_tasks = calls;
  foreach.is_error = false;

  get_scheduler(presto_master_exp)->InitializeForeach(&foreach);
  get_scheduler(presto_master_exp)->AddTask(tasks, scheduler_policy, &inputs);
#if _POSIX_VERSION > 198800L
  struct sigaction action;
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);
  signal(SIGINT, m_sigint_handler);
/*
  action.sa_handler = m_sigint_handler;
  sigemptyset(&(action.sa_mask));
  action.sa_flags = 0;
  sigaction(SIGINT, &action, 0);
*/
#else
  signal(SIGINT, m_sigint_handler);
#endif

  if (wait) {
    // pool.wait();
    int prev = -1;
    for (volatile int i = 0; i < calls; i++) {
      if (progress) {
        if (i*100/calls != prev) {
          printf("\rprogress: %3d%%", i*100/calls);
          prev = i*100/calls;
          fflush(stdout);
        }
      }
      sema.wait();
    }
    if (progress) {
      printf("\rprogress: 100%%\n");
    }

    for(volatile int i = 0; i < calls; i++) {
      sema.wait();
    }
  }
  signal(SIGINT, r_sigint_handler);  // revert to default

  if (server_thread != NULL) {
    ShutdownRawDataTransfer(server_port);
    server_thread->interrupt();
    server_thread->try_join_for(boost::chrono::seconds(5));
    SetServerThread(presto_master_exp, NULL);
    delete server_thread;
  }
  
  bool success = !foreach.is_error;
  std::ostringstream error_msg;
  if(!success) {
   error_msg << "\n" << exception_prefix << foreach.error_stream.str() << "\n";
   if(!error_msg.str().empty()){
     LOG_ERROR(error_msg.str());
     printf("\n");
     forward_exception_to_r(PrestoWarningException(error_msg.str()));
   }
  }

  return Rcpp::wrap(success);	
  END_RCPP;
}


RcppExport SEXP DistributedObject_PrintStats(SEXP presto_master_exp,
                                            SEXP splits) {
  BEGIN_RCPP;
  Rcpp::S4 split = splits;
  Rcpp::IntegerVector split_ids(split.slot("split_ids"));

  Rcpp::XPtr<DistributedObject> d = get_dobject(split.slot("dobject_ptr"));

  vector<string> splitnames;

  for (int32_t j = 0; j < split_ids.size(); ++j) {
    int32_t split_id = split_ids[j];
    splitnames.push_back(d->GetSplitFromPos(split_id)->name());
  }

  get_scheduler(presto_master_exp)->PrintArrayStats(splitnames);

  return Rcpp::wrap(true);
  END_RCPP;
}


/**
 * Helper functions for Data Loader
 *
 */


/**
 * Gets DataLoader UDx Parameter 'DR_worker_info' for local 
 * node-to-node data transfer from Vertica to distributedR
 *
 */
RcppExport SEXP GetLoaderParameter_local(SEXP presto_master_exp_,
                                         SEXP table_metadata_) {
  BEGIN_RCPP;
  Rcpp::Environment env(presto_master_exp_);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  if(NULL == ptr1->GetDataLoader()) {
    fprintf(stderr, "Data Loader is not running.\n");
    return Rcpp::wrap(false);
  }

  return ptr1->GetDataLoader()->GetLoaderParameterLocal(table_metadata_);
  END_RCPP;
}


/**
 * Gets DataLoader UDx Parameter 'DR_worker_info' for uniform 
 * all nodes-to-all nodes data transfer from Vertica to distributedR
 *
 */
RcppExport SEXP GetLoaderParameter_uniform(SEXP presto_master_exp_) {
  BEGIN_RCPP;
  Rcpp::Environment env(presto_master_exp_);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  if(NULL == ptr1->GetDataLoader()) {
    fprintf(stderr, "Data Loader is not running.\n");
    return Rcpp::wrap(false);
  }

  return ptr1->GetDataLoader()->GetLoaderParameterUniform();
  END_RCPP;
}


/**
 * Fetch Loader Stats from Workers
 * Executed after UDx is complete
 *
 */
RcppExport SEXP GetLoaderResult(SEXP presto_master_exp_,
                              SEXP export_result_) {
  BEGIN_RCPP;
  Rcpp::Environment env(presto_master_exp_);
  Rcpp::XPtr<PrestoMaster> ptr1(env.get(".pointer"));
  if(NULL == ptr1->GetDataLoader()) {
    fprintf(stderr, "Data Loader is not running.\n");
    return Rcpp::wrap(false);
  }

  return ptr1->GetDataLoader()->FetchLoaderStatus(export_result_);
  END_RCPP;
}

RcppExport SEXP HandleUDxError(SEXP udx_error) {
  BEGIN_RCPP;
  Rcpp::CharacterVector udx_error_sexp(udx_error); 
  vector<std::string> error_vec = vector<std::string>(udx_error_sexp.begin(), udx_error_sexp.end());
  //LOG_ERROR("Error while loading data from Vertica: %s", error_vec[0].c_str());
  std::string error = error_vec[0];

  /*int pos_start = error.find("error code: ");
    if(pos_start == -1)
        return Rcpp::List::create(Rcpp::Named("error_code") = Rcpp::wrap(0),
                                  Rcpp::Named("error_msg") = Rcpp::wrap(error));
    int error_code = atoi(error.substr(pos_start+strlen("error code: "), 1).c_str());*/

  std::string error_msg;
  int pos_start = error.find("message: ");
  if(pos_start == -1) 
    error_msg = error;
  else 
    error_msg = error.substr(pos_start+strlen("message: "));

  return Rcpp::wrap(error_msg);
  END_RCPP;
}

}  // namespace presto
