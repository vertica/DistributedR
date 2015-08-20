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

#include <math.h>
#include <stdio.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <cstdlib>
#include <iostream>
#include <set>
#include <algorithm>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <RInside.h>
#include <Rinterface.h>

#include "DistributedObjectMap.h"
#include "PrestoMasterHandler.h"
#include "Scheduler.h"
#include "WorkerInfo.h"
#include "PrestoMaster.h"
#include "PrestoException.h"
#include "ResourceManager.h"

using namespace boost;
using namespace google::protobuf;

namespace presto {

#ifdef PERF_TRACE   
  ZTracer::ZTraceEndpointRef ztrace_inst;
#endif

::uint64_t abs_start_time;
StorageLayer DATASTORE = WORKER;

sighandler_t r_sigint_handler;

/** Presto custom signal handler. It enables interrupting a long running task.
 * @param sig a received input signal
 * @return NULL
 */
extern "C" void m_sigint_handler(int sig) {
  signal(SIGINT, SIG_IGN); // ignore further sigint message in the handler
  printf("Presto SIGINT handler\n");
  printf("#########################################\n");
  printf("Which action do you want to take?\n");
  printf("1. Do nothing and wait\n");
  printf("2. Terminate master and workers (DEFAULT)\n");
  printf("#########################################\n");
  printf("Type (Default: 2) - ");
  std::string uinput = "2";
  cin.clear();
  std::getline(cin, uinput);
  if (uinput != "1") { 
    signal(SIGINT, r_sigint_handler);
    forward_exception_to_r(PrestoShutdownException("User-initiated distributedR shutdown"));
  } else {
    signal(SIGINT, m_sigint_handler); // revert back to master sigint handler
  }
}

/** PrestoMaster constructor. This creates a zmq context and worker information semaphore (check if worker is up)
 * It creates a scheduler, message handler, resource manager and darraymap
 * @param a location of input configuration (contains worker hostname and port)
 */
PrestoMaster::PrestoMaster(const string& config_file)
  : zmq_ctx_(NUM_CTX_THREADS),
    workerinfos_sema_(0) {
  
  // Initialize random number generator
  srand(time(NULL) + getpid());

#ifdef PERF_TRACE
int tracer = ZTracer::ztrace_init();
ztrace_inst = ZTracer::create_ZTraceEndpoint("127.0.0.1", 1, "master"); 
#endif

  // Initialize absolute time
  timeval now;
  gettimeofday(&now, NULL);
  abs_start_time = now.tv_usec + now.tv_sec * 1000000;
  // Don't do any stack checking, see R Exts, '8.1.5 Threading issues'
  ResetCurPortNum();
  R_CStackLimit = (uintptr_t)-1;
//  LOG_INFO("Initializing Master");
  // darray_map_ keeps track of darray name to an object mapping
  dobject_map_.reset(new DistributedObjectMap());
//  LOG_INFO("Distributed Object Map Created.");
  ParseXMLConfig(config_file, &master_, &workers_);
  CheckMasterWorkerAddrSanity();

#ifdef OOC_SCHEDULER
  scheduler_ = new OOCScheduler(master_.name(), this);
#else
  scheduler_ = new InMemoryScheduler(master_.name(), this);
#endif
//  LOG_INFO("Scheduler Created.");
  // PrestoMasterHandler takes responsibility of
  // handling incoming messages from workers
  handler_.reset(new PrestoMasterHandler(dobject_map_.get(), scheduler_, this));  
  InitProtoThread();
  workerinfos_sema_.wait();  // wait until MasterHandler starts to run
  char masterLogname[1024];
  sprintf(masterLogname, "/tmp/R_master_%s_%s.%d.log", getenv("USER"), master_.name().c_str(), master_.presto_port());

  InitializeFileLogger(masterLogname);
  LOG_INFO("Master node is listening at %d port.", master_.presto_port());
  // ResourceManager monitors workers if they are alive
  res_manager_.reset(new ResourceManager(scheduler_, this));
  LOG_INFO("Resource Manager Created");
  boost::timed_mutex::scoped_timed_lock run_check_lock(is_running_mutex_, boost::get_system_time()+boost::posix_time::milliseconds(2000));
  resource_manager_thread_ = NULL;
  // is_running_ is used from R-session to check
  // if this PrestoMaster session is running (not shutdown)
  is_running_ = true;
  dataloader_manager_ = NULL;
  lrg_chunk_trnfr_thr_ptr = NULL;
  LOG_INFO("Master Initialization done");
  run_check_lock.unlock();
//  fprintf(stderr, "Master is at %s\n", master_.name().c_str());
//  fprintf(stderr, "Number of workers %d\n", num_workers);
}

/** PrestoMaster destructor
 * This calls shutdown function for safe. Shutdown will not execute, if it was already called
 */
PrestoMaster::~PrestoMaster() {
  Shutdown();
}

/**  Shutdowns a presto session. It first notifies workers to shutdown
 * It kills PrestoMasterHandler/Resource manager and delete scheduler object
 * This uses a varaible is_running_ to prevent multiple ShutDown action to happen.
 * @return NULL
 */
void PrestoMaster::Shutdown() {
  // shut down workers
  // In order to ensure shutdown is called only once
  try {
    bool ret = is_running_mutex_.timed_lock(boost::get_system_time()+boost::posix_time::milliseconds(2000));
    if(ret  == false) {
      return;
    }
    if (is_running_ == false) {
      is_running_mutex_.unlock();
      return;
    }
    is_running_ = false;  // to prevent further Shutdown() execution.
    is_running_mutex_.unlock();
  } catch (std::exception e) {
    ostringstream msg;
    msg << "Shutdown: failure to check if the distributedR is running. do not perform shutdown\n" << e.what(); 
    LOG_ERROR(msg.str());
    return;
  }
  LOG_DEBUG("Sending Shutdown message to Workers.");
  for (int i = 0; i < NumClients(); ++i) {
    WorkerInfo* wi = GetClientInfoByIndex(i);
    if (wi->IsRunning() == true) {
      wi->Shutdown();  // send shutdown
      wi->Close();  // set worker process to not running
      LOG_DEBUG("Sent SHUTDOWN message to Worker %s:%d", wi->hostname().c_str(), wi->port());
    }
  }
  if (lrg_chunk_trnfr_thr_ptr != NULL) {
    LOG_INFO("Shutting down large chunk transfer thread");
    lrg_chunk_trnfr_thr_ptr->interrupt();
    lrg_chunk_trnfr_thr_ptr->try_join_for(boost::chrono::seconds(15));
    delete lrg_chunk_trnfr_thr_ptr;
    lrg_chunk_trnfr_thr_ptr = NULL;
  }

  if (NULL != dataloader_manager_) {
    delete dataloader_manager_;
    dataloader_manager_ = NULL;
    LOG_DEBUG("Killed DataLoaderManager");
  }
  // kill handler
  try {
    if (NULL != resource_manager_thread_) {
      // set a flag of resource manager interrupted to true
      *res_manager_interrupted = true;
      // this interrupt call can fail if the thread disables interruption (R-studio)
      resource_manager_thread_->interrupt();  // stop resource manager
      try {
        bool ret = resource_manager_thread_->try_join_for(boost::chrono::seconds(1));
      } catch (...) {}
      delete resource_manager_thread_;
      resource_manager_thread_ = NULL;
      LOG_DEBUG("Killed Resource Manager");
    }
    if (NULL != handler_thread_) {
      // Stop handler thread if it is running
      if (CheckIfHandlerRunning() == true) {
        socket_t sock(zmq_ctx_, ZMQ_PUSH);
#ifdef ZMQ_LINGER
    int linger_period = SOCK_LINGER_TIME;
    sock.setsockopt(ZMQ_LINGER, &linger_period, sizeof(linger_period));
#endif
        string endpoint = "tcp://" + master_.name() + ":" +
          int_to_string(master_.presto_port());
        sock.connect(endpoint.c_str());
        MasterRequest req;
        req.set_type(MasterRequest::SHUTDOWN);
        message_t msg(req.ByteSize());
        req.SerializeToArray(msg.data(), msg.size());
        sock.send(msg);
        bool ret = handler_thread_->try_join_for(boost::chrono::seconds(1));
        if (ret == false) {
          handler_thread_->interrupt();
          try {
            ret = handler_thread_->try_join_for(boost::chrono::seconds(10));
            if (ret == false) {
              LOG_ERROR("distributedR failed to SHUTDOWN Master Message Handler thread. Recommend restart");
              fprintf(stderr, "\n%sdistributedR failed to shutdown a thread. We recommend restarting R\n", exception_prefix.c_str());
            }
          } catch (...) {}
        }
      }
      delete handler_thread_;
      handler_thread_ = NULL;
      LOG_DEBUG("Killed Master Message Handler");
    }
  } catch(...) {}
  // shut down protobuf
  if (NULL != scheduler_) {
    delete scheduler_;
    scheduler_ = NULL;
    LOG_DEBUG("Killed Scheduler");
  }
  ClearClientInfo();  // to clear buffered packets to workers!!
  LOG_INFO("distributedR shutdown complete.");
  stop_logging();
  fprintf(stderr, "Shutdown complete\n");
}

/** Gets a list of worker host names. This function can be called from R-session
 * @return a list of host names
 */
vector<string> PrestoMaster::WorkerHosts() {
  vector<string> wh;
  for (uint32_t i = 0; i < workers_.size(); ++i) {
    wh.push_back(workers_[i].name());
  }
  return wh;
}

/** Gets a list of worker port start range. This function can be called from R-session
 * @return a list of port numbers
 */
vector<int32_t> PrestoMaster::WorkerStartPortRange() {
  vector<int32_t> wp;
  for (uint32_t i = 0; i < workers_.size(); ++i) {
    wp.push_back(workers_[i].start_port_range());
  }
  return wp;
}

/** Gets a list of worker port end range. This function can be called from R-session
 * @return a list of port numbers
 */
vector<int32_t> PrestoMaster::WorkerEndPortRange() {
  vector<int32_t> wp;
  for (uint32_t i = 0; i < workers_.size(); ++i) {
    wp.push_back(workers_[i].end_port_range());
  }
  return wp;
}


/** Gets a list of worker port numbers. This function can be called from R-session
 * @return a list of port numbers
 */
vector<int32_t> PrestoMaster::WorkerPorts() {
  vector<int32_t> wp;
  for (uint32_t i = 0; i < workers_.size(); ++i) {
    wp.push_back(workers_[i].presto_port());
  }
  return wp;
}

/** Get the number of executors of workers
 * worker info is a vector of string with the following order
 * available memory size, used memory size, number of executor
 * @return a map of name (ip:port) with the information of each worker
 */
map<string, vector<size_t> > PrestoMaster::GetWorkerStatus() {
  boost::unordered_map<std::string, Worker*> wi = scheduler_->GetWorkerInfo();
  map<string, vector<size_t> > worker_info;
  boost::unordered_map<std::string, Worker*>::iterator wiit;
  for (wiit = wi.begin(); wiit != wi.end(); ++wiit) {
    vector<size_t> w_stat;
    w_stat.push_back((size_t)(wiit->second->executors));
    w_stat.push_back((wiit->second->mem_total_worker)>>20);
    w_stat.push_back((wiit->second->mem_used_worker)>>20);
    w_stat.push_back((wiit->second->size)>>20);
    w_stat.push_back((wiit->second->used)>>20);
    worker_info.insert(pair<string, vector<size_t> >(wiit->first, w_stat));
  }
  return worker_info;
}

/** Get darray information int the master
 * darray  info contains its size in KB and a list of workers that contains this split 
 * @return a map of name (split name) with the information of each split
 */
map<string, vector<string> > PrestoMaster::GetDobjectInfo() {
  boost::unordered_map<std::string, Split*> si = scheduler_->GetSplitInfo();
  map<string, vector<string> > split_info;
  boost::unordered_map<std::string, Split*>::iterator siit;
  for (siit = si.begin(); siit != si.end(); ++siit) {
    vector<string> s_stat;
    // split size in KByte
    s_stat.push_back(int_to_string(siit->second->size >> 10));
    boost::unordered_set<Worker*> w = siit->second->workers;
    boost::unordered_set<Worker*>::iterator wit;
    for (wit = w.begin(); wit != w.end(); ++wit) {
      s_stat.push_back(server_to_string((*wit)->server));
    }
    split_info.insert(pair<string, vector<string> >(siit->first, s_stat));
  }
  return split_info;
}

/** deletes an darray from worker's shared memory region.
 * WE DO NOT RECOMMEND USERS TO CALL THIS FUNCTION 
 * AS A DARRAY HANDLE IN R-SESSION CAN BE VALID AFTER THIS CALL
 * @param da_name: name of darray in shared memory (not a split name)
 * @return NULL
 */
void PrestoMaster::DeleteDobject(string da_name) {
  boost::unordered_map<string, Split*> si = scheduler_->GetSplitInfo();
  boost::unordered_map<string, Split*>::const_iterator sit = si.begin();
  for (; sit != si.end(); ++sit) {
    // iterate splits and find matching names
    if (sit->first.find(da_name) != std::string::npos) {
      scheduler_->DeleteSplit(sit->first);
    }
  }
}

/** It starts the PrestoSession. This function starts PrestoMaster handler and and connect to workers
 * It starts resource manager thread
 * @return NULL
 */
void PrestoMaster::Start(int loglevel, std::string storage) {
  // Initialize zmq server for worker->master communication
  // disable SIGINT to have reliable initialization
  LoggerFilter(loglevel);
  r_sigint_handler = signal(SIGINT, SIG_IGN);

  presto::DATASTORE = (storage == "worker") ? WORKER : RINSTANCE;
  LOG_INFO("Data storage layer in use: %s", getStorageLayer().c_str());

  // This waits until PrestoMasterHandler thread starts (open port and wait for the post message)
  // handler thread will be inited in the PrestoMaster constructor
  try {
    ConnectWorkers(workers_);
  } catch (zmq::error_t err) {
    ostringstream msg;
    msg << "ConnectWorkers: worker ZMQ Error - " << err.what();
    LOG_ERROR(msg.str());
    fprintf(stderr, "ConnectWorkers: worker ZMQ Error - %s\n", err.what());
  }
  // begin resource manager thread
  resource_manager_thread_ = new boost::thread(boost::bind(
    &ResourceManager::Run, res_manager_.get()));
  signal(SIGINT, r_sigint_handler);  // revert to R-sigint handler
}

/** It starts a PrestoMasterHandler thread. This handles incoming messages from workers
 * @return NULL
 */
void PrestoMaster::InitProtoThread() {
  handler_thread_ = new boost::thread(boost::bind(
      &PrestoMasterHandler::Run,
      handler_.get(),
      &zmq_ctx_,
      master_.start_port_range(),
      master_.end_port_range()));
}

/** Make connection to workers
 * @param workers a list of information about workers (hostname and port number)
 * @return NULL
 */
void PrestoMaster::ConnectWorkers(const vector<ServerInfo>& workers) {
  if (CheckIfHandlerRunning() == false) {
    ostringstream msg;
    msg << "Handler thread is not running. Check Port number: " << master_.presto_port();
    throw PrestoShutdownException(msg.str());
  }
  // Disabling handshake message (HELLO) initiation from a master.
  // Instead, a worker will send a hello reply message upon start up.
  // The master address:port is given to a worker using arguments
/*
  unique_lock<mutex> lock(workerinfos_mutex_);
  // The worker_infos_vec imposes an order on the worker infos
  // This is the order in which they are specified in the config file.
  vector<string> worker_infos_vec;

  for (int i = 0; i < workers.size(); ++i) {
    if (workers[i].presto_port() == 0) continue;
    boost::shared_ptr<WorkerInfo> w_info(new WorkerInfo(workers[i].name(),
          workers[i].presto_port(), &zmq_ctx_, i));
    string w_info_key =
      string(w_info->hostname() + ":" + int_to_string(w_info->port()));
    worker_infos_vec.push_back(w_info_key);
    worker_infos.insert(make_pair(i, w_info));

    // Send a hello message
    HelloRequest hello;
    hello.mutable_master_location()->CopyFrom(master_);
    hello.mutable_worker_location()->CopyFrom(workers[i]);
    hello.set_is_heartbeat(false);
    hello.set_reply_attr_flag(
      1 << SERVER_LOCATION |
      1 << SHARED_MEM_QUOTA |
      1 << NUM_EXECUTOR |
      1 << ARRAY_STORES |
      1 << SYS_MEM_TOTAL |
      1 << SYS_MEM_USED);
    w_info->Hello(hello);  // set reply flag to get information from a Reply
//    printf("Opened connection to %s %d\n", w_info->hostname().c_str(),
//            w_info->port());
  }

  lock.unlock();
*/  
  volatile int i;
  boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time()); 
  LOG_INFO("Master awaiting HELLO handshaking with Workers.");
  for (i = 0; i < workers.size(); i++) {
    if (i == 0) {
      fprintf(stdout, "Workers registered - 0/%lu. Will wait upto %d seconds.",
        workers.size(), WORKER_CONNECT_WAIT_SECS);
      fflush(stdout);
    }
    bool ret = workerinfos_sema_.timed_wait(t + boost::posix_time::seconds(WORKER_CONNECT_WAIT_SECS));
    if (ret == false) break;
    char out_msg[1024];
    memset(out_msg, 0x00, sizeof(out_msg));
    sprintf(out_msg, "\rWorkers registered - %d/%lu.",(i+1), workers.size());
    if ((i+1) < workers.size()) {
      sprintf(out_msg, "%s Will wait upto %d seconds.", out_msg, WORKER_CONNECT_WAIT_SECS);
    } else {
      sprintf(out_msg, "%s                                      ", out_msg);
    }
    fprintf(stdout, "%s", out_msg);
    fflush(stdout);
  }
  fprintf(stderr,"\n");
  boost::unordered_map<std::string, Worker*> reg_workers = scheduler_->GetWorkerInfo();
  if(i == workers.size()) {
    fprintf(stderr, "All %lu workers are registered.\n", workers.size());
    LOG_INFO("All %d workers are registered. Master Started.", workers.size());
  } else {    
    if(reg_workers.size() == 0) {
      map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator wit = worker_infos.begin();
      for(; wit != worker_infos.end(); ++wit) {
        wit->second.reset();
      }
      worker_infos.clear();
      throw PrestoShutdownException("No workers are registered");
    } else {
      fprintf(stderr, "Using only %lu workers. Check with distributedR_status().\n\nNot registered workers\n",
        reg_workers.size());
      LOG_WARN("Only %d workers are registered. Check with distributedR_status()", reg_workers.size());
      // At this point, the order of worker_infos_vec and worker_infos is in-sync

      for (int j = 0; j < workers.size(); ++j) {
        bool registered = false;
        // We have to compare only the address part (excluding port number!!!)
        // as a worker can be assigned a different available port number
        // One worker in one machine per one master is enforced.
        string cand_addr = workers[j].name();
        LOG_INFO("Checking non-registered workers - %s %s", cand_addr.c_str());
        boost::unordered_map<std::string, Worker*>::iterator it;
        for (it = reg_workers.begin(); it != reg_workers.end(); ++it) {
          LOG_INFO("Comparing with the registered worker - %s\n", it->second->server.name().c_str());
          if (cand_addr.compare(it->second->server.name()) == 0) {
            registered = true;
            break;
          }
        }
        if (registered == false) {
          // clear workers info that is not registered
 //         worker_infos[j].reset();
 //         worker_infos.erase(j);
          fprintf(stderr, "%s\n", cand_addr.c_str());
          LOG_INFO("Master started.");
        }
      }
      fprintf(stderr, "Check log files (/tmp/R_worker_...) in each node.\n");
    }
  }
  
  boost::unordered_map<std::string, Worker*>::iterator wit;
  for (wit = reg_workers.begin(); wit != reg_workers.end(); ++wit) {
    res_manager_->SendHello(wit->second->workerinfo);
  }
  fflush(stderr);
}

/** Handles a hello reply message from workers. 
 * After extracting worker information, a semaphore is posted
 * @param reply HelloReply messge
 * @return NULL
 */
void PrestoMaster::HandleHelloReply(HelloReplyRequest reply) {
  ServerInfo worker_addr = *reply.mutable_location();
  if (worker_addr.name().size() == 0) {
    LOG_WARN("Invalid worker registration detected (no worker address)");
    fprintf(stderr, "Invalid worker registration detected (no worker address)\n");
    return;
  }
  string key = server_to_string(worker_addr);
  if (reply.is_heartbeat() == false) {
    unique_lock<mutex> lock(workerinfos_mutex_);
    // If this worker is not in the table, ignore it
    // iterate the map
    WorkerInfo *wi = NULL;
    map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator wit = worker_infos.begin();
    for (; wit != worker_infos.end(); ++wit) {
      if(key.compare(wit->second->get_hostname_port_key()) == 0) {
        wi = wit->second.get();
        break;
      }
    }
    if (NULL == wi) {
      boost::shared_ptr<WorkerInfo> w_info(new WorkerInfo(worker_addr.name(),
            worker_addr.presto_port(), &zmq_ctx_, worker_infos.size()));
      worker_infos.insert(make_pair(worker_infos.size(), w_info));
      wi = w_info.get();
    }
    vector<ArrayStoreData> array_stores;
    get_vector_from_repeated_field<
      RepeatedPtrField<ArrayStoreData>::const_iterator,
      ArrayStoreData>(reply.array_stores().begin(),
                      reply.array_stores_size(), &array_stores);
    scheduler_->AddWorker(wi, reply.shared_memory(),
                          reply.executors(), array_stores);
    LOG_INFO( "Worker %s:%d registered. Number of executors: %d; Shared Memory Segment: %zu", 
              wi->hostname().c_str(), wi->port(), reply.executors(), reply.shared_memory());
    scheduler_->UpdateWorkerMem(key, reply.mem_total(), reply.mem_used());
    lock.unlock();
    workerinfos_sema_.post();
  } else {
    scheduler_->UpdateWorkerMem(key, reply.mem_total(), reply.mem_used());
  }
  scheduler_->RefreshWorker(key);
}

/** Parses Presto worker and master information
 * XML sample: $PRESTO_HOME/workers.xml
 * @ param config a file path of configuration file
 * @param master an output parameter where parsed master information willl be written
 * @param workers a list of workers information (hostname and port number)
 * @return a number of workers
 */
int32_t PrestoMaster::ParseXMLConfig(const string& config,
    ServerInfo* master,
    vector<ServerInfo>* workers) {
  property_tree::ptree pt;
  try {
    read_xml(config, pt, boost::property_tree::xml_parser::no_comments);
  } catch(...) {
      throw PrestoShutdownException("While parsing XML configuration file. Check file permissions and content.\n");
  }

  property_tree::ptree master_conf = pt.get_child("MasterConfig");
  property_tree::ptree server_conf = master_conf.get_child("ServerInfo");
  try {
    string master_hostname = server_conf.get_child("Hostname").data();
    presto::strip_string(master_hostname);
    master->set_name(master_hostname);
  } catch (...) {
    throw PrestoShutdownException("Cluster configuration error.\n"
      "Under ServerInfo (master node) field, the hostname of a master node has to specified with <Hostname> element.");
  }
  try {
    string master_start_port = server_conf.get_child("StartPortRange").data();
    presto::strip_string(master_start_port);
    master->set_presto_port(atoi(master_start_port.c_str()));
  } catch (...) {
    throw PrestoShutdownException("Cluster configuration error.\n"
      "Under ServerInfo (master node) field, <StartPortRange> element has to be specified to determine the port number range");
  }
  master->set_start_port_range(atoi(server_conf.get_child("StartPortRange").data().c_str()));
  try {
    string master_end_port = server_conf.get_child("EndPortRange").data();
    presto::strip_string(master_end_port);
    master->set_end_port_range(atoi(master_end_port.c_str()));    
  } catch (...) {
    throw PrestoShutdownException("Cluster configuration error.\n"
      "Under ServerInfo (master node) field, <EndPortRange> element has to be specified to determine the port number range");
  }

  property_tree::ptree workers_conf;   
  try {
      workers_conf = master_conf.get_child("Workers");
    } catch (...) {
      throw PrestoShutdownException("Cluster configuration error. Could not parse Workers field.\n");
  }
  for (property_tree::ptree::iterator it = workers_conf.begin();
       it != workers_conf.end(); it++) {
    property_tree::ptree info = it->second;
    ServerInfo worker;
    try {
      string worker_name = info.get_child("Hostname").data();
      presto::strip_string(worker_name);
      worker.set_name(worker_name);      
    } catch (...) {
      throw PrestoShutdownException("Cluster configuration error.\n"
        "Under Worker field, the hostname of a worker node has to specified with <Hostname> element.");
    }
    try {
      string worker_start_port = info.get_child("StartPortRange").data();
      presto::strip_string(worker_start_port);
      worker.set_presto_port(atoi(worker_start_port.c_str()));
    } catch (...) {
      throw PrestoShutdownException("Cluster configuration error.\n"
        "Under Worker field, <StartPortRange> element has to be specified to determine the port number range");
    }
    worker.set_start_port_range(atoi(info.get_child("StartPortRange").data().c_str()));
    try {
      string worker_end_port = info.get_child("EndPortRange").data();
      presto::strip_string(worker_end_port);
      worker.set_end_port_range(atoi(worker_end_port.c_str()));      
    } catch (...) {
      throw PrestoShutdownException("Cluster configuration error.\n"
        "Under Worker field, <EndPortRange> element has to be specified to determine the port number range");
    }
    workers->push_back(worker);
  }

  return workers->size();
}
ddc::WorkerSelector PrestoMaster::worker_selector() const
{
    return worker_selector_;
}

/**
 * Create a worker map. This map includes information about the workers
 * (hostname, port and number of executors).
 */
Rcpp::List PrestoMaster::WorkerMap() {
    std::map<std::string, std::vector<size_t> > workerExtraInfo = GetWorkerStatus();

    typedef map<int, boost::shared_ptr<WorkerInfo> >::iterator it_type;

    Rcpp::List workerMap;
    for(it_type it = worker_infos.begin(); it != worker_infos.end(); ++it) {
        ::uint64_t numExecutors = 1;  // default to 1 executor per worker
        std::string fullWorkerId = "";
        fullWorkerId += (it->second)->hostname();
        fullWorkerId += ":";
        std::ostringstream os ;
        os << (it->second)->port();
        std::string portStr = os.str() ;
        fullWorkerId += portStr;
        std::map<std::string, std::vector<size_t> >::iterator it2 = workerExtraInfo.find(fullWorkerId);
        if (it2 != workerExtraInfo.end()) {
            std::vector<size_t> v = it2->second;
            if (v.size() > 0) {
                numExecutors = v[0];  // num executors is elem 0
            }
        }

        Rcpp::List w;
        w["hostname"] = (it->second)->hostname();
        w["port"] = (it->second)->port();
        w["num_executors"] = numExecutors;
        std::ostringstream os2;
        os2 << it->first;
        workerMap[os2.str()] = w;
    }
    return workerMap;
}

/** Check if a master handler thead is running
 * @return return True if a handler is running. Otherwise, false
 */
bool PrestoMaster::CheckIfHandlerRunning(int wait_time_sec) {
  bool ret = false;
  if (handler_thread_== NULL) {
    throw PrestoWarningException("CheckIfHandlerRunning - handler thread is NULL");
  }  
  try {
    ret = handler_thread_->try_join_for(boost::chrono::seconds(wait_time_sec));
  } catch (...) {}
  return !ret;
}

/** Check if a master address in the configuration file is same as the real master address. Check is workers have duplicate entries.
 * @return return True if the master and worker addresses seems to be correct. Otherwise, return false and print an warning message
 */
bool PrestoMaster::CheckMasterWorkerAddrSanity() {
  string master_conf = master_.name();
  vector<string> ips = get_ipv4_addresses();
  string master_ip_address;
  bool result = false;
  if (master_.end_port_range() > 65536 || master_.start_port_range() < 1024) {
    fprintf(stderr, "[WARNING] The master node port range should be larger than 1024 (StartPortRange) and less than 65536 (EndPortRange).\n");
  }
  // there should be at least two ports - one master handler and one large chunk data transfer
  if ((master_.end_port_range() - master_.start_port_range() + 1) < 2) {
    throw PrestoShutdownException
      ("The master node does not have enough port numbers.\n"
       "At least two port numbers have to be available between StartPortRange and EndPortRange.");
  }
  /*for(int i = 0;i<ips.size();++i) {
    if(ips[i].find(master_conf) != std::string::npos) {
      return true;
    }
    }*/


  //LOG_INFO("Master hostname: '%s'", master_conf.c_str());
  hostent* master_record = gethostbyname(master_conf.c_str());
  if(master_record == NULL) {
    fprintf(stderr, "[WARNING] Could not map master hostname (%s) to ip address for configuration checks.\n", master_conf.c_str());
    return false;
  } else {
    in_addr* address = (in_addr*)master_record->h_addr;
    master_ip_address = inet_ntoa(* address);
    for(int i = 0;i<ips.size();++i) {
      if(ips[i].find(master_ip_address) != std::string::npos) {
	result = true;
      }
    }
  }
  if(!result){
    fprintf(stderr, "[WARNING] The master node address in the configuration seems to be different from the IP address of this server.\n"
    "Please check the hostname on cluster configuration file: %s\n", master_conf.c_str());
  }

  // Check if there is duplicate worker in a config file
  set<string> worker_addrs;
  vector<ServerInfo>::iterator it;
  hostent* record;
  // per each worker, there should be two ports for data loader and fetch thread for every other worekrs.
  // One port to have a messaging server
  int req_port_range = (workers_.size() * 2) + 1; 
  for (it = workers_.begin(); it != workers_.end();) {
    string worker_hostname = it->name();
    //remove whitespace and other problematic chars

    if (it->end_port_range() > 65536 || it->start_port_range() < 1024) {
      fprintf(stderr, "[WARNING] Worker node's (%s) port number should be larger than 1024 (StartPortRange) and less than 65536 (EndPortRange).\n", worker_hostname.c_str());
    }
    if ((it->end_port_range() - it->start_port_range() + 1) < req_port_range) {
      ostringstream msg;
      msg << "Worker " << worker_hostname << " does not have enough port numbers." << endl
          << "At least " << req_port_range << " port numbers have to be available between StartPortRange and EndPortRange.";
      throw PrestoShutdownException(msg.str());      
    }

    //LOG_INFO("Worker hostname: '%s'", worker_hostname.c_str());
    record = gethostbyname(worker_hostname.c_str());
    if(record == NULL) {
      fprintf(stderr, "[WARNING] Could not map worker hostname (%s) to ip address for configuration checks.\n", worker_hostname.c_str());
      ++it;
    } else {
      in_addr* address = (in_addr*)record->h_addr;
      string ip_address = inet_ntoa(* address);

      //If the master and worker are on the same node, check availability of ports when ranges overlap
      if(master_ip_address.compare(ip_address)==0){
	bool violation=false;
	if( (master_.end_port_range() >= it->start_port_range()) && (master_.start_port_range() <= it->end_port_range())){
	  //Now we are sure that master and worker port ranges overlap
	  int overlap = std::min(master_.end_port_range(),it->end_port_range()) - std::max(master_.start_port_range(),it->start_port_range()) +1;
	  //Does the worker have enough ports if the master picks ports from the overlap region?
	  int residual = (overlap -2) > 0 ?  (overlap-2) : 0;
	  if((it->end_port_range() - it->start_port_range() + 1 -overlap + residual) < req_port_range) violation = true;
	  //Does the master have enough ports if the worker picks ports from the overlap region?
	  residual = (overlap - req_port_range) > 0 ?  (overlap- req_port_range) : 0;
	  if((master_.end_port_range() - master_.start_port_range() + 1 -overlap + residual) < req_port_range) violation = true;
	}
	if(violation){
	  fprintf(stderr, "[WARNING] Master and worker on %s have overlapping port ranges, and may contend for ports. Need 2 ports for Master, %d for Worker.\n", worker_hostname.c_str(), req_port_range);
	}
      }

      if (worker_addrs.count(ip_address) > 0) { // already registered worker
	ostringstream msg;
	msg << "Remove duplicate worker " << worker_hostname << ":" << it->presto_port() <<" in configuration file, and retry." << endl;
	throw PrestoShutdownException(msg.str());      
      } else {
	worker_addrs.insert(ip_address);
	++it;
      }
    }
  }

  if ((worker_addrs.count("127.0.0.1") > 0) && (workers_.size() >1)) { // localhost or 127.0.0.1 used in multi-worker configuration file
   ostringstream msg;
    msg << "Specify IP address of Worker instead of localhost (127.0.0.1) in the configuration file." << endl;
    throw PrestoShutdownException(msg.str());  
  }
  
  if ((master_ip_address.compare("127.0.0.1")== 0) && ((workers_.size()>1) || (workers_.size()==1 && worker_addrs.count("127.0.0.1")==0))) { // localhost used in multi-server case
   ostringstream msg;
    msg << "Specify IP address of Master instead of localhost (127.0.0.1). Change 'ServerInfo' in the configuration file." << endl;
    throw PrestoShutdownException(msg.str());  
  }

  return result;
}


bool PrestoMaster::StartDataLoader(::uint64_t split_size, string split_prefix) {
  if(NULL != dataloader_manager_) {
    fprintf(stderr, "<DataLoader> A session of data loading is already running.\nPlease wait for it to finish before starting another load process.\n");
    return false;
  }
  dataloader_manager_ = new DataLoaderManager(this, split_size);
  for(map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator itr = worker_infos.begin();
      itr!=worker_infos.end(); ++itr) {
    WorkerInfo* wi = itr->second.get();
    scheduler_->InitiateDataLoader(wi, split_size, split_prefix);
  }

  for(int i = 0; i<worker_infos.size(); i++)
    dataloader_manager_->LoaderSemaWait();

  if(NULL != dataloader_manager_ && IsRunning()) {
    if (dataloader_manager_->InvalidPorts()) {
       fprintf(stderr, "<DataLoader> One of the Worker could not open ports for loading data from Vertica.\nPlease check Worker log files for more informaton.\n");
       StopDataLoader();
       return false;
    }
  } else {
    fprintf(stderr, "<DataLoader> Vertica Connector process could not be initiated. This may be caused by Distributed R shutdown.\n");
    return false;
  }

  return true;
}


void PrestoMaster::StopDataLoader() {

  LOG_INFO("<DataLoader> Stopping Data Loader");
  for(map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator itr = worker_infos.begin();
      itr!=worker_infos.end(); ++itr) {
    WorkerInfo* wi = itr->second.get();
    scheduler_->StopDataLoader(wi);
  } 

  if (NULL != dataloader_manager_ && IsRunning()) {
    delete dataloader_manager_;
    dataloader_manager_ = NULL;
    LOG_DEBUG("<DataLoader> Stopped Data Loader Manager");
  }
}

void PrestoMaster::SetResMgrInterrupt(volatile bool* var_addr) {
  res_manager_interrupted = var_addr; 
}
}  // namespace presto


RCPP_MODULE(master_module) {
  class_<presto::PrestoMaster>("PrestoMaster")
    .constructor<string>()
    .method("get_num_workers", &presto::PrestoMaster::NumClients)
    .method("worker_hosts", &presto::PrestoMaster::WorkerHosts)
    .method("worker_start_port_range", &presto::PrestoMaster::WorkerStartPortRange)
    .method("worker_end_port_range", &presto::PrestoMaster::WorkerEndPortRange)
    .method("start", &presto::PrestoMaster::Start)
    .method("shutdown", &presto::PrestoMaster::Shutdown)
    .method("presto_status", &presto::PrestoMaster::GetWorkerStatus)
    .method("presto_ls", &presto::PrestoMaster::GetDobjectInfo)
    .method("get_master_addr", &presto::PrestoMaster::GetMasterAddr)
    .method("get_master_port", &presto::PrestoMaster::GetMasterPort)
    .method("running", &presto::PrestoMaster::IsRunning)
    .method("start_dataloader", &presto::PrestoMaster::StartDataLoader)
    .method("stop_dataloader", &presto::PrestoMaster::StopDataLoader)
    .method("worker_map", &presto::PrestoMaster::WorkerMap)
    .method("ddc_set_chunk_worker_map", &presto::PrestoMaster::DdcSetChunkWorkerMap)
      ;  // NOLINT(whitespace/semicolon)
}

int main(int argc, char *argv[]) {
  char cmd[20000];
  size_t in = fread(cmd, 1, 20000, stdin);
  cmd[in] = '\0';

  optind--;  // need this for args to be passed correctly
  RInside R(argc, argv);
  R.parseEvalQ(cmd);
}
