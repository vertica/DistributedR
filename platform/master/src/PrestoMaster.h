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
 * Main class for Presto master module. Responsible initialization of
 * workers and maintaining information of worker state.
 */

#ifndef _PRESTO_MASTER_
#define _PRESTO_MASTER_

#include <string>
#include <vector>

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/bind.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

#include <Rcpp.h>

#include <zmq.hpp>

#include "shared.pb.h"
#include "master.pb.h"
#include "common.h"
#include "PrestoException.h"
#include "dLogger.h"
#include "DataLoaderManager.h"

#ifdef PERF_TRACE
#include <ztracer.hpp>
#endif

#include "DdcWorkerSelector.h"

//using namespace boost;
using namespace Rcpp;
using namespace std;
using namespace zmq;

#define NUM_CTX_THREADS 1
#define NUM_PROTO_EXEC_THREADS 10

typedef void (*sighandler_t)(int nSig);

namespace presto {

class DistributedObjectMap;
class PrestoMasterHandler;
class Scheduler;
class WorkerInfo;
class ResourceManager;
class DataLoaderManager;

class PrestoMaster {
 public:
  explicit PrestoMaster(const string& config_file);
  ~PrestoMaster();

  void Start(int loglevel, std::string storage_layer);
  vector<string> WorkerHosts();
  vector<int32_t> WorkerPorts();
  vector<int32_t> WorkerStartPortRange();
  vector<int32_t> WorkerEndPortRange();
  map<string, vector<size_t> > GetWorkerStatus();
  map<string, vector<string> > GetDobjectInfo();
  pair<int, int> GetMasterPortRange(){
    return make_pair<int, int>(master_.start_port_range(), master_.end_port_range());
  }

  void DeleteDobject(string da_name);

  /** Check if this PrestoMaster session is alive
   *  @return true if this session is not shutdown
   */
  bool IsRunning() {
    return is_running_;
  }
  string GetMasterAddr() {
    return master_.name();
  }
  
  string GetMasterPort() {
    return int_to_string(master_.presto_port());
  }

  bool StartDataLoader(uint64_t split_size, string split_prefix);

  void StopDataLoader();

  WorkerInfo* GetClientInfo(int id) {
    if (worker_infos.count(id) != 0){
      return worker_infos[id].get();
    } else {
      ostringstream msg;
      msg << "GetClientInfo: Invalid ID: " << id;
      throw PrestoWarningException(msg.str());
    }
  }

  WorkerInfo* GetClientInfoByIndex(int index) {
    if (NumClients() <= index) {
      ostringstream msg;
      msg << "GetClientInfoByIndex: Invalid index: " << index <<
        "Number of workers: " << NumClients();
      throw PrestoWarningException(msg.str());
    }
    map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator itr =
      worker_infos.begin();
    advance(itr, index);
    if (itr == worker_infos.end()) {
      throw PrestoWarningException
        ("GetClientInfoByIndex: We cannot locate the index");
    } else {
      return itr->second.get();
    }
  }


/*
Release WorkerInfo resources to enable garbage-collection
*/
  void ClearClientInfo() {
    map<int32_t, boost::shared_ptr<WorkerInfo> >::iterator it;
    for (it = worker_infos.begin();
        it != worker_infos.end(); ++it) {
      boost::shared_ptr<WorkerInfo> wi = it->second;
      wi.reset();
    }
    worker_infos.clear();
  }

  int32_t NumClients() {
    return worker_infos.size();
  }

  DistributedObjectMap* GetDistributedObjectMap() {
    return dobject_map_.get();
  }


  Scheduler* GetScheduler() {
    return scheduler_;
  }

  DataLoaderManager* GetDataLoader() {
    return dataloader_manager_;
  }

  void HandleHelloReply(HelloReplyRequest hello);

  void Shutdown();

  bool CheckIfHandlerRunning(int wait_time_sec = 1);

  void WorkerInfoSemaPost() {workerinfos_sema_.post();}

  void SetMasterPortNum(int port_num) {master_.set_presto_port(port_num);}
  void SetResMgrInterrupt(volatile bool*);
  void SetLargeChunkServerThread(boost::thread* ptr) {lrg_chunk_trnfr_thr_ptr = ptr;}

  void DdcSetChunkWorkerMap(const Rcpp::List& chunkWorkerMap) {
      ddc::ChunkWorkerMap chunkWorkerMapCpp;
      for (uint64_t i = 0; i < chunkWorkerMap.size(); ++i) {
          chunkWorkerMapCpp[i] = chunkWorkerMap[i];
      }
      worker_selector_.setChunkWorkerMap(chunkWorkerMapCpp);
  }

  ddc::WorkerSelector worker_selector() const;

  Rcpp::List WorkerMap();

private:
  void InitProtoThread();
  void ConnectWorkers(const vector<ServerInfo>& workers);
  bool CheckMasterWorkerAddrSanity();

  int32_t ParseXMLConfig(const string& config, ServerInfo* master,
      vector<ServerInfo>* workers);

  // ZeroMQ context used for creating sockets.
  context_t zmq_ctx_;

  boost::shared_ptr<boost::thread> proto_bind_thread_;

  boost::shared_ptr<PrestoMasterHandler> handler_;
  boost::thread *handler_thread_;
  boost::shared_ptr<ResourceManager> res_manager_;
  boost::thread *resource_manager_thread_;

  // This map is used to get to the actual WorkerInfo object.
  // the key is ID of a worker (starting from 0) and 
  // determined based on the order in the XML file
  map<int32_t, boost::shared_ptr<WorkerInfo> > worker_infos;
  ServerInfo master_;
  vector<ServerInfo> workers_;
  // darray_map_ keeps tack of darray string name with its object
  boost::scoped_ptr<DistributedObjectMap> dobject_map_;

  Scheduler* scheduler_;
  DataLoaderManager* dataloader_manager_;

  boost::mutex workerinfos_mutex_;
  boost::interprocess::interprocess_semaphore workerinfos_sema_;
  boost::timed_mutex  is_running_mutex_;
  bool is_running_;
  volatile bool* res_manager_interrupted;
  boost::thread* lrg_chunk_trnfr_thr_ptr;

  ddc::WorkerSelector worker_selector_;

};
}  // namespace presto
#endif  // _PRESTO_MASTER_
