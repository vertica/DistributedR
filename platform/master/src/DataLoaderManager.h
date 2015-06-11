/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2014] Hewlett-Packard Development Company, L.P.

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

#ifndef _DATA_LOADER_MANAGER_
#define _DATA_LOADER_MANAGER_

#include <Rcpp.h>
#include <vector>

#include "Scheduler.h"
#include "PrestoMaster.h"

class PrestoMaster;
class Scheduler;

using namespace std;
using namespace Rcpp;

namespace presto {  

class DataLoaderManager {

  public:

  DataLoaderManager(
  PrestoMaster* presto_master, int split_size) :
  presto_master_(presto_master),
  loader_sema_(0), split_size_(split_size), 
  invalid_ports(false), transfer_success_(true) {
    
    totalPartitions = 0;
    partitions_worker_id_map_.clear();
    loader_workers_.clear();    
    file_id_map_.clear();
    worker_port_info.clear();
    transfer_error_message_ = std::string("");
  }

  ~DataLoaderManager();

  void HandlePortAssignment(WorkerInfo* worker, int loader_port);

  void LoaderSemaWait() { loader_sema_.wait(); }
  void LoaderSemaPost() { loader_sema_.post(); }

  SEXP GetLoaderParameterUniform();
  SEXP GetLoaderParameterLocal(SEXP table_metadata);
  SEXP FetchLoaderStatus(SEXP UDx_result);
  SEXP getNPartitions();
  SEXP getFileIDs();

  void WorkerLoaderComplete(WorkerInfo* workerinfo, int64_t nparitions,
                            int transfer_success, std::string transfer_message);
  void LoadComplete();

  boost::unordered_map<int32_t, uint64_t> GetPartitionMap() {
    return partitions_worker_id_map_;
  }

  vector<int32_t> GetLoaderWorkers() {
    return loader_workers_;
  }

  bool InvalidPorts() {
    return invalid_ports;
  }

  private:

  int split_size_;
  int totalPartitions;
  bool invalid_ports;
  boost::unordered_map<int32_t, uint64_t> partitions_worker_id_map_;
  vector<int32_t> loader_workers_;
  map<int32_t, int32_t> file_id_map_;
  map<WorkerInfo*, int> worker_port_info;

  bool transfer_success_;
  std::string transfer_error_message_; 

  PrestoMaster* presto_master_;

  boost::interprocess::interprocess_semaphore loader_sema_;

  boost::mutex loader_mutex;
};

}

#endif  // _DATA_LOADER_MANAGER_
