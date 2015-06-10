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

/**
 * Class that handles a request from a Worker to Master
 * This class is thread-safe as multiple worker threads invoke the Run
 * function.
 */ 

#ifndef _DATA_LOADER_
#define _DATA_LOADER_

#include <map>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread.hpp>
#include <boost/threadpool.hpp>

#include "common.h"
#include "dLogger.h"
#include "PrestoWorker.h"

using namespace std;
using namespace boost::threadpool;

namespace presto {

class PrestoWorker;

struct Buffer {
  char* buf;
  size_t size;
  size_t buffersize;
};

struct Metadata {
  uint64_t size;
  uint32_t nrows;
  bool isEOF;
  char vNode_name[100];
};

struct DLStatus {
  bool transfer_complete_;
  bool transfer_success_;
  std::string transfer_error_msg_;
};

class DataLoader {
 public:
  DataLoader(PrestoWorker *presto_worker, int port, int32_t sock_fd, ::uint64_t split_size, string split_prefix);
  ~DataLoader();

  void Run();
  void ReadFromVertica(int32_t new_fd);
  std::string getNextShmName();
  void CreateCsvShm(std::string shm_name, ::uint64_t size);
  void AppendDataBytes(const char* shm_file, ::uint64_t size, uint32_t nrows);
  std::pair<DLStatus, ::uint64_t> SendResult(std::vector<std::string> qry_result);
  void Flush();
  bool IsTransferComplete() {
    return result.transfer_complete_;
  }
  void RegisterTransferError(const char* error_msg);
  DLStatus result;

 private:
  PrestoWorker *presto_worker_;
  int port_;
  int32_t sock_fd_;
  uint64_t total_data_size;
  uint64_t total_nrows;
  uint64_t file_id; 
  uint64_t DR_partition_size;
  string split_prefix_;
  
  std::map<std::string, int> vnode_EOFs;
  
  Buffer buffer;
  pool read_pool;
  boost::recursive_mutex metadata_mutex_;
  boost::mutex shm_name_mutex_;
  boost::mutex metadata_update_mutex_;
};

}// namespace

#endif  // _DATA_LOADER_
