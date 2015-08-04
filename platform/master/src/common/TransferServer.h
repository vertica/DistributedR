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
 * Class to setup Server socket for transfer and to parse / create a new
 * array
 */

#ifndef _TRANSFER_SERVER_
#define _TRANSFER_SERVER_

#include <stdio.h>
#include <semaphore.h>
#include <pthread.h>

#include <boost/function.hpp>
#include <string>
#include <vector>

#include "WorkerInfo.h"

namespace presto {

extern void* worker_transfer_pthread(void* ptr);
extern void* R_transfer_pthread(void* ptr);

class TransferServer {
 public:
  TransferServer(StorageLayer source, StorageLayer destination, int start_port = 50000, int end_port = 50100, void* addr = NULL, size_t size = 0) :
    start_port_range_(start_port), end_port_range_(end_port),
    source_(source), destination_(destination) {
    dest_= addr;
    size_= size;
  }

  ~TransferServer() {
    if(dest_!=NULL)
      free(dest_);
  }

 std::pair<void*, int64_t> transfer_blob(const string &name,
                             WorkerInfo* client, const string& myhostname,
                             const string &store, uint64_t taskid = 0);

  void* worker_transfer_server(void);
  void* R_transfer_server(void);

  int64_t bytes_fetched() {
    return bytes_fetched_;
  }

 private:
  vector<double>* vec_;
  int64_t nnz_;
  int64_t bytes_fetched_;
  bool sparse_;
  string name_;
  int32_t server_socket_port;
  int32_t serverfd;

  vector<int>* i_vals_;
  vector<int>* j_vals_;
  vector<double>* x_vals_;

  void *dest_;  // a buffer where the transferred data will be written
  size_t size_;
  StorageLayer source_;
  StorageLayer destination_;  
  int start_port_range_;
  int end_port_range_;
  sem_t server_ready;
  bool error_in_transfer_thread;  //communicates error from transfer server to the creating thread. Non-zero means error.
};

}  // namespace presto

#endif  // _TRANSFER_SERVER_
