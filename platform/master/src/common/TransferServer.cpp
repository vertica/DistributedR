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

#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "atomicio.h"
#include "common.h"
#include "TransferServer.h"

namespace presto {

/** Fetch a split from remote worker and keep it in the shared memory region
 * @param dest a memory pointer where the split will be written (usually shared mem)
 * @param name a name of split to fetch
 * @param size the size of split to fetch
 * @param client a worker information from where we will fetch the split
 * @param myhostname a name of worker who initiate the transfer request
 * @param store a location other than dram where the split will be written. It can be zero-length
 * @return return code from server
 */
int32_t TransferServer::transfer_blob(void *dest, const string &name,
                                      size_t size, WorkerInfo* client,
                                      const string& myhostname,
                                      const string &store) {
  // Setup the transfer
  this->dest_ = dest;
  this->size_ = size;
  bytes_fetched_ = 0;
  error_in_transfer_thread= false;
  // Initialize semaphore to zero. Server thread will increment
  // when it is ready
  sem_init(&server_ready, 0, 0);

  // Setup a receiving thread.
  // Later, we will send a Fetch request,
  // and the data will arrive to this thread
  pthread_t server_thread;
  pthread_create(&server_thread, 0, transfer_pthread, this);

  // fprintf(stderr,
  //   "Waiting for server to be ready for transfer array %s from %s\n",
  //   a.name.c_str(), a.location.name.c_str());
  // Wait till server is ready
  sem_wait(&server_ready);

  //Check if there was an error in creating the transfer server.
  if(error_in_transfer_thread){
    LOG_ERROR("Error in creating TransferServer."); 
    throw PrestoShutdownException
       ("Master or Worker failed to bind to port for data transfer. Check port availability and restart session.");
  }

  // Information of the requester
  ServerInfo location;
  location.set_name(myhostname);
  location.set_presto_port(server_socket_port);
  // Create a fetch request
  FetchRequest req;
  req.mutable_location()->CopyFrom(location);
  req.set_size(size);
  req.set_name(name);
  if (!store.empty()) {
    req.set_store(store);
  }

  client->NewTransfer(req);  // request transfer to remote workers

  void* server_ret;
  pthread_join(server_thread, &server_ret);
  sem_destroy(&server_ready);
  return *reinterpret_cast<int32_t*>(server_ret);

  // TODO(erik): error handling

// } else {
//     // Cancel the server thread if transfer failed.
//     // TODO(shivaram): Check if this behaves correctly or use signals.
//     fprintf(stderr, "Transfer failed, canceling server\n");
//     sem_destroy(&server_ready);
//     pthread_cancel(server_thread);
//     shutdown(serverfd, SHUT_RDWR);
//     close(serverfd);
//     return -1;
//   }
}

/** A thread that actually receives data from remote worker.
 * @return If the action succeed, it will return 0. Otherwise, socket descriptor.
 */
void* TransferServer::transfer_server_thread(void) {
  static int32_t ret = -1;
  LOG_DEBUG("transfer_server_thread started - %u", pthread_self());
  try {
    serverfd = CreateBindedSocket(start_port_range_, end_port_range_, &server_socket_port);
  } catch (...) {
    LOG_ERROR("transfer_server_thread - fail to open/bind a socket.");
    error_in_transfer_thread = true;
    sem_post(&server_ready);
    return reinterpret_cast<void*>(&ret);
  }
  LOG_DEBUG("transfer_server_thread socket bind complete. binded port: %u %d", pthread_self(), server_socket_port);
  int32_t new_fd = -1;
  struct sockaddr_in their_addr;
  socklen_t sin_size = sizeof(their_addr);
  ret = listen(serverfd, MAX_CONN);
  if (ret < 0) {
    LOG_ERROR("transfer_server_thread - socket listen failed.");
    error_in_transfer_thread = true;
    sem_post(&server_ready);
    return reinterpret_cast<void*>(&ret);
  }

  sem_post(&server_ready);

  // TODO(shivaram): Check if it is okay if we only wait for one connection
  // Also this is blocking ! We need to use a select / libevent call
  // here
  new_fd = accept(serverfd, (struct sockaddr *) &their_addr,
                  &sin_size);
  if (new_fd < 0) {
    ret = new_fd;
    return reinterpret_cast<void*>(&ret);
  }

  uint32_t sz;
  uint32_t size_bytes_read = 0;
  uint64_t recv_t = 0;

#ifdef PROFILE_TRANSFER_TIME
  clock_gettime(CLOCK_REALTIME, &start_t);
#endif

  bytes_fetched_ += (size_);
  atomicio(read, new_fd, dest_, size_);

#ifdef PROFILE_TRANSFER_TIME
  clock_gettime(CLOCK_REALTIME, &end_t);
  recv_t = diff_clocktime(&start_t, &end_t);
#endif

  close(new_fd);
  close(serverfd);
  ret = 0;
  return &ret;
}

void* transfer_pthread(void* ptr) {
  return reinterpret_cast<TransferServer*>(ptr)->transfer_server_thread();
}

}  // namespace presto
