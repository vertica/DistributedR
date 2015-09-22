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
#include "ArrayData.h"

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
std::pair<void*, int64_t> TransferServer::transfer_blob(const string &name, WorkerInfo* client,
                                    const string& myhostname, const string& store, uint64_t taskid) {
  // Setup the transfer
  //this->dest_ = addr;
  //this->size_ = size;
  this->name_ = name;
  
  bytes_fetched_ = 0;
  error_in_transfer_thread= false;
  // Initialize semaphore to zero. Server thread will increment
  // when it is ready
  sem_init(&server_ready, 0, 0);

  // Setup a receiving thread.
  // Later, we will send a Fetch request,
  // and the data will arrive to this thread
  pthread_t server_thread;
  
  if (source_ == WORKER)
    pthread_create(&server_thread, 0, worker_transfer_pthread, this);
  else
    pthread_create(&server_thread, 0, R_transfer_pthread, this);
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
  req.set_name(name);
  req.set_uid(taskid);
  if (source_ == WORKER)
    req.set_policy(FetchRequest::WORKER_CONN);
  else
    req.set_policy(FetchRequest::R_CONN);

  if (!store.empty()) {
    req.set_store(store);
  }
  client->NewTransfer(req);  // request transfer to remote workers

  void* server_ret;
  pthread_join(server_thread, &server_ret);
  sem_destroy(&server_ready);

  if(error_in_transfer_thread) {
    forward_exception_to_r(PrestoWarningException
       ("Master failed to fetch data from Worker. Check master logs for more information."));
  }

  return std::pair<void*, int64_t>(dest_, bytes_fetched_);
  // TODO(erik): error handling

// } else {
//     // Cancel the server thread if transfer failed.
//     // TODO(shivaram): Check if this behaves correctly or use signals.
//     fprintf(stderr, "Transfer failed, cancelling server\n");
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
void* TransferServer::worker_transfer_server(void) {
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

  //Get size of split
  char split_size[24];
  memset(split_size, 0x00, 24);
  int rbytes = recv(new_fd, split_size, sizeof(split_size), 0);
  size_ = (size_t)(atoll(split_size));

  if(destination_ == WORKER) {
    SharedMemoryObject shm(
      boost::interprocess::open_or_create, name_.c_str(),
      boost::interprocess::read_write);  // create shared memory region to write
    shm.truncate(size_);  // allocate region
    boost::interprocess::mapped_region region(shm,
      boost::interprocess::read_write);
    void* dest_ = region.get_address(); // address where the data will be written

    bytes_fetched_ += (size_);
    atomicio(read, new_fd, dest_, size_);
    dest_ = NULL;

    LOG_DEBUG("worker_transfer_server: Transfered to a Shared memory file %s (size: %zu)", name_.c_str(), size_);
  } else {
    if(dest_ == NULL) {
      dest_ = malloc(size_);
      if(dest_ == NULL) {
        LOG_ERROR("worker_transfer_server: Failed to allocate buffer of size %zu", size_);
        error_in_transfer_thread = true;
        return reinterpret_cast<void*>(&ret);
      }
    }

    LOG_DEBUG("worker_transfer_server: Transferred to temporary buffer of size %zu", size_);
    bytes_fetched_ += (size_);
    atomicio(read, new_fd, dest_, size_);
  }

#ifdef PROFILE_TRANSFER_TIME
  clock_gettime(CLOCK_REALTIME, &end_t);
  recv_t = diff_clocktime(&start_t, &end_t);
#endif

  close(new_fd);
  close(serverfd);
  ret = 0;
  return &ret;
}


/** A thread that receives R object from remote executor.
 * @return If the action succeed, it will return 0. Otherwise, socket descriptor.
 */
void* TransferServer::R_transfer_server(void) {
  static int32_t ret = -1;
  LOG_DEBUG("R_transfer_server started - %u", pthread_self());
  try {
    serverfd = CreateBindedSocket(start_port_range_, end_port_range_, &server_socket_port);
  } catch (...) {
    LOG_ERROR("R_transfer_server - fail to open/bind a socket.");
    error_in_transfer_thread = true;
    sem_post(&server_ready);
    return reinterpret_cast<void*>(&ret);
  }
  LOG_DEBUG("R_transfer_server socket bind complete. binded port: %u %d", pthread_self(), server_socket_port);
  int32_t new_fd = -1;
  struct sockaddr_in their_addr;
  socklen_t sin_size = sizeof(their_addr);
  ret = listen(serverfd, MAX_CONN);
  if (ret < 0) {
    LOG_ERROR("R_transfer_server - socket listen failed.");
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

  // Read size of split
  char split_size[128];
  memset(split_size, 0x00, 128);
  int rbytes = read(new_fd, split_size, sizeof(split_size)-1);
  size_ = (size_t)(atoi(split_size));
  LOG_INFO("R_transfer_server: Size of split transferred(%zu)", size_);

  if(destination_ == WORKER) {
    LOG_ERROR("Cannot send data partition from an R instance to a Worker");
    error_in_transfer_thread = true;
    return reinterpret_cast<void*>(&ret);
  } else {
    dest_ = (void*)malloc(size_);
    LOG_INFO("R_transfer_server: Transferred to buffer(%zu)", size_);
  }

  int64_t type = BINARY;
  memcpy(dest_, &type, sizeof(int64_t));
  size_t bytes_read;
  while((bytes_read = atomicio(read, new_fd, ((char*)dest_) + sizeof(int64_t), (size_- sizeof(int64_t)))) > 0) {
    bytes_fetched_ += bytes_read;
    if (bytes_fetched_ <= size_) 
      break;
    else
      dest_ = (void*)realloc((void*)dest_, (bytes_read + size_)); // increment the buffer size by size_ factor
  }

#ifdef PROFILE_TRANSFER_TIME
  clock_gettime(CLOCK_REALTIME, &end_t);
  recv_t = diff_clocktime(&start_t, &end_t);
#endif

  close(new_fd);
  close(serverfd);
  ret = 0;
  return &ret;
}

void* worker_transfer_pthread(void* ptr) {
  return reinterpret_cast<TransferServer*>(ptr)->worker_transfer_server();
}

void* R_transfer_pthread(void* ptr) {
  return reinterpret_cast<TransferServer*>(ptr)->R_transfer_server();
}

}  // namespace presto
