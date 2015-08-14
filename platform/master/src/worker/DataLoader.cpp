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

#include <string>
#include <exception>
#include <netdb.h>

#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "atomicio.h"
#include "DataLoader.h"
#include "common.h"
#include "PrestoException.h"

using namespace std;
using namespace boost;

namespace presto {

/**
 * DataLoader Constructor
 */
DataLoader::DataLoader(
    PrestoWorker* presto_worker, int32_t port, int32_t sock_fd, ::uint64_t split_size, string split_prefix) :
  presto_worker_(presto_worker),
  port_(port), sock_fd_(sock_fd),
  DR_partition_size(split_size),
  split_prefix_(split_prefix) {
  total_data_size = 0;
  total_nrows = 0;
  file_id = 0;
  vnode_EOFs.clear();
  read_pool = pool(10);

  //Initialize buffer
  buffer.buf = NULL;
  if(buffer.buf != NULL) {
    free(buffer.buf);
    buffer.buf = NULL;
  }
  buffer.size = 0;
  buffer.buffersize = 0;

  //Initialize Result
  result.transfer_complete_ = false;
  result.transfer_success_ = true;
  result.transfer_error_msg_.clear();
}


/**
 * DataLoader Destructor
 *
 */
DataLoader::~DataLoader() {
  close(sock_fd_);
  LOG_DEBUG("<DataLoader> Clearing leftover temporary loader files");
  for(int i = 0; i < file_id ; i++) {
    std::ostringstream loader_name;
    loader_name << "/dev/shm/" << split_prefix_ << (i+1);
    std::string temp_shm_file = loader_name.str();
    unlink(temp_shm_file.c_str());
  }

  if(buffer.buf != NULL) {
    free(buffer.buf);
    buffer.buf = NULL;
  }

  vnode_EOFs.clear();
};


/**
 * Validated load and prepares loader results to be send to the Master
 * - Validates EOFs
 * - Returns number of partitions created on this Worker
 * - Frees buffer
 * 
 **/
std::pair<DLStatus, ::uint64_t> DataLoader::SendResult(std::vector<std::string> qry_result) {
   
   read_pool.wait();
   result.transfer_complete_ = true;

   map<std::string, int> result_EOFs;
   result_EOFs.clear();
   for(int i = 0; i<qry_result.size();i++) {
     std::string vnode_name = qry_result[i];
     if(result_EOFs.find(vnode_name) == result_EOFs.end()) {
      result_EOFs[vnode_name] = 1;
    } else {
      ++result_EOFs[vnode_name];
    } 
   }

   if(!(result_EOFs.size() == vnode_EOFs.size() && std::equal(result_EOFs.begin(), result_EOFs.end(), vnode_EOFs.begin()))) 
     LOG_ERROR("<DataLoader> Result validation failed.");
   else {
    if(total_nrows > 0)
     Flush();  
   }

   if(buffer.buf != NULL) {
     free(buffer.buf);
     buffer.buf = NULL;
   }
   return std::pair<DLStatus, ::uint64_t>(result, file_id);
}


/**
 * Creates a shared memory segment for a given data size.
 *
 **/
void DataLoader::CreateCsvShm(std::string shm_name, ::uint64_t data_size) {
  size_t free_shm_size = get_free_shm_size();
  if (free_shm_size > 0 && free_shm_size <= data_size) {
     file_id--;
     ostringstream msg;
     msg << "<DataLoader> Cannot allocate " << data_size << " bytes to Shared Memory. "
         << "Free shared memory size is " << free_shm_size;
     RegisterTransferError(msg.str().c_str());
     close(sock_fd_);
  } else { 
     boost::interprocess::shared_memory_object csv_shm(boost::interprocess::open_or_create, shm_name.c_str(), boost::interprocess::read_write);
     csv_shm.truncate(data_size);
     boost::interprocess::mapped_region csv_mapped(csv_shm, boost::interprocess::read_write);
     void *address = csv_mapped.get_address();
     memcpy(address, (void*)buffer.buf, data_size);
     LOG_DEBUG("<DataLoader> Created shared memory segment %s", shm_name.c_str());
  }
}


std::string DataLoader::getNextShmName() {
  boost::unique_lock<boost::mutex> lock(shm_name_mutex_);
  file_id++;
  ostringstream shm_file_name;
  shm_file_name << split_prefix_ << file_id;
  std::string shm_name = shm_file_name.str();
  lock.unlock();
  return shm_name;
}

void DataLoader::RegisterTransferError(const char* error_msg) {
  boost::unique_lock<boost::mutex> lock(metadata_update_mutex_);
  LOG_ERROR(error_msg);
  result.transfer_success_ = false;
  result.transfer_error_msg_= std::string(error_msg);
  lock.unlock();
}

/**
 * Flushes final data bytes received by this Worker which has not made it to the partition size.
 * - Creates a shared memory segment for the last partition.
 * - Shared memory segment created contains data in comma-separated format
 *
 */
void DataLoader::Flush() {
  std::string shm_name = getNextShmName();
  CreateCsvShm(shm_name, total_data_size);
}


/**
 * Appends data bytes to main buffer
 * - Mallocs required buffer size. Appends received data bytes to the buffer.
 * - Keeps track to total number of rows and total data size received
 * - When total number of rows is > or equal to partition size, 
 *   creates a shared memory segment for the partition
 * - Shared memory segment created contains data in comma-separated format.
 *
 **/
void DataLoader::AppendDataBytes(const char* shm_file, ::uint64_t data_size, uint32_t nrows) {
  unique_lock<recursive_mutex> lock(metadata_mutex_);

  char* p = (char*)shm_file;
  total_data_size += data_size;
  total_nrows += nrows;

  if(total_data_size > buffer.buffersize) {
     buffer.buf = (char*) realloc ((void*)buffer.buf, total_data_size);
     if(buffer.buf == NULL)  {
       ostringstream msg;
       msg << "<DataLoader> Unable to reserve data buffer of " << total_data_size << " bytes.";
       RegisterTransferError(msg.str().c_str());
       close(sock_fd_);
     }
     
     buffer.buffersize = total_data_size;
  }

  while((p!=NULL) && (*p!=0x0)) {
   size_t len = strlen(p);
   memcpy(buffer.buf + buffer.size, p, len);
   buffer.size += len;
   p += len;
  }

  if(total_nrows >= DR_partition_size) {
    LOG_DEBUG("<DataLoader> Creating a shared memory segment of %d rows", total_nrows);
    std::string shm_name = getNextShmName();
    CreateCsvShm(shm_name, total_data_size);

    total_data_size = 0;
    total_nrows = 0;
    buffer.size = 0;
  }
  lock.unlock();
  boost::this_thread::interruption_point();
}


/**
 * Thread function to read Vertica data per Vertica socket connection
 * - Reads metadata and raw data bytes as received from Vertica
 * - Appends data bytes to Buffer
 *
 **/
void DataLoader::ReadFromVertica(int32_t connection_fd) { 

  struct Metadata metadata;
  int n = recv(connection_fd, &metadata, sizeof(metadata), 0);
  if(n == 0) 
    RegisterTransferError("<DataLoader> No Metadata received from Vertica");
  else if (n<0)
    RegisterTransferError("<DataLoader> Error in receiving Metadata from Vertica");
  
 
  ::uint64_t data_size = metadata.size;
  uint32_t nrows = metadata.nrows;
  bool is_eof = metadata.isEOF;
  
  if(!is_eof) {
    ostringstream shm_file;
    shm_file.str("");

    char data_buffer[data_size];
    //size_t b_read = atomicio(read, connection_fd, data_buffer, data_size); 
    //data_buffer[b_read]='\0';
    while((errno = 0, (n = recv(connection_fd, data_buffer, sizeof(data_buffer), 0))>0) || errno == EINTR) {
      if(n == 0) {
       RegisterTransferError("<DataLoader> No data bytes received from Vertica");
       break;
      } else if (n<0){
       RegisterTransferError("<DataLoader> Error in receiving Metadata from Vertica");
       break;
      } else {
       data_buffer[n]='\0';
       shm_file << data_buffer;
      }   
    }

    AppendDataBytes(shm_file.str().c_str(), data_size, nrows);
  } else {
    unique_lock<recursive_mutex> lock(metadata_mutex_);
    if(vnode_EOFs.find(metadata.vNode_name) == vnode_EOFs.end()) {
      vnode_EOFs[metadata.vNode_name] = 1;
    } else {
      ++vnode_EOFs[metadata.vNode_name];
    }
    lock.unlock();
  }
  close(connection_fd);
}


/**
 * Data Loader Thread function.
 * - Listens for any incoming socket connection requests from Vertica
 * - For each socket connection request, Run() schedules a new thread to read from that socket connection.
 *
 **/
void DataLoader::Run() {
  try { 
    LOG_DEBUG("Started DataLoader thread");
    struct sockaddr_in their_addr;
    socklen_t sin_size = sizeof(their_addr);   
    while(true) {
      boost::this_thread::interruption_point();
      int ret = listen(sock_fd_, 10);
      boost::this_thread::interruption_point();
      if (ret < 0) {
        RegisterTransferError("<DataLoader> Data Loader stopped listening for data connections");
        break;
      }

      int32_t connection_fd = accept(sock_fd_, (struct sockaddr *) &their_addr,
                   &sin_size);
      if (connection_fd < 0) {
        std::ostringstream error;
        error << "<DataLoader> Connection failure: "<< strerror(errno);
        RegisterTransferError(error.str().c_str());
        close(connection_fd);
        break;
      } else {
        std::string vnode_address = inet_ntoa(their_addr.sin_addr);
        schedule(read_pool, boost::bind(&DataLoader::ReadFromVertica, this, connection_fd));
      }
    } 
    close(sock_fd_); 
  } catch(boost::thread_interrupted const&) {
    LOG_DEBUG("<DataLoader> Data Loader thread interrupted");
  }
}

}  // namespace
