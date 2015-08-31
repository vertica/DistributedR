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

/* Struct used for interprocess syncing between executor and worker */

#ifndef __INTERPROCESS_SYNC_H__
#define __INTERPROCESS_SYNC_H__

#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>

#include <google/protobuf/repeated_field.h>

#include <memory>
#include <string>
#include <vector>

#define EXECUTOR_SHUTDOWN_CODE -1
#define SHARED_MEMORY_SIZE 1048576

namespace presto {

// IPC Struct used to signal worker->exec communication
static const char* kIPCSyncStructName = "interprocess_sync_struct";
struct interprocess_sync_struct {
  boost::interprocess::interprocess_condition worker_condition;
  boost::interprocess::interprocess_condition executor_condition;
  boost::interprocess::interprocess_mutex worker_mutex;
  boost::interprocess::interprocess_mutex executor_mutex;
};


// Shared vector used for updates
static const char* kUpdateSetName = "update";
typedef boost::interprocess::allocator<void,
          boost::interprocess::managed_shared_memory::segment_manager>
            VoidAllocator;
typedef boost::interprocess::allocator<char,
          boost::interprocess::managed_shared_memory::segment_manager>
            CharAllocator;
typedef boost::interprocess::basic_string<char,
        std::char_traits<char>, CharAllocator> ShmString;

struct UpdateData {
  ShmString name_;
  int64_t size_;

  UpdateData(const char* name, int32_t name_len, int64_t size,
      const CharAllocator& char_alloc) :
    name_(name, name_len, char_alloc), size_(size) {
  }

  UpdateData& operator=(const UpdateData& x) {
    name_ = x.name_;
    size_ = x.size_;
    return *this;
  }

  // We seem to have issues with null-termination of shared-strings.
  // Use a std::string copy to overcome this.
  // http://stackoverflow.com/questions/10403022/
  std::string name() {
    return std::string(name_.c_str(), name_.size());
  }

  int64_t size() {
    return size_;
  }
};

typedef boost::interprocess::allocator<UpdateData,
          boost::interprocess::managed_shared_memory::segment_manager>
            UpdateDataAllocator;
typedef boost::interprocess::vector<UpdateData, UpdateDataAllocator>
          ShmUpdateDataVector;


typedef boost::interprocess::allocator<ShmString,
          boost::interprocess::managed_shared_memory::segment_manager>
            StringAllocator;
typedef boost::interprocess::vector<ShmString, StringAllocator> ShmStringVector;

static void AddShmStringsFromVector(const std::vector<std::string>& strings,
    const CharAllocator& char_alloc, ShmStringVector* shm_vec) {
  for (int32_t i = 0; i < strings.size(); ++i) {
    shm_vec->push_back(
          ShmString(strings[i].c_str(), char_alloc));
  }
}

static void GetShmStringsToVector(const ShmStringVector& shm_vec,
    std::vector<std::string>* strings) {
  for (int32_t i = 0; i < shm_vec.size(); ++i) {
    strings->push_back(std::string(shm_vec[i].c_str(), shm_vec[i].size()));
  }
}

static void CopyShmStringsToRepeatedField(const ShmStringVector& shm_vec,
    google::protobuf::RepeatedPtrField<std::string>* dest) {
  for (int32_t i = 0; i < shm_vec.size(); ++i) {
    dest->Add()->assign(std::string(shm_vec[i].c_str(), shm_vec[i].size()));
  }
}

}  // namespace presto

#endif
