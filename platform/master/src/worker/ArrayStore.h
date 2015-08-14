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

#ifndef __ARRAYSTORE_H__
#define __ARRAYSTORE_H__

#include <string>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>
#include "SharedMemory.h"

#ifdef USE_MMAP_AS_SHMEM
#include <boost/unordered_map.hpp>
#include <boost/interprocess/mapped_region.hpp>
#endif

namespace presto {

// An ArrayStore object is used to store arrays in some external
// (non-DRAM) location. Arrays are referenced by name.
class ArrayStore {
 public:
  explicit ArrayStore(size_t size) : size_(size) {}
  size_t GetSize() const;
  virtual void Save(const std::string &name)=0;
  virtual void Load(const std::string &name)=0;
  virtual void Delete(const std::string &name)=0;
  virtual ~ArrayStore() {}

 private:
  size_t size_;
};

// Stores arrays in the filesystem as files under the given path
class FSArrayStore : public ArrayStore {
 public:
  FSArrayStore(size_t size, const std::string &path);
  virtual void Save(const std::string &name);
  virtual void Load(const std::string &name);
  virtual void Delete(const std::string &name);
  virtual ~FSArrayStore();

 private:
  std::string path_;
  boost::unordered_set<std::string> files_;
  boost::mutex files_mutex_;
};

#ifdef USE_MMAP_AS_SHMEM

// If we are using mmap as the shared memory backend, we can use
// this to use the mmaped files as files of the arraystore
// (i.e. there is no need to make another file copy if there is
// already one for mmaping)
class MMapArrayStore : public ArrayStore {
 public:
  explicit MMapArrayStore(size_t size);
  virtual void Save(const std::string &name);
  virtual void Load(const std::string &name);
  virtual void Delete(const std::string &name);
  virtual ~MMapArrayStore();

 private:
  typedef std::pair<
      SharedMemoryObject*,
      boost::interprocess::mapped_region*
      > MappingInfo;

  boost::mutex mutex_;
  boost::unordered_map<std::string, MappingInfo> mappings_;
  char dummy;
};
#endif
}

#endif
