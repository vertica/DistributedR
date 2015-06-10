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

#ifndef __SHARED_MEMORY_H__
#define __SHARED_MEMORY_H__

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <map>
#include <string>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "PrestoException.h"
#include "common.h"

#define MAX_FILENAME_LENGTH 200
#define TMP_DIR "/tmp"

namespace presto {

class FileSharedMemoryObject;
class BoostSharedMemoryObject;

#ifndef USE_MMAP_AS_SHMEM
typedef BoostSharedMemoryObject SharedMemoryObject;
#else
typedef FileSharedMemoryObject SharedMemoryObject;
#endif

static int shared_memory_sem = -1;

class BaseSharedMemoryObject {
 public:
  /** A base class to represent shared memory object
   */
  BaseSharedMemoryObject() {}
  virtual ~BaseSharedMemoryObject() {}
  /** allocate a given length of memory
   * @param length size to allocate
   * @return NULL
   */
  virtual void truncate(size_t length) = 0;

  // need to have non-const reference
  // for compatibility with boost::shared_memory_object
  virtual bool get_size(boost::interprocess::offset_t &size) = 0;  // NOLINT

  virtual boost::interprocess::mapping_handle_t get_mapping_handle() const = 0;
};

class BoostSharedMemoryObject : public BaseSharedMemoryObject {
 public:
  BoostSharedMemoryObject(boost::interprocess::open_or_create_t,
                          const char *name,
                          boost::interprocess::mode_t mode) :
  shm_(boost::interprocess::open_or_create, name, mode) {
  }
  BoostSharedMemoryObject(boost::interprocess::open_only_t,
                          const char *name,
                          boost::interprocess::mode_t mode) :
  shm_(boost::interprocess::open_only, name, mode) {
  }

  ~BoostSharedMemoryObject() {
  }
  /** allocate a given length of memory
   * @param length size to allocate
   * @return NULL
   */
  void truncate(size_t length) {
    if (shared_memory_sem < 0) {
      umask(0);
#ifdef O_CLOEXEC
      shared_memory_sem = open(get_shm_size_check_name().c_str(), O_RDWR | O_CREAT | O_CLOEXEC, S_IRWXU | S_IRWXG | S_IRWXO);
#else
      // O_CLOEXEC is relatively new. 
      // Alternative way for systems that don't have it.
      shared_memory_sem = open(get_shm_size_check_name().c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
      fcntl(shared_memory_sem, F_SETFD, FD_CLOEXEC);
#endif
      if(presto::shared_memory_sem < 0){
        ostringstream msg;
        msg << "sem_open failed: " << strerror(errno); 
        LOG_ERROR("%s", msg.str().c_str());
        throw PrestoWarningException(msg.str());
      } else {
        LOG_INFO("succeed to open shared_memory_sem: %d", shared_memory_sem);
      }
    }
    int ret = lockf( shared_memory_sem, F_LOCK, 0 );
    if(ret < 0) {
      ostringstream msg;
      msg << "failed to get shm size sem: shm truncate failed: " << strerror(errno); 
      LOG_ERROR("%s", msg.str().c_str());
      throw PrestoWarningException(msg.str());
    }
    size_t free_shm_size = get_free_shm_size();
    if (free_shm_size > 0 && free_shm_size <= length) {
      ostringstream msg;
      msg << "cannot allocate " << length
          << " bytes to shared memory. free shm size is " << free_shm_size;
      LOG_ERROR("%s", msg.str().c_str());
      int res = lockf( shared_memory_sem, F_ULOCK, 0 );
      if (res == -1) {
          LOG_ERROR("lockf: %s", strerror(errno));
      }
      throw PrestoWarningException(msg.str());
    }
    shm_.truncate(length);
    boost::interprocess::mapped_region mem_region (shm_, boost::interprocess::read_write);
    memset(mem_region.get_address(), 0, mem_region.get_size());
    int res = lockf( shared_memory_sem, F_ULOCK, 0 );
    if (res == -1) {
        LOG_ERROR("lockf: %s", strerror(errno));
    }
  }

  // need to have non-const reference
  // for compatibility with boost::shared_memory_object
  bool get_size(boost::interprocess::offset_t &size) {  // NOLINT
    return shm_.get_size(size);
  }

  boost::interprocess::mapping_handle_t get_mapping_handle() const {
    return shm_.get_mapping_handle();
  }

  static bool remove(const char *name) {
    return boost::interprocess::shared_memory_object::remove(name);
  }

  private:
    boost::interprocess::shared_memory_object shm_;  
};

class FileSharedMemoryObject : public BaseSharedMemoryObject {
 public:
  FileSharedMemoryObject(boost::interprocess::open_or_create_t,
                         const char *name,
                         boost::interprocess::mode_t mode) {
    Create(name, TMP_DIR, mode);
  }
  FileSharedMemoryObject(boost::interprocess::open_only_t,
                         const char *name,
                         boost::interprocess::mode_t mode) {
    Create(name, TMP_DIR, mode);
  }
  FileSharedMemoryObject(const char *name,
                         const char *dir) {
    Create(name, dir, boost::interprocess::read_write);
  }

  ~FileSharedMemoryObject() {
    fclose(f);
    delete m;
  }

  void truncate(size_t length) {
    if (ftruncate(m->get_mapping_handle().handle, length) != 0) {
      ostringstream msg;
      msg << "failed to allocate memory in shm region with length: " << length
          << "\nRestart session using distributedR_shutdown()";
      fprintf(stderr, "%s\n", msg.str().c_str());
      throw PrestoWarningException(msg.str());
    }
  }

  // need to have non-const reference
  // for compatibility with boost::shared_memory_object
  bool get_size(boost::interprocess::offset_t &size) {  // NOLINT
    fseek(f, 0L, SEEK_END);
    size = ftell(f);
    return true;
  }

  boost::interprocess::mapping_handle_t get_mapping_handle() const {
    return m->get_mapping_handle();
  }

  static bool remove(const char *name) {
    char filename[MAX_FILENAME_LENGTH];
    snprintf(filename, MAX_FILENAME_LENGTH, TMP_DIR "/%s", name);
    return boost::interprocess::file_mapping::remove(filename);
  }

 private:
  void Create(const char *name, const char *dir,
              boost::interprocess::mode_t mode) {
    char filename[MAX_FILENAME_LENGTH];
    snprintf(filename, MAX_FILENAME_LENGTH, "%s/%s", dir, name);
    f = fopen(filename, "a");
    m = new boost::interprocess::file_mapping(filename, mode);
  }
  boost::interprocess::file_mapping *m;
  FILE *f;
};
}

#endif

