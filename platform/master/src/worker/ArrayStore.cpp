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

#include <cstdio>
#include <unistd.h>

#include "ArrayStore.h"
#include <boost/interprocess/mapped_region.hpp>

using namespace boost::interprocess;
using namespace boost;
using namespace std;

namespace presto {

static size_t get_file_size(FILE *f) {
  size_t original = ftell(f);
  fseek(f, 0L, SEEK_END);
  size_t ret = ftell(f);
  fseek(f, original, SEEK_SET);
  return ret;
}

size_t ArrayStore::GetSize() const {
  return size_;
}

FSArrayStore::FSArrayStore(size_t size, const string &path)
    : ArrayStore(size), path_(path) {
}

void FSArrayStore::Save(const string &name) {
  SharedMemoryObject shm(open_only, name.c_str(), read_only);
  offset_t size;
  shm.get_size(size);
  if (size == 0) {
    return;
  }
  mapped_region region(shm, read_only);

  FILE *f = fopen((path_+"/"+name).c_str(), "w");
  for (size_t written = 0;
       written < region.get_size();
       written += fwrite(
           reinterpret_cast<char*>(region.get_address())+written,
           1, region.get_size()-written, f)) {
  }

  fclose(f);

  unique_lock<mutex> lock(files_mutex_);
  files_.insert(name);
  lock.unlock();
}

void FSArrayStore::Load(const string &name) {
  FILE *f = fopen((path_+"/"+name).c_str(), "r");
  size_t size = get_file_size(f);

  SharedMemoryObject shm(open_or_create, name.c_str(), read_write);
  shm.truncate(size);
  mapped_region region(shm, read_write);

  for (size_t read = 0;
       read < size;
       read += fread(reinterpret_cast<char*>(region.get_address())+read,
                     1, size-read, f)) {
  }

  fclose(f);
}

void FSArrayStore::Delete(const string &name) {
  unlink((path_+"/"+name).c_str());
  unique_lock<mutex> lock(files_mutex_);
  files_.erase(name);
  lock.unlock();
}

FSArrayStore::~FSArrayStore() {
  for (unordered_set<string>::iterator i = files_.begin();
       i != files_.end(); i++) {
    unlink((path_+"/"+*i).c_str());
  }
};

#ifdef USE_MMAP_AS_SHMEM
MMapArrayStore::MMapArrayStore(size_t size)
    : ArrayStore(size),
      dummy(0) {
}

void MMapArrayStore::Save(const string &name) {
  SharedMemoryObject shm(open_only, name.c_str(), read_write);
  offset_t size;
  shm.get_size(size);
  if (size == 0) {
    return;
  }
  mapped_region region(shm, read_write);

  msync(region.get_address(), size, MS_SYNC|MS_INVALIDATE);
}

void MMapArrayStore::Load(const string &name) {
  unique_lock<mutex> lock(mutex_);
  SharedMemoryObject *shm;
  mapped_region *region;
  // we need to keep mappings alive for the duration of
  // the arraystore to discourage flush on unmap
  if (mappings_.find(name) == mappings_.end()) {
    shm = new SharedMemoryObject(open_only, name.c_str(), read_only);
    region = new mapped_region(*shm, read_only);
    mappings_[name] = make_pair(shm, region);
  } else {
    shm = mappings_[name].first;
    region = mappings_[name].second;
  }
  lock.unlock();

  // touch every page to load
  // for (size_t i = 0; i < region->get_size(); i += getpagesize()) {
  //   dummy |= reinterpret_cast<char*>(region->get_address())[i];
  // }
  madvise(region->get_address(), region->get_size(), MADV_SEQUENTIAL);
}

void MMapArrayStore::Delete(const string &name) {
  // don't delete the backing file!
}

MMapArrayStore::~MMapArrayStore() {
  char filename[MAX_FILENAME_LENGTH];
  unique_lock<mutex> lock;
  // it's safe to delete now because the program is shutting down
  for (unordered_map<string, MappingInfo>::iterator i = mappings_.begin();
       i != mappings_.end(); i++) {
    delete i->second.second;
    delete i->second.first;
    snprintf(filename, MAX_FILENAME_LENGTH, "%s/%s", TMP_DIR, i->first.c_str());
    unlink(filename);
  }
  lock.unlock();
};
#endif
}
