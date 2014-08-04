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

#include <string>

#include "DistributedObject.h"
#include "PrestoMaster.h"

#include "DistributedObjectMap.h"

namespace presto {

/** DistributedObjectMap constructor
 */
DistributedObjectMap::DistributedObjectMap() {
  dobject_map_.reset(new unordered_map<string, DistributedObject*>());
  mutex_.reset(new mutex());
}

DistributedObjectMap::~DistributedObjectMap() {
}

/** Keep a DistributedObject with the dobject name
 * @param name the name of a dobject to keep
 * @param d distributed object to keep with the corresponding name
 * @return NULL
 */
void DistributedObjectMap::PutDistributedObject(const string& name,
    DistributedObject* d) {
  unique_lock<mutex> d_lock(*mutex_);
  dobject_map_->insert(make_pair(name, d));
  d_lock.unlock();
}

/** Delete dobject with a corresponding name
 * @param name a name of dobject to delete
 * @return NULL
 */
void DistributedObjectMap::DeleteDobject(const string& name) {
  unique_lock<mutex> d_lock(*mutex_);
  dobject_map_->erase(name);
  d_lock.unlock();
}

/** get dobject in the map
 * @param name a name of dobject to get
 * @return a distributed dobject with the input name
 */
DistributedObject* DistributedObjectMap::GetDistributedObject(const string& name) {
  unique_lock<mutex> d_lock(*mutex_);
  unordered_map<string, DistributedObject*>::iterator it =
    dobject_map_->find(name);

  DistributedObject* ret = NULL;
  if (it != dobject_map_->end()) {
    ret = it->second;
  }
  d_lock.unlock();
  return ret;
}
}  // namespace presto
