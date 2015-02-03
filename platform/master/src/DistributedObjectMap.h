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
 * Class which maintains a map from names of Distributed arrays to their
 * pointers and provides thread safe operations on it.
 */

#ifndef __DISTRIBUTED_OBJECT_MAP_
#define __DISTRIBUTED_OBJECT_MAP_

#include <boost/unordered_map.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include <string>

using namespace std;
//using namespace boost;

namespace presto {

class DistributedObject;

class DistributedObjectMap {
 public:
  DistributedObjectMap();
  ~DistributedObjectMap();

  void PutDistributedObject(const string& name, DistributedObject* d);
  void DeleteDobject(const string& name);
  DistributedObject* GetDistributedObject(const string& name);

 private:
  boost::shared_ptr<boost::unordered_map<string, DistributedObject*> > dobject_map_;
  boost::shared_ptr<boost::mutex> mutex_;
};

}  // namespace presto

#endif  // __DISTRIBUTED_ARRAY_MAP_
