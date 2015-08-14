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
 * Class to represent a Distributed Array
 */

#ifndef __DISTRIBUTED_OBJECT_
#define __DISTRIBUTED_OBJECT_

// Needed for fast-updating of Master's metadata
#ifndef FAST_UPDATE
#define FAST_UPDATE
#endif

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/random_access_index.hpp>
#include <boost/multi_index/member.hpp>

#include <boost/unordered_map.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include <Rcpp.h>

#include <map>
#include <string>
#include <vector>

#include "shared.pb.h"
#include "worker.pb.h"

//using namespace std;
//using namespace boost;
using boost::multi_index_container;
using namespace Rcpp;

namespace presto {

class PrestoMaster;

enum SplitType {
  ROW,
  COL,
  BOTH
};

enum DobjectType {
  DARRAY = 1,
  DFRAME,
  DLIST,
  DOBJECT
};

enum SplitDistribution {
  ROUNDROBIN = 1,
  RANDOM,
  CUSTOM,
  DDC
};

/* A datastructure can have a subtype.
 * For example, darrays or dframes may be uninitialized.Or darrays and dlists can have flexible partition sizes.
*/
 enum DobjectSubType {
   STD = 1,  //standard dobject. Sizes cannot be changed.
   UNINIT_DECLARED,   //uninitialized object declared for the first time as empty (e.g., darray with empty flag
   UNINIT,   //uninitialized object (i.e, initialized to 0 by the system). The user can write to it in future
   FLEX_DECLARED,  //object with flexible or unequal partition sizes. This is the first time it is declared, so no values in it
   FLEX_UNINIT  //object with flexible or unequal partition sizes but has been initialized to 0 by the system. The user can update the sizes once and only once after this point.
};

struct WorkerIdxInfo {
  int idx;
  int worker_client_idx;
  boost::unordered_map<int32_t, uint64_t> worker_npartitions_map;
  std::vector<int32_t> loader_workers;
};

class DistributedObject {
 public:
  explicit DistributedObject(SEXP presto_master, std::string type, std::string subtype, std::string distribution);

  ~DistributedObject();

  int NextWorkerIdx(bool initialize = false);

  void ClearWorkerIdxInfo();

  // Creates the distributed array in the workers.
  bool Create(const std::string& name,
              const std::vector< ::int64_t> dimensions,
              const std::vector< ::int64_t> blocks);

  /** Get the number of splits with given version
    * @version the version number
    * @return the number of splits with the input version
    */
  int32_t NumSplits(int32_t version = -1) {
    int32_t version_to_use = version == -1 ? version_ : version;
    if (version_split_map.find(version_to_use) !=
        version_split_map.end()) {
      return version_split_map[version_to_use].size();
    }
    return 0;
  }

  int32_t NumSplitsDflt() {
    return NumSplits(-1);
  }


  /** generate split ID. Increment by one the current split ID
    * @return old split ID
    */
  int32_t GenerateSplitId() {
    // TODO(shivaram): Use an atomic inc and get ?
    int32_t ret = 0;
    boost::unique_lock<boost::mutex> d_lock(*mutex_);
    ret = next_split_id_;
    next_split_id_++;
    d_lock.unlock();
    return ret;
  }

  /** Get name of the darray
    * @return name of a darray
    */
  std::string Name() {
    return name_;
  }

  DobjectType Type() {
    return dobject_type_;
  }

  DobjectSubType SubType() {
    return dobject_subtype_;
  }

  DobjectSubType setSubType(DobjectSubType v) {
    dobject_subtype_ = v;
  }

  // Is the sub type of the object a flexible object. Note that the state of the object can move from Flexible to Standard after the first update from the user.
  bool isSubTypeFlex() {
    return ((dobject_subtype_ == FLEX_DECLARED) ||(dobject_subtype_ == FLEX_UNINIT));
  }

  //Return if the object is currently invalid. This can occur if the object has subtype UNINT_* or FLEX_* type, i.e, 
  //contencts have not been written to the partitions by the user
  bool isObjectInvalid() {
    return ((dobject_subtype_ == FLEX_DECLARED) ||(dobject_subtype_ == FLEX_UNINIT)|| (dobject_subtype_ == UNINIT) || (dobject_subtype_ == UNINIT_DECLARED));
   }

  /** Get a dimension (total size) of this darray
    * @return dimension expressed in the std::vector
    */
  std::vector<int64_t> GetDims() {
    return GetDimensions(-1);
  }
  /** Get a block size of this darray
    * @return block size expressed in the std::vector
    */
  std::vector<int64_t> GetBlocks() {
    return blocks_;
  }

  /** get a split type of this darray (row, col or block)
    * @return partition type
    */
  SplitType GetSplitType() {
    return split_type_;
  }

  std::vector<int64_t> GetBoundary(int32_t split,
      int32_t version = -1) {
    int32_t version_to_use = version == -1 ? version_ : version;
    if (version_split_map.find(version_to_use) ==
        version_split_map.end() ||
        split >= version_split_map[version_to_use].size()) {
      return std::vector<int64_t>();
    }
    return version_split_map[version_to_use][split]->start_offset;
  }

  //iR: return the dimensions stored in the class field. version is unused as it is the latest version.
  std::vector<int64_t> GetDimensions(int32_t version = -1) {
    return dimensions_;
  }

  /*(TODO) This code is outdated. Remove. We explicitly store dimensions_
  std::vector<int64_t> GetDimensions(int32_t version = -1) {
    int32_t version_to_use = version == -1 ? version_ : version;
    DistributedObjectSplit* last_split =
      version_split_map[version_to_use].back().get();
    std::vector<int64_t> dimensions_to_return;
    if (last_split != NULL) {
      // Dimensions is start_offset of last split + last split
      // dimensions
      dimensions_to_return = last_split->start_offset;
      for (uint32_t i = 0; i < dimensions_to_return.size(); ++i) {
        dimensions_to_return[i] += last_split->array->dim().val(i);
      }
    }
    return dimensions_to_return;
    }*/

  Array* GetSplitFromId(uint32_t split_id, int32_t version = -1);
  Array* GetSplitFromPos(uint32_t split_pos, int32_t version = -1);
  int32_t GetSplitPosFromId(uint32_t split_id, int32_t version = -1);
  void PutSplitWithId(uint32_t split_id, const Array& arr);
  bool UpdateDimAndBoundary(int32_t version = -1);

 private:
  // Details of each split
  struct DistributedObjectSplit {
    int32_t id;
    std::vector<int64_t> start_offset;
    boost::shared_ptr<Array> array;
  };

  // tags
  struct pos { };
  struct id { };

  // There are two indices we have for holding the splits in a version.
  // a. The positions are maintained in a sequence/random-access
  // b. The ids are maintend as a hash index
  typedef multi_index_container<
    boost::shared_ptr<DistributedObjectSplit>,
    boost::multi_index::indexed_by<
      boost::multi_index::random_access<boost::multi_index::tag<pos> >,
      boost::multi_index::hashed_unique<boost::multi_index::tag<id>,
        boost::multi_index::member<DistributedObjectSplit, int,
                                   &DistributedObjectSplit::id> >
    >
  > VersionSplits;

  // std::map of version number to split
  // Every version number has a list of pointers to Splits
  //
  // NOTE: When any new split is created in a new version,
  // we copy over all the pointers from the previous version.
  // This is used so that we can easily change the number of splits per
  // version and start_offsets could vary for the same split in
  // different versions.
  //
  // The split id could be different from the position of
  // the split in the array. The position in the array is obtained by
  // traversing the random access index in VersionSplits while the id
  // can be used to look elements in the hashed index.
  //
  // TODO(shivaram): Check how much memory this uses and optimize
  // if neccessary
  std::map<int32_t, VersionSplits> version_split_map;

  bool created;
  std::string name_;
  std::vector<int64_t> blocks_;
  std::vector<int64_t> dimensions_; //iR: explicitly store the dimensions

  // Right now this is the largest version of the different splits
  int32_t version_;

  int32_t next_split_id_;

  Rcpp::XPtr<PrestoMaster> pm_;

  // Mutex to be locked when updating clients or splits
  //
  boost::shared_ptr<boost::mutex> mutex_;

  SplitType split_type_;

  DobjectType dobject_type_;
  DobjectSubType dobject_subtype_;

  SplitDistribution split_distribution_;

  WorkerIdxInfo *info;
};

}  // namespace presto

#endif  // __DISTRIBUTED_ARRAY_
