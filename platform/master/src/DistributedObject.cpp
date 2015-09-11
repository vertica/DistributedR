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
 * Class which contains metadata for a distributed array
 * Used to lookup number of partitions and locations of partitions etc.
 */

#include <math.h>

#include <boost/thread/locks.hpp>
#include <boost/bind.hpp>
#include <boost/functional/hash.hpp>

#include <deque>
#include <string>
#include <vector>

#include "DistributedObjectMap.h"
#include "DistributedObject.h"
#include "Scheduler.h"
#include "PrestoMaster.h"
#include "DataLoaderManager.h"
#include "PrestoException.h"

using namespace boost;
namespace presto {
/** A constructor of DistributedArray (darray).
 * This object is created in the R sessio
 * @param presto_master a pointer of presto master
 */
  DistributedObject::DistributedObject(SEXP presto_master, std::string type, std::string subtype, std::string distribution)
  : created(false), version_(0), next_split_id_(0),
    pm_(Rcpp::Environment(presto_master).get(".pointer")){
  mutex_.reset(new mutex());
  split_type_ = ROW;
  dobject_type_ = (type=="darray") ? DARRAY : ((type=="dframe") ? DFRAME : ((type=="dlist") ? DLIST : DOBJECT));
  dobject_subtype_ = (subtype=="UNINIT_DECLARED") ? UNINIT_DECLARED : ((subtype=="FLEX_DECLARED") ? FLEX_DECLARED : STD);
  split_distribution_ = (distribution=="custom") ? CUSTOM :
                          (distribution=="ddc") ? DDC :
                          (distribution=="random") ? RANDOM :
                          ROUNDROBIN;

  info = new WorkerIdxInfo;
}

/** A destructor of DistributedArray
 * It cleans the darray in the worker's shared memory segment
 * This destructor is called when we perform gc() on R session
 * if there is no darray handler that is referencing this darray object
 */
DistributedObject::~DistributedObject() {
  // TODO(erik): cleanup
  if (created) {
    if (pm_->IsRunning() == true) {
      if (IsCompositeObject(name_) == false) {
        pm_->GetDistributedObjectMap()->DeleteDobject(name_); 
        pm_->DeleteDobject(name_);
      }
    }
  }
}

int DistributedObject::NextWorkerIdx(bool initialize) { 
  if(initialize) {
    info->worker_npartitions_map.clear();
    info->loader_workers.clear();
    switch(split_distribution_) {
    case ROUNDROBIN: {
      info->worker_client_idx = 0;
      break;
    }
    case RANDOM: {
      info->worker_client_idx = rand() % pm_->NumClients();
      break;
    }
    case CUSTOM: {
      DataLoaderManager* dataloader_ = pm_->GetDataLoader();
      if(dataloader_ == NULL) {
        std::string errormsg = "Unable to gather pre-defined partition distribution to workers.\n"
                                "  Try with roundrobin distribution policy";
        LOG_ERROR(errormsg);
        throw PrestoWarningException(errormsg);
      }
      else {
        info->worker_npartitions_map = dataloader_->GetPartitionMap();
        info->loader_workers = dataloader_->GetLoaderWorkers();
      }
      info->idx = 0;
      info->worker_client_idx = info->loader_workers[info->idx];  
      info->worker_npartitions_map[info->worker_client_idx]--;
      break;
    }
    case DDC: {
           info->worker_client_idx = pm_->worker_selector().getNextWorker();
           break;
    }
    default : {
        std::string msg = "Unknown split_distribution_";
        LOG_ERROR(msg);
        throw PrestoWarningException(msg);
    }
    } 
  } else {
    switch(split_distribution_) {
    case ROUNDROBIN: {
      info->worker_client_idx++;
      info->worker_client_idx = info->worker_client_idx % pm_->NumClients();
      break;
    }
    case RANDOM: {
      info->worker_client_idx = rand() % pm_->NumClients();
      break;
    }
    case CUSTOM: {
      int idx_ = info->idx;
      idx_++;
      idx_ = idx_ % info->worker_npartitions_map.size();
      while(info->worker_npartitions_map[info->loader_workers[idx_]]==0) {
        idx_++;
        idx_ = idx_ % info->worker_npartitions_map.size();
      }
      info->idx = idx_;
      info->worker_client_idx = info->loader_workers[idx_];
      info->worker_npartitions_map[info->worker_client_idx]--;
      break;   
    }
    case DDC: {
      info->worker_client_idx = pm_->worker_selector().getNextWorker();
      break;
    }

    default : {
        std::string msg = "Unknown split_distribution_";
        LOG_ERROR(msg);
        throw PrestoWarningException(msg);
    }
    }
  }

  return info->worker_client_idx;
}

/** Create a darray object in the master. First it determines the location of each split to workers.
 * and register the mapping to worker in the scheduler. It also update DistributedArrayMap.
  * At this point, darray is not distributed to workers yet.
 * @param name a name of this darray
 * @param dimensions dimension of the darray
 * @param blocks block size of the input darray
 * @param sparse indicates if this darray is sparse array or not
 * @param data data to be written into the drray
 * @return indicates if this action succeed
 */
bool DistributedObject::Create(const string& name,
                              const vector< ::int64_t> dimensions,
                              const vector< ::int64_t> blocks) {
  // Calculate number of splits first
  if (dimensions.size() != blocks.size()) {
    fprintf(stderr, "object \"%s\": number of blocks and dimensions should be same\n", name.c_str());
    LOG_ERROR("DistributedObject %s - number of blocks and dimensions should be the same", name.c_str());
    return false;
  }

  this->name_ = name;
  this->blocks_ = blocks;
  this->dimensions_ = dimensions;
  if (blocks[0] == dimensions[0] && blocks[1] != dimensions[1]) {
    split_type_ = COL;
  } else if (blocks[0] != dimensions[0] && blocks[1] != dimensions[1]) {
    split_type_ = BOTH;
  }

  int32_t num_splits = 1;

  // TODO(shivaram): Surely this can be made more efficient ?
  vector<vector< ::int64_t> > boundaries;
  vector<vector< ::int64_t> > current_boundaries;
  deque< ::int64_t> boundary;
  vector< ::int64_t> boundary_vec;
  for (int i = blocks.size() - 1; i >= 0; --i) {
    double num_splits_in_dim =
      static_cast<double>(dimensions[i]) / static_cast<double>(blocks[i]);
    num_splits = num_splits * ceil(num_splits_in_dim);
    boundaries.clear();
    for (::int64_t j = 0; j < dimensions[i]; j = j + blocks[i]) {
      boundary.clear();
      if (i != blocks.size() - 1) {
        for (::int64_t k = 0; k < current_boundaries.size(); ++k) {
          boundary = deque< ::int64_t>(current_boundaries[k].begin(),
                           current_boundaries[k].end());
          boundary.push_front(j);
          boundaries.push_back(vector< ::int64_t>(
                boundary.begin(), boundary.end()));
        }
      } else {
        boundary.push_front(j);
        // current_boundaries.push_back(boundary);
        boundaries.push_back(vector< ::int64_t>(boundary.begin(),
              boundary.end()));
      }
    }
    current_boundaries = boundaries;
  }

  Response res;

  VersionSplits splits_created;
  version_ = 0;  // set version to zero when create is called
  for (int i = 0; i < num_splits; ++i) {
    boost::shared_ptr<Array> arr(new Array());
    // Split name is:
    // <darray_name> + "_" + <split number> + "_" + <version_number>
    // TODO(shivaram): Create a utility function for this and share it
    // between master and worker
    int32_t id = GenerateSplitId();
    arr->set_name(BuildSplitName(name_, id, version_));
    arr->mutable_dim()->Clear();
    arr->mutable_psizes()->Clear();
    for (int k = 0; k < blocks.size(); ++k) {
      arr->mutable_dim()->add_val(blocks[k]);
    }
    
    if(dobject_subtype_ == FLEX_DECLARED) { 
      for (int k = 0; k < blocks.size(); ++k) {
          arr->mutable_psizes()->add_val(0);
      }
    } else {
      for (int k = 0; k < blocks.size(); ++k) {
       arr->mutable_psizes()->add_val(blocks[k]);
      }
    }

    for (int k = 0; k < arr->dim().val_size(); ++k) {
      if (boundaries[i][k] + arr->dim().val(k) > dimensions[k]) {
        arr->mutable_dim()->set_val(k, dimensions[k] - boundaries[i][k]);
      }
    }

    int worker_client_idx = 0;
    if(i == 0) 
      worker_client_idx = NextWorkerIdx(true);
    else
      worker_client_idx = NextWorkerIdx();

    // Pick a location for this partition
    WorkerInfo* info = pm_->GetClientInfoByIndex(worker_client_idx);

    pm_->GetScheduler()->AddSplit(
        arr->name(), 0, "",
        info->hostname()+":"+int_to_string(info->port()));

    boost::shared_ptr<DistributedObjectSplit> split(new DistributedObjectSplit());
    split->start_offset = boundaries[i];
    split->array = arr;
    split->id = i;
    // split->client = info;

    // Position and name are same right now
    splits_created.get<pos>().push_back(split);
  }

  unique_lock<mutex> d_lock(*mutex_);
  version_split_map.insert(make_pair(version_, splits_created));
  d_lock.unlock();

  created = true;
  pm_->GetDistributedObjectMap()->PutDistributedObject(name_, this);
  ClearWorkerIdxInfo();
  return true;
}

void DistributedObject::ClearWorkerIdxInfo() {
  info->worker_npartitions_map.clear();
  info->loader_workers.clear();
  delete info;
}

/** Get a split from position
 * @param split split
 * @param version version
 * @return corresponding split
 */
Array* DistributedObject::GetSplitFromPos(uint32_t split, int32_t version) {
  int32_t version_to_use = version == -1 ? version_ : version;
  Array* ret;
  unique_lock<mutex> d_lock(*mutex_);
  if (version_split_map.find(version_to_use) ==
      version_split_map.end()) {
    ostringstream msg;
    msg << "returning null as version_to_use is missing. " << (version_split_map.find(version_to_use) == version_split_map.end())
      << " split " << split << ", name \"" << name_ << "\", version " << version;
    throw PrestoShutdownException(msg.str());
  } else {
    if (split >= version_split_map[version_to_use].get<pos>().size()) {
      ostringstream msg;
      msg << "returning null as split is missing. Split " << split << ", name \"" << name_ << "\", version " << version;
      LOG_ERROR(msg.str());
      throw PrestoShutdownException(msg.str());
    } else {
      ret = version_split_map[version_to_use].get<pos>()[split]->array.get();
    }
  }
  d_lock.unlock();
  return ret;
}

/** Get position of a split from ID
 * @param split_id split ID to get
 * @param version version of split to get position
 * @return position of corresponding to an ID
 */
int32_t DistributedObject::GetSplitPosFromId(uint32_t split_id,
    int32_t version) {
  int32_t version_to_use = version == -1 ? version_ : version;
  int32_t ret = -1;
  unique_lock<mutex> d_lock(*mutex_);
  if (version_split_map.find(version_to_use) ==
      version_split_map.end()) {
    fprintf(stderr, "returning -1 as version_to_use is missing. %d "
       "split %d, name \"%s\", version %d",
       version_split_map.find(version_to_use) == version_split_map.end(),
       split_id, name_.c_str(), version);
  } else {
    if (version_split_map[version_to_use].get<id>().find(split_id)
        != version_split_map[version_to_use].get<id>().end()) {
      VersionSplits::iterator pos_iter =
        version_split_map[version_to_use].project<pos>(
        version_split_map[version_to_use].get<id>().find(split_id));
      ret = std::distance(version_split_map[version_to_use].get<pos>().begin(),
          pos_iter);
    } else {
      fprintf(stderr, "returning -1 as split is missing. %d "
         "split %d, name \"%s\", version %d",
         version_split_map.find(version_to_use) == version_split_map.end(),
         split_id, name_.c_str(), version);
    }
  }
  d_lock.unlock();
  return ret;
}
/** With input id and version, this function returns a corresponding split
 * @param split_id id of a split to get
 * @param version a version of a split to get. If this valeu is -1, we return the newest version
 * @return a split with the corresponding id and version.
 */
Array* DistributedObject::GetSplitFromId(uint32_t split_id,
    int32_t version) {
  int32_t version_to_use = version == -1 ? version_ : version;
  Array* ret;
  unique_lock<mutex> d_lock(*mutex_);
  if (version_split_map.find(version_to_use) ==
      version_split_map.end()) {
    ostringstream msg;
    msg << "returning null as version_to_use is missing." << (version_split_map.find(version_to_use) == version_split_map.end())
      << " split " << split_id << ", name \"" << name_ << "\", version " << version;
    throw PrestoShutdownException(msg.str());
  } else {
    if (version_split_map[version_to_use].get<id>().find(split_id)
        != version_split_map[version_to_use].get<id>().end()) {
      ret = (*version_split_map[version_to_use].get<id>().find(
              split_id))->array.get();
      // fprintf(stderr, "Returning for split %d, version %d name %s\n",
      //    split, version_to_use, ret->name().c_str());
    } else {
      ostringstream msg;
      msg << "returning null as split is missing. Split " << split_id << ", name \"" << name_ << "\", version " << version;
      LOG_ERROR(msg.str());
      throw PrestoShutdownException(msg.str());
    }
  }
  d_lock.unlock();
  return ret;
}

/**  Keep a split with the input split id
 * @param split_id a split id to kepp
 * @param arr an array to insert with the split ID
 * @return NULL
 */
void DistributedObject::PutSplitWithId(uint32_t split_id,
    const Array& arr) {
  // NOTE: version_ refers to the entire darray's version
  // It is composed of splits, each of which may be at a
  // different version.
  // The worker picks the version for the split, while the master
  // controls the version for the entire darray.
    
    

  unique_lock<mutex> d_lock(*mutex_);

  // Update DistributedArray's version.
  if (version_split_map.find(version_) == version_split_map.end()) {
    // Old version not found !
    fprintf(stderr, "old version not found in version_split map "
        "split %d, name \"%s\", version %d\n", split_id, 
        arr.name().c_str(), version_);

    d_lock.unlock();
    return;
  }

  // NOTE: Old version should be present for PutSplit. New splits are
  // only created by PartitionSplit
  if (version_split_map[version_].get<id>().find(split_id) ==
      version_split_map[version_].get<id>().end()) {
    // Old version not found !
    // fprintf(stderr, "Split not found in old version "
    //     "name:%s, split:%d, version_:%d size of old version:%d\n",
    //     arr.name().c_str(), split_id, version_,
    //     static_cast<int>(version_split_map[version_].size()));

    d_lock.unlock();
    return;
  }

  version_++;

#ifndef FAST_UPDATE
  // First check if this version already exists
  if (version_split_map.find(version_) == version_split_map.end()) {
    // Create the version. Copy all split pointers from previous
    // version
    VersionSplits& old_splits = version_split_map[version_-1];
    VersionSplits new_splits(old_splits);

    version_split_map.insert(make_pair(version_, new_splits));
  }

  VersionSplits& splits_for_version = version_split_map[version_];
#else
  VersionSplits& splits_for_version = version_split_map[version_ - 1];
#endif

  boost::shared_ptr<DistributedObjectSplit> new_split(new DistributedObjectSplit());

  // Set the start_offset based on previous version
  // Assumes the same split exists before
  DistributedObjectSplit* old_split_version =
    version_split_map[version_-1].get<id>().find(split_id)->get();

  new_split->id = split_id;

  boost::shared_ptr<Array> new_arr(new Array());
  new_split->array = new_arr;
  new_split->array->CopyFrom(*old_split_version->array.get());
  new_split->array->set_name(arr.name());
  new_split->array->set_size(arr.size());
  new_split->start_offset.insert(new_split->start_offset.begin(),
      old_split_version->start_offset.begin(),
      old_split_version->start_offset.end());

  //Update the dimension size only if the object is a flexobject. Otherwise, the size is the same, as per the ValidateUpdate() function
  if(this->isSubTypeFlex()) {
    new_split->array->mutable_dim()->set_val(0, arr.dim().val(0));
    new_split->array->mutable_dim()->set_val(1, arr.dim().val(1));
  }

  // Update psizes. Again, separate variable is not needed, but currently using it 
  // to get around dim_size "hack" for flex variable
  new_split->array->mutable_psizes()->set_val(0, arr.psizes().val(0));
  new_split->array->mutable_psizes()->set_val(1, arr.psizes().val(1));

  // Get the random access iterator by projecting the iterator from
  // name
   VersionSplits::iterator pos_iter = splits_for_version.project<pos>(
      splits_for_version.get<id>().find(split_id));

  splits_for_version.replace(pos_iter, new_split);

  //(TODO) iR: Remove this dead code
  // Check if this split's dimension changed. If so update
  // start_offset for following splits.
  // NOTE: This only handles single-dimension splits right now.
  // Also in single dimension splits, only the dimension on which it
  // is split can be changed.

  /*if (new_split->array->dim().val(0) !=
      old_split_version->array->dim().val(0)) {
    // row dim changed.
    int32_t diff = new_split->array->dim().val(0) -
      old_split_version->array->dim().val(0);

    while (pos_iter != splits_for_version.get<pos>().end()) {
      // Pre-increment as we don't want to change pos_iter
      ++pos_iter;
      boost::shared_ptr<DistributedObjectSplit> s = *pos_iter;
      s->start_offset[0] += diff;
      splits_for_version.replace(pos_iter, s);
    }
  } else if (new_split->array->dim().val(1) !=
      old_split_version->array->dim().val(1)) {
    int32_t diff = new_split->array->dim().val(1) -
      old_split_version->array->dim().val(1);

    while (pos_iter != splits_for_version.get<pos>().end()) {
      // Pre-increment as we don't want to change pos_iter
      ++pos_iter;
      boost::shared_ptr<DistributedObjectSplit> s = *pos_iter;
      s->start_offset[1] += diff;
      splits_for_version.replace(pos_iter, s);
    }
    }*/

  // fprintf(stderr, "Putting for split %d, version %d name %s %s\n",
  //     split, version_, new_split->array->name.c_str(), arr.name.c_str() );

  int32_t split_version;
  ParseVersionNumber(arr.name(), &split_version);

  d_lock.unlock();

#ifdef FAST_UPDATE
  version_--;
#endif
}

Rcpp::NumericMatrix DistributedObject::GetPartitionSizes() {
    ::int64_t split_id, split_rdim, split_cdim;
    DistributedObjectSplit *ret;
    int numDims = dobject_type_ == DLIST ? 1 : 2;
    
    // Store the results in a matrix
    Rcpp::NumericMatrix partitionSizes(NumSplitsDflt(), numDims);
    
    for(split_id = 0; split_id < NumSplitsDflt(); split_id++) {
        ret = version_split_map[version_].get<id>().find(split_id)->get();
        split_rdim = ret->array->psizes().val(0);
        split_cdim = ret->array->psizes().val(1);
        partitionSizes(split_id,0) = split_rdim;
      
        if(numDims > 1) partitionSizes(split_id,1) = split_cdim;
    }
    
    return partitionSizes;
}

/** Update the dimension and boundary field of flexible object. Used when the object is updated the first time.
 * @param version version of object to use
 * @return whether the update succeeded
 */

bool DistributedObject::UpdateDimAndBoundary(int32_t version) {
  int32_t version_to_use = version == -1 ? version_ : version;
  bool result = true;
  ostringstream msg;
  char err_msg[150];
  
  if (version_split_map.find(version_to_use) == version_split_map.end()) {
    msg << "In update dim and boundary: returning null as version_to_use is missing." << ", name \"" << name_ << "\", version " << version;
    throw PrestoShutdownException(msg.str());
  }
  
  //Initially the dimensions are equal to the number for logical blocks in each direction                                                                       
  ::int64_t rdim = dimensions_[0];
  ::int64_t cdim = dimensions_[1];
  
  ::int64_t row,col,split_id, split_rdim, split_cdim, new_rdim =0, new_cdim=0, c_offset=0, r_offset=0;
  ::int64_t prev_cdim=0;
  ::int64_t prev_rdim[rdim];
  DistributedObjectSplit *ret;

  
  //We will walk the blocks column wise, i.e. all blocks in col 1 then col 2 and so on. 
  for(col = 0; col < cdim; ++col){
    r_offset=0;
    for(row = 0; row< rdim; ++row){
      split_id = col + (cdim*row);
      
      if (version_split_map[version_to_use].get<id>().find(split_id)
	  != version_split_map[version_to_use].get<id>().end()) {
	ret = version_split_map[version_to_use].get<id>().find(split_id)->get();
	split_rdim = ret->array->dim().val(0);
	split_cdim = ret->array->dim().val(1);
	ret->start_offset[0] = r_offset;
	ret->start_offset[1] = c_offset;
      } else {
	msg << "returning null as split is missing. Split " << split_id << ", nam\
e \"" << name_ << "\", version " << version <<": rdim="<<rdim<<" :cdim="<<cdim;
	//throw PrestoShutdownException(msg.str());
	throw PrestoWarningException(msg.str());
      }
      
      //Column sum only requires the blocks from first row
      if(row==0){
	new_cdim = new_cdim + split_cdim;
	prev_cdim = split_cdim;
        }
      //Row dim sum requires the blocks from first column     
      if(col==0){
	new_rdim = new_rdim + split_rdim;
	prev_rdim[row] = split_rdim;
      }
      
      //Columns on each split should match splits just above it        
      if(prev_cdim!=split_cdim){
	result = false;
	if(split_rdim == split_cdim ==1){
    sprintf(err_msg,"Split %ld was either not updated or updated with size (%ld,%ld). Mismatch with adjacent partition's column size (=%ld)", (split_id+1),split_rdim, split_cdim, prev_cdim);
	}else{
    sprintf(err_msg,"Update to split %ld with size (%ld,%ld). Mismatch with adjacent partition's column size (=%ld)", (split_id+1),split_rdim, split_cdim, prev_cdim);
	}
	LOG_ERROR(err_msg);
      }
      //Rows on each split should match those of the leftmost split   
      if(prev_rdim[row]!=split_rdim){
	result = false;
	if(split_rdim == split_cdim ==1){
	  //The user probably did not update the split. Provide more information in the error message.
     sprintf(err_msg,"Split %ld was either not updated or updated with size (%ld,%ld). Mismatch with adjacent partition's row size (=%ld)", (split_id+1),split_rdim, split_cdim, prev_rdim[row]);
	}else{
      sprintf(err_msg,"Update to split %ld with size (%ld,%ld). Mismatch with adjacent partition's row size (=%ld)", (split_id+1),split_rdim, split_cdim, prev_rdim[row]);
	}
	LOG_ERROR(err_msg);
      }
      r_offset = r_offset + split_rdim;
    }
      c_offset = c_offset + split_cdim;
  }

  if(!result){
    //We print only the last error message
    fprintf(stderr, "\n%s%s. Redo object update to avoid unintended behavior later on.\n", exception_prefix.c_str(), err_msg);
  }else{
    //Set the new dimensions of the object. Since this code is run at the end of foreach loop. It should be threadsafe
    dimensions_[0] = new_rdim;
    dimensions_[1] = new_cdim;
  }
  return result;
}
  
RCPP_MODULE(dobject_module) {
  class_<presto::DistributedObject>("DistributedObject")
    .constructor<SEXP,std::string,std::string, std::string>()
    .method("create", &DistributedObject::Create)
    .method("num_splits", &DistributedObject::NumSplitsDflt)
    .method("name", &DistributedObject::Name)
    .method("dim", &DistributedObject::GetDims)
    .method("blocks", &DistributedObject::GetBlocks)
    .method("is_object_invalid", &DistributedObject::isObjectInvalid)
    .method("parts_sizes", &DistributedObject::GetPartitionSizes)
      ;  // NOLINT(whitespace/semicolon)
}

}  // namespace presto
