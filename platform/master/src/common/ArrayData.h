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
 * Classes for serializing and de-serializing R arrays to/from shared memory
 */

#ifndef __ARRAYDATA_H__
#define __ARRAYDATA_H__

#include <malloc.h>

#include "SharedMemory.h"
/* #include <boost/interprocess/shared_memory_object.hpp> */
#include <boost/interprocess/mapped_region.hpp>

#include <string>
#include <utility>
#include <vector>
#include <boost/unordered_set.hpp>

#include "Rinternals.h"
#include <RInside.h>
#include "Rinternals_snippet.h"

namespace presto {

// this is used to turn of installing R symbols
// when ArrayData is used from the worker (which
// has no embedded R instance)
extern bool has_R_instance;

void presto_malloc_init_hook();
SEXP PrestoCreateVector(SEXPTYPE type, size_t x);
pair<void*, size_t> createmapping(
    const BaseSharedMemoryObject &mapping,
    boost::interprocess::offset_t offset,
    std::size_t size,
    void *address);

enum ARRAYTYPE {
  EMPTY,
  DENSE,
  SPARSE,
  SPARSE_TRIPLET,
  DATA_FRAME,
  LIST
};

enum DobjectType {
  DARRAY_DENSE = 1,
  DARRAY_SPARSE,
  DFRAME,
  DLIST
};

ARRAYTYPE GetClassType(const std::string& split_name);

class ArrayData;

// Struct needed to cut up composites (for updating)
struct Composite {
  vector<pair<std::int64_t, std::int64_t> > offsets;
  vector<pair<std::int64_t, std::int64_t> > dims;
  vector<string> splitnames;
  DobjectType dobjecttype;
};

enum sprs_encoding_t {
  CSC,
  TRIPLET
};

typedef struct {
  int64_t type;
  int64_t dims[2];
  SEXPTYPE value_type;  // either int or real
  //    SEXPREC_ALIGN sexp;
} dense_header_t;

typedef struct {
  int64_t type;
  int64_t nnz;
  int64_t dims[2];
  sprs_encoding_t encoding;
} sparse_header_t;

typedef struct {
  int64_t type;
  int64_t nnz;
  int64_t dims[2];
} sparse_triplet_header_t;

/* Abstract class for R array serialization to shared memory */
class ArrayData {
 public:
  /* Children should implement 2 constructors: (string name) to 'load' an array
     already in shared memory with name name, and (string name, SEXP sexp) to
     create an array in shared memory with name name from R object sexp.

     For the latter, the first 64 bits of the serialization should contain
     the type of the array using enum ARRAYTPE.
     TODO(erik): enforce this somehow through class hierarchy? */
  ArrayData(const std::string &name_, int type_);

  // Load the array in R
  virtual void LoadInR(RInside &R, const std::string &varname) = 0;

  // Compress array, return address
  // and size of compressed blob
  virtual std::pair<void*, size_t> Compress();

  // Decompress array (under same shared memory name)
  virtual void Decompress();

  // Get the shared memory name
  virtual std::string GetName() const;
  // Get total size in shared memory
  virtual size_t GetSize();
  // Get array dimensions
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const = 0;

  // Create an 'empty' array in R that has the same type as this array
  // (used for creating composite arrays)
  // virtual void CreateEmpty(RInside &R, const std::string &varname, int dimx,
  //    int dimy) const=0;

  virtual ~ArrayData();

  BaseSharedMemoryObject *shm;
  static const char *spill_dir_;

 protected:
  // Open corresponding SharedMemoryObject
  void OpenShm(bool external);

  boost::interprocess::mapped_region *header_region;
  const int type;

 private:
  const std::string name;
};

// Create the appropriate ArrayData structure by parsing shared memory
// with name name
ArrayData* ParseShm(const std::string &name);

// Create the appropriate ArrayData structure by parsing R variable with
// name varname
ArrayData* ParseVariable(RInside &R, const std::string &varname,
    const std::string &newname, ARRAYTYPE org_class);

// Create composite array from metadata described in ca
size_t CreateComposite(
    const std::string &name,
    const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
    const std::vector<ArrayData*> &splits,
    std::pair<std::int64_t, std::int64_t> dims,
    ARRAYTYPE type);

// Store dense array data
class DenseArrayData : public ArrayData {
 public:
  explicit DenseArrayData(const std::string &name);
  DenseArrayData(const std::string &name, const SEXP sexp,
      const boost::unordered_set<std::string> &classname);
  DenseArrayData(
      const std::string &name,
      const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
      const std::vector<ArrayData*> &splits,
      std::pair<std::int64_t, std::int64_t> dims);
  virtual void LoadInR(RInside &R, const std::string &varname);
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const;

  // virtual void CreateEmpty(RInside &R, const std::string &varname, int dimx,
  // int dimy) const;
  /* static void CreateComposite(const std::string &name, */
  /*     const CompositeArray *ca); */
  virtual ~DenseArrayData();

 private:
  dense_header_t *header;
  std::pair<void*, size_t> array_region;
};

// Store sparse array data in dgCMatrix format
class SparseArrayData : public ArrayData {
 public:
  explicit SparseArrayData(const std::string &name);
  SparseArrayData(const std::string &name, const SEXP sexp,
      const boost::unordered_set<std::string> &classname);
  SparseArrayData(const std::string &name,
      const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
      const std::vector<ArrayData*> &splits,
      std::pair<std::int64_t, std::int64_t> dims);

  virtual void LoadInR(RInside &R, const std::string &varname);
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const;
//  virtual std::pair<void*, size_t> Compress();
//  virtual void Decompress();
  static int* ConvertJtoPVector(int* j_vector, int nnz, int num_col);
  virtual ~SparseArrayData();

 private:
  sprs_encoding_t ChooseEncoding(int64_t nnz, int64_t x, int64_t y);
  sparse_header_t* header;
};

// Store sparse array data in dgTMatrix format (triplet)
class SparseArrayTripletData : public ArrayData {
 public:
  explicit SparseArrayTripletData(const std::string &name);
  SparseArrayTripletData(const std::string &name, const SEXP sexp,
      const boost::unordered_set<std::string> &classname);
  SparseArrayTripletData(const std::string &name,
      const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
      const std::vector<ArrayData*> &splits,
      std::pair<std::int64_t, std::int64_t> dims);

  SparseArrayTripletData(const string& name, const SEXP sexp_from,
    const boost::unordered_set<std::string> &classname, const int32_t startx, const int32_t starty,
    const int32_t endx, const int32_t endy, const int64_t nonzeros,
    bool is_row_split);

  virtual void LoadInR(RInside &R, const std::string &varname);
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const;
  virtual ~SparseArrayTripletData();

 private:
  sparse_triplet_header_t* header;
  /* boost::interprocess::mapped_region *header_region; */
  /* std::pair<void*, size_t> i_region, j_region, x_region; */
};

// Represents an uninitialized R variable
// (use for newly created splits that haven't been loaded yet)
class EmptyArrayData : public ArrayData {
 public:
  explicit EmptyArrayData(const std::string name);
  virtual void LoadInR(RInside &R, const std::string &varname);
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const;
  // virtual void CreateEmpty(RInside &R, const std::string &varname, int dimx,
  //   int dimy) const;
};

}  // namespace presto

#endif  // __ARRAYDATA_H__
