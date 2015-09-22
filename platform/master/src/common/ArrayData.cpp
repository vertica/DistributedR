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

// NOTE: the code in this file has to match up with the
// serialization/deserialization code in DeserializeArray.*

#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>  // for getpagesize

#include <algorithm>  // sort
#include <cstdlib>
#include <cerrno>  // MAP_FAILED
#include <map>
#include <string>
#include <utility>  // make_pair
#include <vector>

#include "timer.h"
#include "common.h"

#include "ArrayData.h"
#include "PrestoException.h"
#include "DistDataFrame.h"
#include "DistList.h"

#define CMD_BUF_SIZE 200  // buffer size for R commands

// #define SPARSE_COMPOSITE_TIMING 1

using namespace std;
using namespace boost::interprocess;

namespace presto {

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_j = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

const char *ArrayData::spill_dir_ = NULL;

bool has_R_instance = true;

// Install a symbol in R
#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = install(#symbol); \
  }

static void install_symbols() {
  if (has_R_instance) {
    if (RSymbol_dim == NULL) {
      RSymbol_dim = R_DimSymbol;
    }
    INSTALL_SYMBOL(Dim);
    INSTALL_SYMBOL(i);
    INSTALL_SYMBOL(j);
    INSTALL_SYMBOL(p);
    INSTALL_SYMBOL(x);
  }
}

/** create a mapping of given input (usually in a shared memory region) to the output address (R object or memory in heap or stack)
 * @param mapping the pointer of shared memory region that will be mapped into target address
 * @param offset offset from the mapping (usually need to exclude the header region)
 * @param size the length of shared memory region to be mapped into target address
 * @param address target address where the input shared memory segment will be mapped
 * @return a pair of address where the shared memory segment is mapped and the size of mapping
 */
pair<void*, size_t> createmapping(
    const BaseSharedMemoryObject &mapping,
    boost::interprocess::offset_t offset,
    std::size_t size,
    void *address) {
  if (size == 0 && address != NULL) {
    return make_pair(address, size);
  }

  void *result = mmap(address,
                      size,
                      PROT_READ|PROT_WRITE,
                      MAP_PRIVATE | (address != NULL ? MAP_FIXED : 0),
                      mapping.get_mapping_handle().handle,
                      offset);

  // Check if mapping was successful
  if (result == MAP_FAILED || (address != NULL && result != address)) {
    LOG_WARN("Mapping of shm(size %zu) to R memory address %p failed", size, address);
    throw PrestoWarningException
      ("failed to map shared memory object to R object\n"
      "Restart session using distributedR_shutdown()");
  }

  return make_pair(result, size);
}

// Keep track of our specially allocated R objects
map<void*, size_t> *freemap = NULL;
static void *presto_malloc_hook(size_t, const void*);
static void *(*old_malloc_hook)(size_t, const void *);
static void presto_free_hook(void*, const void*);
static void (*old_free_hook)(void*, const void *);
static void *(*saved_malloc_hook)(size_t, const void *);
static void (*saved_free_hook)(void*, const void *);

void presto_malloc_init_hook() {
  if (freemap == NULL) {
    freemap = new map<void*, size_t>();
    old_malloc_hook = __malloc_hook;
    old_free_hook = __free_hook;
  }
}

static inline void old_hooks() {
  __malloc_hook = old_malloc_hook;
  __free_hook = old_free_hook;
}

static inline void new_hooks() {
  __malloc_hook = presto_malloc_hook;
  __free_hook = presto_free_hook;
}
/** Special malloc that allocates such that R vector data
 * starts at page boundaries
 * NOTE(erik): assuming sizeof(SEXPREC_ALIGN) < pagesize;
 * this will probably hold for all eternity...
 * @param size to malloc
 * @param caller address to be allocated
 * @return NULL
 */
static void *presto_malloc_hook(size_t size, const void *caller) {
  old_hooks();
  size_t datasize = size-sizeof(SEXPREC_ALIGN);
  size_t mmap_size = getpagesize() + datasize;
  void *result = mmap(0, mmap_size, PROT_READ|PROT_WRITE,
      MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
  if (result == MAP_FAILED) {
    throw PrestoWarningException("malloc_hook: mmap failed");
  }
  void *sexp = (reinterpret_cast<char*>(result)) +
      getpagesize() - sizeof(SEXPREC_ALIGN);
  freemap->insert(make_pair(sexp, mmap_size));
  new_hooks();
  return sexp;
}

/** Checks if an object was allocated by special presto hook and frees accordingly
 * @param ptr a pointer to be freed
 * @param caller a caller
 */
static void presto_free_hook(void *ptr, const void *caller) {
  saved_malloc_hook = __malloc_hook;
  saved_free_hook = __free_hook;
  old_hooks();
  map<void*, size_t>::iterator i = freemap->find(ptr);
  if (i == freemap->end()) {
    // malloc-ed the old way
    free(ptr);
  } else {
    // alloc-ed by my hook
    int ret = munmap(reinterpret_cast<char*>(ptr) +
           sizeof(SEXPREC_ALIGN) - getpagesize(),
           i->second);
    if (ret != 0) {
      throw PrestoWarningException("free_hook: munmap failed");
    }
    freemap->erase(i);
  }
  __malloc_hook = saved_malloc_hook;
  __free_hook = saved_free_hook;
}

/** It creates an matrix with dimension x,y in R-session and return the object. This data segment begins at page boundary
 * @param x the number of row
 * @param y the number of columns
 * @return the resulting matrix object in R-session
 */
static SEXP PrestoCreateArray(int x, int y, SEXPTYPE val_type) {
  new_hooks();
  SEXP result;
  PROTECT(result = Rf_allocMatrix(val_type, x, y));
  __malloc_hook = old_malloc_hook;
  if (Rf_isNull(result)) {
    throw PrestoWarningException("CreateArray: Rf_allocVector failed");
  }
  return result;
}

/** It creates a vector with length x in R-session and return the object. This data segment begins at page boundary
 * @param x the length of a vector
 * @return the resulting matrix object in R-session
 */
SEXP PrestoCreateVector(SEXPTYPE type, size_t x) {
  new_hooks();
  SEXP result;
  PROTECT(result = Rf_allocVector(type, x));
  __malloc_hook = old_malloc_hook;
  if (Rf_isNull(result)) {
    throw PrestoWarningException("CreateVector: Rf_allocVector failed");
  }
  return result;
}

/** Read a given name of shared memory segment and return the pointer 
 * This function simply creates a object and do not allocate shared memory yet
 * @param name a name of the shared memory segment
 * @return corresponding ArrayData class
 */
ArrayData* ParseShm(const string &name) {
  try {
    SharedMemoryObject shm(
        boost::interprocess::open_only, name.c_str(),
        boost::interprocess::read_only);
    boost::interprocess::mapped_region region(shm,
        boost::interprocess::read_only);
    int64_t type = *reinterpret_cast<int64_t*>(region.get_address());
    switch (type) {
    case DENSE: return new DenseArrayData(name);
    case SPARSE: return new SparseArrayData(name);
    case SPARSE_TRIPLET:  return new SparseArrayTripletData(name);
    case DATA_FRAME: return new DistDataFrame(name);
    case LIST: return new DistList(name);
    default:
      ostringstream msg;
      msg << "ParseShm: unknown type \"" << type
          << "\" for variable" << name;
      throw PrestoWarningException(msg.str());
    }
  } catch (boost::interprocess::interprocess_exception &) {
    LOG_DEBUG("ParseShm: Uninited array %s (this should only happen at array creation)", name.c_str());
    return new EmptyArrayData(name);
  }
}

ArrayData* CreateDobjectData(RInside &R, ARRAYTYPE org_class, const string &varname, const string &newname, StorageLayer store, size_t r_size) {
   size_t size = 0;

   if(store == WORKER) {
     char cmd[CMD_BUF_SIZE];
     snprintf(cmd, CMD_BUF_SIZE, "`%s.serializedtmp...` <- serialize(`%s`, NULL)",
         varname.c_str(), varname.c_str());
      R.parseEval(cmd);
      snprintf(cmd, CMD_BUF_SIZE, "as.numeric(object.size(`%s.serializedtmp...`))", varname.c_str());
      size = Rcpp::as<size_t>(R.parseEval(cmd));
    }

    if (org_class == DATA_FRAME) {
       if(store == WORKER) 
         return new DistDataFrame(newname, store, r_size, R[varname+".serializedtmp..."], (size - SEXP_HEADER_SIZE));
       else 
         return new DistDataFrame(newname, store, r_size, NULL, size);
    } else if (org_class == LIST) {
       SEXP sexp_from = R[varname];     // Extracting length of the list in the split.
       int split_len = LENGTH(sexp_from);
       if(store == WORKER) 
         return new DistList(newname, store, r_size, R[varname+".serializedtmp..."], (size - SEXP_HEADER_SIZE), split_len);
       else
         return new DistList(newname, store, r_size, NULL, size, split_len);
    } else {
       ostringstream msg;
       msg << "Unhandled dobject: "<< org_class ;
       throw PrestoWarningException(msg.str());
    }
}

/** Read values of input variable name from R session and write it to shared memory region 
 * This function allocates appropriate size to shared memory segment and write values into the region
 * @param R R inside object
 * @param varname a name of a variable in R-session
 * @param newname a new darray name that will be used in the shared memory region
 * @return a pointer of ArrayData (R array serialization to shared memory) with appropriate type.
 */
ArrayData* ParseVariable(RInside &R, const string &varname, const string &newname, ARRAYTYPE org_class,
                         StorageLayer store) {
  char cmd[CMD_BUF_SIZE];
  snprintf(cmd, CMD_BUF_SIZE, "class(`%s`)", varname.c_str());
  vector<string> classname_vec = Rcpp::as<vector<string>> (R.parseEval(cmd));
  boost::unordered_set<string> classname(classname_vec.begin(), classname_vec.end());

  snprintf(cmd, CMD_BUF_SIZE, "paste(class(`%s`), collapse=', ')", varname.c_str());
  string classname_str = Rcpp::as<string>(R.parseEval(cmd));

  snprintf(cmd, CMD_BUF_SIZE, "object.size(`%s`)", varname.c_str());
  size_t data_size = Rcpp::as<size_t>(R.parseEval(cmd));

  LOG_INFO("org_class: %d, class: %s", org_class, classname_str.c_str());

  if (org_class == DENSE) {
    if (classname.find("matrix") == classname.end() && classname.find("numeric") == classname.end()
            && classname.find("dsyMatrix") == classname.end() && classname.find("dgeMatrix") == classname.end()) {
      ostringstream msg;
      msg << "update(" << varname <<") failure due to data type inconsistency. " <<
        "Variable '" << varname << "' should be a dense matrix instead of " << classname_str <<".";
      throw PrestoWarningException(msg.str().c_str());
    }
    
    if (classname.find("matrix") == classname.end()){
        strcpy(cmd,"");
        snprintf(cmd, CMD_BUF_SIZE, "`%s` <- as(`%s`, \"matrix\")", varname.c_str(),varname.c_str());
        R.parseEval(cmd);
        classname.insert("matrix");
        classname.erase("dsyMatrix");
        classname.erase("dgeMatrix");
    }
    
    return new DenseArrayData(newname, store, data_size, R[varname], classname);
  } else if (org_class == SPARSE) { 
    if (classname.find("dgCMatrix") == classname.end() && classname.find("dsCMatrix") == classname.end()) {    
      ostringstream msg;
      msg << "update(" << varname <<") failure due to data type inconsistency. " <<
        "Variable '" << varname << "' should be a sparse matrix instead of "<< classname_str <<".";
      throw PrestoWarningException(msg.str().c_str());
    }
    
    if(classname.find("dsCMatrix") != classname.end()){
        strcpy(cmd,"");
        snprintf(cmd, CMD_BUF_SIZE, "`%s` <- as(`%s`, \"dgCMatrix\")", varname.c_str(),varname.c_str());
        R.parseEval(cmd);
        classname.insert("dgCMatrix");
        classname.erase("dsCMatrix");
    }
    
    return new SparseArrayData(newname, store, data_size, R[varname], classname);
  } else if (org_class == SPARSE_TRIPLET) {
    if (classname.find("dgTMatrix") == classname.end()) {
      ostringstream msg;
      msg << "update(" << varname <<") failure due to data type inconsistency. " <<
        "Variable '" << varname << "' should be a sparse matrix with triplet format instead of " << classname_str <<".";
      throw PrestoWarningException(msg.str().c_str());
    }

    return new SparseArrayTripletData(newname, store, data_size, R[varname], classname);
  } else if (org_class == DATA_FRAME) {
    if (classname.find("data.frame") == classname.end()) {  // for data.frame we give a serialized input
      ostringstream msg;
      msg << "update(" << varname <<") failure due to data type inconsistency. " <<
        "Variable '" << varname <<"' should be a data frame instead of " << classname_str <<".";
      throw PrestoWarningException(msg.str().c_str());
    }
    return CreateDobjectData(R, DATA_FRAME, varname, newname, store, data_size);
  } else if (org_class == LIST) {
    if (classname.find("list") == classname.end()) {
      ostringstream msg;
      msg << "update(" << varname <<") failure due to data type inconsistency. " <<
        "Variable '" << varname <<"' should be a list instead of " << classname_str <<".";
      throw PrestoWarningException(msg.str().c_str());
    }
    return CreateDobjectData(R, LIST, varname, newname, store, data_size);
  } else if (org_class == EMPTY){
    if (classname.find("matrix") != classname.end() || classname.find("numeric") != classname.end())
       return new DenseArrayData(newname, store, data_size, R[varname], classname);
    else if (classname.find("dgCMatrix") != classname.end()) 
       return new SparseArrayData(newname, store, data_size, R[varname], classname);
    else if (classname.find("dgTMatrix") != classname.end())
       return new SparseArrayTripletData(newname, store, data_size, R[varname], classname);
    else if (classname.find("data.frame") != classname.end())
       return CreateDobjectData(R, DATA_FRAME, varname, newname, store, data_size);
    else if (classname.find("list") != classname.end()) 
       return CreateDobjectData(R, LIST, varname, newname, store, data_size);
    else {
       ostringstream msg;
       msg << "Parse R variable to shm: wrong class \"" << classname_str
        << "\" for variable \"" << varname <<"\"";
       throw PrestoWarningException(msg.str());
    }
  }
}

/** Create a composite array in a shared memory region with given splits
 * @param name name of the composite array in the shared memory region
 * @param offsets a vector of offsets of each split in the entire matrix
 * @param splits a vector of splits ArrayData
 * @param dims dimension of the entire matrix
 * @return the occupied size of array in the shared memory region. 
 *             If the return size is 0, we regard it as DataFrame creation
 *             The composite creation will be done as an EXEC task
 */
size_t CreateComposite(
    const std::string &name,
    const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
    const std::vector<ArrayData*> &splits,
    std::pair<std::int64_t, std::int64_t> dims,
    ARRAYTYPE type,
    StorageLayer store) {
    try {
    switch (type) {
      case DENSE:
        delete new DenseArrayData(name, store, offsets, splits, dims);
        break;
      case SPARSE:
        delete new SparseArrayData(name, store, offsets, splits, dims);
        break;
      case SPARSE_TRIPLET:
        delete new SparseArrayTripletData(name, store, offsets, splits, dims);
        break;
      default:
        char numstr[64];
        sprintf(numstr, "%d", type);
        throw PrestoWarningException
          (std::string("CreateComposite: unknown input type ") + numstr);
    }
  } catch (boost::interprocess::interprocess_exception &) {
    ostringstream msg;
    msg << "CreateComposite: uninitialized split " << splits[0]->GetName()
        << " for composite array" << endl << FILE_DESCRIPTOR_ERR_MSG; 
    throw PrestoWarningException(msg.str());
  }

  SharedMemoryObject shm(open_only, name.c_str(), read_only);
  offset_t size;
  shm.get_size(size);
  return size;
}

/**
 * Returns Type if data partitions is stored in workers
 **/
ARRAYTYPE GetClassType(const std::string& name) {
  SharedMemoryObject shm(
      boost::interprocess::open_only, name.c_str(),
      boost::interprocess::read_only);
  boost::interprocess::mapped_region region(shm,
      boost::interprocess::read_only);
  return (ARRAYTYPE)(*reinterpret_cast<int64_t*>(region.get_address()));
}


/** ArrayData constructor
 * @param name of the ArrayData segment in the shared memory segment
 * @param type determines a type of the ArrayData (dense/sparse matrix)
 * @return an ArrayData object
 */
ArrayData::ArrayData(const string &name_, int type_, StorageLayer store_, size_t size_)
    : name(name_), type((ARRAYTYPE)type_), shm(NULL), store(store_), r_size(size_), header_region(NULL){
  install_symbols();
}

/** Create a shared memory segment. At this time, the size is not determined
 * @param external indicates if the object will be created in the file or shared memory region
 * @return NULL
 */
void ArrayData::OpenShm(bool external) {
  if (external) {
    shm = new FileSharedMemoryObject(name.c_str(), spill_dir_);
  } else {
    shm = new SharedMemoryObject(boost::interprocess::open_or_create,
                                 name.c_str(),
                                 boost::interprocess::read_write);
  }
}

/** ArrayData destructor
 *
 */
ArrayData::~ArrayData() {
  if(shm != NULL)
    delete shm;
}

/** Compress the data structure to reduce network movement
 *
 */
pair<void*, size_t> ArrayData::Compress() {
  return pair<void*, size_t>((void *)NULL, 0);
}

/** Decompress compressed data
 *
 */
void ArrayData::Decompress() {
}

/** Get a name of the shared memory segment
 * @return the name of the shared memory segment
 */
string ArrayData::GetName() const {
  return name;
}

ARRAYTYPE ArrayData::GetClassType() {
  return type;
}

/** Get the size of shared memory segment
 * @return the size of the shared memory segment
 */
size_t ArrayData::GetSize() {
  if(store == WORKER) { 
    boost::interprocess::offset_t size;
    if (!shm->get_size(size)) {
      LOG_ERROR("Array GetSize: could not get shmem size");
      return 0;
    }
    return (size_t) size;
  } else 
    return r_size;
}

/** DenseArrayData constructor. It creates a shared memory segment object. It does not reserve space yet.
 * @param name the name of shared memory segment in the shared memory region
 */
DenseArrayData::DenseArrayData(const string &name, StorageLayer store)
  : ArrayData(name, DENSE, store, 0), array_region(pair<void*, int>((void *)NULL, 0)), header(NULL) {
  //  header_region = new mapped_region(shm, read_only);
  if(store == WORKER) {
    OpenShm(false);
    header_region = new boost::interprocess::mapped_region(*shm,
        boost::interprocess::read_write);
    header = reinterpret_cast<dense_header_t*>(header_region->get_address());
  }
}

/* Create shared memory segment and fill input R object into it. This function gets assigned memory on the shared memory
 * @param name the name of shared memory segment of this array
 * @param sexp_from a R object where we will read the value from. The value will be written into the shared memory region
 * @param classname the name of the class in R of the ArrayData
 */
DenseArrayData::DenseArrayData(const string &name, StorageLayer store, size_t r_size, 
    const SEXP sexp_from, const boost::unordered_set<std::string> &classname)
  : ArrayData(name, DENSE, store, r_size), array_region(pair<void*, int>((void *)NULL, 0)), header(NULL) {
  // Get dimensions, create array
  if (Rf_isNull(sexp_from)) {
    throw PrestoWarningException("DenseArray: updated value of split is NULL");
  }
  int64_t x, y;
  if (classname.find("matrix") != classname.end()) {
    // get dimension symbol variable
    SEXP dimsexp = getAttrib(sexp_from, RSymbol_dim);
    if (Rf_isNull(dimsexp)) {
      throw PrestoWarningException("DenseArray: dimension of updated value of split is NULL");
    }
    x = INTEGER(dimsexp)[0];  // number of rows
    y = INTEGER(dimsexp)[1];  // number of columns
  } else if (classname.find("numeric") != classname.end()) {
    x = LENGTH(sexp_from);
    y = 1;
  }
  SEXPTYPE val_type = TYPEOF(sexp_from);
  if (val_type != INTSXP && val_type != REALSXP && val_type != LGLSXP) {
    throw PrestoWarningException
      ("DenseArray supports only integer, logical, or numeric (real) values");
  }

  if (store == WORKER) {
    // the size of input array (header + data size)
    size_t data_size = x*y*(val_type==REALSXP ? sizeof(double) : sizeof(int));
    size_t size = mapped_size(sizeof(*header)) + mapped_size(data_size);
    if (size < INMEM_UPDATE_SIZE_LIMIT) {
      OpenShm(false);
    } else {
      OpenShm(true);
      LOG_DEBUG("Creating Dense array in external.");
    }

    shm->truncate(size);  // allocate the size in the shared memory region
    header_region = new boost::interprocess::mapped_region(*shm,
        boost::interprocess::read_write);
    // header part
    header = reinterpret_cast<dense_header_t*>(header_region->get_address());

    header->type = type;
    header->dims[0] = x;
    header->dims[1] = y;
    header->value_type = val_type;
    header->store = store;
    // data part (excluding header size)
    void *data = reinterpret_cast<char*>(header)+mapped_size(sizeof(*header));
    if (data == NULL) {
      throw PrestoWarningException("DenseArray: data writable to shm is NULL");
    }
    memcpy(data, (val_type==REALSXP ? (void*)REAL(sexp_from) : (void*)INTEGER(sexp_from)), data_size);
  } else {
    header = new dense_header_t;
    header->type = type;
    header->dims[0] = x;
    header->dims[1] = y;
    header->value_type = val_type;
    header->store = store;
  }
}

/** Load shared memory segments into R-session
 * @param R R-session object
 * @param varname a name of variable in R-session. The shared memory segment value will be written into the varname variable in R-session.
 * @return NULL
 */
void DenseArrayData::LoadInR(RInside &R, const string &varname) {
  if (header == NULL) {
    throw PrestoWarningException("DenseArray::LoadInR: array/header is NULL");
  }

  // create an array in R-session
  SEXP arr = PrestoCreateArray(header->dims[0], header->dims[1], header->value_type);
  // freemap is filled in malloc_hook
  size_t data_size = header->dims[0]*header->dims[1]*
    (header->value_type==REALSXP ? sizeof(double) : sizeof(int));
  if (freemap->find(arr) != freemap->end()) {
    // R used malloc and we intercepted.
    // Overwrite the array pointer in R-session.
    array_region = createmapping(
        *shm,
        mapped_size(sizeof(*header)), data_size,
        reinterpret_cast<SEXPREC_ALIGN*>(arr)+1);
  } else {
    // R didn't use malloc. Need to copy data
    //iR: Map only if array is not zero sized.
    if(data_size >0){
    LOG_DEBUG("DenseArray::Loading shm to R session: R didnt malloc for size %zu", data_size);
    array_region = createmapping(
        *shm, mapped_size(sizeof(*header)), data_size, 0);
    memcpy((header->value_type==REALSXP ? (void*)REAL(arr) : (void*)INTEGER(arr)),
      array_region.first, data_size);
    munmap(array_region.first, array_region.second);
    }else{
      LOG_DEBUG("DenseArray::Skipped loading from shm to R session: Array has size %zu", data_size);
    }
  }
  R[varname] = arr;  // assign the varname with corresponding value in R-session
  UNPROTECT(1);
}

/** create composite shared memory object and write the composite dense array into the shared memory region
 * @param name name of the darray in the shared memory segment
 * @param offsets offsets of each splits. This determine the coordinate of an split in the enitre matrix
 * @param splits the information of splits to be written
 * @param dims dimension of the array (# rows, # cols)
 * @return an object creator
 */
DenseArrayData::DenseArrayData(
    const std::string &name,
    StorageLayer store,
    const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
    const std::vector<ArrayData*> &splits,
    std::pair<std::int64_t, std::int64_t> dims)
    : ArrayData(name, DENSE, store),
      array_region(pair<void*, int>((void *)NULL, 0)), header(NULL) {
  OpenShm(false);  // create a object in the shm object
  if (splits.size() <= 0){
    throw PrestoWarningException("DenseArray composite: number of splits is less than 0");
  }
  boost::interprocess::mapped_region region(*splits[0]->shm, boost::interprocess::read_write);
  dense_header_t* s_header = reinterpret_cast<dense_header_t*>(region.get_address());
  SEXPTYPE dst_val_type = s_header->value_type;

  //Check if all splits are of the same type: logical/int or double/numeric
   for (int i = 1; i < splits.size(); i++) {
    // this should not be NULL, as this function is called after FETCH is done
    if (splits[i]->shm == NULL) {
      throw PrestoWarningException
        ("DenseArray: split shm is NULL");
    }
    boost::interprocess::mapped_region region(*splits[i]->shm,
        boost::interprocess::read_write);
    dense_header_t* tmp_header = reinterpret_cast<dense_header_t*>(region.get_address());
    if(dst_val_type != tmp_header->value_type){
      //Some partitions are numeric while others are logical/int. We need to make everyone numeric
      LOG_DEBUG("Partitions are of different types (numeric,logical,int) when creating dense composite array. Will make composite array numeric");
      dst_val_type = REALSXP;
      break;
    }
   }

  // the size will be header size + #col*#row*sizeof(data_type)
  size_t size = mapped_size(sizeof(*header))+
      mapped_size(dims.first*dims.second*(dst_val_type==REALSXP ? sizeof(double) : sizeof(int)));
  shm->truncate(size);  // allocate the size in the shared memory region

  header_region = new mapped_region(*shm, read_write);
  header = reinterpret_cast<dense_header_t*>(header_region->get_address());
  header->type = type;  // set the type
  header->dims[0] = dims.first;  // number of rows
  header->dims[1] = dims.second;  // number of columns
  header->value_type = dst_val_type;
  // the location where the values of the composite array will be written
  double* dbl_dest = NULL;
  int* int_dest = NULL;
  if (dst_val_type == REALSXP) {
    dbl_dest = reinterpret_cast<double*>(reinterpret_cast<char*>(header)+mapped_size(sizeof(*header)));
  } else if (dst_val_type == INTSXP || dst_val_type == LGLSXP) {
    int_dest = reinterpret_cast<int*>(reinterpret_cast<char*>(header)+mapped_size(sizeof(*header)));
  } else {
    throw PrestoWarningException("DenseArray supports only integer or numeric(real) values");
  }
  if (dbl_dest == NULL && int_dest == NULL){
    throw PrestoWarningException
      ("DenseArray: destination array is NULL");
  }

   //iR(TODO): We should try to use R's NA_REAL constant 
   //NA_REAL in R is IEEE's NA value with lower word as 1954
   //This is the code from R-X.X.X/src/main/arithmetic.c
   //This code assume little endian (i.e. x86 architecture). Otherwise flip the 1 and 0 words for big endian
   typedef union
   {
     double value;
     unsigned int word[2];
   } ieee_double;
   volatile ieee_double R_NA_REAL;
   R_NA_REAL.word[1] = 0x7ff00000;
   R_NA_REAL.word[0] = 1954;

  for (int i = 0; i < splits.size(); i++) {
    // dimension of input split
    pair<std::int64_t, std::int64_t> split_dims = splits[i]->GetDims();
    LOG_DEBUG("Processing split: %d with dimensions (%lld, %lld)", i, split_dims.first, split_dims.second);
    // this should not be NULL, as this function is called after FETCH is done
    if (splits[i]->shm == NULL) {
      throw PrestoWarningException
        ("DenseArray: split shm is NULL");
    }
    boost::interprocess::mapped_region region(*splits[i]->shm,
        boost::interprocess::read_write);
    dense_header_t* s_header = reinterpret_cast<dense_header_t*>(region.get_address());
    SEXPTYPE src_val_type = s_header->value_type;

    double* dbl_data = NULL;
    int* int_data = NULL;
    if (src_val_type == REALSXP) {
      dbl_data = reinterpret_cast<double*>(
        reinterpret_cast<char*>(region.get_address()) +
        mapped_size(sizeof(*header)));  // value of the split
    } else {
      int_data = reinterpret_cast<int*>(
        reinterpret_cast<char*>(region.get_address()) +
        mapped_size(sizeof(*header)));  // value of the split
    }
    if (dbl_data == NULL && int_data == NULL){
      throw PrestoWarningException
        ("DenseArray composite: split data is NULL");
    }

    // determine the absolute row-coordinate of this split
    std::int64_t x = offsets[i].first;
    // optimize for vectors
    if (dims.first == 1) {  // #row = 1
      // fill the composite array shared memory region      
      if (dst_val_type == REALSXP) {
	if(src_val_type == REALSXP){
	  memcpy(&dbl_dest[offsets[i].second], dbl_data,
		 split_dims.second*sizeof(double));
	} else{//Slow path. We have to cast logical/int vectors to numeric(double), and also take care of NA values.
	  for(std::int64_t kk=0; kk<split_dims.second;kk++){
	    dbl_dest[(offsets[i].second)+kk]=(int_data[kk]== std::numeric_limits<int>::min() ? R_NA_REAL.value : int_data[kk]) ;
	  }
	}
      } else {//If it's logical/int, we are sure that the src and dst are of the same type. Just use memcpy.
        memcpy(&int_dest[offsets[i].second], int_data,
               split_dims.second*sizeof(int));
      }
    } else {
      // fill the composite array in row-major order
      for (std::int64_t j = 0; j < split_dims.second; j++) {
        std::int64_t y = offsets[i].second + j;
        // fill the composite array shared region
        if (dst_val_type == REALSXP) {
	  if(src_val_type == REALSXP){
	    memcpy(&dbl_dest[y*dims.first + x], &dbl_data[j*split_dims.first],
		   split_dims.first*sizeof(double));
	  } else{//Slow path. We have to cast logical/int vectors to numeric(double), and also take care of NA values.
	    for(std::int64_t kk=0; kk<split_dims.first;kk++){
	      int v = int_data[(j*split_dims.first)+kk];
	      dbl_dest[(y*dims.first)+x+kk]= (v == std::numeric_limits<int>::min() ? R_NA_REAL.value : v) ;
	    }
	  }
        } else {
          memcpy(&int_dest[y*dims.first + x], &int_data[j*split_dims.first],
               split_dims.first*sizeof(int));
        }
      }
    }
  }
}

/** DenseArrayData destructor
 *
 */
DenseArrayData::~DenseArrayData() {
  if(store = WORKER) {
    delete header_region;
  } else {
    delete header;
  }
}

/** Get dimension of the dense array data (number of rows/columns)
 * @return a pair of number of rows/columns
 */
pair<std::int64_t, std::int64_t> DenseArrayData::GetDims() const {
  return make_pair(header->dims[0], header->dims[1]);
}

/** A constructor for a SparseArray. This constructor does not assgin a shared memory with specific size.
 * @param name name of the sparse array in the shared memory region
 * @return an object of sparse array data
 */
SparseArrayData::SparseArrayData(const string &name, StorageLayer store)
  : ArrayData(name, SPARSE, store, 0) {
  //  header_region = new mapped_region(shm, read_only);
  OpenShm(false);
  header_region = new boost::interprocess::mapped_region(*shm,
      boost::interprocess::read_write);
  header = reinterpret_cast<sparse_header_t*>(header_region->get_address());
}

/** Choose optimal encoding mechanism based on the number of non-zero
 * @param nnz the number of non-zero element in the sparse array
 * @param x the number of rows of sparse matrix
 * @param y the number of columns of sparse matrix
 * @return optimal encoding method (triplet or CSC)
 */
sprs_encoding_t SparseArrayData::ChooseEncoding(
    int64_t nnz, int64_t x, int64_t y) {
  size_t csc_size = nnz*(8+4) + y*4;
  size_t triplet_size = nnz*(8+8+8);

  if (triplet_size < csc_size) {
    return TRIPLET;
  } else {
    return CSC;
  }
}

/** A SparseArrayData constructor. It creates a sparse array data in shared memory region by reading data from input R variable
 * @param name a name of the shared memory segment (usually the darray name)
 * @param sexp_from a R object where we will read the data from. The data will be written into shared memory region
 * @param classname a name of the class of input array
 * @return a SparseArrayData object
 */
SparseArrayData::SparseArrayData(const string &name, StorageLayer store, size_t r_size, 
    const SEXP sexp_from, const boost::unordered_set<std::string> &classname)
  : ArrayData(name, SPARSE, store, r_size) {
  // Get dimensions*nnzs, create array
  int64_t dim0, dim1, nonzeros;
  if (Rf_isNull(sexp_from)) {
    throw PrestoWarningException
      ("SparseArray: updated value of split is NULL");
  }
  if (classname.find("dgCMatrix") != classname.end()) {   // CSC sparse format
    SEXP dimsexp = getAttrib(sexp_from, RSymbol_Dim);
    SEXP fromi = getAttrib(sexp_from, RSymbol_i);  // get i attribute
    if (Rf_isNull(dimsexp) || Rf_isNull(fromi)) {
      throw PrestoWarningException
        ("SparseArray: dimension of updated value of split or fromi is NULL");
    }
    dim0 = INTEGER(dimsexp)[0];  // number of rows
    dim1 = INTEGER(dimsexp)[1];  // number of columns
    nonzeros = length(fromi);
  }

  if(store == WORKER) {

    int *data_i, *data_p;  // in CSC format, we use i,p,x attributes
    double *data_x;
    size_t size = mapped_size(sizeof(*header)) +
        mapped_size(nonzeros*sizeof(data_i[0])) +
        mapped_size(nonzeros*sizeof(data_x[0])) +
        mapped_size((dim1+1)*sizeof(data_p[0]));

    if (size < INMEM_UPDATE_SIZE_LIMIT) {
      OpenShm(false);
    } else {
      OpenShm(true);
      LOG_DEBUG("SparseArray: Creating sparse array in external");
    }

    shm->truncate(size);  // allocate the given size into shared memory region
    // beginning region of the shared memory region
    header_region = new boost::interprocess::mapped_region(
        *shm, boost::interprocess::read_write);
    header = reinterpret_cast<sparse_header_t*>(header_region->get_address());
    // Set header fields
    header->type = type;
    header->nnz = nonzeros;
    header->dims[0] = dim0;
    header->dims[1] = dim1;
    header->encoding = CSC;
    header->store = store;

    // Copy data, the sequence is i -> x -> p
    data_i = reinterpret_cast<int*>(
        reinterpret_cast<char*>(header)+
        mapped_size(sizeof(*header)));
    data_x =  reinterpret_cast<double*>(
        reinterpret_cast<char*>(data_i)+
        mapped_size(header->nnz*sizeof(data_i[0])));
    data_p = reinterpret_cast<int*>(
        reinterpret_cast<char*>(data_x)+
        mapped_size(header->nnz*sizeof(data_x[0])));

    if (classname.find("dgCMatrix") != classname.end()) {
      SEXP fromi = getAttrib(sexp_from, RSymbol_i);
      SEXP fromx = getAttrib(sexp_from, RSymbol_x);
      SEXP fromp = getAttrib(sexp_from, RSymbol_p);

      if (Rf_isNull(fromi) || Rf_isNull(fromx) || Rf_isNull(fromp)) {
        throw PrestoWarningException
          ("SparseArray: fromi/fromp/fromx read failure");
      }
      if (data_i == NULL || data_x == NULL || data_p == NULL) {
        throw PrestoWarningException
          ("SparseArray: data_i/data_x/data_p is NULL");
      }
      memcpy(data_i, INTEGER(fromi), nonzeros*sizeof(data_i[0]));
      memcpy(data_x, REAL(fromx), nonzeros*sizeof(data_x[0]));
      memcpy(data_p, INTEGER(fromp), (dim1+1)*sizeof(data_p[0]));
    }
  } else {    //StorageLayer == EXECUTOR

    header = new sparse_header_t;
    header->type = type;
    header->nnz = nonzeros;
    header->dims[0] = dim0;
    header->dims[1] = dim1;
    header->encoding = CSC;
    header->store = store;
  }
}

/** Load shared memory segments into R-session
 * @param R R-session object
 * @param varname a name of variable in R-session. The shared memory segment value will be written into the varname variable in R-session.
 * @return NULL
 */
void SparseArrayData::LoadInR(RInside &R, const string &varname) {
  char cmd[CMD_BUF_SIZE];
  SEXP i_vec, p_vec, x_vec;
  size_t offset, size;
  // create an array in R-session
  i_vec = PrestoCreateVector(INTSXP, header->nnz);
  offset = mapped_size(sizeof(*header));
  // size of i-vector that is the same of nnz
  size = header->nnz*sizeof(INTEGER(i_vec)[0]);
  if (freemap->find(i_vec) != freemap->end()) {
    // R used malloc and we intercepted
    createmapping(
        *shm,
        offset,
        size,
        reinterpret_cast<SEXPREC_ALIGN*>(i_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArray::Loading shm to R session: R didnt malloc for size %zu", size);
    if (header->nnz > 0) {
      std::pair<void*, size_t> i_region =
        createmapping(*shm, offset, size, 0);
      memcpy(INTEGER(i_vec), i_region.first, size);
      munmap(i_region.first, i_region.second);
    }
  }
  // fill in x vector
  x_vec = PrestoCreateVector(REALSXP, header->nnz);
  offset += mapped_size(size);
  size = header->nnz*sizeof(REAL(x_vec)[0]);
  if (freemap->find(x_vec) != freemap->end()) {
    // R used malloc and we intercepted
    createmapping(*shm, offset, size,
        reinterpret_cast<SEXPREC_ALIGN*>(x_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArray::Loading shm to R session: R didnt malloc for size %zu", size);
    if (header->nnz > 0) {
      std::pair<void*, size_t> x_region =
        createmapping(*shm, offset, size, 0);
      memcpy(REAL(x_vec), x_region.first, size);
      munmap(x_region.first, x_region.second);
    }
  }

  // fill in p vector
  p_vec = PrestoCreateVector(INTSXP, header->dims[1]+1);
  offset += mapped_size(size);
  size = (header->dims[1]+1)*sizeof(INTEGER(p_vec)[0]);
  if (freemap->find(p_vec) != freemap->end()) {
    createmapping(*shm, offset, size,
        reinterpret_cast<SEXPREC_ALIGN*>(p_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArray::Loading shm to R session: R didnt malloc for size %zu", size);
    std::pair<void*, size_t> p_region =
      createmapping(*shm, offset, size, 0);
    memcpy(INTEGER(p_vec), p_region.first, size);
    munmap(p_region.first, p_region.second);
  }
  snprintf(cmd, CMD_BUF_SIZE,
          "%s <- sparseMatrix(x=1.0, i=c(1), j=c(1), dims=c(1,1))",
          varname.c_str());
  SEXP newobj = R.parseEval(cmd);
  snprintf(cmd, CMD_BUF_SIZE,
           "%s@Dim <- as.integer(c(%d,%d))",
           varname.c_str(),
           static_cast<int>(header->dims[0]),
           static_cast<int>(header->dims[1]));
  R.parseEvalQ(cmd);

  setAttrib(newobj, RSymbol_i, i_vec);
  setAttrib(newobj, RSymbol_p, p_vec);
  setAttrib(newobj, RSymbol_x, x_vec);
  UNPROTECT(3);
}

/** A SparseArrayData constructor for a composite spasre array
 * @param name a name of the sparse array
 * @param offsets a vector of offsets (location from (0,0)) for each split
 * @param splits a vector of split array data
 * @param dims the dimension of input array
 * @return a compoiste sparse array object 
 */
SparseArrayData::SparseArrayData(
    const std::string &name,
    StorageLayer store,
    const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
    const std::vector<ArrayData*> &splits,
    std::pair<std::int64_t, std::int64_t> dims)
    : ArrayData(name, SPARSE, store) {
  // need to make sure that we go through splits in right order
  int nnz = 0;

  // count total nnz
  for (int i = 0; i < splits.size(); i++) {
    nnz += reinterpret_cast<SparseArrayData*>(splits[i])->header->nnz;
  }

  typedef pair<pair<int, int>, double> triplet;

#ifdef SPARSE_COMPOSITE_TIMING
  Timer t;
  t.start();
#endif

  // first construct in triplet format
  vector<triplet> triplets(nnz);
  // iterate over each split
  for (int i = 0, n = 0; i < splits.size(); i++) {
    SparseArrayData *split = reinterpret_cast<SparseArrayData*>(splits[i]);
    if (split == NULL) {
      throw PrestoWarningException("SparseArray: split is NULL");
    }
    // data region of the split is after the header region
    mapped_region data_region(*split->shm, read_write,
                              mapped_size(sizeof(*header)));
    // the data is kept in the order of i->x->p vector
    int *data_i = reinterpret_cast<int*>(data_region.get_address());
    if (data_i == NULL) {
      throw PrestoWarningException("SparseArray: data_i is NULL");
    }
    double *data_x = reinterpret_cast<double*>(
        reinterpret_cast<char*>(data_i)+
        mapped_size(split->header->nnz*sizeof(data_i[0])));
    if (data_x == NULL) {
      throw PrestoWarningException("SparseArray: data_x is NULL");
    }
    int *data_p = reinterpret_cast<int*>(
        reinterpret_cast<char*>(data_x)+
        mapped_size(split->header->nnz*sizeof(data_x[0])));
    if (data_p == NULL) {
      throw PrestoWarningException("SparseArray: data_p is NULL");
    }

    for (int j = 0, col = 0; j < split->header->nnz; j++) {
      while (j == data_p[col+1]) {
        col++;
      }
      // we need to swap the coordinates in order to sort efficiently
      // for dgCMatrix format!
      triplets[n++] = make_pair(make_pair(offsets[i].second + col,
                                          offsets[i].first + data_i[j]),
                                data_x[j]);
    }
  }

#ifdef SPARSE_COMPOSITE_TIMING
  LOG_DEBUG("SparseArray composite: triplet construction");
#endif

  // sort triplets
  sort(triplets.begin(), triplets.end());

#ifdef SPARSE_COMPOSITE_TIMING
  LOG_DEBUG("SparseArray composite: triplet sort");
#endif

  // construct composite in shared memory
  // allocate shared memory region with corresponding size
  OpenShm(false);

  int *data_i, *data_p;
  double *data_x;
  size_t size = mapped_size(sizeof(*header)) +
      mapped_size(nnz*sizeof(data_i[0])) +
      mapped_size((dims.second+1)*sizeof(data_p[0])) +
      mapped_size(nnz*sizeof(data_x[0]));
  
  shm->truncate(size);


  header_region = new boost::interprocess::mapped_region(*shm,
      boost::interprocess::read_write);
  header = reinterpret_cast<sparse_header_t*>(header_region->get_address());
  header->type = type;
  header->nnz = nnz;
  header->dims[0] = dims.first;
  header->dims[1] = dims.second;
  header->encoding = CSC;

  // Copy data into shared memory region from triplet format to CSC format
  data_i = reinterpret_cast<int*>(
      reinterpret_cast<char*>(header)+
      mapped_size(sizeof(*header)));
  if (data_i == NULL) {
    throw PrestoWarningException("SparseArray: data_i is NULL");
  }
  data_x =  reinterpret_cast<double*>(
      reinterpret_cast<char*>(data_i)+
      mapped_size(header->nnz*sizeof(data_i[0])));
  if (data_x == NULL) {
    throw PrestoWarningException("SparseArray: data_x is NULL");
  }
  data_p = reinterpret_cast<int*>(
      reinterpret_cast<char*>(data_x)+
      mapped_size(header->nnz*sizeof(data_x[0])));
  if (data_p == NULL) {
    throw PrestoWarningException("SparseArray: data_p is NULL");
  }

#define COL(x) (x.first.first)
#define ROW(x) (x.first.second)

  // fill
  int j = 0;
  if (triplets.size() > 0) {
    for (j = 0; j <= COL(triplets[0]); j++) {
      data_p[j] = 0;
    }
  }
  for (int i = 0; i < triplets.size(); i++) {
    if (i>0 && COL(triplets[i]) != COL(triplets[i-1])) {
      for (j = COL(triplets[i-1])+1; j <= COL(triplets[i]); j++) {
        data_p[j] = i;
      }
    }
    data_i[i] = ROW(triplets[i]);
    data_x[i] = triplets[i].second;
  }
  for (; j < dims.second+1; j++) {
    data_p[j] = nnz;
  }

#undef COL
#undef ROW

#ifdef SPARSE_COMPOSITE_TIMING
  LOG_DEBUG("SparseArray composite: Fill");
#endif
}

/** A static function that converts j vector to p vector.
 * This function is necessary when we create dgCMatrix using triplet matrix
 * Note that the output vector has to be freed!!!
 * @param j_vector a list of j values (column values in triplet format)
 * @param nnz the number of non-zero
 * @param num_col the number of column
 * @return a converted P vector. This has to be freeed after use!!!!!!!
 */
int* SparseArrayData::ConvertJtoPVector(int* j_vector, int nnz, int num_col) {
  int *p_vector = new int[num_col+1];
  if (NULL == p_vector) {
    ostringstream msg;
    msg << "SparseArray::ConvertJtoPVector: failed to allocate vector memory\n"
      << "Restart session using distributedR_shutdown()";
    throw PrestoWarningException(msg.str());
  }

  int j = 0;
  if (nnz != 0) {
    for (j = 0; j <= j_vector[0]; ++j)
      p_vector[j] = 0;
  }

  for (int i = 1; i < nnz; ++i) {
    if (j_vector[i] != j_vector[i-1]) {
      for (j = j_vector[i-1]+1; j <= j_vector[i]; ++j) {
        p_vector[j] = i;
      }
    }
  }

  for (; j < (num_col+1); ++j) {
    p_vector[j] = nnz;
  }
  return p_vector;
}

/** SparseArrayData destructor
 *
 */
SparseArrayData::~SparseArrayData() {
  if(store = WORKER) {
    delete header_region;
  } else {
    delete header;
  }
}

/** Get the dimensions of the sparse array
 * @return a pair of the number of row/column
 */
pair<std::int64_t, std::int64_t> SparseArrayData::GetDims() const {
  return make_pair(header->dims[0], header->dims[1]);
}

// SparseArrayTripletData
SparseArrayTripletData::SparseArrayTripletData(const string &name, StorageLayer store)
  : ArrayData(name, SPARSE_TRIPLET, store, 0) {
  if(store == WORKER) {
    OpenShm(false);
    header_region = new boost::interprocess::mapped_region(*shm,
      boost::interprocess::read_write);
    header = reinterpret_cast<sparse_triplet_header_t*>
      (header_region->get_address());
  }
}

SparseArrayTripletData::SparseArrayTripletData(const string& name, StorageLayer store, size_t r_size,
    const SEXP sexp_from,
    const boost::unordered_set<std::string> &classname, const int32_t startx, const int32_t starty,
    const int32_t endx, const int32_t endy, const int64_t nonzeros,
    bool is_row_split)
  : ArrayData(name, SPARSE_TRIPLET, store, r_size) {
  throw PrestoWarningException
    ("SparseArrayTriplet: triplet partitioning is not supported\n");
}

/** A SparseArrayTripletData constructor. It creates a sparse array data in shared memory region by reading data from input R variable
 * @param name a name of the shared memory segment (usually the darray name)
 * @param sexp_from a R object where we will read the data from. The data will be written into shared memory region
 * @param classname a name of the class of input array
 * @return a SparseArrayData object
 */
SparseArrayTripletData::SparseArrayTripletData(const string &name, StorageLayer store, size_t r_size,
    const SEXP sexp_from,
    const boost::unordered_set<std::string> &classname)
  : ArrayData(name, SPARSE_TRIPLET, store, r_size) {
  // Get dimensions*nnzs, create array
  int64_t dim0, dim1, nonzeros;
  if (classname.find("dgTMatrix") == classname.end()) {
    std::string classname_str = (classname.size() > 0) ? *(classname.begin()) : ""; 
    ostringstream msg;
    msg << "SparseArrayTriplet: classtype \""
    << classname_str << "\" is not supported";
    throw PrestoWarningException(msg.str());
  }
  SEXP dimsexp = getAttrib(sexp_from, RSymbol_Dim);
  dim0 = INTEGER(dimsexp)[0];
  dim1 = INTEGER(dimsexp)[1];
  SEXP fromi = getAttrib(sexp_from, RSymbol_i);
  nonzeros = length(fromi);

  if(store == WORKER) {

    int *data_i, *data_j;
    double *data_x;
    size_t size = mapped_size(sizeof(*header)) +
        mapped_size(nonzeros*sizeof(data_i[0])) +
        mapped_size(nonzeros*sizeof(data_x[0])) +
        mapped_size(nonzeros*sizeof(data_j[0]));

    // size needs to be a multiple of 512 for libaio to work
    if (size % 512 != 0)
      size = (size/512 + 1) * 512;

    if (size < INMEM_UPDATE_SIZE_LIMIT) {
      OpenShm(false);
    } else {
      OpenShm(true);
      LOG_DEBUG("SparseArrayTriplet: creating sparse array triplet in external");
    }

    shm->truncate(size);  // allocate the size in the shared memory region
    header_region = new boost::interprocess::mapped_region(*shm,
        boost::interprocess::read_write);
    // header part
    header = reinterpret_cast<sparse_triplet_header_t*>
    (header_region->get_address());

    header->type = type;
    header->nnz = nonzeros;
    header->dims[0] = dim0;
    header->dims[1] = dim1;
    header->store = store;

    // Copy data
    data_i = reinterpret_cast<int*>(
        reinterpret_cast<char*>(header)+
        mapped_size(sizeof(*header)));
    data_x =  reinterpret_cast<double*>(
        reinterpret_cast<char*>(data_i)+
        mapped_size(header->nnz*sizeof(data_i[0])));
    data_j = reinterpret_cast<int*>(
        reinterpret_cast<char*>(data_x)+
        mapped_size(header->nnz*sizeof(data_x[0])));

    SEXP fromj = getAttrib(sexp_from, RSymbol_j);
    SEXP fromx = getAttrib(sexp_from, RSymbol_x);

    memcpy(data_i, INTEGER(fromi), nonzeros*sizeof(data_i[0]));
    memcpy(data_j, INTEGER(fromj), nonzeros*sizeof(data_j[0]));
    memcpy(data_x, REAL(fromx), nonzeros*sizeof(data_x[0]));
  } else {
    header = new sparse_triplet_header_t;
    header->type = type;
    header->nnz = nonzeros;
    header->dims[0] = dim0;
    header->dims[1] = dim1;
    header->store = store;
  }
}

void SparseArrayTripletData::LoadInR(RInside &R, const string &varname) {
  char cmd[CMD_BUF_SIZE];
  SEXP i_vec, j_vec, x_vec;
  size_t offset, size;

  // i_vec = PrestoCreateVector(INTSXP, header->nnz);
  offset = mapped_size(sizeof(*header));

  i_vec = PrestoCreateVector(INTSXP, header->nnz);
  // size of i-vector that is the same of nnz
  size = header->nnz*sizeof(INTEGER(i_vec)[0]);
  if (freemap->find(i_vec) != freemap->end()) {
    // R used malloc and we intercepted
    createmapping(
        *shm,
        offset,
        size,
        reinterpret_cast<SEXPREC_ALIGN*>(i_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArrayTriplet::Loading shm to R session: R didnt malloc for size %zu", size);
    if (header->nnz > 0) {
      std::pair<void*, size_t> i_region =
        createmapping(*shm, offset, size, 0);
      memcpy(INTEGER(i_vec), i_region.first, size);
      munmap(i_region.first, i_region.second);
    }
  }
  // fill in x vector
  x_vec = PrestoCreateVector(REALSXP, header->nnz);
  offset += mapped_size(size);
  size = header->nnz*sizeof(REAL(x_vec)[0]);
  if (freemap->find(x_vec) != freemap->end()) {
    // R used malloc and we intercepted
    createmapping(
        *shm, offset, size,
        reinterpret_cast<SEXPREC_ALIGN*>(x_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArrayTriplet::Loading shm to R session: R didnt malloc for size %zu", size);
    if (header->nnz > 0) {
      std::pair<void*, size_t> x_region =
        createmapping(*shm, offset, size, 0);
      memcpy(REAL(x_vec), x_region.first, size);
      munmap(x_region.first, x_region.second);
    }
  }


  offset += mapped_size(size);
  size = header->nnz*sizeof(INTEGER(j_vec)[0]);



  j_vec = PrestoCreateVector(INTSXP, header->nnz);
  offset += mapped_size(size);
  size = (header->nnz)*sizeof(INTEGER(j_vec)[0]);
  if (freemap->find(j_vec) != freemap->end()) {
    createmapping(*shm, offset, size,
        reinterpret_cast<SEXPREC_ALIGN*>(j_vec)+1);
  } else {
    // R didn't use malloc
    LOG_DEBUG("SparseArrayTriplet::Loading shm to R session: R didnt malloc for size %zu", size);
    std::pair<void*, size_t> j_region =
      createmapping(*shm, offset, size, 0);
    memcpy(INTEGER(j_vec), j_region.first, size);
    munmap(j_region.first, j_region.second);
  }
  snprintf(cmd, CMD_BUF_SIZE,
          "%s <- new(\"dgTMatrix\")",
          varname.c_str());
  SEXP newobj = R.parseEval(cmd);
  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = header->dims[0];
  INTEGER(dim)[1] = header->dims[1];

  setAttrib(newobj, RSymbol_Dim, dim);
  setAttrib(newobj, RSymbol_i, i_vec);
  setAttrib(newobj, RSymbol_j, j_vec);
  setAttrib(newobj, RSymbol_x, x_vec);
  UNPROTECT(4);
}

SparseArrayTripletData::SparseArrayTripletData(
    const std::string &name,
    StorageLayer store,
    const std::vector<std::pair<std::int64_t, std::int64_t> > &offsets,
    const std::vector<ArrayData*> &splits,
    std::pair<std::int64_t, std::int64_t> dims)
    : ArrayData(name, SPARSE_TRIPLET, store) {
  int nnz = 0;

  // count total nnz
  for (int i = 0; i < splits.size(); i++) {
    nnz += reinterpret_cast<SparseArrayTripletData*>(splits[i])->header->nnz;
  }

  // construct composite in shared memory
  int *data_i, *data_j;
  double *data_x;
  size_t size = mapped_size(sizeof(*header)) +
      mapped_size(nnz*sizeof(data_i[0])) +
      mapped_size(nnz*sizeof(data_j[0])) +
      mapped_size(nnz*sizeof(data_x[0]));


  // size needs to be a multiple of 512 for libaio to work
  if (size % 512 != 0)
    size = (size/512 + 1) * 512;


  OpenShm(false);  // create a object in the shm object
  shm->truncate(size);  // allocate the size in the shared memory region

  header_region = new mapped_region(*shm, read_write);
  header = reinterpret_cast<sparse_triplet_header_t*>
    (header_region->get_address());
  header->type = type;
  header->nnz = nnz;
  header->dims[0] = dims.first;
  header->dims[1] = dims.second;

  data_i = reinterpret_cast<int*>(
      reinterpret_cast<char*>(header)+
      mapped_size(sizeof(*header)));
  data_x =  reinterpret_cast<double*>(
      reinterpret_cast<char*>(data_i)+
      mapped_size(header->nnz*sizeof(data_i[0])));
  data_j = reinterpret_cast<int*>(
      reinterpret_cast<char*>(data_x)+
      mapped_size(header->nnz*sizeof(data_x[0])));

#ifdef SPARSE_COMPOSITE_TIMING
  Timer t;
  t.start();
#endif

  for (int i = 0, n = 0; i < splits.size(); i++) {
    SparseArrayTripletData *split =
      reinterpret_cast<SparseArrayTripletData*>(splits[i]);

    boost::interprocess::mapped_region region(*splits[i]->shm,
        boost::interprocess::read_write);
    char* addr = reinterpret_cast<char*>(
        reinterpret_cast<char*>(region.get_address()) +
        mapped_size(sizeof(*header)));  // value of the split

    int *cur_data_i = reinterpret_cast<int*>
        (addr + mapped_size(sizeof(*header)));
    double *cur_data_x = reinterpret_cast<double*>(
        reinterpret_cast<char*>(cur_data_i)+
        mapped_size(split->header->nnz*sizeof(cur_data_i[0])));

    int *cur_data_j = reinterpret_cast<int*>(
        reinterpret_cast<char*>(cur_data_x)+
        mapped_size(split->header->nnz*sizeof(cur_data_x[0])));

    for (int j = 0; j < split->header->nnz; j++) {
      data_i[n+j] = cur_data_i[j] + offsets[i].first;
      data_j[n+j] = cur_data_j[j] + offsets[i].second;
      data_x[n+j] = cur_data_x[j];
    }

    n += split->header->nnz;
  }
}

SparseArrayTripletData::~SparseArrayTripletData() {
  if(store = WORKER) {
    delete header_region;
  } else {
    delete header;
  }
}

pair<std::int64_t, std::int64_t> SparseArrayTripletData::GetDims() const {
  return make_pair(header->dims[0], header->dims[1]);
}

/** An EmptyArrayData constructor. It does not determine the type yet
 * @param name the name of the ArrayData
 */
EmptyArrayData::EmptyArrayData(const string name) : ArrayData(name, EMPTY) {
}

/** Loading v variable name into R session
 * @param R R-session object
 * @param varname the name of R object where the data will be loaded
 * @return NULL
 */
void EmptyArrayData::LoadInR(RInside &R, const string &varname) {
  R[varname] = R_NilValue;
}

/** Get dimension of array data
 * @return the dimension (0,0)
 */
pair<std::int64_t, std::int64_t> EmptyArrayData::GetDims() const {
  return make_pair(0, 0);
}

}  // namespace presto
