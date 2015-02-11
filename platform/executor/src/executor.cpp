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
 * Executor process that creates an R instance on startup and then waits in
 * an infinite loop for tasks to execute. Updated arrays are serialized into
 * shared memory and IPC'd back to the Worker.
 */
/**
 * Executor task input format:
 * number_of_vars shared_mem_name1 var_name1 shared_mem_name2 var_name2 ...
 * number_of_raws raw_name1 size1 data1 size2 data2 ...
 * number_of_composites var_name1 number_of_splits1 split_name1 x_offset1 y_offset1 split_name2 x_offset2 y_offset2 ...
 * number_of_vars==EXECUTOR_SHUTDOWN_CODE is a shutdown request
 */

#include <malloc.h>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <sys/prctl.h>
#include <signal.h>
#include <zmq.hpp>

#include <tuple>
#include <cstdio>
#include <map>
#include <string>
#include <set>
#include <utility>
#include <vector>
#include <math.h>
#include <RInside.h>
#include "atomicio.h"
#include "common.h"
#include "dLogger.h"
#include "interprocess_sync.h"
#include "timer.h"

#include "ArrayData.h"
#include "UpdateUtils.h"

#define CMD_BUF_SIZE 512  // R command buffer size
#define MAX_LOGNAME_LENGTH 200  // executor log file name length
#define DUMMY_FILE_SIZE (128LL<<20)  // 128MB

using namespace std;
using namespace presto;

namespace presto {

uint64_t abs_start_time;

FILE *logfile;
FILE *out;

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define LOG(n, a...) fprintf(logfile, n, ## a )
// #define LOG(n, a...)

#define INSTALL_SYMBOL(symbol)                  \
  if (RSymbol_##symbol == NULL) {               \
    RSymbol_##symbol = Rf_install(#symbol);     \
  }

// contains a list of variables to update
// (a function that usually called within foreach)
set<tuple<string, bool, int64_t, int64_t> > *updatesptr;

/** Propages new value to other workers. 
 * This is generally called from Presto R funtion update(). 
 * It keeps the variable within a global variable updatesptr
 * @param updates_ptr_sexp R variable (updates.ptr...) that keeps track of a name of update object. 
 * This variable shared between R and C++ library using RInside.
 * @param name of a variable in R session - deparsed name in string 
 * @param empty indicates if this variable is empty
 * @return NULL
 */
  RcppExport SEXP NewUpdate(SEXP updates_ptr_sexp, SEXP name_sexp, SEXP empty_sexp, SEXP dim_sexp) {
  BEGIN_RCPP
      if (updatesptr == NULL) {
        LOG_WARN("NewUpdate => updateptr is NULL");
        throw PrestoWarningException("NewUpdate::updatesptr is null");
      }

      Rcpp::CharacterVector name_vec(name_sexp);
      Rcpp::LogicalVector empty_vec(empty_sexp);
      Rcpp::NumericVector dim_vec(dim_sexp);
      typedef Rcpp::CharacterVector::iterator char_itr;
      typedef Rcpp::LogicalVector::iterator log_itr;
      typedef Rcpp::NumericVector::iterator numeric_itr;

      char_itr name_itr = name_vec.begin();
      log_itr empty_itr = empty_vec.begin();
      numeric_itr dim_itr = dim_vec.begin();

      std::string name = std::string(name_itr[0]);
      bool empty = empty_vec[0];
      int64_t nr = dim_vec[0];
      int64_t nc = dim_vec[1];

      updatesptr->insert(make_tuple(name, empty, nr, nc));
  return Rcpp::wrap(true);
  END_RCPP
}

/** Send task result update to worker at the end of task execution. 
 * It creates new split version and its name, then it read value of new variable from R session 
 * and sync it with new split by writing it into shared memory region
 * @param varname a name of updated variable in R-session
 * @param splitname a name of split that is internal to Presto library
 * @param R a pointer of RInside
 * @empty indicates if this variable is empty
 * @obj_nrow no. of rows in split. -1 in case of composite arrays and lists. Primarily used for dframes.
 * @obj_ncol no. of cols in split. -1 in case of composite arrays and lists. Primarily used for dframes.
 * @return NULL
 */
static inline void CreateUpdate(const string &varname, const string &splitname,
                                RInside &R,
                                bool empty,
				int64_t obj_nrow, int64_t obj_ncol) {
  Timer t;
  t.start();
  string prev_arr_name = splitname;  // darray name
  ARRAYTYPE org_class = EMPTY;
  try {
    org_class = GetClassType(prev_arr_name);
  } catch (std::exception e) {}
  LOG_DEBUG("Updating %s (%s in R)", prev_arr_name.c_str(), varname.c_str());
  const char* old_ver = rindex(prev_arr_name.c_str(), '_');
  if (old_ver == NULL) {
    ostringstream msg;
    msg << "Presto Executor: CreateUpdate: Wrong darray name: "
      <<prev_arr_name;
    LOG_ERROR("CreateUpdate => Wrong darray/dframe name: %s", prev_arr_name.c_str());
    throw PrestoWarningException(msg.str());
  }
  string new_arr_name;
  if (IsCompositeArray(splitname) == true) {    
    new_arr_name = string(splitname);  // in case of composite, keep the current name
    LOG_DEBUG("Variable %s is a Composite Array. Its new Array Name is %s", prev_arr_name.c_str(), new_arr_name.c_str());
  } else {
    int32_t cur_version = (int32_t)(strtol(old_ver + 1, NULL, 10));
    // if it were not a composite array, increment it.
    int32_t new_version = cur_version + 1;
    new_arr_name = prev_arr_name.substr(
        0, old_ver - prev_arr_name.c_str() + 1);
    new_arr_name += int_to_string(new_version);
    LOG_DEBUG("Variable %s is a Split. Its new Array Name is %s", prev_arr_name.c_str(), new_arr_name.c_str());
  }
  t.start();
  ArrayData *ad = ParseVariable(R, varname, new_arr_name, org_class);
  if (ad == NULL) {
    LOG_ERROR("CreateUpdate => Failed to write R value into Shared memory");
    throw PrestoWarningException
      ("Executor:CreateUpdate Fail to write R value into shared memory");
  }
  // UpdateData upd(new_arr_name.c_str(), new_arr_name.size(), ad->GetSize(),
  //     char_allocator);
  // // update_set->push_back(::move(upd));
  // update_set->push_back(upd);

  // fprintf(stderr, "sending update %s %zu %zu %d\n",
  //         new_arr_name.c_str(),
  //         ad->GetOffset(), ad->GetSize(), empty ? 1 : 0);
  t.start();
  // write task result to send it to worker.
  std::pair<size_t, size_t> dims = ad->GetDims();  

  //TODO(iR) Hack for obtaining size of dataframe splits, since GetDims() is 0 for dframes and lists
  if(org_class == DATA_FRAME){
    dims = make_pair(obj_nrow,obj_ncol);
  }
  
  LOG_DEBUG("New value of variable written in Shared memory. Size=(%d,%d)", dims.first, dims.second);
  AppendUpdate(new_arr_name, ad->GetSize(), empty, dims.first, dims.second, out);
  delete ad;
  LOG_INFO("Variable %s (%s in R) updated successfully.", prev_arr_name.c_str(), varname.c_str());
}
}  // namespace presto

#ifdef INCREASE_R_HEAP
static void *dummy_malloc_hook(size_t, const void*);
static void *(*old_malloc_hook)(size_t, const void *);
#define DUMMY_FILE_NAME "/dev/shm/presto_dummy"
static size_t executor_heap_size = 4000 * (1LL<<20);
static const float R_heap_grow_threshold = .7f;

static inline size_t dummy_object_size() {
  return R_heap_grow_threshold * executor_heap_size /
      (1 - R_heap_grow_threshold);
}

static void *dummy_malloc_hook(size_t size, const void *caller) {
  __malloc_hook = old_malloc_hook;
  void *ret;
  if (size >= dummy_object_size()) {
    FILE *f = fopen(DUMMY_FILE_NAME, "r+");
    if (f == NULL)
      f = fopen(DUMMY_FILE_NAME, "w+");
    int fd = fileno(f);
    if (ftruncate(fd, DUMMY_FILE_SIZE) != 0) {
      LOG_ERROR("dummy_malloc_hook => Mapping Dummy file %s for Shared Memory failed", DUMMY_FILE_NAME);
      ret = malloc(size);
      close(fd);
      goto end;
    }

    if (size % DUMMY_FILE_SIZE != 0)
      size = (size / DUMMY_FILE_SIZE + 1) * DUMMY_FILE_SIZE;

    ret = mmap(NULL, size,
               PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ret == MAP_FAILED) {
      LOG_ERROR("dummy_malloc_hook => Mapping Virtual Memory Failed.");
      ret = malloc(size);
      close(fd);
    }

    void *ret2;
    for (size_t mapped = 0; mapped < size; mapped += DUMMY_FILE_SIZE) {
      void *ret2 = mmap(reinterpret_cast<char*>(ret) + mapped, DUMMY_FILE_SIZE,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_FIXED,
                        fd, 0);
      if (ret2 == MAP_FAILED) {
        LOG_ERROR("dummy_malloc_hook => Overwrite dummy file mapping page Failed.");
        munmap(ret, size);
        ret = malloc(size);
        close(fd);
      }
    }

    ret2 = mmap(ret, getpagesize(),
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
                -1, 0);
    if (ret2 == MAP_FAILED) {
      LOG_ERROR("dummy_malloc_hook => Overwrite dummy file mapping first page Failed.");
      munmap(ret, size);
      ret = malloc(size);
      close(fd);
    }
  } else {
    ret = malloc(size);
  }
 end:
  __malloc_hook = dummy_malloc_hook;
  return ret;
}

/** Increase R Heap size
 * @param R Rinside session
 */
void IncreaseRHeap(const RInside &R) {
  LOG_INFO("Increasing R Heap size for Function Execution.");
  char cmd[CMD_BUF_SIZE];
  LOG_INFO("Dummy variable allocated of size %zuMB. R Heap limit raised to >=%zuMB", dummy_object_size()>>20, static_cast<size_t>((dummy_object_size()>>20) / R_heap_grow_threshold));
  // R heap size grows while mem >= .7 * heap,
  // by 5% in each gc, so we need -ln(.7)/ln(1.05) ~= 8 gc-s
  // to raise heap size to its desired max
  // (which will be approx. dummy_size * 1.4)
  snprintf(cmd, CMD_BUF_SIZE,
           ".presto.dummy <- numeric(%zu); "
           "gc(); gc(); gc(); gc(); gc(); gc(); gc(); "
           "tmp <- gc()\n"
           "cat(\"R effective gc trigger raised to\","
           "tmp[8]-tmp[4],\"MB\\n\")",
           dummy_object_size()/8);
  old_malloc_hook = __malloc_hook;
  __malloc_hook = dummy_malloc_hook;
  R.parseEvalQ(cmd);
  __malloc_hook = old_malloc_hook;
}

#endif


/** Clear array data and temporary R data that were used to perform a task
 * @param R RInside object
 * @param var_to_ArrayData a map of R-variable name to ArrayData (Split)
 * @param var_to_Composite a map of R-variable name to ArrayData (Composite)
 * @return NULL
 */
void ClearTaskData(RInside &R,  // NOLINT
        map<string, ArrayData*>& var_to_ArrayData,
        map<string, Composite*>& var_to_Composite) {
  R.parseEvalQ("rm(list=ls(all.names=TRUE));gc()");

  // delete splits information in this task
  for (map<string, ArrayData*>::iterator i = var_to_ArrayData.begin();
       i != var_to_ArrayData.end(); i++) {
    delete i->second;
  }
  // delete composite information in this task
  for (map<string, Composite*>::iterator i =
           var_to_Composite.begin();
       i != var_to_Composite.end(); i++) {
    delete i->second;
  }
}

/** Parse arguments that are related to splits. The split array information will be loaded from R into the shared memory session
 * @param R the R-session that the task is running on
 * @param v2a map of a variable name in R to ArrayData in the shared memory region
 * @return the number of split variables: NOTE: -1 means worker shutdown
 */
int ReadSplitArgs(RInside& R, map<string, ArrayData*>& v2a) {  // NOLINT
  int vars;
  char shmname[100], varname[100];
  Timer timer;
  timer.start();
  int res = scanf(" %d ", &vars);

  if (res != 1) {
    LOG_ERROR("ReadSplitArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParseSplitArgs::Executor cannot recognize commands from a worker");
  }

  LOG_DEBUG("Number of Split variables: %d", vars);

  if (vars == EXECUTOR_SHUTDOWN_CODE) {  // shutdown
    return EXECUTOR_SHUTDOWN_CODE;
  }

  timer.start();
  for (int i = 0; i < vars; i++) {
    res = scanf(" %s %s ", shmname, varname);
    LOG_DEBUG("Read Split Variable %s (%s in R)", shmname, varname);
    if (res != 2) {
      LOG_ERROR("ReadSplitArgs => Variable %s (%s in R) - Bad file format for Executor. Executor cannot recognize commands from Worker.");
      throw PrestoWarningException
        ("ParseSplitArgs::Executor cannot recognize commands from a worker");
    }
    // mapping of R-session variable name to shared memory segments
    v2a[varname] = ParseShm(shmname);
    // Load the shared memory segment into R-session
    // with corresponding variable name
    v2a[varname]->LoadInR(R, varname);
  }
  return vars;
}

/** Read Raw args (not split or composite array) from Worker
 * @param R the R-session that the task is running on
 * @return the number of raw variables
 */
int ReadRawArgs(RInside& R) {  // NOLINT
  LOG_DEBUG("Reading Raw Arguments of Function from Worker and load it in R-session");
  int raw_vars;  // number of non-split variables
  Timer timer;
  timer.start();
  int res = scanf(" %d ", &raw_vars);
  if (res != 1) {
    LOG_WARN("ReadRawArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ReadRawArgs::Executor cannot recognize commands from a worker");
  }

  LOG_DEBUG("Number of Raw variables: %d", raw_vars);
  
  for (int i = 0; i < raw_vars; i++) {
    char name[256];
    int size;
    res = scanf(" %s %d:", name, &size);
    LOG_DEBUG("Read Raw variable %s index: %d total: %d", name,i,raw_vars);
    if (res != 2) {
      LOG_WARN("ReadRawArgs => Variable %s - Bad file format for Executor. Executor cannot recognize commands from Worker.", name);
      throw PrestoWarningException
        ("ReadRawArgs::Executor cannot recognize commands from a worker");
    }
    char cmd[CMD_BUF_SIZE];
    // keep the value of a variable into the corresponding R-variable name
    snprintf(cmd, CMD_BUF_SIZE,
             "%s <- unserialize(rawtmpvar...)", name);
    
    // if the raw varaible is passed through protobuf (size > 0)
    if (size > 0) {
      Rcpp::RawVector raw(size);  // the value of this variable
      for (int j = 0; j < size; j++) {
        if (scanf("%c", &raw[j]) != 1)
          break;
      }      
      R["rawtmpvar..."] = raw;      
      R.parseEvalQ(cmd);
    } else {
      // the raw variable will be passed without using protobuf - external fetch needed
      char server_addr[1024], fetch_id[1024], sip[1024];
      int port;
      int64_t data_size;
      // executor pool passes server name followed by the data id
      res = scanf(" %s %d %s %zu ", server_addr, &port, fetch_id, &data_size);
      if (res != 4) {
        LOG_WARN("ReadRawArgs => Variable %s - Bad file format for Executor. Executor cannot recognize commands from Worker to fetch raw data", name);
        throw PrestoWarningException
          ("ReadRawArgs::Executor cannot recognize commands from a worker to fetch raw data");
      }      
      LOG_DEBUG("fetch raw data from %s:%d data-id %s size: %zu", server_addr, port, fetch_id, data_size);

      /* Send the fetch data id to the server */
      string tgt_host = string(server_addr);
      int32_t sock = presto::connect(tgt_host, port);
      if (sock < 0) {
        LOG_ERROR("connect failure : %s", strerror(errno));
        throw PrestoWarningException("ReadRawArgs - fail to open connection to a master");
      }
      if (send(sock, fetch_id, strlen(fetch_id), 0) != strlen(fetch_id)) {
        LOG_ERROR("send() sent a different number of bytes than expected");
        close(sock);
        throw PrestoWarningException("ReadRawArgs - fail to send fetchID to a master");
      }
      LOG_DEBUG("ReadRawArgs - FetchID send done");
      
      char* dest_ = (char*) malloc(data_size);
      if (dest_ == NULL) {
        LOG_ERROR("ReadRawArgs - malloc failed: %zu", data_size);
        close(sock);
        throw PrestoWarningException("ReadRawArgs - fail to malloc");        
      }
      LOG_DEBUG("ReadRawArgs - Receive from a master start");
      size_t bread = atomicio(read, sock, dest_, data_size);
      if (bread != data_size) {
        LOG_ERROR("atomicio read bytes - %d:%s", bread, strerror(errno));
        close(sock);
        throw PrestoWarningException("ReadRawArgs - fail to receive from a master");
      }
      LOG_DEBUG("ReadRawArgs - Receive from a master done - read size %d", bread);
      close(sock);

      Rcpp::RawVector raw(data_size);
      for (int i=0; i<data_size;++i) {
        raw[i] = dest_[i];
      }
      R["rawtmpvar..."] = raw;
      R.parseEvalQ(cmd);
      free(dest_);
    }
  }

  return raw_vars;
}

/** Parse arguments that are related to composite array. 
 * The split array information will be loaded from R into the shared memory session
 * @param R the R-session that the task is running on
 * @param v2c map of a variable name in R to Composite array in the shared memory region
 * @param v2a map of a variable name in R to split array in the shared memory region
 * @return the number of composite variables
 */
int ReadCompositeArgs(RInside& R,  // NOLINT
      map<string, Composite*>& v2c, map<string, ArrayData*>& v2a) {
  LOG_DEBUG("Reading Composite Arguments of Function from Worker and load its Shared memory segment in R-session");
  int composite_vars;  // number of compoiste array input
  Timer timer;
  timer.start();

  int res = scanf(" %d ", &composite_vars);
  if (res != 1) {
    LOG_ERROR("ReadCompositeArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
    ("ReadCompositeArgs::Executor cannot recognize commands from a worker");
  }

  LOG_DEBUG("Number of Composite arrays: %d", composite_vars);
  
  for (int i = 0; i < composite_vars; i++) {
    // varname is a variable name in the R-session.
    // shmname is a name of a variable in the shared memory segment
    char varname[100], shmname[100];
    DobjectType type; 
    int num_splits;  // a number of splits of the input darray
    res = scanf(" %s %s %d %d: ", varname, shmname, &type, &num_splits);
    LOG_DEBUG("Read Composite Array %s (%s in R) of type %d and %d number of splits", shmname, varname, type, num_splits);
    if (res != 4) {
      LOG_WARN("ReadCompositeArgs => Variable %s (%s in R) - Bad file format for Executor. Executor cannot recognize commands from Worker.", shmname, varname);
      throw PrestoWarningException
        ("ReadCompositeArgs::Executor cannot recognize commands from a worker");
    }
    // insert the R-session variable name
    // load the composite
    // Get a pointer for a composite array in the shared memory session
    ArrayData *array = ParseShm(shmname);
    array->LoadInR(R, varname);  // Load the data in shared memory into R
    v2a[varname] = array;  // keep the variable name to array pointer.

    // read splits data (needed for composite update)
    Composite *composite;
    v2c[varname] = composite = new Composite;

    for (int i = 0; i < num_splits; i++) {
      char splitname[100];
      pair<size_t, size_t> offsets, dims;
      res = scanf(" %s %zu %zu %zu %zu ",
                  splitname,
                  &offsets.first, &offsets.second,
                  &dims.first, &dims.second);
      if (res != 5) {
        LOG_WARN("ReadCompositeArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
        throw PrestoWarningException
        ("ReadCompositeArgs::Executor cannot recognize commands from a worker");
      }
      composite->splitnames.push_back(splitname);
      composite->offsets.push_back(offsets);
      composite->dims.push_back(dims);
    }
    composite->dobjecttype = type;    
  }
  return composite_vars;
}

int main(int argc, char *argv[]) {
  // let this executor to be killed when a parent process (worker) terminates.
  prctl(PR_SET_PDEATHSIG, SIGKILL);
  Timer timer;

  presto::presto_malloc_init_hook();

  string scoping_prefix = "f <- function() {";
  string scoping_postfix = "} ; f()";
// In order to automatically add tryCatch block to prevent the executor
// being killed while evaluating R-command.
#ifndef EXECUTOR_TRYCATCH
  string ft_prefix;
  string ft_postfix;
#else
  string ft_prefix("{\ntryCatch(");
  string ft_postfix(",error = function(ex){print(ex) ; "
                    "cat(ex[[1]],\"\n\",file=stderr())}, finally={})\n}");
#endif

  int res;
  /*char logname[MAX_LOGNAME_LENGTH];
  snprintf(logname, MAX_LOGNAME_LENGTH, "%s", strstr(argv[1], "executor"));
  snprintf(logname+strlen(logname), MAX_LOGNAME_LENGTH-strlen(logname), "_log");
  logfile = stderr;  // fopen(logname, "w");

  int res;*/

  InitializeConsoleLogger();
  LOG_INFO("Executor started.");
  LoggerFilter(atoi(argv[4]));
  
  // name of shared memory segment
  // to synchorize/share memory between worker and executor
  //  const char *sync_struct_name = argv[1];
  // a pipe to send/receive task and result between worker and executor
  
  out = fdopen(atoi(argv[1]), "w");
  if (out == NULL) {
    LOG_ERROR("Pipe to send/receive Task and Result to Worker failed to open.");
    exit(1);
  }
  LOG_INFO("Communication pipe with Worker opened on %d", atoi(argv[1]));
  // a name of storage in case memory is not sufficient
  ArrayData::spill_dir_ = argv[2];
  LOG_DEBUG("Spill directory setup");
  RInside R(0, 0);
  LOG_DEBUG("RInside is created");
  R_CStackLimit = (uintptr_t)-1;
  LOG_DEBUG("R_CStackLimit is set");
#ifdef INCREASE_R_HEAP
  IncreaseRHeap(R);
#endif
  LOG_DEBUG("Loading libraries");
  // load packages
  R.parseEvalQ("tryCatch({library(Matrix);library(MatrixHelper);library(Executor);gc.time()}, error=function(ex){Sys.sleep(2);library(Matrix);library(MatrixHelper);library(Executor);gc.time()})");
  
  updatesptr = new set<tuple<string, bool, int64_t, int64_t> >();
  set<tuple<string, bool, int64_t, int64_t> > &updates = *updatesptr;
  LOG_DEBUG("New Update pointer to maintain list of updated split/composite variables in Function execution created.");
  // map of variable name (in R) with corresponding ArrayData object
  map<string, ArrayData*> var_to_ArrayData;
  // map of variable name (in R) with correspponding Composite array
  map<string, Composite*> var_to_Composite;
  // update pointer in R-session
  Rcpp::XPtr<set<tuple<string, bool,int64_t,int64_t> > > updates_ptr(&updates, false);
  Rcpp::Language exec_call;
  string prev_cmd;
  // a buffer to keep the task result from RInside
  char err_msg[EXCEPTION_MSG_SIZE];
  memset(err_msg, 0x00, sizeof(err_msg));
  while (true) {
    if (err_msg[0] != '\0') {
      ostringstream msg;
      msg << "Executor Exception : " << err_msg;
      LOG_ERROR(msg.str());

      AppendTaskResult(TASK_EXCEPTION, err_msg, out);      
      LOG_INFO("TASK_EXCEPTION sent as Task result to Worker");
      ClearTaskData(R, var_to_ArrayData, var_to_Composite);
      LOG_INFO("Flushing Task Data from Executor memory");
    }
    memset(err_msg, 0x00, sizeof(err_msg));
    Timer total_timer;
    total_timer.start();
    try {
      // keep a pointer of update variable in R-session
      R["updates.ptr..."] = updates_ptr;
      var_to_ArrayData.clear();
      var_to_Composite.clear();

      updates.clear();  // clear update vector
      LOG_INFO("*** No Task under execution. Waiting from Task from Worker **");
      res = ReadSplitArgs(R, var_to_ArrayData);  // Read split arguments
      if (res == EXECUTOR_SHUTDOWN_CODE) {
        LOG_INFO("SHUTDOWN message received from Worker. Shutting down Executor");
        break;
      }

      LOG_INFO("New Task received from Worker. Reading Function Arguments and Body");
      
      ReadRawArgs(R);
      ReadCompositeArgs(R, var_to_Composite, var_to_ArrayData);
      
      timer.start();

      // read fun
      char c;
      string cmd;
      cmd += scoping_prefix;  // append function definition
      cmd += ft_prefix;  // apend tryCatch block
      
      size_t fun_str_length;  // the length of function string
      if (scanf(" %zu ", &fun_str_length) != 1) {
        LOG_ERROR("ReadFunctionString => Executor cannot recognize commands from a worker");
        throw PrestoWarningException
        ("ReadFunctionString::Executor cannot recognize commands from a worker");
      }
      for(size_t fi = 0; fi < fun_str_length; ++fi) {
        if(scanf("%c", &c) != 1) {
          LOG_ERROR("ReadFunctionString => Cannot read characters");
          throw PrestoWarningException
            ("ReadFunctionString::Cannot read characters");
        }
        cmd.push_back(c);
      }
      cmd += ft_postfix;  // complete try catch block
      cmd += scoping_postfix;  // complete function scoping

      if (cmd != prev_cmd) {
        string quoted_cmd = "quote({";
        quoted_cmd += cmd;
        quoted_cmd += "})";
        // generate execution call that is callable using RInside
        exec_call = R.parseEval(quoted_cmd);
        prev_cmd = cmd;
      }

      timer.start();
      LOG_INFO("Executing Function:");
      LOG_INFO(cmd);
      
      //Rcpp::Evaluator::run(exec_call, R_GlobalEnv);  // Run the given fucntion with Rcpp < 0.11
      Rcpp::Rcpp_eval(exec_call, R_GlobalEnv);
      LOG_INFO("Function execution Complete.");

      // Send back updates to worker
      timer.start();
      // updates contains a list of variable in R-session.
      // This set is filled by calling update function
      
      LOG_INFO("There are %d variables that are updated. Sending them to the Worker.", updates.size());

      for (set<tuple<string, bool,int64_t,int64_t> >::iterator i = updates.begin();
           i != updates.end(); i++) {
        
	const string &name = get<0>(*i);   // name of variable in R-session
        bool empty = get<1>(*i);
        int64_t obj_nrow = get<2>(*i);
        int64_t obj_ncol = get<3>(*i);

        // if the variable is a composite array
        if (contains_key(var_to_Composite, name)) {
          // updating composite
          Composite *composite = var_to_Composite[name];
          if (NULL == composite) {
            LOG_ERROR("Invalid Composite variable name: %s", name.c_str());
            continue;
          }

          // Update each split of the Dlist.
          // dlist will be re-distributed equally(length-wise) across all splits of the dlist
          if (composite->dobjecttype == DLIST) {              
             std::stringstream len_cmd;
             len_cmd << "length(" << name << ")";

             int num_splits = composite->splitnames.size();
             int list_len = Rcpp::as<int>(R.parseEval(len_cmd.str()));
             int persplit = ceil((float)list_len/(float)num_splits);
             int start=1, end=persplit, processed=0;

             for(int j = 0; j<=num_splits-1; j++) {
               char cmd[CMD_BUF_SIZE];
               if(end==0)
                 snprintf(cmd, CMD_BUF_SIZE, "compositetmpvar... <- list()");
               else {
                 snprintf(cmd, CMD_BUF_SIZE, "compositetmpvar... <- %s[%d:%d]", name.c_str(),
                         start, end);

                 processed += (end-start)+1;
                 start += persplit;
                 end += persplit;
                 if(end > list_len)
                   end = (processed == list_len) ? 0 : list_len;
               }
               R.parseEvalQ(cmd);
               string var_name = string("compositetmpvar...");
               CreateUpdate(var_name, composite->splitnames[j], R, empty, -1, -1);
             }   
          }else if (composite->dobjecttype == DARRAY_DENSE || composite->dobjecttype == DARRAY_SPARSE) {
	    //Flag to determine if all split sizes are zero
	    bool all_split_size_zero = true;

          // iterate over all splits in the composite
          for (int j = 0; j < composite->offsets.size(); j++) {
            char cmd[CMD_BUF_SIZE];
            if (composite->dobjecttype == DARRAY_DENSE) {
              snprintf(cmd, CMD_BUF_SIZE,
                       "compositetmpvar... <- matrix(%s[%zu:%zu,%zu:%zu],%zu,%zu)", name.c_str(),
                       composite->offsets[j].first+1,
                       composite->offsets[j].first+composite->dims[j].first,
                       composite->offsets[j].second+1,
                       composite->offsets[j].second+composite->dims[j].second,
                       composite->dims[j].first, composite->dims[j].second);
            } else if (composite->dobjecttype == DARRAY_SPARSE){
              // when either # row or column is one, R converts it to a vector (not a sparseMatrix!!
              if (composite->dims[j].first == 1 || composite->dims[j].second == 1) {
                     snprintf(cmd, CMD_BUF_SIZE,
                       "compositetmpvar... <- as(matrix(%s[%zu:%zu,%zu:%zu], nrow=%zu), \"sparseMatrix\")", name.c_str(),
                       composite->offsets[j].first+1,
                       composite->offsets[j].first+composite->dims[j].first,
                       composite->offsets[j].second+1,
                       composite->offsets[j].second+composite->dims[j].second,
                       composite->dims[j].first);                
              } else {
                snprintf(cmd, CMD_BUF_SIZE,
                       "compositetmpvar... <- %s[%zu:%zu,%zu:%zu]", name.c_str(),
                       composite->offsets[j].first+1,
                       composite->offsets[j].first+composite->dims[j].first,
                       composite->offsets[j].second+1,
                       composite->offsets[j].second+composite->dims[j].second);
              }
            }
	    //Check if the original split had non-zero size. Otherwise there is no point in making the update.
	    if(!(composite->dims[j].first==0 & composite->dims[j].second==0)){
	      all_split_size_zero = false;
	      R.parseEvalQ(cmd);
	      string var_name = string("compositetmpvar...");
	      CreateUpdate(var_name, composite->splitnames[j], R, empty, -1, -1);
	    }
          }
	  //(iR)If all splits were zero sized, we are possibly using an empty darray
	  //Updates to empty, composite arrays is not supported, since we don't know the declared size of splits
	  if(all_split_size_zero){
	    throw PrestoWarningException("Update to empty composite array is not supported.");
	  }

         }else if (composite->dobjecttype == DFRAME ) {
	    LOG_ERROR("Update to composite dframe");
	    throw PrestoWarningException("Update to composite dframe is not supported.");
	  }
        } else {
          // updating a split
          CreateUpdate(name, var_to_ArrayData[name]->GetName(), R,
                       empty, obj_nrow, obj_ncol);
        }
      }
    }catch (const Rcpp::eval_error& eval_err) {
      // Error will be caught at the try-catch block
      LOG_ERROR("Function Execution(eval_error) Exception => %s", eval_err.what());
      strncpy(err_msg, eval_err.what(), sizeof(err_msg)-1);
      continue;
    } catch(const std::exception &ex) {
      LOG_ERROR("Executor Exception : %s", ex.what());
      strncpy(err_msg, ex.what(), sizeof(err_msg)-1);
      continue;
    }
    // As error is detected, the error message is filled and comes
    // to the beginning. Here, we can assume the task succeed
    AppendTaskResult(TASK_SUCCEED, "Succeed", out);
    LOG_INFO("Task Success sent to Worker");
    timer.start();
    timer.start();

    timer.start();
    ClearTaskData(R, var_to_ArrayData, var_to_Composite);
    LOG_DEBUG("Flushing Task Data from Executor memory");

    /*static int gci = 0;
    if (gci++ % 100 == 0)
      R.parseEvalQ("cat(\"gc time:\", gc.time(), \"\n\")");*/
  }

  return 0;
}
