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

#include <cstdio>
#include <utility>
#include <math.h>

#include <RInside.h>
#include <Rinterface.h>
#include "common.h"
#include "atomicio.h"
#include "dLogger.h"
#include "interprocess_sync.h"
#include "UpdateUtils.h"
#include "ArrayData.h"
#include "executor.h"
#include "timer.h"
#include "SharedMemory.h"

#define CMD_BUF_SIZE 512  // R command buffer size
#define MAX_LOGNAME_LENGTH 200  // executor log file name length
#define DUMMY_FILE_SIZE (128LL<<20)  // 128MB

using namespace std;
using namespace presto;

namespace presto {
    
uint64_t abs_start_time;
StorageLayer DATASTORE = WORKER;

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define INSTALL_SYMBOL(symbol)                  \
  if (RSymbol_##symbol == NULL) {               \
    RSymbol_##symbol = Rf_install(#symbol);     \
  }

// Global variables 
// contains a list of variables to update (called inside a foreach)
set<tuple<string, bool, std::vector<std::pair<int64_t,int64_t>>>> *updatesptr;
Executor* ex;

/** Propagates new value to other workers. 
 * This is generally called from Presto R function update(). 
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
      
      if(dim_vec.size() % 2 != 0){
          LOG_ERROR("Received dimensions list has an odd number of elements.");
          throw PrestoWarningException("dframe dimensions received had an odd number of elements");
      }
      
      std::vector<pair<int64_t,int64_t>> dimensions;
      
      for(int i=0;i < dim_vec.size(); i+=2){
          dimensions.push_back(std::make_pair<int64_t,int64_t>(dim_vec[i],dim_vec[i+1]));
      }
      
      updatesptr->insert(make_tuple(name, empty, dimensions));
  return Rcpp::wrap(true);
  END_RCPP
}


/**************** Class Function definitions ****************************/

ExecutorEvent Executor::GetNextEvent() {
  ExecutorEvent nexteventtype;
  int res = scanf(" %d ", &nexteventtype);
  
  if (res != 1) {     
    LOG_ERROR("GetNextEvent => Bad file format for Executor. Executor cannot recognize commands from Worker.");     
    throw PrestoWarningException     
      ("ParseEvent::Executor cannot recognize commands from Worker");
  }

  return nexteventtype;
}

/** Send task result update to worker at the end of task execution. 
 * It creates new split version and its name, then it read value of new variable from R session 
 * and sync it with new split by writin it into shared memory region
 * @param varname a name of updated variable in R-session
 * @param splitname a name of split that is internal to Presto library
 * @param R a pointer of RInside
 * @empty indicates if this variable is empty
 * @obj_nrow no. of rows in split. -1 in case of composite arrays and lists. Primarily used for dframes.
 * @obj_ncol no. of cols in split. -1 in case of composite arrays and lists. Primarily used for dframes.
 * @return NULL
 */
 void Executor::CreateUpdate(const string &varname, const string &splitname,
                             int64_t obj_nrow, int64_t obj_ncol,
                             StorageLayer store) {
  LOG_INFO("Started CreateUpdate(%s)", splitname.c_str());
  Timer t;
  t.start();
  string prev_dobj_name = splitname;
  ARRAYTYPE orig_class = EMPTY;

  try {
    if(store == WORKER || (in_memory_partitions.find(prev_dobj_name) == in_memory_partitions.end()))
      orig_class = GetClassType(prev_dobj_name);
    else 
      orig_class = in_memory_partitions[prev_dobj_name]->GetClassType();

  } catch (std::exception e) {}
  LOG_INFO("orig_class is %d", orig_class);

  LOG_DEBUG("Updating %s (%s in R)", prev_dobj_name.c_str(), varname.c_str());
  const char* old_ver = rindex(prev_dobj_name.c_str(), '_');
  if (old_ver == NULL) {
    ostringstream msg;
    msg << "Presto Executor: CreateUpdate: Wrong darray name: "
      <<prev_dobj_name;
    LOG_ERROR("CreateUpdate => Wrong darray/dframe name: %s", prev_dobj_name.c_str());
    throw PrestoWarningException(msg.str());
  }
  string new_dobj_name;
  if (IsCompositeArray(splitname) == true) {    
    new_dobj_name = string(splitname);  // in case of composite, keep the current name
    LOG_DEBUG("Variable %s is a Composite Array. Its new Array Name is %s", prev_dobj_name.c_str(), new_dobj_name.c_str());
  } else {
    int32_t cur_version = (int32_t)(strtol(old_ver + 1, NULL, 10));
    // if it were not a composite array, increment it.
    int32_t new_version = cur_version + 1;
    new_dobj_name = prev_dobj_name.substr(
        0, old_ver - prev_dobj_name.c_str() + 1);
    new_dobj_name += int_to_string(new_version);
    LOG_DEBUG("Variable %s is a Split. Its new Array Name is %s", prev_dobj_name.c_str(), new_dobj_name.c_str());
  }

  LOG_INFO("Previous name is %s, New name is %s", prev_dobj_name.c_str(), new_dobj_name.c_str());
  t.start();

  ArrayData *newsplit = ParseVariable(RR, varname, new_dobj_name, orig_class, store);
  if (newsplit == NULL) {
    LOG_ERROR("CreateUpdate => Failed to write R value into Shared memory");
    throw PrestoWarningException
      ("Executor:CreateUpdate Fail to write R value into shared memory");
  }
  t.start();

  // write task result to send it to worker.
  std::pair<size_t, size_t> dims = newsplit->GetDims();

  //TODO(R) Hack for obtaining size of dataframe splits, since GetDims() is 0 for dframes and lists
  if(orig_class == DATA_FRAME){
    dims = make_pair(obj_nrow,obj_ncol);
  }

  if(store == RINSTANCE) {
    // Add the new dobject name + assign varname to shmname
    in_memory_partitions[new_dobj_name] = newsplit;

    // Save the new value.
    char cmd[CMD_BUF_SIZE];
    snprintf(cmd, CMD_BUF_SIZE, ".tmp.shmname <- deparse(substitute(`%s`)); print(.tmp.shmname); assign(.tmp.shmname, `%s`, .GlobalEnv); ", new_dobj_name.c_str(), varname.c_str());
    //LOG_INFO("command executed is %s", cmd);
    RR.parseEvalQ(cmd);

    snprintf(cmd, CMD_BUF_SIZE, "print(`%s`)", varname.c_str());
    LOG_INFO("Printing variable. cmd is %s", cmd);
    RR.parseEvalQ(cmd);
    snprintf(cmd, CMD_BUF_SIZE, "print(`%s`)", new_dobj_name.c_str());
    LOG_INFO("Printing new partition. cmd is %s", cmd);
    RR.parseEvalQ(cmd);
  }

  AppendUpdate(new_dobj_name, newsplit->GetSize(), false, dims.first, dims.second, out);
  LOG_INFO("Variable %s (%s in R) updated successfully.", new_dobj_name.c_str(), varname.c_str());
}


/** Parse arguments that are related to splits. The split array information will be loaded from R into the shared memory session
 * @return the number of split variables: NOTE: -1 means worker shutdown
 */
int Executor::ReadSplitArgs() {  // NOLINT

  RR.parseEvalQ("print('Printing ls'); print(ls())");
  int vars;
  char splitname[100], varname[100];
  std::string list_string("list_type...");
  Timer timer;
  timer.start();
  int res = scanf(" %d ", &vars);

  if (res != 1) {
    LOG_ERROR("ReadSplitArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParseSplitArgs::Executor cannot recognize commands from a worker");
  }

  LOG_DEBUG("Number of Split variables: %d", vars);

  timer.start();
  for (int i = 0; i < vars; i++) {
    res = scanf(" %s %s ", splitname, varname);
    LOG_INFO("Read Split Variable %s (%s in R)", splitname, varname);
    if (res != 2) {
      LOG_ERROR("ReadSplitArgs => Variable %s (%s in R) - Bad file format for Executor. Executor cannot recognize commands from Worker.");
      throw PrestoWarningException
        ("ParseSplitArgs::Executor cannot recognize commands from a worker");
    }
    
    //list-type argument detected
    if(list_string.compare(splitname) == 0){
      int nsplits;
      int result = scanf(" %d ", &nsplits);
      if (result != 1) {
        LOG_ERROR("ReadSplitArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
        throw PrestoWarningException
          ("ParseSplitArgs::Executor cannot recognize commands from a worker");
      }   
      LOG_INFO("Number of splits in list_type is %d", nsplits);
      
      //Use the composite class to store splits information about this list-type argument
      Composite* composite = new Composite;
      Rcpp::List list(nsplits);
      
      for(int m = 0;m < nsplits;m ++){
        result = scanf(" %s ", splitname);
        if (result != 1) {
            LOG_ERROR("ReadSplitArgs => Bad file format for Executor. Executor cannot recognize commands from Worker.");
            throw PrestoWarningException
              ("ParseSplitArgs::Executor cannot recognize commands from a worker");
        }
        composite->splitnames.push_back(splitname);
        
        char new_name[100];
        strcpy(new_name,varname);
        //create a name for this array to store in v2a for deletion
        char num[10];
        sprintf(num,"%d",m);
        strcat(new_name,num);  //eg a5

        LOG_INFO("Processing partitions %s", splitname);

        // add to v2a a fake name so it's cleaned up later. Load split from Executor or Worker
        if(in_memory_partitions.find(splitname) != in_memory_partitions.end()) {
          LOG_INFO("Partition %s is on Executor. Loading..", splitname);

          char cmd[CMD_BUF_SIZE];
          snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); print(.tmp.varname); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, splitname);
          RR.parseEvalQ(cmd);
        } else {  //Load from Worker
          LOG_INFO("Partition %s is on Worker. Loading..", splitname);

          ArrayData* split = ParseShm(splitname);
          split->LoadInR(RR, varname);
          delete split;
        }
        list[m] = RR[varname];
      }
     
      RR[varname] = list;
      
      // Need to store a flag in executor to disambiguate a list of splits of a dframe and a single dlist 
      // of data frames (for getting dimensions -- see executor.R)
      char flagstr[100];
      strcpy(flagstr, ".");
      strcat(flagstr, varname);
      strcat(flagstr, ".isListType");
      RR[flagstr] = true;
      
      var_to_list_type[varname] = composite;
    } else {

      LOG_INFO("Processing partitions %s", splitname);

      // Check if executor contains the partitions.
      if(in_memory_partitions.find(splitname) != in_memory_partitions.end()) {
        LOG_INFO("Partition %s is on Executor. Loading..", splitname);

        char cmd[CMD_BUF_SIZE];
        snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); print(.tmp.varname); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, splitname);
        RR.parseEvalQ(cmd);
      } else {
        LOG_INFO("Partition %s is on Worker. Loading..", splitname);

        ArrayData* split = ParseShm(splitname);
        split->LoadInR(RR, varname);
        delete split;
      }
      var_to_Partition[varname] = splitname;
    }
  }
  return vars;
}

/** Read Raw args (not split or composite array) from Worker
 * @return the number of raw variables
 */
int Executor::ReadRawArgs() {  // NOLINT
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
    
    // if the raw variable is passed through protobuf (size > 0)
    if (size > 0) {
      Rcpp::RawVector raw(size);  // the value of this variable
      for (int j = 0; j < size; j++) {
        if (scanf("%c", &raw[j]) != 1)
          break;
      }      
      RR["rawtmpvar..."] = raw;      
      RR.parseEvalQ(cmd);
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
      RR["rawtmpvar..."] = raw;
      RR.parseEvalQ(cmd);
      free(dest_);
    }
  }

  return raw_vars;
}

/** Parse arguments that are related to composite array. 
 * The split array information will be loaded from R into the shared memory session
 * @return the number of composite variables
 */
int Executor::ReadCompositeArgs() {
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
    // compositename is a name of a variable in the shared memory segment
    char varname[100], compositename[100];
    DobjectType type; 
    int num_splits;  // a number of splits of the input darray
    res = scanf(" %s %s %d %d: ", varname, compositename, &type, &num_splits);
    LOG_DEBUG("Read Composite Array %s (%s in R) of type %d and %d number of splits", compositename, varname, type, num_splits);
    if (res != 4) {
      LOG_WARN("ReadCompositeArgs => Variable %s (%s in R) - Bad file format for Executor. Executor cannot recognize commands from Worker.", compositename, varname);
      throw PrestoWarningException
        ("ReadCompositeArgs::Executor cannot recognize commands from a worker");
    }

    //Load composite dobject into R executor memory
    if(in_memory_partitions.find(compositename) != in_memory_partitions.end()) {
      LOG_INFO("Composite Dobject %s is on Executor. Loading..", compositename);

      char cmd[CMD_BUF_SIZE];
      snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); print(.tmp.varname); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, compositename);
      RR.parseEvalQ(cmd);
    } else {
      LOG_INFO("Partition %s is on Worker. Loading..", compositename);

      ArrayData* split = ParseShm(compositename);
      split->LoadInR(RR, varname);
      delete split;
    }
    var_to_Partition[varname] = compositename;

    // read splits data (needed for composite update)
    Composite *composite;
    var_to_Composite[varname] = composite = new Composite;

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


int Executor::Execute(set<tuple<string, bool, vector<pair<int64_t,int64_t>>>> const & updates) {

  string scoping_prefix = ".DistributedR_exec_func <- function() {";
  string scoping_postfix = "} ; .DistributedR_exec_func()";

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

  try {
  //Read variables
  int nvars;
  nvars = ReadSplitArgs(); 
  ReadRawArgs();
  ReadCompositeArgs();

  //Read function body
  string func_str;
  char c;
  func_str += scoping_prefix;     // append function definition
  func_str += ft_prefix;          // append tryCatch block

  size_t fun_str_length;
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
    func_str.push_back(c);
  }
  func_str += ft_postfix;       // complete try catch block
  func_str += scoping_postfix;  // complete function scoping

  if (func_str != prev_func_body) {
    LOG_INFO("Before execution ---->  IN IF");
    string quoted_cmd = "quote({";
    quoted_cmd += func_str;
    quoted_cmd += "})";
    exec_call = RR.parseEval(quoted_cmd);  // Get RInside callable execution call
    prev_func_body = func_str;
  }

  LOG_INFO("Executing Function:");
  LOG_INFO(func_str);

  //Rcpp::Evaluator::run(exec_call, R_GlobalEnv);  // Run the given fucntion with Rcpp < 0.11
  Rcpp::Rcpp_eval(exec_call, R_GlobalEnv);
  LOG_INFO("Function execution Complete. Processing updated variables(%d)", updates.size());

  for (set<tuple<string, bool,std::vector<std::pair<int64_t,int64_t> >> >::iterator i = updates.begin();
       i != updates.end(); i++) {

    const string &name = get<0>(*i);   // name of variable in R-session
    bool empty = get<1>(*i);
    std::vector<std::pair<int64_t,int64_t>> dimensions = get<2>(*i);
    LOG_INFO("name: %s, empty: %d, dim: %d %d", name.c_str(), empty, dimensions.at(0).first, dimensions.at(0).second);

    if (contains_key(var_to_Composite, name)) {
      // updating composite
      Composite *composite = var_to_Composite[name];
      if (NULL == composite) {
	LOG_ERROR("Invalid Composite variable name: %s", name.c_str());
	strncpy(err_msg, "Error in processing Composite arguments", sizeof(err_msg)-1);
	return -1;
      }

      if (composite->dobjecttype == DLIST) {
	std::stringstream len_cmd;
	len_cmd << "length(" << name << ")";

	int num_splits = composite->splitnames.size();
	int list_len = Rcpp::as<int>(RR.parseEval(len_cmd.str()));
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
	  RR.parseEvalQ(cmd);
	  string var_name = string("compositetmpvar...");
	  CreateUpdate(var_name, composite->splitnames[j], -1, -1, DATASTORE);
	}
      }  else if (composite->dobjecttype == DARRAY_DENSE || composite->dobjecttype == DARRAY_SPARSE) {
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
	    RR.parseEvalQ(cmd);
	    string var_name = string("compositetmpvar...");
	    CreateUpdate(var_name, composite->splitnames[j], -1, -1, DATASTORE);
	  }
	}
	//(R)If all splits were zero sized, we are possibly using an empty darray
	//Updates to empty, composite arrays is not supported, since we don't know the declared size of splits
	if(all_split_size_zero){
	  throw PrestoWarningException("Update to empty composite array is not supported.");
	}

      }else if (composite->dobjecttype == DFRAME ) {
	LOG_ERROR("Update to composite dframe");
	throw PrestoWarningException("Update to composite dframe is not supported.");
      }
    } else {
      //if this check is true, then we're updating a list-type argument
      if(contains_key(var_to_list_type,name)){
	Composite *composite = var_to_list_type[name];
	if (NULL == composite) {
	  LOG_ERROR("Invalid Composite variable name: %s", name.c_str());
	  return -1;

	}
	//if updating a list of splits we need to update one split at a time
	for(int z = 0; z < composite->splitnames.size(); z++){
	  char cmd[CMD_BUF_SIZE];
          string tmp_var = string("tmpvar...");

	  snprintf(cmd, CMD_BUF_SIZE,"tmpvar... <- %s[[%d]]", name.c_str(),z+1);
	  RR.parseEvalQ(cmd);
	  CreateUpdate(tmp_var, composite->splitnames[z], dimensions.at(z).first, dimensions.at(z).second, DATASTORE);
	}
      }else{ // updating a split
	CreateUpdate(name, var_to_Partition[name], dimensions.at(0).first, dimensions.at(0).second, DATASTORE);
      }
    }
  }

  } catch (const Rcpp::eval_error& eval_err) {
    // Error will be caught at the try-catch block
    LOG_ERROR("Function Execution(eval_error) Exception => %s", eval_err.what());
    strncpy(err_msg, eval_err.what(), sizeof(err_msg)-1);
    return -1;
  } catch(const std::exception &ex) {
    LOG_ERROR("Executor Exception : %s", ex.what());
    strncpy(err_msg, ex.what(), sizeof(err_msg)-1);
    return -1;
  }
  return 0;
}


int Executor::Clear() {
  LOG_INFO("Starting Clear");
  int num;
  char split[100];
  std::vector<std::string> clear_vec;

  int res = scanf(" %d ", &num);
  if (res != 1) {
    LOG_ERROR("Clear => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParseClear::Executor cannot recognize commands from a worker");
  }
  
  for(int i=0; i<num; i++) {
    LOG_INFO("Scanning name %d", num);
    int res = scanf(" %s", split);
    LOG_INFO("Marking shmname %s for clear", split);
    if(res != 1) {
      //LOG_ERROR("Read Clear variable (%s)", shmname);
      throw PrestoWarningException
        ("ParseClearArgs::Executor cannot recognize commands from the worker");
    }

    //Remove partition
    std::string split_str = std::string(split);
    if (in_memory_partitions.find(split_str) != in_memory_partitions.end()) {
       LOG_INFO("Removing %s from in_memory", split);
       delete in_memory_partitions[split_str];
    }

    clear_vec.push_back(split);
  }

  LOG_INFO("Clear vector size is %zu", clear_vec.size());

  RR[".tmp.clear"] = clear_vec;
  char cmd[CMD_BUF_SIZE];
  snprintf(cmd, CMD_BUF_SIZE, "rm(list=.tmp.clear);");
  RR.parseEvalQ(cmd);
  RR.parseEvalQ("print(ls())");

  //AppendClearLine(1, out);
  return 0;
}


int Executor::PersistToWorker() {
  LOG_INFO("Starting PersistToWorker");

  char split[100];
  int res = scanf(" %s", split);
  if (res != 1) { 
    LOG_ERROR("PersistToWorker => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParsePersistToWorker::Executor cannot recognize commands from a worker");
  }
  LOG_INFO("Partition %s should be persist to worker", split);

  //Serialize first
  char cmd[CMD_BUF_SIZE];
  if(in_memory_partitions.find(split) == in_memory_partitions.end()) {
    LOG_ERROR("PersistToWorker => Split %s to be transferred is not found", split);
    throw PrestoWarningException("PersistToWorker => Split to be transferred is not found");
  }

  ARRAYTYPE orig_class = in_memory_partitions[std::string(split)]->GetClassType();
  ArrayData* data = ParseVariable(RR, split, split, orig_class, WORKER);
  delete data;
  LOG_INFO("Split %s persisted to worker", split);
}


static void SendResult(const char* err_msg) {
  if (err_msg[0] != '\0') {
    ostringstream msg;
    msg << "Executor Exception : Command[] "<< err_msg;
    LOG_ERROR(msg.str());

    AppendTaskResult(TASK_EXCEPTION, err_msg, out);
    LOG_INFO("TASK_EXCEPTION sent as Task result to Worker");
  } else {
    AppendTaskResult(TASK_SUCCEED, "Succeed", out);
    LOG_INFO("Task Success sent to Worker");
  }

  //ClearTaskData();
  LOG_INFO("Flushing Task Data from Executor memory");
}

/** Clear array data and temporary R data that were used to perform a task                                                      
  * @param R RInside object
  * @param var_to_ArrayData a map of R-variable name to ArrayData (Split)
 * @param var_to_Composite a map of R-variable name to ArrayData (Composite)
 * @return NULL                                                                                                               
 */
void Executor::ClearTaskData() {
  LOG_INFO("Clearing TaskData: DataStore(%d)", DATASTORE);

  if(DATASTORE == RINSTANCE) {
    std::vector<std::string> v;
    for(map<string,ArrayData*>::iterator i = in_memory_partitions.begin(); 
       i != in_memory_partitions.end(); ++i) {
      v.push_back(i->first);
    }

    for(map<string,Composite*>::iterator i = in_memory_composites.begin(); 
        i != in_memory_composites.end(); ++i) {
      v.push_back(i->first);
    }

    RR[".tmp.keep"] = v;//keep;
    RR.parseEvalQ("rm(list=setdiff(ls(all.names=TRUE), .tmp.keep));");
  } else {
    RR.parseEvalQ("rm(list=ls(all.names=TRUE));gc()");
  }
    
  // clear splits information in this task                                 
  var_to_Partition.clear();
  // delete composite information in this task
  for (map<string, Composite*>::iterator i = var_to_Composite.begin();
     i != var_to_Composite.end(); i++) {
    delete i->second;
  }
  var_to_Composite.clear();

  // delete information for list_type composites
  for (map<string, Composite*>::iterator i = var_to_list_type.begin();
       i != var_to_list_type.end(); i++) {
    delete i->second;
  }
  var_to_list_type.clear();
  memset(err_msg, 0x00, sizeof(err_msg));
}


Executor::~Executor() {
  //ClearTaskData();
  LOG_INFO("Executor destructor called");
  for (map<string, ArrayData*>::iterator i = in_memory_partitions.begin();
       i != in_memory_partitions.end(); i++) {
    delete i->second;
  }

  for (map<string, Composite*>::iterator i = in_memory_composites.begin();
       i != in_memory_composites.end(); i++) {
    delete i->second;
  }
}

} // End namespace

int main(int argc, char *argv[]) {

  // let this executor to be killed when a parent process (worker) terminates.
  prctl(PR_SET_PDEATHSIG, SIGKILL);
  Timer timer;

  presto::presto_malloc_init_hook();

  InitializeConsoleLogger();
  LOG_INFO("Executor started.");
  LoggerFilter(atoi(argv[4]));

  //sleep for N secs if that file exists
  FILE* f = fopen("/tmp/r_executor_startup_sleep_secs", "r");
  if(f) {
    char buffer[10];
    size_t nread = fread(buffer, 1, 10, f);
    int secs = atoi(buffer);
    if(secs > 0 and secs < 120) {
      LOG_INFO("Sleeping for %d secs, pid: %d\n", secs, getpid());
      sleep(secs);
    }
  }

  DATASTORE = (atoi(argv[5]) == 1) ? WORKER: RINSTANCE;
  LOG_INFO("Data storage layer in used : %s", getStorageLayer().c_str());

  // a pipe to send/receive task and result between worker and executor
  out = fdopen(atoi(argv[1]), "w");
  if (out == NULL) {
    LOG_ERROR("Pipe to send/receive Task and Result to Worker failed to open.");
    exit(1);
  }
  LOG_INFO("Communication pipe with Worker opened on %d", atoi(argv[1]));

  RInside R(0, 0);
  LOG_DEBUG("RInside is created");
  R_CStackLimit = (uintptr_t)-1;
  LOG_DEBUG("R_CStackLimit is set");
#ifdef INCREASE_R_HEAP
  IncreaseRHeap(R);
#endif
  ex = new Executor(R);  // Create a new and single instance of executor

  LOG_DEBUG("Loading libraries");
  // load packages
  R.parseEvalQ("tryCatch({library(Matrix);library(MatrixHelper);library(Executor);gc.time()}, error=function(ex){Sys.sleep(2);library(Matrix);library(MatrixHelper);library(Executor);gc.time()})");
  
  updatesptr = new set<tuple<string, bool, std::vector<std::pair<int64_t,int64_t>>>>;
  set<tuple<string, bool, std::vector<std::pair<int64_t,int64_t>>>> &updates = *updatesptr;
  // update pointer in R-session
  Rcpp::XPtr<set<tuple<string, bool,std::vector<std::pair<int64_t,int64_t>>> > > updates_ptr(&updates, false);
  LOG_DEBUG("New Update pointer to maintain list of updated split/composite variables in Function execution created.");

  // a buffer to keep the task result from RInside
  while (true) {
    Timer total_timer;
    total_timer.start();
    ExecutorEvent next;

    try {
      //ex->ClearTaskData();
      // keep a pointer of update variable in R-session
      R["updates.ptr..."] = updates_ptr;

      updates.clear();  // clear update vector
      LOG_INFO("*** No Task under execution. Waiting from Task from Worker **");
      R.parseEvalQ("print('Printing ls'); print(ls())");

      int result = -1;
      next = ex->GetNextEvent();

      if (next == EXECUTOR_SHUTDOWN_CODE) {
        delete ex;
        LOG_INFO("SHUTDOWN message received from Worker. Shutting down Executor");
        break;
      }

      LOG_INFO("====> New Event received of type %d", next);

      switch(next) {
        case EXECR:
	  result = ex->Execute(updates);
	  break;
        case CLEAR:
	  result = ex->Clear();
	  break;
        case PERSIST:
          result = ex->PersistToWorker();
          break;
      }
	  
    } catch(const std::exception &exception) {
      strncpy(ex->err_msg, exception.what(), sizeof(ex->err_msg)-1);
    }
    
    if(ex != NULL && (next == EXECR || next == PERSIST)) {
       SendResult(ex->err_msg);
    }

    if(ex != NULL && next == EXECR) {
       ex->ClearTaskData();
    }
  } // end while(true)
  return 0;
}


/*********************************  IN-DEV CODE BASE ********************************************/
/*
int Executor::Fetch() {
  LOG_INFO("Starting Fetch");

  int port_number, serverfd;
  char split[100];
  boost::thread* server_thread=NULL;

  int res = scanf(" %s %d %d ", split, &serverfd, &port_number);
  if (res != 3) {
    LOG_ERROR("Fetch => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParseFetch::Executor cannot recognize commands from a worker");
  }

  partitions_to_fetch.insert(std::string(split));
  //schedule(fetch_pool, boost::bind(&ReadRemotePartition, split, serverfd, port_number));
}


void Executor::ReadRemotePartition (std::string splitname, int32_t serverfd, int port_number) {
  LOG_INFO("Starting ReadRemotePartition thread");

  bool connected = false;
  int32_t new_fd = -1;
  struct sockaddr_in their_addr;
  socklen_t sin_size = sizeof(their_addr);
  static int32_t ret = 0;
  ret = listen(serverfd, SOMAXCONN);

  if (ret < 0) {
    close(serverfd);
    LOG_ERROR("ReadRemotePartition - listen failure");
    throw PrestoWarningException("ReadRemotePartition - listen failure");
  }

  while(!connected) {

    ret = listen(serverfd, SOMAXCONN);

    if (ret < 0) {
      close(serverfd);
      LOG_ERROR("ReadRemotePartition - listen failure");
      throw PrestoWarningException("ReadRemotePartition - listen failure");
    }
    boost::this_thread::interruption_point();
    int conn_fd = accept(serverfd, (struct sockaddr *) &their_addr, &sin_size);
    if (conn_fd < 0) {
      LOG_ERROR("ReadRemotePartition - accept new_fd less than zero");
      throw PrestoWarningException("ReadRemotePartition - accept new_fd less than zero");
    }
    connected = true;

    //Start reading the contents
    size_t data_size;
    size_t bytes_read = read(conn_fd, &data_size, sizeof(size_t));
    if(bytes_read == 0) {
      LOG_ERROR("ReadRemotePartition - data size not received");
      throw PrestoWarningException("ReadRemotePartition - data size not received");
    }

    boost::this_thread::interruption_point();

    ARRAYTYPE orig_class;
    bytes_read = read(conn_fd, &orig_class, sizeof(ARRAYTYPE));
    if(bytes_read == 0) {
      LOG_ERROR("ReadRemotePartition - orig_class not received");
      throw PrestoWarningException("ReadRemotePartition - orig_class not received");
    }

    boost::this_thread::interruption_point();
   
    char* dest_ = (char*) malloc(data_size);
    if (dest_ == NULL) {
        close(conn_fd);
        LOG_ERROR("ReadRemotePartition - malloc failed: %zu", data_size);
        throw PrestoWarningException("ReadRemotePartition - fail to malloc");
    }

    bytes_read=atomicio(read, conn_fd, dest_, data_size);
    if (bytes_read != data_size) {
        LOG_ERROR("atomicio read bytes - %d:%s", bytes_read, strerror(errno));
        close(conn_fd);
        throw PrestoWarningException("ReadRemotePartition - fail to receive from a remote worker");
    }
    LOG_INFO("Finished readinfg data from remote connection");

    boost::unique_lock<boost::mutex> lock(R_mutex);

    SEXP arr;
    PROTECT(arr = Rf_allocVector(RAWSXP, data_size));
    if (Rf_isNull(arr)) {
      throw PrestoWarningException("DistList::LoadInR: array is NULL");
    }
    //for (int i=0; i<data_size;++i)
    //   arr[i] = dest_[i];
    memcpy(RAW(arr), reinterpret_cast<unsigned char*>(dest_), data_size);

    Rcpp::Language unserialize_call("unserialize", arr);
    SEXP dobj;
    PROTECT(dobj = Rf_eval(unserialize_call, R_GlobalEnv));
    RR[splitname] = dobj;  // assign the varname with corresponding value in R-session
    UNPROTECT(2);
    
    //Get dims and size
    char cmd[CMD_BUF_SIZE];
    snprintf(cmd, CMD_BUF_SIZE, "dim(%s)", splitname.c_str());
    vector<int64_t> dim_vec = Rcpp::as<vector<int64_t>> (RR.parseEval(cmd));
    std::pair<int64_t, int64_t> dims_ = std::make_pair<int64_t, int64_t>(dim_vec[0], dim_vec[1]);

    snprintf(cmd, CMD_BUF_SIZE, "object.size(%s)", splitname.c_str());
    size_t size_ = Rcpp::as<size_t> (RR.parseEval(cmd));

    lock.unlock();

    //Create partition
    ArrayData *np = ParseVariable(RR, splitname, splitname, orig_class, RINSTANCE);

    //Update in-mem partitions
    boost::unique_lock<boost::recursive_mutex> metadata_lock(metadata_mutex);
    in_memory_partitions[splitname] = np;
    partitions_to_fetch.erase(splitname);  // Wait till all to be fetched partitions are fetched.
    metadata_lock.unlock();

    LOG_INFO("Done Reading data");
  }
}



int Executor::NewTransfer() {
  LOG_INFO("Starting NewTransfer");

  int port_number;
  char split[100], server_name[250];
  int res = scanf(" %s %s %d", split, server_name, &port_number);
  if (res != 3) {
    LOG_ERROR("Newtransfer => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParseNewtransfer::Executor cannot recognize commands from a worker");
  }
  LOG_INFO("split name is %s, server_name %s, port_no", split, server_name, port_number);

  //Serialize first
  char cmd[CMD_BUF_SIZE];
  if(in_memory_partitions.find(split) == in_memory_partitions.end()) {
    LOG_ERROR("Newtransfer => Split %s to be transferred is not found", split);
    throw PrestoWarningException("Newtransfer => Split to be transferred is not found");
  }

  // DESIGN 1: Write to shm (using serialize to file) and send to remote worker.
  std::string temp_shm_file = std::string("/dev/shm/")+ std::string(split);
  snprintf(cmd, CMD_BUF_SIZE, ".tmpfile.serialize <- '%s'; .tmpcon <- file(.tmpfile.serialize, open = 'wb'); serialize(`%s`, .tmpcon);close(.tmpfile.serialize);", 
           temp_shm_file.c_str(), split);
  RR.parseEvalQ(cmd);
  LOG_INFO("Serialized %s to temporary location /dev/shm%s", split, split);

  //Connect and send data.
  int32_t client_fd = presto::connect(server_name, port_number);
  if (client_fd < 0) {
    ostringstream msg;
    msg << "connect failed for transfer to " << server_name
      << ":" << port_number;
    LOG_ERROR(msg.str());//"NEWTRANSFER Task               - Connection to Worker %s:%d failed for Split transfer", location.name().c_str(), location.presto_port());
    close(client_fd);
    throw PrestoWarningException(msg.str());
  }

  SharedMemoryObject shm(boost::interprocess::open_or_create, split, boost::interprocess::read_write);
  boost::interprocess::mapped_region region(shm, boost::interprocess::read_write);
  void* addr = region.get_address();

  //Send data_size
  boost::interprocess::offset_t file_size;
  if (!shm.get_size(file_size)) {
    LOG_ERROR("Array GetSize: could not get shmem size");
    throw PrestoWarningException("Array GetSize: could not get shmem size");
  }

  int bytes_written=send(client_fd, (char*)file_size, sizeof(boost::interprocess::offset_t), 0);
  if(bytes_written == 0)
    LOG_ERROR("Error sending data size");

  //Send orig_class
  bytes_written=send(client_fd, (char*)in_memory_partitions[split]->GetType(), sizeof(RType), 0);
  if(bytes_written == 0)
    LOG_ERROR("Error sending RType");

    //Send actual data
  bytes_written = atomicio(vwrite, client_fd, addr, file_size);
  if (bytes_written != file_size)
    LOG_ERROR("Error sending partition");

  close(client_fd);
  //SharedMemoryObject::remove(temp_shm_file.name().c_str()); //Cmmented for debugging


  // DESIGN 2: Send directly to remote worker/R instance by serializing to socket.
  // Send data via serialization to socket.
  snprintf(cmd, CMD_BUF_SIZE, ".tmpsockconnection <- socketConnection(host='%s', port = %d, blocking=TRUE, server=FALSE, open='r+'); print(.tmpsockconnection);", server_name, port_number);
  LOG_INFO(cmd);
  RR.parseEvalQ(cmd);

  snprintf(cmd, CMD_BUF_SIZE, "writeChar('%d', .tmpsockconnection)", in_memory_partitions[split]->GetSize());
  RR.parseEval(cmd);
  LOG_INFO(cmd);

  snprintf(cmd, CMD_BUF_SIZE, "serialize(`%s`, .tmpsockconnection); close(.tmpsockconnection);", split);
  LOG_INFO("Evaluating final command %s", cmd);
  RR.parseEvalQ(cmd);
  LOG_INFO("Serialized %s to socket connection", split);
}*/
