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
#include <setjmp.h>

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

void executor_sigint_handler(int sig);

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
  sigjmp_buf env;

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
 * and sync it with new split by writing it into shared memory region
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

  //TODO(iR) Hack for obtaining size of dataframe splits, since GetDims() is 0 for dframes and lists
  if(orig_class == DATA_FRAME || orig_class == LIST || orig_class == EMPTY){
    dims = make_pair(obj_nrow,obj_ncol);
  }

  if(store == RINSTANCE) {
    // Add the new dobject name + assign varname to shmname
    in_memory_partitions[new_dobj_name] = newsplit;

    // Save the new value.
    char cmd[CMD_BUF_SIZE];
    snprintf(cmd, CMD_BUF_SIZE, ".tmp.shmname <- deparse(substitute(`%s`)); assign(.tmp.shmname, `%s`, .GlobalEnv); ", new_dobj_name.c_str(), varname.c_str());
    RR.parseEvalQ(cmd);
  }

  AppendUpdate(new_dobj_name, newsplit->GetSize(), false, dims.first, dims.second, out);
  delete newsplit;
  LOG_INFO("Variable %s (%s in R) updated successfully.", new_dobj_name.c_str(), varname.c_str());
}


/** Parse arguments that are related to splits. The split array information will be loaded from R into the shared memory session
 * @return the number of split variables: NOTE: -1 means worker shutdown
 */
int Executor::ReadSplitArgs() {  // NOLINT

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
    LOG_DEBUG("Read Split Variable %s (%s in R)", splitname, varname);
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
        strcat(new_name,num);

        // add to v2a a fake name so it's cleaned up later. Load split from Executor or Worker
        if(in_memory_partitions.find(splitname) != in_memory_partitions.end()) {
          LOG_DEBUG("Partition %s is on Executor. Loading..", splitname);

          char cmd[CMD_BUF_SIZE];
          snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, splitname);
          RR.parseEvalQ(cmd);
        } else {  //Load from Worker
          LOG_DEBUG("Partition %s is on Worker. Loading..", splitname);

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
      // Check if executor contains the partitions.
      if(in_memory_partitions.find(splitname) != in_memory_partitions.end()) {
        LOG_DEBUG("Partition %s is on Executor. Loading..", splitname);

        char cmd[CMD_BUF_SIZE];
        snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, splitname);
        RR.parseEvalQ(cmd);
      } else {
        LOG_DEBUG("Partition %s is on Worker. Loading..", splitname);

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
  int composite_vars;  // number of composite array input
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
      LOG_DEBUG("Composite Dobject %s is on Executor. Loading..", compositename);

      char cmd[CMD_BUF_SIZE];
      snprintf(cmd, CMD_BUF_SIZE, ".tmp.varname <- deparse(substitute(`%s`)); assign(.tmp.varname, `%s`, .GlobalEnv); ", varname, compositename);
      RR.parseEvalQ(cmd);
    } else {
      LOG_DEBUG("Composite Dobject %s is on Worker. Loading..", compositename);

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
  string scoping_postfix = "rm(list = ls())} ; .DistributedR_exec_func()";

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
    //LOG_INFO("name: %s, empty: %d, dim: %d %d", name.c_str(), empty, dimensions.at(0).first, dimensions.at(0).second);

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
    int res = scanf(" %s", split);
    LOG_DEBUG("Marking split(%s) for clear", split);
    if(res != 1) {
      //LOG_ERROR("Read Clear variable (%s)", shmname);
      throw PrestoWarningException
        ("ParseClearArgs::Executor cannot recognize commands from the worker");
    }

    //Remove partition
    std::string split_str = std::string(split);
    if (in_memory_partitions.find(split_str) != in_memory_partitions.end()) {
       delete in_memory_partitions[split_str];
       in_memory_partitions.erase(split_str);
    }

    clear_vec.push_back(split);
  }

  RR[".tmp.clear"] = clear_vec;
  char cmd[CMD_BUF_SIZE];
  snprintf(cmd, CMD_BUF_SIZE, "rm(list=.tmp.clear);");
  RR.parseEvalQ(cmd);

  LOG_DEBUG("Clear complete");

  return 0;
}


int Executor::PersistToWorker() {
  char split[100];
  int res = scanf(" %s", split);
  if (res != 1) { 
    LOG_ERROR("PersistToWorker => Bad file format for Executor. Executor cannot recognize commands from Worker.");
    throw PrestoWarningException
      ("ParsePersistToWorker::Executor cannot recognize commands from a worker");
  }
  LOG_DEBUG("Persist split(%s) to worker", split);

  //Serialize first
  char emessage[CMD_BUF_SIZE];
  if(in_memory_partitions.find(split) == in_memory_partitions.end()) {
    LOG_ERROR("PersistToWorker => Split(%s) not found", split);
    snprintf(emessage, CMD_BUF_SIZE, "PersistToWorker => Split(%s) not found", split);
    throw PrestoWarningException(std::string(emessage));
  }

  ARRAYTYPE orig_class = in_memory_partitions[std::string(split)]->GetClassType();
  ArrayData* data = ParseVariable(RR, split, split, orig_class, WORKER);
  delete data;

  char cmd[CMD_BUF_SIZE];
  snprintf(cmd, CMD_BUF_SIZE, "rm(list=ls(pattern='serializedtmp...'));");
  RR.parseEvalQ(cmd);

  LOG_DEBUG("Persist split(%s) to worker complete", split);
}


static void SendResult(const char* err_msg) {
  if (err_msg[0] != '\0') {
    ostringstream msg;
    msg << "Executor Exception: "<< err_msg;
    LOG_ERROR(msg.str());

    AppendTaskResult(TASK_EXCEPTION, err_msg, out);
    LOG_INFO("TASK_EXCEPTION sent as Task result to Worker");
  } else {
    AppendTaskResult(TASK_SUCCEED, "Succeed", out);
    LOG_INFO("Task Success sent to Worker");
  }

  LOG_INFO("Flushing Task Data from Executor memory");
}

/** Clear array data and temporary R data that were used to perform a task                                                      
  * @param R RInside object
  * @param var_to_ArrayData a map of R-variable name to ArrayData (Split)
 * @param var_to_Composite a map of R-variable name to ArrayData (Composite)
 * @return NULL                                                                                                               
 */
void Executor::ClearTaskData() {

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

    RR[".tmp.keep"] = v;
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
  LOG_INFO("Data storage layer in use : %s", getStorageLayer().c_str());

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
  R.parseEvalQ("tryCatch({library(Matrix);library(Executor);gc.time()}, error=function(ex){Sys.sleep(2);library(Matrix);library(Executor);gc.time()})");
  
  updatesptr = new set<tuple<string, bool, std::vector<std::pair<int64_t,int64_t>>>>;
  set<tuple<string, bool, std::vector<std::pair<int64_t,int64_t>>>> &updates = *updatesptr;
  // update pointer in R-session
  Rcpp::XPtr<set<tuple<string, bool,std::vector<std::pair<int64_t,int64_t>>> > > updates_ptr(&updates, false);
  LOG_DEBUG("New Update pointer to maintain list of updated split/composite variables in Function execution created.");

  // a buffer to keep the task result from RInside
  //R.parseEvalQ("gcinfo(verbose=TRUE)");

#ifdef PERF_TRACE
  int exec_id;
  scanf(" %d ",&exec_id);
  int tracer = ZTracer::ztrace_init();

  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  ZTracer::ZTraceEndpointRef executor_ztrace_inst = ZTracer::create_ZTraceEndpoint("127.0.0.1",3,string(hostname) + "_" + std::to_string(exec_id));
#endif

  signal(SIGUSR2, executor_sigint_handler);
  if(sigsetjmp(env,0))
    {
      LOG_DEBUG("Deleting Executor memory");
      delete ex;
      return(0);
    }

  while (true) {
    Timer total_timer;
    total_timer.start();
    ExecutorEvent next;

    try {
      //ex->ClearTaskData();
      // keep a pointer of update variable in R-session
      R["updates.ptr..."] = updates_ptr;
      updates.clear();  // clear update vector

//Getting trace information from worker
#ifdef PERF_TRACE
      struct blkin_trace_info info;
      scanf(" %ld " , &info.parent_span_id);
      scanf(" %ld " , &info.span_id);
      scanf(" %ld ",  &info.trace_id);
      
      //don't trace
      if(info.trace_id == 1337) {
          trace_executor = false;
      } else {
          trace_executor = true;
      }
      executor_trace = ZTracer::create_ZTrace("Executor", executor_ztrace_inst, &info, true);
#endif

      LOG_INFO("*** No Task under execution. Waiting from Task from Worker **");
      //R.parseEvalQ("print(ls())");

      int result = -1;
      next = ex->GetNextEvent();

      if (next == EXECUTOR_SHUTDOWN_CODE) {
        delete ex;
        LOG_INFO("SHUTDOWN message received from Worker. Shutting down Executor");
        break;
      }

      LOG_INFO("*** New Event received of type %d (1 - EXECUTE, 2 - CLEAR, 3 - PERSIST) **", next);

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
      if(strlen(ex->err_msg) == 0) {  //Capture this exception only when it doesnt already have an exception
        strncpy(ex->err_msg, exception.what(), sizeof(ex->err_msg)-1);
      }
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

void executor_sigint_handler(int sig)
{
  sigset_t mask;
  sigfillset(&mask);
  sigprocmask(SIG_SETMASK, &mask, NULL);

  LOG_DEBUG("Executor shutting down");
  siglongjmp(env,1);
}
