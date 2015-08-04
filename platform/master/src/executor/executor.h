#ifndef _DR_EXECUTOR_H__
#define _DR_EXECUTOR_H__

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include <map>
#include <string>
#include <vector>
#include <tuple>
#include <set>

#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
//#include <boost/threadpool.hpp>

namespace presto {

FILE* out;

class Executor {
public:
  Executor(RInside & R) : RR(R)
  {
    in_memory_partitions.clear();
    in_memory_composites.clear();
    
    var_to_Partition.clear();
    var_to_Composite.clear();
    var_to_list_type.clear();

    memset(err_msg, 0x00, sizeof(err_msg));
  }

  ~Executor();

  ExecutorEvent GetNextEvent();
  int Execute(std::set<std::tuple<std::string, bool, std::vector<std::pair<int64_t,int64_t>>>> const & updates);
  int Clear();
  int PersistToWorker();

  int ReadSplitArgs();
  int ReadRawArgs();
  int ReadCompositeArgs();

  void ClearTaskData();
  //void HandleResult();
  void CreateUpdate(const std::string& varname, const std::string& splitname, int64_t nrow, int64_t ncol, StorageLayer store);
    
  char err_msg[EXCEPTION_MSG_SIZE];

 private:

  // reference of the R instance passed through constructor
  RInside & RR;

  //Permanent data strucctures
  std::map<std::string, ArrayData*> in_memory_partitions;
  std::map<std::string, Composite*> in_memory_composites;

  //Temporary data structures / iteration
  std::map<std::string, std::string> var_to_Partition;
  std::map<std::string, Composite*> var_to_Composite;
  std::map<std::string, Composite*> var_to_list_type;

  std::string prev_func_body;
  Rcpp::Language exec_call;

  //boost::mutex R_mutex;
  //boost::recursive_mutex metadata_mutex;

  //std::set<boost::thread*> server_threads;
};

}  // end namespace presto

#endif
