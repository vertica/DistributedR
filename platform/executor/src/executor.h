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
#include <boost/threadpool.hpp>

namespace presto {

enum CmdType {
  EXECUTE = 1,
  CLEAR,
  PERSIST,
  DEPLOY
};

FILE* out;

class Executor {
public:
  Executor(RInside & R) : RR(R), fetch_pool(10)
  {
    in_memory_partitions.clear();
    in_memory_composites.clear();
    
    var_to_Partition.clear();
    var_to_Composite.clear();
    var_to_list_type.clear();

    memset(err_msg, 0x00, sizeof(err_msg));
  }

  ~Executor();

  int GetNextTask();
  int Execute(std::set<std::tuple<std::string, bool, std::vector<std::pair<int64_t,int64_t>>>> const & updates);
  int Clear();
  int Fetch();
  int NewTransfer();
  int PersistToWorker();

  int ReadSplitArgs();
  int ReadRawArgs();
  int ReadCompositeArgs();
  void ReadRemotePartition(std::string split, int32_t serverfd, int port_number);

  void ClearTaskData();
  //void HandleResult();
  void CreateUpdate(const std::string& varname, const std::string& splitname, int64_t nrow, int64_t ncol, StorageLayer store);
    
  char err_msg[EXCEPTION_MSG_SIZE];

 private:

  RInside & RR; // reference of the R instance passed through constructor
  std::set<boost::thread*> server_threads; // fix this too many threads.
  //Permanent data strucctures
  std::map<std::string, ArrayData*> in_memory_partitions;
  std::map<std::string, Composite*> in_memory_composites;

  //Temporary data structures / session
  std::map<std::string, std::string> var_to_Partition;
  std::map<std::string, Composite*> var_to_Composite;
  std::map<std::string, Composite*> var_to_list_type;

  std::set<std::string> partitions_to_fetch;

  std::string prev_func_body;
  Rcpp::Language exec_call;

  boost::threadpool::pool fetch_pool;
  boost::mutex R_mutex;
  boost::recursive_mutex metadata_mutex;
};

}  // end namespace presto

#endif
