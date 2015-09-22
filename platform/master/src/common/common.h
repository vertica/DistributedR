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
 * Common header file for defining constants, util functions etc.
 */

#ifndef _PRESTO_COMMON_
#define _PRESTO_COMMON_
#include <stdio.h>
#include <inttypes.h>

#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <sys/statvfs.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/version.hpp>

#include <string>
#include <sstream>
#include <vector>
#include <google/protobuf/repeated_field.h>
#include <zmq.hpp>

#include <boost/algorithm/string/erase.hpp>

#include "shared.pb.h"
#include "PrestoException.h"

#define GC_DEFAULT_GEN 1

#define MAX_FILENAME_LENGTH 200

#define NUM_SERVER_THREADS 64

#define PRESTO_WORKER_PORT 0
#define PRESTO_MASTER_PORT 0
#define MAX_CONN 8

#define NUM_TRIPLETS_PER_SEND 1024
#define EXCEPTION_MSG_SIZE 4096
// will be used to identify Preso SHM object in a shared memory region.
// UID is appended at the end
#define PRESTO_SHM_PREFIX "R-shm-"
#define LOADER_SHM_PREFIX "DR-loader-"
#define SHM_FOLDER "/dev/shm/"
#define EXECUTOR_ISS_STR "executor_iss_"

// check if a worker is alive every WORKER_HEARTBEAT_PERIOD seconds
#define WORKER_HEARTBEAT_PERIOD 30
// Determine a worker that is not contated within this time window
#define WORKER_DEAD_THRESHOLD 120
// ZMQ socket linger time in msec after close
#define SOCK_LINGER_TIME 2000

#define SEXP_HEADER_SIZE sizeof(SEXPREC_ALIGN)

// 90% of memory will be set to Presto shm by default
#define MAX_PRESTO_SHM_FRACTION 0.9
#define SHMALL_SYS_FILE "/proc/sys/kernel/shmall"
#define FILE_DESCRIPTOR_ERR_MSG "This error might happen due to the limited number of open file descriptors.\nCheck system configurations - refer to FAQ"

#define WORKER_CONNECT_WAIT_SECS 60

#define ARBT_PORT_NUM_BEGIN 49152
#define ARBT_PORT_NUM_END 65535
#define PORT_NUM_RETRY 100
#ifdef OOC_SCHEDULER
#define INMEM_UPDATE_SIZE_LIMIT (100LLU<<20)  // 100MB
#else
#define INMEM_UPDATE_SIZE_LIMIT (1LLU<<40)  // 1TB
#endif

using namespace std;
using namespace zmq;

namespace presto {
// Express a task result
typedef enum {
  TASK_SUCCEED = 0,
  TASK_EXCEPTION
} TaskStatus;

enum StorageLayer {
  WORKER = 1,
  RINSTANCE
};

enum ExecutorEvent {
  EXECR = 1,
  CLEAR,
  PERSIST
};

/** This enum indicates a bit index of each attribute
 * when HelloReply message is sent from a worker.
 */
typedef enum {
  SERVER_LOCATION = 0,
  SHARED_MEM_QUOTA,
  NUM_EXECUTOR,
  ARRAY_STORES,
  SYS_MEM_TOTAL,
  SYS_MEM_USED
} HelloReplyFlag;

static string exception_prefix = "DistributedR Exception";
extern StorageLayer DATASTORE;

static std::string getStorageLayer() {
  return ((DATASTORE == WORKER) ? "Worker" : "Executor");
}

/** Convert integer to string
 * @param i integer to convert to string
 * @return a string that is coverted
 */
static string int_to_string(const uint64_t& i) {
  stringstream ss;
  ss << i;
  return ss.str();
}

static bool IsCompositeArray(const string& da_name) {
  return (da_name.find(".c_") != std::string::npos);
}

static bool IsCompositeObject(const string& do_name) {
  return (do_name.find(".c_") != std::string::npos);
}

/** parse version number from a split name. Split name is created as presto-shm-`uid`-splitname_splitID_splitVersion
 * @param split_name a name of split that we will get a version number
 * @param version where the version information will be written
 * @return NULL
 */
static void ParseVersionNumber(const string& split_name, int32_t* version) {
  const char* version_idx = rindex(split_name.c_str(), '_');
  *version = (int32_t)(strtol(version_idx + 1, NULL, 10));
}

/** Parse split name from a split. Split name is created as presto-shm-`uid`-splitname_splitID_splitVersion
 * @param split_name a name of split to parse
 * @param darray_name name of the input darray
 * @param split_id id of the split
 * @return NULL
 */
static void ParseSplitName(const string& split_name, string* darray_name,
    int32_t* split_id) {
  // Extract the split id from the array
  int name_token = IsCompositeArray(split_name) ? '.' : '_';
  const char* idx = index(split_name.c_str(), name_token);
  *split_id = (int32_t)(strtol(idx + 1, NULL, 10));
  darray_name->assign(split_name.substr(0, idx - split_name.c_str()));
}

/** Create a new split name by incrementing only the version part
 * @param split_name a name of split to build a new name
 * @param new_split_name newly created split name
 * @return NULL
 */
static void NextVersionSplitName(const string& split_name,
    string* new_split_name) {
  const char* old_ver = rindex(split_name.c_str(), '_');
  int32_t new_version = (int32_t)(strtol(old_ver + 1, NULL, 10)) + 1;
  new_split_name->assign(split_name.substr(
      0, old_ver - split_name.c_str() + 1));
  new_split_name->append(int_to_string(new_version));
}

/** Build a split name with given darray name, split ID and version name
 * @param darray_name a name of darray
 * @param split_id the id of a split
 * @param version the version of darray
 * @return split name that is built from the inputs
 */
static string BuildSplitName(const string& darray_name, int32_t split_id,
    int32_t version) {
  return darray_name + "_" + int_to_string(split_id) + "_" +
    int_to_string(version);
}

/** Fill in a vector with input Tuple value
 * @param t input tuple
 * @param res output vector
 * @return NULL
 */
static void get_vector_from_tuple(Tuple t, vector<int64_t>* res) {
  for (int32_t i = 0; i < t.val_size(); ++i) {
    res->push_back(t.val(i));
  }
}

/** gets a vector from a buffer in protobuf. It is generally used to convert input protobuf into vector
 * @param itr iterator that contains vales
 * @param size the size of iterator
 * @param res output vector where the value will be written
 * @return NULL
 */
template<class Iterator, class Element>
static void get_vector_from_repeated_field(Iterator itr, int32_t size,
    vector<Element>* res) {
  for (int32_t i = 0; i < size; ++i) {
    res->push_back(*itr);
    itr++;
  }
}

/** Add elements to output vector
 * @param source input source of values to write
 * @param dest output vector where we will write the output
 * @return NULL
 */
template<class Element>
static void add_elements_from_vector(const vector<Element>& source,
    google::protobuf::RepeatedPtrField<Element>* dest) {
  for (int32_t i = 0; i < source.size(); ++i) {
    dest->Add()->assign(source[i]);
  }
}

/** Convert server information into a string (hostname:port_number)
 * @param server ServerInfo structure
 * @return name of output with server name and port number
 */
static string server_to_string(const ServerInfo &server) {
  return server.name()+":"+int_to_string(server.presto_port());
}

/** connect to the target hostname:port and return a socket descriptor
 * @param hostname host name of the target
 * @param port port number of target
 * @return socket descriptor
 */
int32_t connect(std::string hostname, int32_t port);

/* a function that creates a ZMQ socket and bind an open port in the configured range
 * @param start_port the start port range
 * @param end_port the end port range
 * @param ctx ZeroMQ socket context
 * @param port_in_use an out parameter that returns the port address that is bind to a socket
 * @param type the socket type. default is ZMQ_PULL
 * @ret a socket that is created from the function - THIS HAS TO BE FREED AFTER USAGE!!!!!!
 * @throws PrestoWarningException when there is no available open port in the configured range
 */
socket_t* CreateBindedSocket(int start_port, int end_port, context_t* ctx, int* port_in_use, int type = ZMQ_PULL);

/* a function that creates a Berkeley socket and bind an open port in the configured range
 * @param start_port the start port range
 * @param end_port the end port range
 * @param port_in_use an out parameter that returns the port address that is bind to a socket
 * @ret a socket that is created from the function
 * @throws PrestoWarningException when there is no available open port in the configured range
 */
int32_t CreateBindedSocket(int start_port, int end_port, int* port_in_use);

void ResetCurPortNum();

/** calculate time difference (unit in nano seconds) between two input.
 * @param time1 time1
 * @param time2 time2
 * @return time difference between time1 and time2 in nano seconds
 */
static int64_t diff_clocktime(struct timespec *time1, struct timespec *time2) {
  struct timespec result;
  /* We assume time1 < time2 */
  result.tv_sec = time2->tv_sec - time1->tv_sec;
  if (time2->tv_nsec < time1->tv_nsec) {
    result.tv_nsec = time2->tv_nsec + 1000000000L - time1->tv_nsec;
    result.tv_sec--;       /* Borrow a second. */
  } else {
    result.tv_nsec = time2->tv_nsec - time1->tv_nsec;
  }
  return result.tv_sec*1000000000L + result.tv_nsec;
}

/** Size rounded up to page boundary
 * @param s size to get mapped size
 * @return mapped size of input
 */
static size_t mapped_size(size_t s) {
  int ps = getpagesize();
  if (s%ps == 0)
    return s;
  else
    return s/ps*ps+ps;
}

/** Check if a set or map contains a key
 * @param container a container to check if an input key exists
 * @param key a key value to check
 * @return boolean value if the key exists in the container
 */
template<class C, class K>
static bool contains_key(const C &container, const K &key) {
  return container.find(key) != container.end();
}

/** get total memory of input system
 * @return total memory size
 */
size_t get_total_memory();

/** get used memory size of the system
 * @return used memory size (this contains cached memory size also)
 */
size_t get_used_memory();

/** get used memory size of the system based on the process id's.
 * @return used memory size
 */
size_t get_used_memory(std::vector<pid_t> pids);

/** It generates presto shm segment name.  Format - presto-shm-UID
 * @return presto darray shared memory prefix
 */
static string get_presto_shm_prefix() {
  std::string shm_prefix;
  shm_prefix += PRESTO_SHM_PREFIX;
//  shm_prefix += int_to_string(getuid());
//  shm_prefix += "-";
  return shm_prefix;
}

static size_t get_free_shm_size() {
  size_t shm_size = 0;
  struct statvfs* buff = (struct statvfs *)malloc(sizeof(struct statvfs));
  if(buff != NULL) {
    if (statvfs(SHM_FOLDER, buff) == 0) {
      shm_size = buff->f_bavail * buff->f_bsize;  //get free size for unprivileged users
    }
    free(buff);
  }
  return shm_size;
}

static string get_shm_size_check_name() {
  return string("/dev/shm/") + get_presto_shm_prefix() + "sema-excl";
}

static int get_random_port_num() {
  return (ARBT_PORT_NUM_BEGIN + rand()%(ARBT_PORT_NUM_END - ARBT_PORT_NUM_BEGIN));
}

static std::string presto_exec(const char* cmd) {
    FILE* pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[128];
    std::string result = "";
    while(!feof(pipe)) {
        if(fgets(buffer, 128, pipe) != NULL)
                result += buffer;
    }
    pclose(pipe);
    return result;
}

std::string check_out_of_memory(vector<pid_t> child_pids);

/**
 * retrun a list of IPV4 addresses of a local machine
 */
std::vector<std::string> get_ipv4_addresses();

/**
 * Get boost version info
 * @ret a pair of major and minor version
 */
static pair<int, int> get_boost_version() {
  return make_pair<int, int>(BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000);
}

/** Send a shutdown message to a large chunk (over 64MB) transfer thread ("-1") and interrupt the thread
   * @param server_thread a thread to interrupt
   * @param port_numbe a port number the server is listening to
   */
  static void ShutdownRawDataTransfer (int port_number) {
    // connect to a local server
    int32_t sockfd = presto::connect(string("127.0.0.1"), port_number);
    if (sockfd < 0) {
      return;
    }
    char shutdown_msg[4] = "-1";
    send(sockfd, shutdown_msg, strlen(shutdown_msg), 0);
  }
//remove whitespace and \t \n \r from string in place
void strip_string(string &s);

}  // namespace presto


#endif  // _PRESTO_COMMON_
