/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2014] Hewlett-Packard Development Company, L.P.

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

#include <common.h>

using namespace std;
using namespace zmq;
extern "C" int bind(int, const sockaddr*, socklen_t);

namespace presto {

static boost::mutex port_bind_mutex;
static volatile int cur_port_assigned = -1;

/** connect to the target hostname:port and return a socket descriptor
 * @param hostname host name of the target
 * @param port port number of target
 * @return socket descriptor
 */
int32_t connect(std::string hostname, int32_t port) {
  struct addrinfo *res, *p, hints;
  int32_t sockfd = -1;
  string port_str = int_to_string(port);

  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  int32_t response = getaddrinfo(hostname.c_str(), port_str.c_str(), NULL,
                                 &res);
  if (response != 0) {
    return -1;
  }

  for (p = res; p != NULL; p = p->ai_next) {
    sockfd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0) {
      return sockfd;
    }
    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      // TODO(shivaram): Should we try other interfaces ?
      continue;
    }
    break;
  }
  if (p == NULL) {
    return -1;
  }
  freeaddrinfo(res);
  return sockfd;
}

/* a function that creates a ZMQ socket and bind an open port in the configured range
 * @param start_port the start port range
 * @param end_port the end port range
 * @param ctx ZeroMQ socket context
 * @param port_in_use an out parameter that returns the port address that is bind to a socket
 * @param type the socket type. default is ZMQ_PULL
 * @ret a socket that is created from the function - THIS HAS TO BE FREED AFTER USAGE!!!!!!
 * @throws PrestoWarningException when there is no available open port in the configured range
 */
socket_t* CreateBindedSocket(int start_port, int end_port, context_t* ctx, int* port_in_use, int type) {
  socket_t* sock = new socket_t(*ctx, type);
#ifdef ZMQ_LINGER
  int linger_period = SOCK_LINGER_TIME;
  sock->setsockopt(ZMQ_LINGER, &linger_period, sizeof(linger_period));
#endif
  int num_port_cand = end_port - start_port + 1;
  bool bind_succeed = false;
  port_bind_mutex.lock();
  for (int i = 0; i < num_port_cand; ++i){
    cur_port_assigned = (cur_port_assigned >= start_port && cur_port_assigned <= end_port) ?
      cur_port_assigned : start_port;  // to make the value circulate after hitting the end_port
    string endpoint = "tcp://*:"+int_to_string(cur_port_assigned);
    try {
      sock->bind(endpoint.c_str());
      bind_succeed = true;
      *port_in_use = (cur_port_assigned++);
      break;
    } catch(const std::exception& e) {
      ++cur_port_assigned;
      continue;
    }
  }
  port_bind_mutex.unlock();
  if (bind_succeed == false) {
    LOG_ERROR("CreateBindedSocket ZMQ - fail to bind a port (no available port)");
    throw PrestoWarningException("There is no open port in the configured range");
  }
  return sock;
}

/* a function that creates a Berkeley socket and bind an open port in the configured range
 * @param start_port the start port range
 * @param end_port the end port range
 * @param port_in_use an out parameter that returns the port address that is bind to a socket
 * @ret a socket that is created from the function
 * @throws PrestoWarningException when there is no available open port in the configured range
 */
int32_t CreateBindedSocket(int start_port, int end_port, int* port_in_use) {
  int32_t serverfd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (serverfd < 0) {
    LOG_ERROR("CreateBindedSocket - fail to create a socket");
    throw PrestoWarningException("CreateBindedSocket - fail to open a socket");
  }
  int optval = 1;
  // set SO_REUSEADDR to allow reusing a port number that is in close_wait state
  setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  int num_port_cand = end_port - start_port + 1;

  bool bind_succeed = false;
  port_bind_mutex.lock();
  for (int i = 0; i < num_port_cand; ++i){
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    cur_port_assigned = (cur_port_assigned >= start_port && cur_port_assigned <= end_port) ?
      cur_port_assigned : start_port;  // to make the value circulate after hitting the end_port
    addr.sin_port = htons(cur_port_assigned);
    int ret = ::bind(serverfd, (struct sockaddr*) &addr, sizeof(addr));
    if (ret < 0) {
      ++cur_port_assigned; // try the next large value
      continue;
    } else {
      *port_in_use = (cur_port_assigned++);
      bind_succeed = true;
      break;
    }
  }
  port_bind_mutex.unlock();
  if (bind_succeed == false) {
    LOG_ERROR("CreateBindedSocket - fail to bind a port (no available port)");
    throw PrestoWarningException("There is no open port in the configured range");
  }
  return serverfd;
}

void ResetCurPortNum() {
  port_bind_mutex.lock();
  cur_port_assigned = -1;
  port_bind_mutex.unlock();
}

/** get total memory of input system
 * @return total memory size
 */
size_t get_total_memory() {
  struct sysinfo mem_info;
  sysinfo(&mem_info);
  size_t total_mem = mem_info.totalram;
  total_mem *= mem_info.mem_unit;
  return total_mem;
}

/** get used memory size of the system
 * @return used memory size (this contains chached memory size also)
 */
size_t get_used_memory() {
  size_t used_mem = 0;
  FILE* fp = fopen( "/proc/meminfo", "r" );
  if (fp != NULL) {
    size_t bufsize = 1024 * sizeof(char);
    char* buf = (char*)malloc(bufsize);
    long value = -1L;
    while (getline(&buf, &bufsize, fp) >= 0) {
      if (strncmp(buf, "Active:", 7) != 0) continue;
      sscanf(buf, "%*s%ld", &value);
      break;
    }
    fclose(fp);
    free((void*)buf);
    if (value != -1L) {
      used_mem = (size_t)value * 1024L;
    }
  }
  if (used_mem == 0) {
    struct sysinfo mem_info;
    sysinfo(&mem_info);
    used_mem = mem_info.totalram - mem_info.freeram - mem_info.bufferram;
    used_mem *= mem_info.mem_unit;
  }
  return used_mem;
}

size_t get_used_memory(std::vector<pid_t> pids){
    pids.push_back(getpid());
    size_t used_mem = 0;
    for(size_t i = 0; i < pids.size(); i++){
        stringstream ss;
        ss << "/proc/" << pids[i] << "/status";
        FILE *fp = fopen(ss.str().c_str(), "r");
        if(fp != NULL){
            size_t bufsize = 1024 * sizeof(char);
            char* buf = (char*)malloc(bufsize);
            long value = -1L;
            while(getline(&buf, &bufsize, fp) >= 0){
                if(strncmp(buf, "VmRSS:", 6) != 0) continue;
                sscanf(buf, "%*s%ld", &value);
                break;
            }
            fclose(fp);
            free((void *)buf);
            if(value != -1L){
                used_mem += (size_t)value*1024L;
            }
        }
    }
    return used_mem;
}

std::string check_out_of_memory(vector<pid_t> child_pids) {
  ostringstream msg;
  std::string exec_msg = presto_exec("dmesg | tail | grep 'Out of memory' | grep 'R-executor-bin'");
  // check the process id of executors to see if the Out of memory is really Presto executor ID
  // if it matches, add out of memory message
  if (exec_msg.size() > 0) {
    for (int i = 0; i < child_pids.size(); ++i) {
      std::stringstream ss;
      ss << child_pids[i];
      std::size_t found = exec_msg.find(ss.str());
      if (found != std::string::npos) {
        //fprintf(stderr, "%8.3lf : Exec message : %s\n", abs_time()/1e6, exec_msg.c_str());
        msg << endl << "Executor killed probably due to out-of-memory. Check dmesg or memory usage";
        break;
      }
    }
  }
  return msg.str();
}
/**
 * retrun a list of IPV4 addresses of a local machine
 */
std::vector<std::string> get_ipv4_addresses() {
  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;
  std::vector<std::string> ips;
  if (getifaddrs(&ifAddrStruct) == -1) {
    return ips;
  }

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa ->ifa_addr == NULL) {
      continue;
    }
    if (ifa ->ifa_addr->sa_family == AF_INET) {
      // is a valid IP4 Address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      ips.push_back(string(addressBuffer));
    } else if (ifa->ifa_addr->sa_family == AF_INET6) {
      // skip IPV6 for the time being
    }
  }
  if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
  return ips;
}

void strip_string(string &s) {
    boost::algorithm::erase_all(s, " ");
    boost::algorithm::erase_all(s, "\t");
    boost::algorithm::erase_all(s, "\r");
    boost::algorithm::erase_all(s, "\n");

}

}  // namespace presto

