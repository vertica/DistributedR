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
 * Class which encapsulates information about workers and provides
 * helper methods to send RPC requests
 * This class is thread-safe
 */

#ifndef _PRESTO_WORKER_INFO_
#define _PRESTO_WORKER_INFO_

#include <boost/thread/tss.hpp>
#include <boost/shared_ptr.hpp>

#include <string>
#include <sstream>
#include <vector>
#include <list>

#include <zmq.hpp>

#include "shared.pb.h"
#include "worker.pb.h"
#include "common.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>

#ifdef PERF_TRACE
#include <ztracer.hpp>
#endif

//using namespace boost;
using namespace std;
using namespace zmq;

namespace presto {
    
class WorkerInfo {
 public:
  WorkerInfo(const string& hostname, int32_t port, context_t* zmq_ctx, int32_t id = 0);
  ~WorkerInfo();
  // NOTE: This only closes the thread-local socket. This must be called
  // by all threads using this WorkerInfo or the threads must exit
  // cleanly.
  void Close();

  void Hello(const HelloRequest& hello);
  void Shutdown();

  void Fetch(const FetchRequest& fetch);
  void NewTransfer(const FetchRequest& fetch);
  void NewExecuteR(const NewExecuteRRequest& newexecr);
  void IO(const IORequest& io);
  void Clear(const ClearRequest& clear);
  void CreateComposite(const CreateCompositeRequest& createcomposite);
  void Log(const LogRequest& log);
  void VerticaLoad(const VerticaDLRequest& verticaload);
  bool IsRunning();
  string hostname() const {
    return hostname_;
  }

  int32_t port() const {
    return port_;
  }

  int32_t getID() const {
    return id_;
  }
  
  void setID(int32_t new_id) {
    id_ = new_id;
  }

  std::string get_hostname_port_key() {
    return std::string(hostname_)+":"+int_to_string(port_);
  }
  void SendZMQMessagePush(const WorkerRequest& req);

 private:
  void Pusher();

  string hostname_;
  int32_t port_;
  int32_t id_;
  context_t* ctx_;

  string endpoint_;
  bool running_;
  boost::thread *thr_;

  boost::mutex message_queue_mutex_;
  boost::condition_variable message_queue_empty_;
  list<WorkerRequest> message_queue_;
};
   
}  // namespace presto

#endif  // _PRESTO_WORKER_INFO_
