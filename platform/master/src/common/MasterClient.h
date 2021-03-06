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
 * Class which encapsulates information about presto master and provides
 * helper methods to send RPC requests. This class is thread-safe
 */

#ifndef _PRESTO_MASTER_CLIENT_
#define _PRESTO_MASTER_CLIENT_

#include <boost/thread/tss.hpp>
#include <boost/shared_ptr.hpp>

#include <string>
#include <list>

#include <zmq.hpp>

#include "shared.pb.h"
#include "master.pb.h"
#include "common.h"
#include "dLogger.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>

#include "Observer.h"
#include <google/protobuf/message.h>

//using namespace boost;
using namespace std;
using namespace zmq;

namespace presto {

class MasterClient : public ISubject<google::protobuf::Message> {
 public:
  MasterClient(string hostname, int32_t port, context_t* zmq_ctx);
  ~MasterClient();
  // NOTE: This only closes the thread-local socket. This must be called
  // by all threads using this object or the threads must exit
  // cleanly.
  void Close();

  void NewUpdate(const NewUpdateRequest& req);
  void TaskDone(const TaskDoneRequest& req);
  void HelloReply(const HelloReplyRequest& req);
  void WorkerAborting(const WorkerAbortRequest& workerabort);
  void MetadataUpdated(const MetadataUpdateReply& metadataupdate);

  string hostname() {
    return hostname_;
  }

  int32_t port() {
    return port_;
  }

  void SendZMQMessagePush(const MasterRequest& req);

 private:
  void Pusher();

  string hostname_;
  int32_t port_;
  context_t* ctx_;

  string endpoint_;
  bool running_;
  boost::thread *thr_;

  boost::mutex message_queue_mutex_;
  boost::condition_variable message_queue_empty_;
  list<MasterRequest> message_queue_;

};

}  // namespace presto

#endif  // _PRESTO_WORKER_INFO_
