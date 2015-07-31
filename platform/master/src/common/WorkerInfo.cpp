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

#include <stdio.h>
#include <string>

#include <boost/thread.hpp>

#include "WorkerInfo.h"
#include "PrestoException.h"

#ifdef PERF_TRACE
#include "dLogger.h"
#endif

using namespace boost;
namespace presto {
   
/** This function sends a message (or task) in a queue to a worker. This is called from a master side
 * @return NULL
 */
void WorkerInfo::Pusher() {
  socket_t socket(*ctx_, ZMQ_PUSH);
#ifdef ZMQ_LINGER
  int linger_period = SOCK_LINGER_TIME;
  socket.setsockopt(ZMQ_LINGER, &linger_period, sizeof(linger_period));
#endif
  try {
    socket.connect(endpoint_.c_str());
  } catch (zmq::error_t err) {
    ostringstream msg;
    msg << "connect to worker " << endpoint_
        << " with error - " << err.what();
    fprintf(stderr, "%s\n", msg.str().c_str());
    return;
  }

  while (true) {
    unique_lock<mutex> lock(message_queue_mutex_);
    while (message_queue_.empty()) {
      message_queue_empty_.wait(lock);

      // to process packets that were queued right before running_ set to false
      if (!running_ && message_queue_.empty()) {
        return;
      }
    }

    // to process packets that were queued right before running_ set to false
    if (!running_ && message_queue_.empty()) {
      // delete this if we want to
      // process remaining messages
      // before shutting down
      return;
    }

    WorkerRequest req = message_queue_.front();
    message_queue_.pop_front();
    lock.unlock();

    message_t zmq_req(req.ByteSize());
    req.SerializeToArray(zmq_req.data(), zmq_req.size());

    try {
      socket.send(zmq_req);   
    } catch (zmq::error_t err) {
      fprintf(stderr, "send to worker %s failed %s\n", endpoint_.c_str(),
              err.what());
      return;
    }
  }
}

/** keep message in a send queue. It will be processed eventually.
 * @param req a structure with request information
 * @return NULL
 */
void WorkerInfo::SendZMQMessagePush(const WorkerRequest& req) {
  // If the target worker is not in running status,
  // we do not allow further messages.
  if (running_ == false) {
    return;
  }
  unique_lock<mutex> lock(message_queue_mutex_);
  // if the queue is currently empty, we will notify to process the request
  // Otherwise, it will be processed after prior requests were handled
  bool notify = message_queue_.empty();
  message_queue_.push_back(req);
  lock.unlock();

  if (notify)
    message_queue_empty_.notify_one();
}

/** A WorkerInfo constructor. This class which encapsulates information 
 * about workers and provides helper methods to send RPC requests.
 * @param hostname a host name of a target worker
 * @param port port number of a target worker
 * @param zmq_ctx ZMQ context
 * @return WorkerInfo object
 */
WorkerInfo::WorkerInfo(const string& hostname, int32_t port,
    context_t* zmq_ctx, int32_t id)
    : hostname_(hostname), port_(port), ctx_(zmq_ctx),
      id_(id), running_(true) {
  endpoint_ = "tcp://" + hostname + ":" + int_to_string(port);

  thr_ = new thread(bind(&WorkerInfo::Pusher, this));
}

/** WorkerInfo destructor.
 */
WorkerInfo::~WorkerInfo() {
  // signal the Pusher that we're done
  unique_lock<mutex> lock(message_queue_mutex_);
  running_ = false;
  lock.unlock();
  message_queue_empty_.notify_one();

  // wait until Pusher exits
  try {
    thr_->try_join_for(boost::chrono::seconds(1));
  } catch (...) {}
  delete thr_;
}

/** Check if a destination worker is running
 * @return a boolean to indicate if this worker is running
 */
bool WorkerInfo::IsRunning() {
  return running_;
}

/** Set the worker as not running
 */
void WorkerInfo::Close() {
  running_ = false;
}

/** A method to send hello message to a target worker. This is to initiate connection from worker and master
 * @param hello contains hello request information master location and session begin time
 * @return NULL
 */
void WorkerInfo::Hello(const HelloRequest& hello) {
  WorkerRequest req;
  req.set_type(WorkerRequest::HELLO);
  req.mutable_hello()->CopyFrom(hello);
  SendZMQMessagePush(req);
}

/** A function that sends a shutdown message to a worker. Upon getting this, a worker will perform clean-shutdown
 * @return NULL
 */
void WorkerInfo::Shutdown() {
  WorkerRequest req;
  req.set_type(WorkerRequest::SHUTDOWN);
  SendZMQMessagePush(req);
}

/** This function delivers a new function initiated from R session
 * @param newexecr contains information to execute a R function (all the arguments)
 * @return NULL
 */
void WorkerInfo::NewExecuteR(const NewExecuteRRequest& newexecr) {
  WorkerRequest req;
  req.set_type(WorkerRequest::NEWEXECR);
  req.mutable_newexecr()->CopyFrom(newexecr);
  
#ifdef PERF_TRACE
  struct blkin_trace_info info;
  master_trace->get_trace_info(&info);
  req.set_parent_span_id(info.parent_span_id);
  req.set_span_id(info.span_id);
  req.set_trace_id(info.trace_id);
#endif
  SendZMQMessagePush(req);
}

/** This performs IO operation (LOAD/SAVE) of darrays in a shared memory region
 * @param io information about this io request
 * @return NULL
 */
void WorkerInfo::IO(const IORequest& io) {
  WorkerRequest req;
  req.set_type(WorkerRequest::IO);
  req.mutable_io()->CopyFrom(io);
  SendZMQMessagePush(req);
}

/** Delete a darray object from a shared memory region
 * @param clear information about a darray to be deleted
 * @return NULL
 */
void WorkerInfo::Clear(const ClearRequest& clear) {
  WorkerRequest req;
  req.set_type(WorkerRequest::CLEAR);
  req.mutable_clear()->CopyFrom(clear);
  SendZMQMessagePush(req);
}

/** Send a command to download (fetch) a split from a target host
 * @param fetch information about the task
 * @return NULL
 */
void WorkerInfo::Fetch(const FetchRequest& fetch) {
  WorkerRequest req;
  req.set_type(WorkerRequest::FETCH);
  req.mutable_fetch()->CopyFrom(fetch);
#ifdef PERF_TRACE
  struct blkin_trace_info info;
  master_trace->get_trace_info(&info);
  req.set_parent_span_id(info.parent_span_id);
  req.set_span_id(info.span_id);
  req.set_trace_id(info.trace_id);
#endif
  //  SendZMQMessage(req, res);
  SendZMQMessagePush(req);
}

/** This function lets a worker to send a split to a remote worker
 * @param fetch Contains information about the fetched object and target host
 * @return NULL
 */
void WorkerInfo::NewTransfer(const FetchRequest& fetch) {
  WorkerRequest req;  // Create a request to worker
  req.set_type(WorkerRequest::NEWTRANSFER);
  req.mutable_fetch()->CopyFrom(fetch);
#ifdef PERF_TRACE
  struct blkin_trace_info info;
  if(worker_trace.get()){
  worker_trace->get_trace_info(&info);
  }else{
      info.trace_id = 1337;
  }
  req.set_parent_span_id(info.parent_span_id);
  req.set_span_id(info.span_id);
  req.set_trace_id(info.trace_id);
#endif
  
  //  SendZMQMessage(req, res);
  SendZMQMessagePush(req);  // send message
}

/** Function to create a composite array
 * @param createcomposite information about this composite array creation
 * @return NULL
 */
void WorkerInfo::CreateComposite(
    const CreateCompositeRequest& createcomposite) {
  WorkerRequest req;
  req.set_type(WorkerRequest::CREATECOMPOSITE);
  req.mutable_createcomposite()->CopyFrom(createcomposite);
  
  #ifdef PERF_TRACE
  struct blkin_trace_info info;
  master_trace->get_trace_info(&info);
  req.set_parent_span_id(info.parent_span_id);
  req.set_span_id(info.span_id);
  req.set_trace_id(info.trace_id);
#endif
  SendZMQMessagePush(req);
}

/** A function used in OOC scheduler
 * @param log information of the task
 * @return NULL
 */
void WorkerInfo::Log(const LogRequest& log) {
  WorkerRequest req;
  req.set_type(WorkerRequest::LOG);
  req.mutable_log()->CopyFrom(log);
  SendZMQMessagePush(req);
}

void WorkerInfo::VerticaLoad(const VerticaDLRequest& verticaload) {
  WorkerRequest req;
  req.set_type(WorkerRequest::VERTICALOAD);
  req.mutable_verticaload()->CopyFrom(verticaload);
  SendZMQMessagePush(req);
}

// NOTE: Right now this does a simple blocking call using a thread-local
// socket. We could try other ZMQ techniques like queues/routers here.
// void WorkerInfo::SendZMQMessage(const WorkerRequest& req, Response* res) {
//   if (socket_.get() == NULL) {
//     socket_.reset(new socket_t(*ctx_, ZMQ_REQ));
//     connected = false;
//   }
//   //  Connect to the worker
//   if (!connected) {
//     try {
//       socket_->connect(endpoint_.c_str());
//       connected = true;
//     } catch (zmq::error_t err) {
//       fprintf(stderr, "Connect to worker %s failed %s\n", endpoint_.c_str(),
//           err.what());
//       return;
//     }
//   }
//   message_t zmq_req(req.ByteSize());
//   req.SerializeToArray(zmq_req.data(), zmq_req.size());

//   try {
//     socket_->send(zmq_req);
//   } catch (zmq::error_t err) {
//     fprintf(stderr, "Send to worker %s failed %s\n", endpoint_.c_str(),
//         err.what());
//     return;
//   }

//   message_t zmq_reply;
//   socket_->recv(&zmq_reply);

//   res->ParseFromArray(zmq_reply.data(), zmq_reply.size());
// }

}  // namespace presto
