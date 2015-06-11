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

#include "MasterClient.h"

using namespace boost;
namespace presto {

/** Push message into master. This is generally called when a worker needs to send messaes to a master
 * This function runs as a seperate thread
 */
void MasterClient::Pusher() {
  socket_t socket(*ctx_, ZMQ_PUSH);
#ifdef ZMQ_LINGER
  // wait for 2 seconds after socket close
  int linger_period = SOCK_LINGER_TIME;
  socket.setsockopt(ZMQ_LINGER, &linger_period, sizeof(linger_period));
#endif
  try {
    socket.connect(endpoint_.c_str());
  } catch (zmq::error_t err) {
    ostringstream msg;
    msg << "connect to master at " << endpoint_.c_str() << " failed: "<<  err.what();
    LOG_ERROR(msg.str());
    return;
  }
  LOG_DEBUG("Connected to master at %s", endpoint_.c_str());

  while (true) {
    unique_lock<mutex> lock(message_queue_mutex_);
    while (message_queue_.empty()) {
      message_queue_empty_.wait(lock);
      if (!running_) {
        return;
      }
    }

    if (!running_) {
      // delete this if we want to
      // process remaining messages
      // before shutting down
      return;
    }
    // MasterRequest class contains message structure from worker to master
    MasterRequest req = message_queue_.front();
    message_queue_.pop_front();
    lock.unlock();

    message_t zmq_req(req.ByteSize());
    req.SerializeToArray(zmq_req.data(), zmq_req.size());

    //notify observers
    Notify(req);

    try {
      socket.send(zmq_req);
    } catch (zmq::error_t err) {
      fprintf(stderr, "send to master %s failed: %s", endpoint_.c_str(),
              err.what());
      return;
    }
  }
}

/** Add a task message to the queue and notify it to sending thread
 * @param req Inpur request that will be sent to a master
 * @return  NULL
 */
void MasterClient::SendZMQMessagePush(const MasterRequest& req) {
  unique_lock<mutex> lock(message_queue_mutex_);
  bool notify = message_queue_.empty();
  message_queue_.push_back(req);
  lock.unlock();

  if (notify)
    message_queue_empty_.notify_one();
}

/** A MasterClient constructor.
 * This encapsulates information about presto master and provides 
 * helper methods to send RPC requests. This class is thread-safe
 * @param hostname a host name of a master
 * @param port a master port number
 * @zmq_ctx zeromq context
 */
MasterClient::MasterClient(string hostname, int32_t port,
    context_t* zmq_ctx)
  : hostname_(hostname), port_(port), ctx_(zmq_ctx),
    running_(true) {
  endpoint_ = "tcp://" + hostname + ":" + int_to_string(port);

  thr_ = new thread(bind(&MasterClient::Pusher, this));
}

/** MasterClient destructor
 */
MasterClient::~MasterClient() {
  // signal the Pusher that we're done
  unique_lock<mutex> lock(message_queue_mutex_);
  running_ = false;
  lock.unlock();
  message_queue_empty_.notify_one();

  // wait until Pusher exits
  thr_->join();
  delete thr_;
}

/** Close socket to the master
 */
void MasterClient::Close() {
  // Explicitly delete socket from this thread.
  // if (socket_.get() != NULL) {
  //   socket_.reset(NULL);
  // }
}

/** Sending NewUpdate task to the master. This function does not seem to be used. TaskDoneRequest includes update information
 * @param newupdate contains information about the new update
 * @return NULL
 */
void MasterClient::NewUpdate(const NewUpdateRequest& newupdate) {
  MasterRequest req;
  req.set_type(MasterRequest::NEWUPDATE);
  req.mutable_newupdate()->CopyFrom(newupdate);
    SendZMQMessagePush(req);
}

/** This message is sent to a master when a task is done. It contains updated darray information, task result (succeed/fail), message
 * @param taskdone a message to be sent to a worker
 * @return NULL
 */
void MasterClient::TaskDone(const TaskDoneRequest& taskdone) {
  MasterRequest req;
  req.set_type(MasterRequest::TASKDONE);
  req.mutable_taskdone()->CopyFrom(taskdone);
  SendZMQMessagePush(req);
}

/** Sending an acknowledgement to a master upon getting Hello message
 * @param helloreply HelloReply message
 * @return NULL
 */
void MasterClient::HelloReply(const HelloReplyRequest& helloreply) {
  MasterRequest req;
  req.set_type(MasterRequest::HELLOREPLY);
  req.mutable_helloreply()->CopyFrom(helloreply);
  SendZMQMessagePush(req);
}

/** Send a worker aborting message upon worker exiting. Upon getting this, the master shutdown the sessions by killing all other workers
 * @param workerabort worker aboring message (include the reason why it is aborting)
 * @return NULL
 */
void MasterClient::WorkerAborting(const WorkerAbortRequest& workerabort) {
  MasterRequest req;
  req.set_type(MasterRequest::WORKERABORT);
  req.mutable_workerabort()->CopyFrom(workerabort);
  SendZMQMessagePush(req);
}
// NOTE: Right now this does a simple blocking call using a thread-local
// socket. We could try other ZMQ techniques like queues/routers here.
// void MasterClient::SendZMQMessage(const MasterRequest& req, Response* res) {
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
