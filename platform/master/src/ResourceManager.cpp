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

#include "PrestoMaster.h"
#include "ResourceManager.h"
using namespace boost;
using namespace google::protobuf;
namespace presto {
ResourceManager::ResourceManager
  (Scheduler* scheduler, PrestoMaster* pm)
  : scheduler_(scheduler), pm_(pm) {
  hello_reply_flag_ =
    (1 << SYS_MEM_USED) |
    (1 << SERVER_LOCATION);
  is_interrupted = false;
  pm_->SetResMgrInterrupt(&is_interrupted);
}

ResourceManager::~ResourceManager() {
}

/** A function that performs monitoring.
 * IT periodically sends HELLO message to workers every WORKER_HEARTBEAT_PERIOD
 * If no response is received within WORKER_DEAD_THRESHOLD
 * It shutsdown all the session
 */
void ResourceManager::Run() {
  try {
    while (true) {
      // sleep for WORKER_HEARTBEAT_PERIOD to check workers again
      // Resource manager is initiated as a seperate thread,
      // and it does not hurt the main thread
      // The sleep time has 5 seconds more than
      // WORKER_HEARTBEAT_PERIOD considering RTT

      // if interrupt is enabled (a master thread can interrupt this_thread)
      // wait for WORKER_HEARTBEAT_PERIOD seconds for the master to interrupt it
      if ( boost::this_thread::interruption_enabled() == true) {
        // in boost 1.50, sleep_for() is not interruptible, while sleep() is
        // from boost 1.56, sleep() function is deprecated
#ifdef BOOST_54
        boost::this_thread::sleep_for(boost::chrono::seconds(WORKER_HEARTBEAT_PERIOD));
#else
        boost::this_thread::sleep(boost::posix_time::milliseconds(1000 * WORKER_HEARTBEAT_PERIOD));
#endif
      } else {
        // if interrupt is not enabled (R-studio), wait for WORKER_HEARTBEAT_PERIOD seconds
        // while chekcing a flag (is_interrupted) that is set by a master thread and exit if interrupted
        for (int i = 0; i< WORKER_HEARTBEAT_PERIOD; ++i) {
#ifdef BOOST_54
          boost::this_thread::sleep_for(boost::chrono::seconds(1));
#else
          boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
#endif
          if (is_interrupted == true) {
            LOG_INFO("Resource manager thread - interrupt is disabled and exits the thread");
            return;
          }
        }
      }
      boost::unordered_map<std::string, Worker*> workers = scheduler_->GetWorkerInfo();
      boost::unordered_map<std::string, Worker*>::iterator wit;
      for (wit = workers.begin(); wit != workers.end(); ++wit) {
        boost::this_thread::interruption_point();
        // Check the time of no contact to detect if it is dead
        if (CheckIfDead(wit->second)) {
          ostringstream msg;
          msg << "Cannot connect to a worker " << wit->first 
            << endl << "The worker might be down." << endl << 
            "Otherwise, check the firewall setup in the worker if the port number range in the configuration file is allowed for incoming traffic.";
          boost::unordered_map<std::string, Worker*>::iterator wwit;
          for (wwit = workers.begin(); wwit != workers.end(); ++wwit) {
            LOG_ERROR("ResourceManager shutting down - %s age is %d\n", wwit->second->server.name().c_str(), wwit->second->last_contacted.age());
          }
          ShutDown(msg.str());
          return;
        } else {
          LOG_DEBUG("ResourceManager: Sending hello message to %s", wit->first.c_str());
          SendHello(wit->second->workerinfo);
        }
      }
    }
  } catch(boost::thread_interrupted const& ) {
    LOG_INFO("Resource manager interrupted");
  }
}

/** Send hello message to a target worker
 * @param worker a worker to send a hello message to
 * @return NULL
 */
void ResourceManager::SendHello(WorkerInfo* worker) {
  // Send a hello message
  HelloRequest hello;
  hello.set_is_heartbeat(true);
  hello.set_reply_attr_flag(hello_reply_flag_);
  hello.set_num_workers(scheduler_->GetWorkerInfo().size());
  ServerInfo wi;
  wi.set_name(worker->hostname());
  wi.set_presto_port(worker->port());
  hello.mutable_worker_location()->CopyFrom(wi);
  worker->Hello(hello);
}

/** Shutdowns a session - This is called when any worker is detected to be dead
 * @param msg a message to be shown to an user
 */
void ResourceManager::ShutDown(string msg) {
  // Make this call as async for this thread to be joined
  fprintf(stderr, "%s\n", msg.c_str());
  LOG_WARN(msg.c_str());
  LOG_ERROR("distributedR will shutdown");
  thread thr(boost::bind(&PrestoMaster::Shutdown, pm_));
  thr.detach();
}

/** This sets the required reply field attribute. For attribute definition,
   * Refer to HelloReplyFlag
   * @param flag a flag value to set (This should be set bit-wise)
   * @return NULL
   */
void ResourceManager::SetReplyAttrFlag(int flag) {
  hello_reply_flag_ = flag;
}

/** This checks if a worker is contacted within the past WORKER_HEARTBEAT_PERIOD
 * If a worker is contacted, we skip to send message
 * Otherwise, we send Hello message to check if a worker is alive
 * @param worker a worker object to be checked if it were contacted
 */
bool ResourceManager::CheckIfContacted(Worker* worker) {
  return ((worker->last_contacted.age()/1e6) < WORKER_HEARTBEAT_PERIOD);
}

/** To check if a worker is dead.
 * A worker is deemed to be dead if it were not contacted in WORKER_DEAD_THRESHOLD
 * @param worker a worker object to be checked if it were dead
 */
bool ResourceManager::CheckIfDead(Worker* worker) {
  return ((worker->last_contacted.age()/1e6) > WORKER_DEAD_THRESHOLD);
}
}
