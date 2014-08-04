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
 * Class that handles a request from a Worker to Master
 * This class is thread-safe as multiple worker threads invoke the Run
 * function.
 */ 

#ifndef _PRESTO_MASTER_HANDLER_
#define _PRESTO_MASTER_HANDLER_

#include <zmq.hpp>
#include "master.pb.h"
#include "shared.pb.h"

#include "common.h"
#include "dLogger.h"

using namespace std;
using namespace zmq;

namespace presto {

class DistributedObjectMap;
class Scheduler;
class PrestoMaster;

class PrestoMasterHandler {
 public:
  PrestoMasterHandler(DistributedObjectMap* darray_map, Scheduler *scheduler,
                      PrestoMaster *presto_master);
  ~PrestoMasterHandler();

  void Run(context_t* ctx, int port_start, int port_end);
 private:
  bool NewUpdate(NewUpdateRequest update);
  bool HandleTaskDone(TaskDoneRequest done);
  bool ValidateUpdates(TaskDoneRequest* req);
  bool UpdateFlexObjectSizes(std::set<string> names);

  DistributedObjectMap* darray_map_;
  Scheduler* scheduler_;
  PrestoMaster *presto_master_;
};

}  // namespace

#endif  // _PRESTO_MASTER_HANDLER_
