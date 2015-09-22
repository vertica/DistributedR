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

#ifndef ___RESOURCE_MANAGER_H__
#define ___RESOURCE_MANAGER_H__

#include <boost/unordered_map.hpp>
#include "Scheduler.h"
#include "dLogger.h"

namespace presto {

class PrestoMaster;
class ResourceManager {
  public:
    ResourceManager(Scheduler* scheduler, PrestoMaster *pm);
    ~ResourceManager();
    void Run();
    void SendHello(WorkerInfo* worker);

  protected:
    void SetReplyAttrFlag(int flag);
    bool CheckIfContacted(Worker* worker);
    bool CheckIfDead(Worker* worker);
    void ShutDown(string msg);

  private:
    int hello_reply_flag_;
    Scheduler* scheduler_;
    PrestoMaster *pm_;
    volatile bool is_interrupted;
};
}
#endif
