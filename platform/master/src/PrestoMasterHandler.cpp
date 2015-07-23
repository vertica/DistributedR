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

#include <string>
#include <exception>

#include "DistributedObject.h"
#include "DistributedObjectMap.h"
#include "Scheduler.h"
#include "PrestoMaster.h"

#include "PrestoMasterHandler.h"
#include "common.h"
#include "PrestoException.h"

using namespace boost;
namespace presto {

PrestoMasterHandler::PrestoMasterHandler(
    DistributedObjectMap* darray_map,
    Scheduler *scheduler,
    PrestoMaster *presto_master) :
  darray_map_(darray_map), scheduler_(scheduler),
  presto_master_(presto_master) {
}

PrestoMasterHandler::~PrestoMasterHandler() {
}

void PrestoMasterHandler::Run(context_t* ctx, int port_start, int port_end) {  
  bool worker_abort_called = false;
  int port_num = -1;
  socket_t* sock;
  bool bind_succeed = true;
  try {
    sock = CreateBindedSocket(port_start, port_end, ctx, &port_num);
  } catch (...) {
    bind_succeed = false;
  }
  presto_master_->SetMasterPortNum(port_num);
  presto_master_->WorkerInfoSemaPost();

  if (bind_succeed == false) {
    fprintf(stderr, "MasterHandler socket bind error. Check port number: %d\n", port_num);
    return;
  }
  MasterRequest master_req;  
  try {
    while (true) {
      message_t request;
      boost::this_thread::interruption_point();  // check if interrupted
      sock->recv(&request);
      boost::this_thread::interruption_point();  // check if interrupted
      master_req.Clear();
      master_req.ParseFromArray(request.data(), request.size());
      switch (master_req.type()) {
        case MasterRequest::SHUTDOWN:
          LOG_INFO("PrestoMasterHanlder Shutdown is called");          
          delete sock;
          return;
        case MasterRequest::HELLOREPLY:
          {
            thread thr(boost::bind(&PrestoMaster::HandleHelloReply,
                                   presto_master_,
                                   master_req.helloreply()));
          }
          break;
        case MasterRequest::NEWUPDATE:
          {
            NewUpdateRequest nur = master_req.newupdate();
            if (scheduler_->IsValidWorker(server_to_string(*nur.mutable_location())) == false) {
              break;
            }
#ifdef MULTITHREADED_SCHEDULER
            thread thr(boost::bind(&PrestoMasterHandler::NewUpdate,
                                   this,
                                   master_req.newupdate()));
#else
            NewUpdate(master_req.newupdate());
#endif
          }
          break;
        case MasterRequest::TASKDONE:
          {      
            TaskDoneRequest tdr = master_req.taskdone();
            if (scheduler_->IsValidWorker(server_to_string(*tdr.mutable_location())) == false) {
              break;
            }
            
#ifdef MULTITHREADED_SCHEDULER
            thread thr(boost::bind(
                                   &PrestoMasterHandler::HandleTaskDone,
                                   this,
                                   master_req.taskdone()));
#else
            bool ret = HandleTaskDone(master_req.taskdone());
            // If an error happens during HandlingTaskDone message, shutdown the whole session
            if (ret == false) {
	      LOG_ERROR("Error during processing task at worker (HandleTaskDone). Shutting down.");
	      fprintf(stderr, 
                    "Error while processing task at worker."
		      " We will shutdown the whole session. Use Ctrl-C to return to R console.\n");
              thread thr(boost::bind(&PrestoMaster::Shutdown,
                                     presto_master_));
              thr.detach();
              return;
            }
#endif
          }
          break;
        case MasterRequest::WORKERABORT:
          {
            WorkerAbortRequest war = master_req.workerabort();
            if (scheduler_->IsValidWorker(server_to_string(*war.mutable_location())) == false) {
              break;
            }
            // if a shutdown message is already processed,
            // ignore further messages
            if (worker_abort_called == true) {
              break;
            }
            worker_abort_called = true;
            fprintf(stderr, 
                    "\n%sWorker aborted\n%s\n"
                    "We will shutdown the whole session.\n",
                    exception_prefix.c_str(), 
                    master_req.workerabort().reason().c_str());
            ostringstream msg;
            msg << exception_prefix << "Worker aborted. "<< master_req.workerabort().reason().c_str() << "\nWe will shutdown the whole session";
            LOG_INFO(msg.str());
            thread thr(boost::bind(&PrestoMaster::Shutdown,
                                   presto_master_));
            thr.detach();
          }
          break;

        default:
          {
            LOG_ERROR("Unknown message received from Worker. Possible reasons - Check the correctness of the configuration - Either Hostname or Port of the Master and Workers has to be different");
            fprintf(stderr, "Unknown message type received - Possible reasons\n"
            "Check the correctness of the configuration - Either Hostname or Port of the Master and Workers has to be different\n");
	  }
      }
    }
  }catch(boost::thread_interrupted const& ) {
    LOG_INFO("PrestoMasterHandler - interrupted.");
    delete sock;
    return;
  } catch (zmq::error_t err) {
    //LOG_INFO("PrestoMasterHandler - zmq error_t exception received. Teminate MasterHandler");
    delete sock;
    return;
  }
  delete sock;
}

bool PrestoMasterHandler::ValidateUpdates(TaskDoneRequest* req) {
   for (int i = 0; i < req->update_names_size(); i++) {
     int32_t split_id;
     string darray_name;
     ParseSplitName(req->update_names(i), &darray_name, &split_id);

     // Return validation failure if updated split does not exist in Master
     DistributedObject* d = darray_map_->GetDistributedObject(darray_name);
     if (d == NULL) {
        ostringstream msg;
        msg << "dobject \"" << darray_name << "\" not found" << endl;
        scheduler_->TaskErrorMsg(msg.str().c_str());
        return false;
     }   

     // Return validation failure if darray split is not updated with defined size
     Tuple split_dim = d->GetSplitFromId(split_id)->dim();

     if((d->Type()== DARRAY || d->Type() == DFRAME)&& (req->row_dim(i) > 0 && req->col_dim(i) > 0) &&
       (req->row_dim(i) != split_dim.val(0) || req->col_dim(i) != split_dim.val(1))) {
       //For flexible arrays we can modify the split dimensions the first time data is updated (i.e. init dimension is 1x1). So it is not an error.
       //if(!(d->SubType() == FLEX_UNINIT && (split_dim.val(0)==1 && split_dim.val(1)==1))) {
       if(d->SubType() != FLEX_UNINIT) {
	 ostringstream msg;
	 msg << "attempt to set partition (ID:" << (split_id+1) << ") size to (" << req->row_dim(i) << "," << req->col_dim(i)<<
	   "), different from declared size (" << split_dim.val(0) << "," << split_dim.val(1) <<")";
	 scheduler_->TaskErrorMsg(msg.str().c_str());
	 return false;
       }
     }   
   }   
   return true;
}

/**
 * TaskDone messages handler
 *
 * TaskDone messages for EXECUTE Tasks: 
 *  - TaskDone messages are not processed in Master until it received Taskdone messages for all EXECUTE tasks for a foreach, 
 *    i.e. until all foreach EXECUTE tasks are complete in worker. They are buffered in a Map for delayed processing.
 *  - If any of the execute task fails(task result is TASK_EXCEPTION) or split updates have errors, foreach function fails.   
 *    - In such a case, Split information is not updated in Master and new splits created by foreach 
 *      Execute tasks in Worker are deleted. 
 *    - Task exception message is printed out on the R console. User can correct the code and
 *      re-run the foreach function.
 *  - If all foreach execute tasks are successful, foreach function succeeds and split information of all Taskdone messages
 *    are read from Map buffer and updated in Master. 
 *
 * TaskDone message for all other task types such as CREATECOMPOSITE, FETCH etc are processed as soon as they 
 * are received by MasterHandler. 
 *
 **/
bool PrestoMasterHandler::HandleTaskDone(TaskDoneRequest done) {

  TaskDoneRequest* task_done = new TaskDoneRequest;
  task_done->CopyFrom(done);

  std::set<string> updatedObjects;
  int32_t split_id;
  string object_name;
  bool success= true;

  if (scheduler_->IsExecTask(task_done->uid())) {
     LOG_INFO("EXECUTE TaskID %14d - Execution complete in Worker", task_done->uid());
     
     bool validated = ValidateUpdates(task_done);

     // TaskResult is updated for each EXECUTE Task Done. In case of error, Foreach error flag is enabled.
     std::pair<bool, bool> foreach_result = scheduler_->UpdateTaskResult(task_done, validated);

     // foreach_result has 2 result flags
     // foreach_complete: If foreach fn is complete. This is set to true when MasterHandler has 
     // received TaskDone message for all foreach EXECUTE tasks.
     // foreach_error: If foreach fn failed. This is set to true if any of the foreach EXECUTE 
     // tasks returned error.
     bool foreach_complete = foreach_result.first;
     bool foreach_error = foreach_result.second;
     
     if (foreach_complete){
       boost::unordered_map< ::uint64_t, TaskDoneRequest*>::iterator itr = scheduler_->taskdones_.begin();
       int num_tasks = scheduler_->taskdones_.size();   
       int cur_task=1;
       
       for(; itr != scheduler_->taskdones_.end(); ++itr, ++cur_task) {
          TaskDoneRequest* i_done = itr->second;
          if(foreach_error) {
            // In case of foreach failure, new split shms created in Worker are deleted.
            scheduler_->PurgeUpdates(i_done);
          } else {
             // If foreach is successful, splits information is updated in Master.
             for (int j = 0; j < i_done->update_names_size(); j++) {
               if(IsCompositeArray(i_done->update_names(j)) == true)
               continue;

	       //Set of objects (not splits) that were updated.
	       ParseSplitName(i_done->update_names(j), &object_name, &split_id);
	       updatedObjects.insert(object_name);

               NewUpdateRequest req;
               req.set_name(i_done->update_names(j));
               req.set_size(i_done->update_sizes(j));
               req.set_row_dim(i_done->row_dim(j));
               req.set_col_dim(i_done->col_dim(j));
               req.mutable_location()->CopyFrom(i_done->location());
               if (NewUpdate(req) == false) {break; return false;}
               scheduler_->AddSplit(i_done->update_names(j), i_done->update_sizes(j),
                                 i_done->update_stores(j), server_to_string(i_done->location()),
                                 i_done->update_empties(j));
             }

	     /**If this is the last task, let's update sizes of all flexible arrays.
	      **We need to complete the size changes before calling "done" in the scheduler. 
	      **Otherwise the next foreach task can start before sizes get updated.*/
	     if(cur_task == num_tasks){
	       //Now update the dimensions and offsets of all flexible dobjets
	       //(TODO) iR: we currently don't do anything if there were errors while updating (result=false).
	       bool result = UpdateFlexObjectSizes(updatedObjects);
	       if(!result) scheduler_->SetForeachError(true);
	     }
          }
         scheduler_->Done(i_done);
       }
    }
  } else {
    //For non-exec tasks we check if there was an error and propagate it up. Note that if there is an error
    //session shutdown will be enforced. Shutdown is not required if there is an error in foreach 
    //(above case of calling scheduler->done
    success = scheduler_->Done(task_done);
  }
  return success;
}

//Update sizes of flexible dobjects whose sizes changed
bool PrestoMasterHandler::UpdateFlexObjectSizes(std::set<string> names){
  bool result= true;
  for(std::set<string>::iterator it=names.begin(); it != names.end();++it){
    DistributedObject* d = darray_map_->GetDistributedObject(*it);
    if (d == NULL) {
      ostringstream msg;
      msg << "dobject \"" << *it << "\" not found" << endl;
      scheduler_->TaskErrorMsg(msg.str().c_str());
      return false;
    }
    //If this is an empty array that was updated by system make it UNINIT
    //If the user updated it, let's make it a standard object
    if((d->Type() == DARRAY || d->Type() == DFRAME)){
      if(d->SubType() == UNINIT_DECLARED){
	d->setSubType(UNINIT); 
      } else if(d->SubType() == UNINIT){ d->setSubType(STD);} 
    }

    // Return validation failure if darray split is not updated with defined size 
    if((d->Type() == DARRAY || d->Type() == DFRAME)&& (d->isSubTypeFlex())) {
      LOG_DEBUG("Updating split sizes of flex object: %s", (*it).c_str());
      result = d->UpdateDimAndBoundary();
      if(result){
	//If the flexobject was just declared by the user, the system has now allocated it on workers. 
	//Let's change the object to uninitiazed state
	if(d->SubType() == FLEX_DECLARED){
	  d->setSubType(FLEX_UNINIT);
	}else{
	  //If the flexobject was updated by user now, we will not allow future changes to the size of the object
	  //Let's make the object a standard object
	  if(d->SubType() == FLEX_UNINIT) {
	    d->setSubType(STD);
	  }
	}
      }

    }
  }
  return result;
}


bool PrestoMasterHandler::NewUpdate(NewUpdateRequest update) {
   boost::this_thread::interruption_point();// check if interrupted
   int32_t split_id;
   string darray_name;
   ParseSplitName(update.name(), &darray_name, &split_id);

   // Replace the specified array in with the new contents.
   Array arr;
   arr.set_name(update.name());
   arr.set_size(update.size());
   arr.mutable_dim()->Clear();
   if(update.row_dim()==0)
     arr.mutable_dim()->add_val(1);
   else
     arr.mutable_dim()->add_val(update.row_dim());

   if(update.col_dim()==0)
     arr.mutable_dim()->add_val(1);
   else
     arr.mutable_dim()->add_val(update.col_dim());

   // scheduler_->AddSplit(arr.name(), update.size(),
   //                      server_to_string(update.location()));
   DistributedObject* d = darray_map_->GetDistributedObject(
         darray_name);
   d->PutSplitWithId(split_id, arr);
   LOG_DEBUG("New value of split %s updated", darray_name.c_str());
   return true;
}

}  // namespace
