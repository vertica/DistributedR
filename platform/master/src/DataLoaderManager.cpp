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

/**
 * Functions to perform operations on Distributed Arrays
 */

#include <time.h>
#include <boost/thread/locks.hpp>
#include <boost/bind.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

#include <string>
#include <vector>
#include <signal.h>
#include <map>

#include <Rcpp.h>
#include "DataLoaderManager.h"

using namespace boost;
namespace presto {

// keeping original R SIGINT hanlder
extern sighandler_t r_sigint_handler;

// a function that is customized for Presto SIGINT handler
extern "C" void m_sigint_handler(int sig);


std::string resolve_hostname(std::string worker_hostname) {
  std::string real_hostname;
  if(worker_hostname == "127.0.0.1" || worker_hostname == "localhost") {
    char hostname[1024];
    hostname[1023] = '\0';
    gethostname(hostname, 1023);
    real_hostname = string(hostname);
  } else {
    real_hostname = worker_hostname;
  }

  return real_hostname;
}


/**
 * Return total number of partitions generated from Data-Loader
 */
SEXP DataLoaderManager::getNPartitions() {
  SEXP npartitions;
  PROTECT(npartitions = Rf_allocVector(INTSXP, 1));
  INTEGER(npartitions)[0] = totalPartitions;
  UNPROTECT(1);        
  return npartitions;
}


/**
 * Return file_ids on all workers
 */
SEXP DataLoaderManager::getFileIDs() {
  vector<int32_t> file_ids;
  for(map<int32_t, int32_t>::iterator itr = file_id_map_.begin(); itr != file_id_map_.end(); ++itr) {
    for(int j = 0; j < itr->second; j++) {
      file_ids.push_back(itr->first);
    }        
  }   

  SEXP file_ids_sexp;
  PROTECT(file_ids_sexp = Rf_allocVector(INTSXP, file_ids.size()));
  for(int i = 0; i < file_ids.size(); i++) 
    INTEGER(file_ids_sexp)[i] = file_ids[i];
  UNPROTECT(1);
  return file_ids_sexp;
}


/**
 * Update Data-Loaders ports opened in each worker
 */
void DataLoaderManager::HandlePortAssignment(WorkerInfo* worker, int loader_port) {
  unique_lock<mutex> lock(loader_mutex);
  if(loader_port == -1) {
    LOG_ERROR("<DataLoader> Could not open port on Worker %s. Check its worker log for more information", worker->hostname().c_str());
    invalid_ports = true;
  }
  std::string worker_hostname = worker->hostname();
  worker_port_info[worker] = loader_port;
  LoaderSemaPost();
  lock.unlock();  
}


/**
 * Handle Data-Loader results from each worker
 * - Update total number of partitions
 * - Update file_ids generated per worker
 */
void DataLoaderManager::WorkerLoaderComplete(WorkerInfo* workerinfo, ::int64_t npartitions, 
                                             int transfer_success, std::string transfer_message) {
  unique_lock<mutex> lock(loader_mutex);
  LOG_DEBUG("<DataLoader> Worker %s : Execution successful: %s", workerinfo->get_hostname_port_key().c_str(), transfer_success ? "true":"false");

  if(!(bool)transfer_success) {
    transfer_success_ = (bool)transfer_success;
    std::ostringstream error_msg;
    error_msg << exception_prefix << " in Worker " << workerinfo->get_hostname_port_key() << ": "
              << transfer_message;
    transfer_error_message_ = error_msg.str();
  }
     
  totalPartitions += npartitions;
  
  for(int i = 0; i < npartitions; i++) {
    if(file_id_map_.find(i+1) == file_id_map_.end())
      file_id_map_[i+1] = 1;
    else 
      file_id_map_[i+1]++; 
  }
  
  if (npartitions > 0)  {
    partitions_worker_id_map_[workerinfo->getID()] = npartitions;
    loader_workers_.push_back(workerinfo->getID());
  }

  LoaderSemaPost();
  lock.unlock();
}


/**
 * Returns DataLoader UDx Parameter 'DR_worker_info' for local
 * nodes-to-nodes data transfer from Vertica to distributedR
 * Format <vertica1-nodename>:<worker1-IP>:<worker1-port>|...|<vertica2-nodename>:<workern-IP>:<worker1-port>
 * @param table_metadata_ vertica nodes that contain the table data
 */
SEXP DataLoaderManager::GetLoaderParameterLocal(SEXP table_metadata_) {
  BEGIN_RCPP;
  Rcpp::DataFrame table_metadata = Rcpp::DataFrame(table_metadata_);
  Rcpp::CharacterVector vnode_name_sexp = table_metadata["node_name"];
  vector<std::string> vnode_name = vector<std::string>(vnode_name_sexp.begin(), vnode_name_sexp.end());

  bool success = true;
  std::string error_code="";
  ostringstream param_str;
  param_str.str(std::string());

  SEXP param_str_sexp, error_code_sexp;
  PROTECT(param_str_sexp = Rf_allocVector(STRSXP, 1));
  PROTECT(error_code_sexp = Rf_allocVector(STRSXP, 1));

  if(vnode_name.size() != worker_port_info.size()) {
    success = false;
    SET_STRING_ELT(param_str_sexp, 0, Rf_mkChar("Unequal number of Vertica nodes and distributedR worker"));
    SET_STRING_ELT(error_code_sexp, 0, Rf_mkChar("ERR01"));
  }
  else {
    int i = 0;
    for(map<WorkerInfo*, int>::iterator itr = worker_port_info.begin(); itr != worker_port_info.end(); ++itr) {
      std::string worker_hostname = resolve_hostname(itr->first->hostname());
      if(itr->second < 0) {
        success = false;
        param_str.str(std::string());
        param_str << "No ports open for loading data from Vertica in Worker " << worker_hostname;
        LOG_ERROR("<DataLoader> %s", param_str.str().c_str());
        error_code = "ERR02";
        break;
      } else {
        param_str << vnode_name[i] << ":" << worker_hostname << ":" << itr->second;
        if(i < worker_port_info.size()-1)
          param_str << "|";
      }
      i++;
    }
    SET_STRING_ELT(param_str_sexp, 0, Rf_mkChar(param_str.str().c_str()));
    SET_STRING_ELT(error_code_sexp, 0, Rf_mkChar(error_code.c_str()));
  }

  UNPROTECT(2);

  return Rcpp::List::create(Rcpp::Named("success") = success,
                            Rcpp::Named("parameter_str") = param_str_sexp,
                            Rcpp::Named("error_code") = error_code_sexp);

  END_RCPP;
}


/**
 * Returns DataLoader UDx Parameter 'DR_worker_info' for uniform 
 * all nodes-to-all nodes data transfer from Vertica to distributedR
 * Format <worker1-IP>:<worker1-port>|<worker2-IP>:<worker1-port>|...|<workern-IP>:<worker1-port>
 */
SEXP DataLoaderManager::GetLoaderParameterUniform() {

  BEGIN_RCPP;
  SEXP param_str_sexp;
  PROTECT(param_str_sexp = Rf_allocVector(STRSXP, 1));

  bool success = true;
  ostringstream param_str;
  param_str.str(std::string());
  int i = 0;

  for(map<WorkerInfo*, int>::iterator itr = worker_port_info.begin(); itr != worker_port_info.end(); ++itr) {
    std::string worker_hostname = resolve_hostname(itr->first->hostname());
    if(itr->second < 0) {
      success = false;
      param_str.str(std::string());
      param_str << "No ports open for loading data from Vertica in Worker " << worker_hostname;
      LOG_ERROR("<DataLoader> %s", param_str.str().c_str());
      break;
    } else {
      param_str << worker_hostname << ":" << itr->second;
      if(i < worker_port_info.size()-1)
        param_str << "|";
    } 
    i++;
  }
  SET_STRING_ELT(param_str_sexp, 0, Rf_mkChar(param_str.str().c_str()));

  UNPROTECT(1);
  
  return Rcpp::List::create(Rcpp::Named("success") = success,
                            Rcpp::Named("parameter_str") = param_str_sexp);

  END_RCPP;  
}


/**
 * Fetches Data-Loader results, #partitions created & file_ids generated, from each workers 
 * Returns results to user
 * @param UDx_result output of Data-Loader UDx query
 * @return List containing - #partitions created & file_ids generated
 */

SEXP DataLoaderManager::FetchLoaderStatus(SEXP UDx_result_) {
  BEGIN_RCPP;
  
  map<std::string, std::vector<std::string> > load_status;
  load_status.clear();
  
  Rcpp::DataFrame UDx_result = Rcpp::DataFrame(UDx_result_);
  Rcpp::CharacterVector workers_served_sexp = UDx_result["DRWorker_EOF"];
  Rcpp::CharacterVector vnode_names_sexp = UDx_result["VerticaNodeName"];
  vector<std::string> workers_served = vector<std::string>(workers_served_sexp.begin(), workers_served_sexp.end());
  vector<std::string> vnode_names = vector<std::string>(vnode_names_sexp.begin(), vnode_names_sexp.end());

  for(int i = 0; i< workers_served.size(); i++) {
    std::string worker_hostname = resolve_hostname(workers_served[i]);
    if(load_status.find(worker_hostname)==load_status.end()) {
      std::vector<std::string> vnodes;
      vnodes.push_back(vnode_names[i]);
      load_status[worker_hostname] = vnodes;
    }
    else {
      std::vector<std::string> vnodes = load_status[worker_hostname];
      vnodes.push_back(vnode_names[i]);
      load_status[worker_hostname] = vnodes;
    }
  }

  for(map<WorkerInfo*, int>::iterator itr = worker_port_info.begin(); itr != worker_port_info.end(); ++itr) { 
    std::string worker_hostname = resolve_hostname(itr->first->hostname());
    presto_master_->GetScheduler()-> FetchLoaderStatus(itr->first, load_status[worker_hostname]);
  }

  for(int i = 0; i<worker_port_info.size(); i++) 
    LoaderSemaWait();

  if(!transfer_success_) {
    LOG_ERROR(transfer_error_message_.c_str());
    forward_exception_to_r(PrestoWarningException(transfer_error_message_.c_str()));
  }

  Rcpp::NumericVector totalsize_sexp = UDx_result["NROWS"];
  vector< ::int64_t> totalsize = vector< ::int64_t>(totalsize_sexp.begin(), totalsize_sexp.end());
  ::int64_t nrows = 0;
  for (uint i = 0; i < totalsize.size(); i++)
      nrows += totalsize[i];

  LOG_INFO("<DataLoader> Transfer of dataset(total %d rows) from Vertica complete. " 
            "Conversion to distributed objects in progress.", nrows);

  return Rcpp::List::create(Rcpp::Named("npartitions") = getNPartitions(),
                            Rcpp::Named("file_ids") = getFileIDs());

  END_RCPP;
}


/** 
 * DataLoaderManager Destructor
 */
DataLoaderManager::~DataLoaderManager() {
  partitions_worker_id_map_.clear();
  loader_workers_.clear();
  file_id_map_.clear();
  worker_port_info.clear();

  // Clearing semaphores
  for(int i = 0; i<presto_master_->NumClients(); i++)
    LoaderSemaPost();
}

}  // namespace presto

