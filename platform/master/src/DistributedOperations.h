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

#ifndef __DISTRIBUTED_OPERATIONS_
#define __DISTRIBUTED_OPERATIONS_

#include <Rcpp.h>
#include <vector>

using namespace std;
using namespace Rcpp;

namespace presto {
    
  RcppExport SEXP DistributedObject_ExecR(SEXP presto_master_exp,
                                         SEXP func_body_exp,
                                         SEXP num_calls_exp,
                                         SEXP arg_names_exp,
                                         SEXP split_names_exp,
                                         SEXP arg_vals_exp,
                                         SEXP list_args_exp,
                                         SEXP raw_names_exp,
                                         SEXP raw_vals_exp,
                                         SEXP wait_exp,
					 SEXP scheduler_policy_exp,		
					 SEXP inputs_sexp,
                                         SEXP progress_sexp,
                                         SEXP trace_sexp);

  RcppExport SEXP DistributedObject_Get(SEXP presto_master_exp,
                                       SEXP split);

  RcppExport SEXP DistributedObject_PrintStats(SEXP presto_master_exp,
                                              SEXP splits);
}  // namespace presto

#endif  // __DISTRIBUTED_OPERATIONS_
