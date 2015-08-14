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

#include <iostream>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <Rcpp.h>

namespace presto {

/** Propagates new value to other workers. 
 * This is generally called from Presto R function update(). 
 * It keeps the variable within a global variable updatesptr
 * @param updates_ptr_sexp R variable (updates.ptr...) that keeps track of a name of update object. 
 * This variable shared between R and C++ library using RInside.
 * @param name of a variable in R session - deparsed name in string 
 * @param empty indicates if this variable is empty
 * @return NULL
 */
  RcppExport SEXP NewUpdate(SEXP updates_ptr_sexp, SEXP name_sexp, SEXP empty_sexp, SEXP dim_sexp) {
  BEGIN_RCPP

      using namespace std;

      Rcpp::XPtr<set<tuple<string, bool, std::vector<std::pair<int64_t, int64_t> > > > > updates_ptr_local(updates_ptr_sexp);

      if (updates_ptr_local.get() == NULL) {
        std::cout << "WARNING: NewUpdate => updateptr is NULL" << std::endl; 
//        throw PrestoWarningException("NewUpdate::updatesptr is null");
      }

      Rcpp::CharacterVector name_vec(name_sexp);
      Rcpp::LogicalVector empty_vec(empty_sexp);
      Rcpp::NumericVector dim_vec(dim_sexp);
      typedef Rcpp::CharacterVector::iterator char_itr;
      typedef Rcpp::LogicalVector::iterator log_itr;
      typedef Rcpp::NumericVector::iterator numeric_itr;
      
      char_itr name_itr = name_vec.begin();
      log_itr empty_itr = empty_vec.begin();
      numeric_itr dim_itr = dim_vec.begin();

      std::string name = std::string(name_itr[0]);
      bool empty = empty_vec[0];
      
      if(dim_vec.size() % 2 != 0){
          std::cout << "ERROR: Received dimensions list has an odd number of elements." << std::endl;
//          throw PrestoWarningException("dframe dimensions received had an odd number of elements");
      }
      
      std::vector<pair<int64_t,int64_t>> dimensions;
      
      for(int i=0;i < dim_vec.size(); i+=2){
          dimensions.push_back(std::make_pair<int64_t,int64_t>(dim_vec[i],dim_vec[i+1]));
      }

      (updates_ptr_local.get())->insert(make_tuple(name, empty, dimensions));
 
  return Rcpp::wrap(true);
  END_RCPP
}

}  // namespace presto
