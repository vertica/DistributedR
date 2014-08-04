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

#include <algorithm>
#include <vector>

// NOTE(shivaram): This is a C++ alignment function for Smith Waterman
// that was ultimately not used
RcppExport SEXP DistributedArray_align(SEXP top_left_exp, SEXP top_exp,
    SEXP left_exp, SEXP x_exp, SEXP y_exp, SEXP s_exp) {
  BEGIN_RCPP
  const double match = 2.0;
  const double mismatch = -1.0;
  const double del = -1.0;
  const double insert = -1.0;
  Rcpp::NumericMatrix top_left = top_left_exp;
  Rcpp::NumericMatrix top = top_exp;
  Rcpp::NumericMatrix left = left_exp;

  Rcpp::NumericMatrix x = x_exp;
  Rcpp::NumericMatrix y = y_exp;

  int64_t length = Rcpp::as<int64_t>(s_exp);
  int64_t s = length;

  vector<double> res;
  res.resize(length*length);
  int32_t check = 0;
  for (int64_t i = 0; i < length; ++i) {
    for (int64_t j = 0; j < length; ++j) {
      check = mismatch;
      if (y(0, i) == x(0, j)) {
        check = match;
      }
      if (i == 0 && j == 0) {
        res[(i*length)+j] = max(0.0, max(top_left(s-1, s-1)+check,
              max(top(s-1, 0)+del, left(0, s-1)+insert)));
      } else if (i == 0 && j>0) {
        res[(i*length)+j] = max(0.0, max(top(s-1, j-1)+check,
              max(top(s- 1, j)+del, res[(i*length)+j-1]+insert)));
      } else if (j == 0 && i > 0) {
        res[(i*length)+j] = max(0.0, max(left(i-1, s-1)+check,
              max(res[(i-1)*length+j]+del, left(i, s-1)+insert)));
      } else {
        res[(i*length)+j] = max(0.0, max(res[((i-1)*length)+j-1]+check,
              max(res[(i-1)*length+j]+del, res[(i*length)+j-1]+insert)));
      }
    }
  }
  return Rcpp::wrap(res);
  END_RCPP
}
