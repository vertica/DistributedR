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

#include <Rcpp.h>
#include <Rinternals.h>
#include <cmath>    // std::sqrt

using namespace std;

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_j = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define NA_IN_R (-2147483648)
#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = Rf_install(#symbol); \
  }

// used in platform/executor/R/executor.R
/*
** This function performs a multiplication of vector/matrix and sparse matrix ("dgCMatrix" format)
** vec: the input vector or matrix. The number of column should be the same as the number of row of mx.
** mx: the input sparse matrix
** size: the number of rows of vec (it should be 1 when vec is a vector)
*/
RcppExport SEXP spvm(SEXP vec, SEXP mx, SEXP size) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int x = INTEGER(dim)[0];
  int y = INTEGER(dim)[1];
  int z = INTEGER(size)[0];

  double *v = REAL(vec);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  int *i_vec = INTEGER(Rf_getAttrib(mx, RSymbol_i));
  int *p_vec = INTEGER(Rf_getAttrib(mx, RSymbol_p));

  SEXP result;
  PROTECT(result = Rf_allocMatrix(REALSXP, z, y));
  double *res = REAL(result);
  memset(res, 0, z*y*sizeof(res[0]));

  if (z > 1) {
    for (int i = 0; i < y; i++) {
      for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
        for (int k = 0; k < z; k++) {
          res[i*z+k] += x_vec[j] * v[i_vec[j]*z+k];
        }
      }
    }
  } else {
    for (int i = 0; i < y; i++) {
      for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
        res[i] += x_vec[j] * v[i_vec[j]];
      }
    }
  }

  UNPROTECT(1);
  return result;
  END_RCPP
}

// used in platform/executor/R/executor.R
/*
** This function performs a multiplication of sparse matix ("dgCMatrix" format) and vector/dense matrix
** mx: the input sparse matrix. The number of column should be the same as the number of row of vec
** vec: the input vector/dense matrix
** size: the number of columns of vec (it should be 1 when vec is a vector)
*/
RcppExport SEXP spmv(SEXP mx, SEXP vec, SEXP size) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int x = INTEGER(dim)[0];
  int y = INTEGER(dim)[1];
  int z = INTEGER(size)[0];

  double *v = REAL(vec);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  int *i_vec = INTEGER(Rf_getAttrib(mx, RSymbol_i));
  int *p_vec = INTEGER(Rf_getAttrib(mx, RSymbol_p));

  SEXP result;
  if (z == 1) {
    PROTECT(result = Rf_allocVector(REALSXP, x));
    double *res = REAL(result);
    memset(res, 0, x*sizeof(res[0]));

    for (int i = 0; i < y; i++) {
      for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
        res[i_vec[j]] += x_vec[j] * v[i];
      }
    }
  } else {
    PROTECT(result = Rf_allocMatrix(REALSXP, x, z));
    double *res = REAL(result);
    memset(res, 0, x*z*sizeof(res[0]));

    for (int i = 0; i < y; i++) {
      for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
        for (int k = 0; k < z; k++) {
          res[k*x+i_vec[j]] += x_vec[j] * v[k*y+i];
        }
      }
    }
  }

  UNPROTECT(1);
  return result;
  END_RCPP
}

// used in algorithms/HPdgraph/R/hpdpagerank.R
/*
** rowSum function for a sparse matrix
** mx: the sparse matrix
*/
RcppExport SEXP rowSums(SEXP mx) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(x);

  int *dim = INTEGER(Rf_getAttrib(mx, RSymbol_Dim));

  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  int *i_vec = INTEGER(Rf_getAttrib(mx, RSymbol_i));

  SEXP result;
  PROTECT(result = Rf_allocVector(REALSXP, dim[0]));
  double *res = REAL(result);
  memset(res, 0, dim[0]*sizeof(res[0]));

  int nnz = Rf_length(Rf_getAttrib(mx, RSymbol_i));
  for (int i = 0; i < nnz; i++) {
    res[i_vec[i]] += x_vec[i];
  }

  UNPROTECT(1);
  return result;
  END_RCPP
}

// used in algorithms/HPdata/R/graphLoader.R
/*
** Creating a sparse matrix
** iIndex: the row indices
** jIndex: the col indices
** xValue: the vector of x values
** d: dimensions of the matrix
*/
RcppExport SEXP hpdsparseMatrix(SEXP iIndex, SEXP jIndex, SEXP xValue, SEXP d) {
  BEGIN_RCPP

  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  Rcpp::IntegerVector i(iIndex);
  Rcpp::IntegerVector j(jIndex);
  Rcpp::NumericVector x(xValue);
  Rcpp::IntegerVector dims(d);

  if (i.size() != j.size())
    Rcpp::stop("iIndex and jIndex must be the same size vectors");

  vector<pair<pair<int, int>, double> > triplets;
  if(x.size() == 1) {
      for (int k=0; k < i.size(); ++k) {
        triplets.push_back(make_pair(make_pair(j[k], i[k]), x[0]));
      }
  } else {
      for (int k=0; k < i.size(); ++k) {
        triplets.push_back(make_pair(make_pair(j[k], i[k]), x[k]));
      }
  }

  sort(triplets.begin(), triplets.end());
  int unique = triplets.empty() ? 0 : 1;
  for (int k = 1; k < triplets.size(); k++) {
    if (triplets[k].first != triplets[k-1].first) {
      unique++;
    }
  }

  Rcpp::IntegerVector i_vec(unique);
  Rcpp::NumericVector x_vec(unique);
  Rcpp::IntegerVector p_vec(dims(1)+1);

  if (!triplets.empty()) {
#define COL(x) (x.first.first)
#define ROW(x) (x.first.second)

    int prev = -1;
    int l = 0;
    int nz = 0;
    for (; l <= COL(triplets[0]); l++)
      p_vec(l) = 0;
    for (int k = 0; k < triplets.size(); k++) {
      if (k > 0 && COL(triplets[k]) != COL(triplets[k-1])) {
        for (l = COL(triplets[k-1]) + 1; l <= COL(triplets[k]); l++) {
          p_vec(l) = nz;
          prev = -1;
        }
      }
      if (ROW(triplets[k]) == prev) {
        x_vec(nz) += triplets[k].second;
      } else {
        x_vec(nz) = triplets[k].second;
        i_vec(nz) = ROW(triplets[k]);
        prev = i_vec(nz);
        nz++;
      }
    }
    for (; l < p_vec.size(); l++)
      p_vec(l) = nz;

#undef COL
#undef ROW
  } else {
    for (int k =  0; k < dims(1)+1; k++) {
      p_vec(k) = 0;
    }
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP mx;
  PROTECT(mx = Rf_eval(create_spm_call, R_GlobalEnv));

  Rf_setAttrib(mx, RSymbol_Dim, dims);
  Rf_setAttrib(mx, RSymbol_i, i_vec);
  Rf_setAttrib(mx, RSymbol_p, p_vec);
  Rf_setAttrib(mx, RSymbol_x, x_vec);

  UNPROTECT(1);

  return mx;

  END_RCPP
}

