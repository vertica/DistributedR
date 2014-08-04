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

/* Calculated the shortest distance given a weight vector(array)
** by computing omputes w = min(w, A min.+ v)  reference Bellman-Ford algorithm
** value of -1 means infinite distance (no direct path)
*/
RcppExport SEXP mmult_dense_minplus(signed int *weightArray, unsigned int *numRow, unsigned int *numCol, signed int *source, signed int *dest){
  BEGIN_RCPP
  int i,j,array_index;
  for (i=0; i<*numCol; i++){
    for (j=0; j<*numRow; j++){
      array_index = i*(*numRow)+j;
      if(weightArray[array_index]==NA_IN_R || source[j]==NA_IN_R){  //there is no path!
        continue;
      }
      int val = weightArray[array_index] + source[j];
      if (val < dest[i] || dest[i]==NA_IN_R){
        dest[i] = val;
      }
    }
  }
  return R_NilValue;
  END_RCPP
}

/* Calculated the shortest distance given a weight vector expressed using SparseMatrix
** by computing omputes w = min(w, A min.+ v)  reference Bellman-Ford algorithm
** emtry entry or -1 in the source/dest vector means no path!
*/
RcppExport SEXP mmult_sparse_minplus(int *nz, int *row, int *p, int *x, int *source, int *dest){
  BEGIN_RCPP
  int i,col;
  for (col=0; p[col]!=*nz; col++){
    for (i=p[col]; i<p[col+1]; i++){
      if(source[row[i]] ==NA_IN_R || x[i]==NA_IN_R){
        continue;
      }
      int val = x[i] + source[row[i]];
      if (val < dest[col] || dest[col]==NA_IN_R){
        dest[col] = val;
      }
    }
  }
  return R_NilValue;
  END_RCPP
}

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

RcppExport SEXP spmv_triplet(SEXP mx, SEXP vec, SEXP size) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(j);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int x = INTEGER(dim)[0];
  int y = INTEGER(dim)[1];
  int z = INTEGER(size)[0];

  double *v = REAL(vec);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  SEXP i_sexp = Rf_getAttrib(mx, RSymbol_i);
  int *i_vec = INTEGER(i_sexp);
  int *j_vec = INTEGER(Rf_getAttrib(mx, RSymbol_j));
  int nnz = Rf_length(i_sexp);

  SEXP result;
  if (z == 1) {
    PROTECT(result = Rf_allocVector(REALSXP, x));
    double *res = REAL(result);
    memset(res, 0, x*sizeof(res[0]));

    for (int i = 0; i < nnz; i++) {
      res[i_vec[i]] += x_vec[i] * v[j_vec[i]];
    }
  } else {
    PROTECT(result = Rf_allocMatrix(REALSXP, x, z));
    double *res = REAL(result);
    memset(res, 0, x*z*sizeof(res[0]));



    for (int i = 0; i < nnz; i++) {
      for (int k = 0; k < z; k++) {
        res[k*x+i_vec[i]] += x_vec[i] * v[k*y+j_vec[i]];
      }
    }
  }

  UNPROTECT(1);
  return result;
  END_RCPP
}

RcppExport SEXP spvm_triplet(SEXP vec, SEXP mx, SEXP size) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(j);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int x = INTEGER(dim)[0];
  int y = INTEGER(dim)[1];
  int z = INTEGER(size)[0];

  double *v = REAL(vec);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  SEXP i_sexp = Rf_getAttrib(mx, RSymbol_i);
  int *i_vec = INTEGER(i_sexp);
  int *j_vec = INTEGER(Rf_getAttrib(mx, RSymbol_j));
  int nnz = Rf_length(i_sexp);

  SEXP result;
  PROTECT(result = Rf_allocMatrix(REALSXP, z, y));
  double *res = REAL(result);
  memset(res, 0, z*y*sizeof(res[0]));

  if (z > 1) {
    for (int i = 0; i < nnz; i++) {
      for (int k = 0; k < z; k++) {
        res[j_vec[i]*z+k] += x_vec[i] * v[i_vec[i]*z+k];
      }
    }
  } else {
    for (int i = 0; i < nnz; i++) {
      res[j_vec[i]] += x_vec[i] * v[i_vec[i]];
    }
  }

  UNPROTECT(1);
  return result;
  END_RCPP
}

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

/*
**  Calculating K-means clustering based on Lloyd algorithm
**  Rx: the matrix of samples
**  Rcen: the matrix of centers
**  Rcl: the vector of cluster labels
**  Rnc: the number of points in each cluster
*/

RcppExport SEXP hpdkmeans_Lloyd(SEXP Rx, SEXP Rcen, SEXP Rcl, SEXP Rnc)
{
    BEGIN_RCPP
    int n, k, p, crow;
    int i, j, c, it, inew = 0;
    double best, dd, tmp;
    double *x, *cen;
    int *cl, *nc;

    int * dimx = INTEGER(Rf_getAttrib(Rx, R_DimSymbol));
    n = dimx[0]; // number of samples
    p = dimx[1]; // number of predictors
    int *dimc = INTEGER(Rf_getAttrib(Rcen, R_DimSymbol));
    k = dimc[0]; // number of centers
    crow = dimc[1];

    Rx = Rf_coerceVector(Rx, REALSXP);
    Rcen = Rf_coerceVector(Rcen, REALSXP);
    Rcl = Rf_coerceVector(Rcl, INTSXP);
    Rnc = Rf_coerceVector(Rnc, INTSXP);

    x = REAL(Rx);
    cen = REAL(Rcen);
    cl = INTEGER(Rcl);
    nc = INTEGER(Rnc);

    if( p != crow )
        Rf_error("Wrong dimention of matrices");
    if( Rf_length(Rcl) != n )
        Rf_error("wrong dimention of cl");
    if( Rf_length(Rnc) != k )
        Rf_error("wrong dimention of nc");

    // clear all the cluster labels
    for(i = 0; i < n; i++) cl[i] = -1;
    for(i = 0; i < n; i++) {
        // find nearest centre for each point 
        best = R_PosInf;
        for(j = 0; j < k; j++) {
	        dd = 0.0;
	        for(c = 0; c < p; c++) {
	            tmp = x[i+n*c] - cen[j+k*c];
	            dd += tmp * tmp;
	        }
	        if(dd < best) {
	            best = dd;
	            inew = j+1;
	        }
        }
        if(cl[i] != inew) {
               cl[i] = inew;
        }
    }
    // update each centre
    for(j = 0; j < k*p; j++) cen[j] = 0.0;
    for(j = 0; j < k; j++) nc[j] = 0;
    for(i = 0; i < n; i++) {
        it = cl[i] - 1; nc[it]++;
        for(c = 0; c < p; c++) cen[it+c*k] += x[i+c*n];
    }
    for(j = 0; j < k*p; j++) cen[j] /= nc[j % k];

    return R_NilValue;
    END_RCPP
}

/*
** The calculation in each iteration of pagerank when the graph (mx) is sparse
** newPR: the new pagerank vector
** PR: the old pagerank vector
** mx: a split of the graph
** TP: the number of outgoing edges of each vertex
** damping: damping factor
** personalized: personalized vector
** weights: matrix of weights
*/
RcppExport SEXP pagerank_spvm(SEXP newPR, SEXP PR, SEXP mx, SEXP TP, SEXP damping, SEXP personalized, SEXP weights) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int nVertices = INTEGER(dim)[0]; // the splits are column-wise partitioned, so #rows == nVertices
  int y = INTEGER(dim)[1];

  int *dim_newpr = INTEGER(Rf_getAttrib(newPR, R_DimSymbol));
  if (dim_newpr[0] != 1 || dim_newpr[1] != y)
    Rcpp::stop("The dimensions of newPR are not correct");
  int *dim_pr = INTEGER(Rf_getAttrib(PR, R_DimSymbol));
  if (dim_pr[0] != 1 || dim_pr[1] != nVertices)
    Rcpp::stop("The dimensions of PR are not correct");
  int *dim_tp = INTEGER(Rf_getAttrib(TP, R_DimSymbol));
  if (dim_tp[0] != nVertices || dim_tp[1] != 1)
    Rcpp::stop("The dimensions of TP are not correct");

  bool no_Per = Rf_isNull(personalized);
  double *personalized_vec;
  if (! no_Per) {
    int *dim_persona = INTEGER(Rf_getAttrib(personalized, R_DimSymbol));
    if (dim_persona[0] != 1 || dim_persona[1] != y)
      Rcpp::stop("The dimensions of personalized are not correct");
    personalized_vec = REAL(personalized); // personlized vector
  }

  bool no_weight = Rf_isNull(weights);
  double *weights_vec;
  int *iw_vec;
  int *pw_vec;
  if (! no_weight) {
    int *dim_weights = INTEGER(Rf_getAttrib(weights, RSymbol_Dim));
    if (dim_weights[0] != nVertices || dim_weights[1] != y)
      Rcpp::stop("The dimensions of weights are not correct");
    weights_vec = REAL(Rf_getAttrib(weights, RSymbol_x)); // weights matrix
    iw_vec = INTEGER(Rf_getAttrib(weights, RSymbol_i));
    pw_vec = INTEGER(Rf_getAttrib(weights, RSymbol_p));
  }

  double damp = Rcpp::as<double>(damping);

  double *v1 = REAL(PR);
  double *v2 = REAL(newPR);
  double *tp = REAL(TP);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  int *i_vec = INTEGER(Rf_getAttrib(mx, RSymbol_i));
  int *p_vec = INTEGER(Rf_getAttrib(mx, RSymbol_p));
  double tempRes;

  for (int i = 0; i < y; i++) {
    v2[i] = 0;
    for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
      tempRes = x_vec[j] * v1[i_vec[j]];

      if (! no_weight) {
        bool found = false;
        // to find the corresponding element of weights
        for (int jw = pw_vec[i]; jw < pw_vec[i+1]; jw++) {
            if (iw_vec[jw] == i_vec[j]) {
                tempRes *= weights_vec[jw];
                found = true;
                break;
            }
        }
        if (! found)
            tempRes = 0;
      }

      if (tp[i_vec[j]] != 0)
        tempRes /= tp[i_vec[j]];

      v2[i] +=  tempRes ;
    }
    v2[i] *= damp;
    if (no_Per) {
      v2[i] += ((1 - damp) / nVertices);
    } else {
      v2[i] += ((1 - damp) * personalized_vec[i]);
    }
  }
  return R_NilValue;
  END_RCPP
}

/*
** The calculation in each iteration of pagerank when the graph (mx) is dense
** newPR: the new pagerank vector
** PR: the old pagerank vector
** mx: a split of the graph
** TP: the number of outgoing edges of each vertex
** damping: damping factor
** personalized: personalized vector
** weights: matrix of weights
*/
RcppExport SEXP pagerank_vm(SEXP newPR, SEXP PR, SEXP mx, SEXP TP, SEXP damping, SEXP personalized, SEXP weights) {
  BEGIN_RCPP

  SEXP dim = Rf_getAttrib(mx, R_DimSymbol);
  int nVertices = INTEGER(dim)[0]; // the splits are column-wise partitioned, so #rows == nVertices
  int y = INTEGER(dim)[1];

  int *dim_newpr = INTEGER(Rf_getAttrib(newPR, R_DimSymbol));
  if (dim_newpr[0] != 1 || dim_newpr[1] != y)
    Rcpp::stop("The dimensions of newPR are not correct");
  int *dim_pr = INTEGER(Rf_getAttrib(PR, R_DimSymbol));
  if (dim_pr[0] != 1 || dim_pr[1] != nVertices)
    Rcpp::stop("The dimensions of PR are not correct");
  int *dim_tp = INTEGER(Rf_getAttrib(TP, R_DimSymbol));
  if (dim_tp[0] != nVertices || dim_tp[1] != 1)
    Rcpp::stop("The dimensions of TP are not correct");

  bool no_Per = Rf_isNull(personalized);
  double *personalized_vec;
  if (! no_Per) {
    int *dim_persona = INTEGER(Rf_getAttrib(personalized, R_DimSymbol));
    if (dim_persona[0] != 1 || dim_persona[1] != y)
      Rcpp::stop("The dimensions of personalized are not correct");
    personalized_vec = REAL(personalized); // personlized vector
  }

  bool no_weight = Rf_isNull(weights);
  double *weights_mx;
  if (! no_weight) {
    int *dim_weights = INTEGER(Rf_getAttrib(weights, R_DimSymbol));
    if (dim_weights[0] != nVertices || dim_weights[1] != y)
      Rcpp::stop("The dimensions of weights are not correct");
    weights_mx = REAL(weights); // weights matrix
  }

  double damp = Rcpp::as<double>(damping);
  double *v1 = REAL(PR);
  double *v2 = REAL(newPR);
  double *tp = REAL(TP);
  double *x_vec = REAL(mx);
  double tempRes;

  for (int i = 0; i < y; i++) {
    v2[i] = 0;
    for (int j = 0; j < nVertices; j++) {
      int k = j+ i * nVertices;
      tempRes = x_vec[k] * v1[j];

      if (! no_weight)
        tempRes *= weights_mx[k];
      if (tp[j] != 0)
        tempRes /= tp[j];

      v2[i] +=  tempRes ;
    }
    v2[i] *= damp;
    if (no_Per) {
      v2[i] += ((1 - damp) / nVertices);
    } else {
      v2[i] += ((1 - damp) * personalized_vec[i]);
    }

  }
  return R_NilValue;
  END_RCPP
}

/*
** Creating a sparse matrix with elemnts x=1
** iIndex: the row indices
** jIndex: the col indices
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

