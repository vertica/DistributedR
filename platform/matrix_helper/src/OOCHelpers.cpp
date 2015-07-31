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


#include <Rinternals.h>
#include <Rcpp.h>

using namespace std;

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define NA_IN_R (-2147483648)
#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = Rf_install(#symbol); \
  }

#define PACK(a,b) ((static_cast<uint64_t>(a)<<32) | (b))
#define UNPACK_FIRST(x) (((x)>>32) & 0xFFFFFFFF)
#define UNPACK_SECOND(x) ((x) & 0xFFFFFFFF)

RcppExport SEXP unpack(SEXP a) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP a_x = getAttrib(a, RSymbol_x);
  uint64_t *x = (uint64_t*)REAL(a_x);
  int total_elems = Rf_length(a_x);

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = INTEGER(getAttrib(a, RSymbol_Dim))[0];
  INTEGER(dim)[1] = 1;
  SEXP new_i, new_p, new_x;
  PROTECT(new_p = Rf_allocVector(INTSXP, 2));
  INTEGER(new_p)[0] = 0;
  INTEGER(new_p)[1] = total_elems;

  PROTECT(new_i = Rf_allocVector(INTSXP, total_elems));
  PROTECT(new_x = Rf_allocVector(REALSXP, total_elems));
  int *newi = (int*)INTEGER(new_i);
  double *newx = (double*)REAL(new_x);
  for (int i = 0; i < total_elems; i++) {
    newi[i] = UNPACK_FIRST(x[i]);
    newx[i] = UNPACK_SECOND(x[i]);
  }

  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, new_i);
  Rf_setAttrib(newobj, RSymbol_p, new_p);
  Rf_setAttrib(newobj, RSymbol_x, new_x);

  UNPROTECT(5);
  return newobj;
  END_RCPP
}

// RcppExport SEXP unpack_first(SEXP a) {
//   SEXP ret;
//   int n = Rf_length(a);
//   PROTECT(ret = Rf_allocVector(INTSXP, n));
//   int *r = (int*)INTEGER(ret);
//   uint64_t *x = (uint64_t*)REAL(a);
//   for (int i = 0; i < n; i++) {
//     r[i] = UNPACK_FIRST(x[i]);
//   }
//   UNPROTECT(1);
//   return ret;
// }

// RcppExport SEXP unpack_second(SEXP a) {
//   SEXP ret;
//   int n = Rf_length(a);
//   PROTECT(ret = Rf_allocVector(REALSXP, n));
//   double *r = REAL(ret);
//   uint64_t *x = (uint64_t*)REAL(a);
//   for (int i = 0; i < n; i++) {
//     r[i] = UNPACK_SECOND(x[i]);
//   }
//   UNPROTECT(1);
//   return ret;
// }

RcppExport SEXP bfs_merge(SEXP a, SEXP b) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP a_i_sexp = getAttrib(a, RSymbol_i);
  SEXP a_x_sexp = getAttrib(a, RSymbol_x);
  SEXP b_i_sexp = getAttrib(b, RSymbol_i);
  SEXP b_x_sexp = getAttrib(b, RSymbol_x);

  SEXP a_p_sexp = getAttrib(a, RSymbol_p);
  SEXP b_p_sexp = getAttrib(b, RSymbol_p);
  int *a_p = INTEGER(a_p_sexp);
  int *b_p = INTEGER(b_p_sexp);

  vector<int> res_i;
  vector<double> res_x;
  SEXP new_p;
  PROTECT(new_p = Rf_allocVector(INTSXP, Rf_length(a_p_sexp)));
  INTEGER(new_p)[0] = 0;
  for (int k = 0; k < Rf_length(a_p_sexp)-1; k++) {
    double *a_x = REAL(a_x_sexp) + a_p[k];
    // uint64_t *a_x = reinterpret_cast<uint64_t*>(REAL(a_x_sexp));
    double *b_x = REAL(b_x_sexp) + b_p[k];
    // uint64_t *b_x = reinterpret_cast<uint64_t*>(REAL(b_x_sexp));
    int *a_i = INTEGER(a_i_sexp) + a_p[k];
    int *b_i = INTEGER(b_i_sexp) + b_p[k];

    int i,j;
    int i_n = a_p[k+1] - a_p[k];
    int j_n = b_p[k+1] - b_p[k];
    for (i = j = 0; i < i_n && j < j_n; ) {
      if (a_i[i] < b_i[j]) {
	res_i.push_back(a_i[i]);
	res_x.push_back(a_x[i]);
	// res_i[total_elems] = UNPACK_FIRST(a_x[i]);
	// res_x[total_elems] = UNPACK_SECOND(a_x[i]);
	i++;
      } else if (a_i[i] > b_i[j]) {
	res_i.push_back(b_i[j]);
	res_x.push_back(b_x[j]);
	// res_i[total_elems] = UNPACK_FIRST(b_x[j]);
	// res_x[total_elems] = UNPACK_SECOND(b_x[j]);
	j++;
      } else {
	res_i.push_back(a_i[i]);
	res_x.push_back(a_x[i] < b_x[j] ? a_x[i] : b_x[j]);
	// res_i[total_elems] = UNPACK_FIRST(a_x[i]);
	// res_x[total_elems] = UNPACK_SECOND(a_x[i]) < UNPACK_SECOND(b_x[j]) ? UNPACK_SECOND(a_x[i]) : UNPACK_SECOND(b_x[j]);
	i++;
	j++;
      }
    }

    for ( ; i < i_n; i++) {
      res_i.push_back(a_i[i]);
      res_x.push_back(a_x[i]);
      // res_i[total_elems] = UNPACK_FIRST(a_x[i]);
      // res_x[total_elems] = UNPACK_SECOND(a_x[i]);
    }
    for ( ; j < j_n; j++) {
      res_i.push_back(b_i[j]);
      res_x.push_back(b_x[j]);
      // res_i[total_elems] = UNPACK_FIRST(b_x[j]);
      // res_x[total_elems] = UNPACK_SECOND(b_x[j]);
    }
    INTEGER(new_p)[k+1] = res_i.size();
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = INTEGER(getAttrib(a, RSymbol_Dim))[0];
  INTEGER(dim)[1] = INTEGER(getAttrib(a, RSymbol_Dim))[1];
  SEXP new_i, new_x;

  PROTECT(new_i = Rf_allocVector(INTSXP, res_i.size()));
  PROTECT(new_x = Rf_allocVector(REALSXP, res_x.size()));
  memcpy(INTEGER(new_i), &res_i[0], res_i.size() * sizeof(res_i[0]));
  memcpy(REAL(new_x), &res_x[0], res_x.size() * sizeof(res_x[0]));
  // for (int i = 0; i < total_elems; i++) {
  //   INTEGER(new_i)[i] = res_i[i];
  //   REAL(new_x)[i] = res_x[i];
  // }

  // delete[] res_i;
  // delete[] res_x;

  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, new_i);
  Rf_setAttrib(newobj, RSymbol_p, new_p);
  Rf_setAttrib(newobj, RSymbol_x, new_x);

  UNPROTECT(5);
  return newobj;
  END_RCPP
}

RcppExport SEXP bfs_step(SEXP g, SEXP f, SEXP d, SEXP start) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

//  presto::Timer t;

  double *done = REAL(d);
  int *g_i = INTEGER(getAttrib(g, RSymbol_i));
  int *g_p = INTEGER(getAttrib(g, RSymbol_p));
  double *g_x = REAL(getAttrib(g, RSymbol_x));

  int *f_i = INTEGER(getAttrib(f, RSymbol_i));
  double *f_x = REAL(getAttrib(f, RSymbol_x));

  int *g_dims = INTEGER(getAttrib(g, RSymbol_Dim));
  int n = g_dims[0];
  int s = INTEGER(start)[0];

  int frontier_elems = Rf_length(getAttrib(f, RSymbol_i));
  // boost::unordered_map<int,int> parents; // change to hash map or something?
  vector<uint64_t> parents;

//  t.start();
  for (int i = 0; i < frontier_elems; i++) {
    int v = f_i[i];
    for (int j = g_p[v]; j < g_p[v+1]; j++) {
      int w = g_i[j];
      if (done[w] == 0) {
	done[w] = 1;
	// parents[w] = v+1;
	parents.push_back(PACK(w,v+1+s));
      }
    }
  }
//  fprintf(stderr, "time frontier: %lf\n", t.restart()/1e6);

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = n;
  INTEGER(dim)[1] = 1;
  SEXP new_i, new_p, new_x;
  PROTECT(new_p = Rf_allocVector(INTSXP, 2));
  INTEGER(new_p)[0] = 0;
  INTEGER(new_p)[1] = parents.size();

  PROTECT(new_i = Rf_allocVector(INTSXP, parents.size()));
  PROTECT(new_x = Rf_allocVector(REALSXP, parents.size()));
  int j = 0;

  sort(parents.begin(), parents.end());
  memcpy(REAL(new_x), &parents[0], parents.size()*sizeof(parents[0]));
  //!!! for (int i = 0; i < parents.size(); i++) {
  //   INTEGER(new_i)[i] = i;
  // }

  // pair<int,int> *vec = new pair<int,int>[parents.size()];
  // for (boost::unordered_map<int,int>::iterator i = parents.begin();
  //      i != parents.end(); i++) {
  //   vec[j++] = *i;
  // }
  // fprintf(stderr, "time create array: %lf\n", t.restart()/1e6);

  // sort(vec, vec+parents.size());
  // fprintf(stderr, "time sort: %lf\n", t.restart()/1e6);

  // j = 0;
  // for (int i = 0;
  //      i < parents.size(); i++) {
  //   INTEGER(new_i)[j] = vec[i].first;
  //   REAL(new_x)[j] = vec[i].second + s;
  //   j++;
  // }
  // delete[] vec;

  // for (map<int,int>::iterator i = parents.begin();
  //      i != parents.end(); i++) {
  //   INTEGER(new_i)[j] = i->first;
  //   REAL(new_x)[j] = i->second + s;
  //   j++;
  // }

//  fprintf(stderr, "time sm construct: %lf\n", t.restart()/1e6);

  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, new_i);
  Rf_setAttrib(newobj, RSymbol_p, new_p);
  Rf_setAttrib(newobj, RSymbol_x, new_x);

  UNPROTECT(5);
  if (newobj == NULL) {
    fprintf(stderr, "newobj is NULL!!\n");
  }
  return newobj;
  END_RCPP
}

RcppExport SEXP bfs_step2(SEXP g, SEXP f, SEXP d, SEXP start) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

//  presto::Timer t;

  double *done = REAL(d);
  int *g_i = INTEGER(getAttrib(g, RSymbol_i));
  int *g_p = INTEGER(getAttrib(g, RSymbol_p));
  double *g_x = REAL(getAttrib(g, RSymbol_x));

  int *f_i = INTEGER(getAttrib(f, RSymbol_i));

  int *g_dims = INTEGER(getAttrib(g, RSymbol_Dim));
  int n = g_dims[0];
  int s = INTEGER(start)[0];

  int frontier_elems = Rf_length(getAttrib(f, RSymbol_i));
  // boost::unordered_map<int,int> parents; // change to hash map or something?
  vector<int> parents;
  vector<double> new_frontier;

//  t.start();
  for (int i = 0; i < frontier_elems; i++) {
    int v = f_i[i];
    for (int j = g_p[v]; j < g_p[v+1]; j++) {
      int w = g_i[j];
      if (done[w] == 0) {
	done[w] = 1;
	parents.push_back(w);
	new_frontier.push_back(v+1+s);
      }
    }
  }
//  fprintf(stderr, "time frontier: %lf\n", t.restart()/1e6);

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = n;
  INTEGER(dim)[1] = 1;
  SEXP new_i, new_p, new_x;
  PROTECT(new_p = Rf_allocVector(INTSXP, 2));
  INTEGER(new_p)[0] = 0;
  INTEGER(new_p)[1] = parents.size();

  PROTECT(new_i = Rf_allocVector(INTSXP, parents.size()));
  PROTECT(new_x = Rf_allocVector(REALSXP, parents.size()));
  int j = 0;

  sort(parents.begin(), parents.end());
  memcpy(REAL(new_x), &parents[0], parents.size()*sizeof(parents[0]));
  //!!! for (int i = 0; i < parents.size(); i++) {
  //   INTEGER(new_i)[i] = i;
  // }

  // pair<int,int> *vec = new pair<int,int>[parents.size()];
  // for (boost::unordered_map<int,int>::iterator i = parents.begin();
  //      i != parents.end(); i++) {
  //   vec[j++] = *i;
  // }
  // fprintf(stderr, "time create array: %lf\n", t.restart()/1e6);

  // sort(vec, vec+parents.size());
  // fprintf(stderr, "time sort: %lf\n", t.restart()/1e6);

  // j = 0;
  // for (int i = 0;
  //      i < parents.size(); i++) {
  //   INTEGER(new_i)[j] = vec[i].first;
  //   REAL(new_x)[j] = vec[i].second + s;
  //   j++;
  // }
  // delete[] vec;

  // for (map<int,int>::iterator i = parents.begin();
  //      i != parents.end(); i++) {
  //   INTEGER(new_i)[j] = i->first;
  //   REAL(new_x)[j] = i->second + s;
  //   j++;
  // }

//  fprintf(stderr, "time sm construct: %lf\n", t.restart()/1e6);

  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, new_i);
  Rf_setAttrib(newobj, RSymbol_p, new_p);
  Rf_setAttrib(newobj, RSymbol_x, new_x);

  UNPROTECT(5);
  if (newobj == NULL) {
    fprintf(stderr, "newobj is NULL!!\n");
  }
  return newobj;
  END_RCPP
}

RcppExport SEXP bfs_getpart(SEXP v, SEXP start_sexp, SEXP size_sexp) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  int *v_i = INTEGER(getAttrib(v, RSymbol_i));
  double *v_x = REAL(getAttrib(v, RSymbol_x));

  int elems = Rf_length(getAttrib(v, RSymbol_i));

  int start_idx_min=0;
  int start_idx_max=elems;
  int start_idx;

  int start = INTEGER(start_sexp)[0];
  int size = INTEGER(size_sexp)[0];

  while (start_idx_min != start_idx_max-1) {
    start_idx = (start_idx_min + start_idx_max)/2;
    if (v_i[start_idx] == start) {
      break;
    } else if (v_i[start_idx] < start) {
      start_idx_min = start_idx;
    } else {
      start_idx_max = start_idx;
    }
  }
  start_idx = (start_idx_min + start_idx_max)/2;

  if (v_i[start_idx] < start) {
    start_idx++;
  }

  int end_idx;
  for (end_idx = start_idx;
       end_idx < elems && v_i[end_idx] < start+size;
       end_idx++) {
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = size;
  INTEGER(dim)[1] = 1;
  SEXP new_i, new_p, new_x;
  PROTECT(new_p = Rf_allocVector(INTSXP, 2));
  INTEGER(new_p)[0] = 0;
  INTEGER(new_p)[1] = end_idx-start_idx;

  PROTECT(new_i = Rf_allocVector(INTSXP, end_idx-start_idx));
  PROTECT(new_x = Rf_allocVector(REALSXP, end_idx-start_idx));

  for (int i = start_idx; i < end_idx; i++) {
    INTEGER(new_i)[i-start_idx] = v_i[i]-start;
    REAL(new_x)[i-start_idx] = v_x[i];
  }

  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, new_i);
  Rf_setAttrib(newobj, RSymbol_p, new_p);
  Rf_setAttrib(newobj, RSymbol_x, new_x);

  UNPROTECT(5);
  return newobj;
  END_RCPP
}

RcppExport SEXP bfs_dense_step(SEXP g, SEXP p, SEXP d) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  int *g_i = INTEGER(getAttrib(g, RSymbol_i));
  int *g_p = INTEGER(getAttrib(g, RSymbol_p));
  double *g_x = REAL(getAttrib(g, RSymbol_x));

  double *parents = REAL(p);
  double *done = REAL(d);

  int *g_dims = INTEGER(getAttrib(g, RSymbol_Dim));
  int n = g_dims[0];
  int s = g_dims[1];

  SEXP res;
  PROTECT(res = Rf_allocMatrix(REALSXP, 1, s));
  memset(REAL(res), 0, s * sizeof(REAL(res)[0]));

  for (int i = 0; i < s; i++) {
    if (done[i] != 0) {
      REAL(res)[i] = done[i];
      continue;
    }

    for (int j = g_p[i]; j < g_p[i+1]; j++) {
      if (parents[g_i[j]] != 0) {
	REAL(res)[i] = g_i[j]+1;
	break;
      }
    }
  }

  UNPROTECT(1);
  return res;
  END_RCPP
}

RcppExport SEXP cc_step(SEXP g, SEXP labels_s, SEXP receive_s, SEXP s_s, SEXP p_s) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP labels = NULL;
  SEXP send = NULL;
  SEXP changed = NULL;

  int *g_i = INTEGER(getAttrib(g, RSymbol_i));
  int *g_p = INTEGER(getAttrib(g, RSymbol_p));
  double *g_x = REAL(getAttrib(g, RSymbol_x));

  int *g_dims = INTEGER(getAttrib(g, RSymbol_Dim));
  int n = g_dims[0];
  int s = g_dims[1];

  int partitions = INTEGER(p_s)[0];
  int partition_size = INTEGER(s_s)[0];

  int *rec_i = INTEGER(getAttrib(receive_s, RSymbol_i));
  double *rec_x = REAL(getAttrib(receive_s, RSymbol_x));

  double *old_labels = REAL(labels_s);
  PROTECT(labels = Rf_allocVector(REALSXP,s));
  double *new_labels = REAL(labels);
  memcpy(new_labels, old_labels, s*sizeof(old_labels[0]));

  int rec_n = Rf_length(getAttrib(receive_s, RSymbol_i));

  vector<bool> updated(s);
  for (int i = 0; i < rec_n; i++) {
    int id = rec_i[i];
    if (rec_x[i] < new_labels[id]) {
      // update label
      updated[id] = true;
      new_labels[id] = rec_x[i];
    }
  }

 vector<int> *send_i = new vector<int>[partitions];
 vector<double> *send_x = new vector<double>[partitions];
 vector<pair<int,double> > *send_ix = new vector<pair<int,double> >[partitions];
  for (int i = 0; i < updated.size(); i++) {
    if (updated[i]) {
      // "send" label to all neighbors
      for (int j = g_p[i]; j < g_p[i+1]; j++) {
	int id = g_i[j];
	int part = id / partition_size;
	// send_i[part].push_back(id - part*partition_size);
	// send_x[part].push_back(new_labels[i]);
	send_ix[part].push_back(make_pair(id /*- part*partition_size*/,
					  new_labels[i]));
      }      
    }
  }
  for (int i = 0; i < partitions; i++) {
    sort(send_ix[i].begin(), send_ix[i].end());
    for (int j = 0; j < send_ix[i].size(); j++) {
      if (j == 0 || send_ix[i][j].first != send_ix[i][j-1].first) {
	send_i[i].push_back(send_ix[i][j].first);
	send_x[i].push_back(send_ix[i][j].second);
      }
    }
  }
  delete[] send_ix;

  // set up sending vectors
  Rcpp::Language create_spm_call("new", "dgCMatrix");
  PROTECT(send = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = n;
  INTEGER(dim)[1] = partitions;
  SEXP new_i, new_p, new_x;
  PROTECT(new_p = Rf_allocVector(INTSXP, partitions+1));
  INTEGER(new_p)[0] = 0;
  int total_elems = 0;
  for (int i = 0; i < partitions; i++) {
    total_elems += send_i[i].size();
    INTEGER(new_p)[i+1] = total_elems;
  }

  PROTECT(new_i = Rf_allocVector(INTSXP, total_elems));
  PROTECT(new_x = Rf_allocVector(REALSXP, total_elems));

  for (int i = 0; i < partitions; i++) {
    if (send_i[i].size() > 0) {
      int offset = INTEGER(new_p)[i];
      memcpy(INTEGER(new_i) + offset, &send_i[i][0], send_i[i].size() * sizeof(send_i[i][0]));
      memcpy(REAL(new_x) + offset, &send_x[i][0], send_x[i].size() * sizeof(send_x[i][0]));
    }
  }

  delete[] send_i;
  delete[] send_x;

  PROTECT(changed = Rf_allocVector(REALSXP, 1));
  REAL(changed)[0] = total_elems != 0;

  Rf_setAttrib(send, RSymbol_Dim, dim);
  Rf_setAttrib(send, RSymbol_i, new_i);
  Rf_setAttrib(send, RSymbol_p, new_p);
  Rf_setAttrib(send, RSymbol_x, new_x);
  
  SEXP ret;
  PROTECT(ret = allocVector(VECSXP,3));
  SET_VECTOR_ELT(ret, 0, labels);
  SET_VECTOR_ELT(ret, 1, send);
  SET_VECTOR_ELT(ret, 2, changed);

  UNPROTECT(8);  // 1 for return list, 3 for return objects, 4 for sparse matrix members (dim, i, x, p)
  return ret;
  END_RCPP
}
