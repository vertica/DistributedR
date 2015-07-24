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

#include <malloc.h>

#include <string>
#include <utility>  // make_pair
#include <vector>
#include <algorithm>
#include <inttypes.h>
#include <unistd.h>

#include <Rcpp.h>
#include <Rinternals.h>

using namespace std;

namespace presto {

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_j = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = Rf_install(#symbol); \
  }

typedef Rcpp::CharacterVector::iterator char_itr;
typedef Rcpp::LogicalVector::iterator log_itr;

RcppExport SEXP ReadSparse(SEXP fn_sexp, SEXP d,
                           SEXP i_offset, SEXP j_offset,
                           SEXP trans_sexp) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  Rcpp::CharacterVector fn_vec(fn_sexp);
  char_itr fn_itr = fn_vec.begin();
  std::string filename = std::string(fn_itr[0]);
  Rcpp::IntegerVector dims(d);
  int i_off = INTEGER(i_offset)[0];
  int j_off = INTEGER(j_offset)[0];

  Rcpp::LogicalVector trans_vec(trans_sexp);
  log_itr trans_itr = trans_vec.begin();
  bool transpose = trans_itr[0];

  FILE *in = fopen(filename.c_str(), "r");

  vector<pair<pair<int, int>, double> > triplets;
  int i, j;
  double x;
  while (fscanf(in, " %d %d %lf ", &i, &j, &x) == 3) {
    if (transpose) {
      int tmp = i;
      i = j;
      j = tmp;
    }
    triplets.push_back(make_pair(make_pair(j-j_off, i-i_off), x));
  }
  fclose(in);

  sort(triplets.begin(), triplets.end());
  int unique = triplets.empty() ? 0 : 1;
  for (int i = 1; i < triplets.size(); i++) {
    if (triplets[i].first != triplets[i-1].first) {
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
    j = 0;
    int nz = 0;
    for (; j <= COL(triplets[0]); j++)
      p_vec(j) = 0;
    for (int i = 0; i < triplets.size(); i++) {
      if (i > 0 && COL(triplets[i]) != COL(triplets[i-1])) {
        for (j = COL(triplets[i-1]) + 1; j <= COL(triplets[i]); j++) {
          p_vec(j) = nz;
          prev = -1;
        }
      }
      if (ROW(triplets[i]) == prev) {
        x_vec(nz) += triplets[i].second;
      } else {
        x_vec(nz) = triplets[i].second;
        i_vec(nz) = ROW(triplets[i]);
        prev = i_vec(nz);
        nz++;
      }
    }
    for (; j < p_vec.size(); j++)
      p_vec(j) = nz;

#undef COL
#undef ROW
  } else {
    for (int i =  0; i < dims(1)+1; i++) {
      p_vec(i) = 0;
    }
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP mx;
  PROTECT(mx = Rf_eval(create_spm_call, R_GlobalEnv));

  Rf_setAttrib(mx, RSymbol_Dim, d);
  Rf_setAttrib(mx, RSymbol_i, i_vec);
  Rf_setAttrib(mx, RSymbol_p, p_vec);
  Rf_setAttrib(mx, RSymbol_x, x_vec);

  UNPROTECT(1);

  return mx;
  END_RCPP
}

RcppExport SEXP ReadSparseColPartition(SEXP fn_sexp, SEXP d,
                                       SEXP col_sexp, SEXP zerobased_sexp) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  Rcpp::LogicalVector zerobased_vec(zerobased_sexp);
  log_itr zerobased_itr = zerobased_vec.begin();
  bool zerobased = zerobased_itr[0];

  Rcpp::CharacterVector fn_vec(fn_sexp);
  char_itr fn_itr = fn_vec.begin();
  std::string filename = std::string(fn_itr[0]);

  int *dims = INTEGER(d);
  int col = INTEGER(col_sexp)[0] - 1;  // change to 0-based
  size_t start;

  string colfilename = filename;
  colfilename += "_offsets";
  FILE *offsets_file = fopen(colfilename.c_str(), "r");
  if (zerobased) {
    fseek(offsets_file, col * sizeof(start), SEEK_SET);
  } else {
    fseek(offsets_file, (col+1) * sizeof(start), SEEK_SET);
  }
  if (fread(&start, sizeof(start), 1, offsets_file) != 1) {
    fprintf(stderr, "Read error in ReadSparseColPartition!\n");
    return R_NilValue;
  }
  fclose(offsets_file);

  FILE *in = fopen(filename.c_str(), "r");
  fseek(in, start, SEEK_SET);

  vector<int> i0_vec;
  SEXP p_vec;
  PROTECT(p_vec = Rf_allocVector(INTSXP, dims[1]+1));
  int *p0_vec = INTEGER(p_vec);

  int i, j;
  int prev_col = -1;
  while (fscanf(in, " %d %d ", &i, &j) == 2) {
    j -= col;
    if (!zerobased) {
      i--;
      j--;
    }
    if (j >= dims[1]) {
      break;
    }
    if (j != prev_col) {
      for (int k = prev_col+1; k <= j; k++) {
        p0_vec[k] = i0_vec.size();
      }
      prev_col = j;
    }

    i0_vec.push_back(i);
  }
  fclose(in);
  for (int k = prev_col+1; k <= dims[1]; k++) {
    p0_vec[k] = i0_vec.size();
  }

  Rcpp::IntegerVector i_vec(i0_vec.size());
  Rcpp::NumericVector x_vec(i0_vec.size());

  memcpy(INTEGER(i_vec), &i0_vec[0], i0_vec.size() * sizeof(i0_vec[0]));
  for (int i = 0; i < i0_vec.size(); i++) {
    REAL(x_vec)[i] = 1.0;
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP mx;
  PROTECT(mx = Rf_eval(create_spm_call, R_GlobalEnv));

  Rf_setAttrib(mx, RSymbol_Dim, d);
  Rf_setAttrib(mx, RSymbol_i, i_vec);
  Rf_setAttrib(mx, RSymbol_p, p_vec);
  Rf_setAttrib(mx, RSymbol_x, x_vec);

  UNPROTECT(2);

  return mx;
  END_RCPP
}

RcppExport SEXP ReadSparse2DPartition(SEXP fn_sexp, SEXP d,
                                      SEXP col_sexp, SEXP row_sexp,
                                      SEXP total_rows_sexp,
                                      SEXP zerobased_sexp,
                                      SEXP hasweights_sexp) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(j);
  INSTALL_SYMBOL(x);

  Rcpp::LogicalVector hasweights_vec(hasweights_sexp);
  log_itr hasweights_itr = hasweights_vec.begin();
  bool hasweights = hasweights_itr[0];

  Rcpp::LogicalVector zerobased_vec(zerobased_sexp);
  log_itr zerobased_itr = zerobased_vec.begin();
  bool zerobased = zerobased_itr[0];

  Rcpp::CharacterVector fn_vec(fn_sexp);
  char_itr fn_itr = fn_vec.begin();
  std::string filename = std::string(fn_itr[0]);

  int *dims = INTEGER(d);
  int col = INTEGER(col_sexp)[0] - 1;  // change to 0-based
  int row = INTEGER(row_sexp)[0] - 1;  // change to 0-based
  int rows = INTEGER(total_rows_sexp)[0];
  size_t start;

  string offsetsfilename = filename;
  offsetsfilename += "_2doffsets";
  FILE *offsets_file = fopen(offsetsfilename.c_str(), "r");
  FILE *in = fopen(filename.c_str(), "r");

  size_t block_size;
  int res = fread(&block_size, sizeof(block_size), 1, offsets_file);
  fprintf(stderr, "dbg: block size is %zu\n", block_size);
  if (res != 1) {
    fprintf(stderr,
            "ERROR: read error in %s!\n",
            offsetsfilename.c_str());
    return R_NilValue;
  }
  fprintf(stderr, "dbg2: block size is %zu\n", block_size);
  if (row % block_size != 0 || col % block_size != 0) {
    fprintf(stderr,
            "ERROR: block size recorded in %s (%zu)"
            "must divide block offsets %d %d!\n",
            offsetsfilename.c_str(), block_size,
            row, col);
    return R_NilValue;
  }

  int block_rows;
  if (rows % block_size == 0)
    block_rows = rows / block_size;
  else
    block_rows = rows / block_size + 1;

  vector<int> i0_vec;
  vector<int> j0_vec;
  vector<double> x0_vec;
  for (size_t current_block_col = col;
       current_block_col < col + dims[1]; current_block_col += block_size) {
    int block_index_j = current_block_col / block_size;
    for (size_t current_block_row = row;
         current_block_row < row + dims[0]; current_block_row += block_size) {
      int block_index_i = current_block_row / block_size;
      int block_index = block_index_j * block_rows + block_index_i;
      size_t offset;
      int res = pread(fileno(offsets_file), &offset, sizeof(offset),
                      (1 + block_index) * sizeof(offset));
      if (res != sizeof(offset)) {
        fprintf(stderr,
                "ERROR: read error in %s!\n",
                offsetsfilename.c_str());
        return R_NilValue;
      }

      fseek(in, offset, SEEK_SET);
      size_t a, b;
      double w;
      int read_items = hasweights ? 3 : 2;
      const char *readcmd = hasweights ? " %zu %zu %lf " : " %zu %zu ";
      while (fscanf(in, readcmd, &a, &b, &w) == read_items) {
        if (!zerobased) {
          a--;
          b--;
        }

        if (!(a >= current_block_row && a < current_block_row + block_size &&
              b >= current_block_col && b < current_block_col + block_size)) {
          break;
        }

        i0_vec.push_back(a - row);
        j0_vec.push_back(b - col);
        x0_vec.push_back(w);
      }
    }
  }
  fclose(offsets_file);
  fclose(in);

  Rcpp::IntegerVector i_vec(i0_vec.size());
  Rcpp::IntegerVector j_vec(j0_vec.size());
  Rcpp::NumericVector x_vec(i0_vec.size());

  memcpy(INTEGER(i_vec), &i0_vec[0], i0_vec.size() * sizeof(i0_vec[0]));
  memcpy(INTEGER(j_vec), &j0_vec[0], j0_vec.size() * sizeof(j0_vec[0]));
  if (hasweights) {
    memcpy(REAL(x_vec), &x0_vec[0], x0_vec.size() * sizeof(x0_vec[0]));
  } else {
    for (int i = 0; i < i0_vec.size(); i++) {
      REAL(x_vec)[i] = 1.0;
    }
  }

  Rcpp::Language create_spm_call("new", "dgTMatrix");
  SEXP mx;
  PROTECT(mx = Rf_eval(create_spm_call, R_GlobalEnv));

  Rf_setAttrib(mx, RSymbol_Dim, d);
  Rf_setAttrib(mx, RSymbol_i, i_vec);
  Rf_setAttrib(mx, RSymbol_j, j_vec);
  Rf_setAttrib(mx, RSymbol_x, x_vec);

  UNPROTECT(1);

  return mx;
  END_RCPP
}
}
