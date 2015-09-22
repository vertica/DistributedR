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

#include <stdint.h>
#include <cstring>

#include "common.h"
#include "ArrayData.h"
#include "DistDataFrame.h"
#include "DistList.h"
#include "DeserializeArray.h"
#include <Rcpp.h>
// NOTE: the code in this file has to match up with the
// serialization/deserialization code in ArrayData.*

namespace presto {

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_j = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

// Install a symbol in R
#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = Rf_install(#symbol); \
  }

/** Write a dense matrix data into R matrix and return the pointer in R
 * @param data data to be written into R variable
 * @return an R variable with the input data
 */
static SEXP DeserializeDense(void *data) {
  dense_header_t *header = reinterpret_cast<dense_header_t*>(data);
  if (header == NULL) {
    throw PrestoWarningException("DeserializeDenseArray: header is null");
  }

  SEXP arr;
  PROTECT(arr = Rf_allocMatrix(header->value_type, header->dims[0], header->dims[1]));
  memcpy((header->value_type==REALSXP ? (void*)REAL(arr) : (void*)INTEGER(arr)),
         reinterpret_cast<char*>(data)+mapped_size(sizeof(*header)),
         header->dims[0]*header->dims[1]* (header->value_type==REALSXP ? sizeof(double) : sizeof(int)));
  UNPROTECT(1);
  return arr;
}

/** Write an input sparse array data inro R variable and return the R variable.
 * The input and output data has to aligned in the order of i->x->p vector
 * @param data sparse array data that will be written into R variable
 * @return an R varible with the input data
 */
static SEXP DeserializeSparse(void *data) {
  sparse_header_t *header = reinterpret_cast<sparse_header_t*>(data);
  if (header == NULL) {
    throw PrestoWarningException("DeserializeSparseArray: header is null");
  }

  size_t offset, size;

  SEXP i_vec, x_vec, p_vec;
  PROTECT(i_vec = Rf_allocVector(INTSXP, header->nnz));
  offset = mapped_size(sizeof(*header));
  size = header->nnz*sizeof(INTEGER(i_vec)[0]);
  memcpy(INTEGER(i_vec), reinterpret_cast<char*>(header)+offset, size);

  PROTECT(x_vec = Rf_allocVector(REALSXP, header->nnz));
  offset += mapped_size(size);
  size = header->nnz*sizeof(REAL(x_vec)[0]);
  memcpy(REAL(x_vec), reinterpret_cast<char*>(header)+offset, size);

  int64_t aux_size = header->encoding == CSC ?
      header->dims[1]+1 :
      header->nnz;  // Based on the encoding method (CSC or triplet),
  // the number of entries to read is determined.
  // regardless of the encoding type,
  // allocate #col+1 for safe mem operation.
  PROTECT(p_vec = Rf_allocVector(INTSXP, header->dims[1]+1));
  // Based on the compress scheme, the size of transferred
  // j or p vector is at most #col+1
  offset += mapped_size(size);
  // size of read object. This depends on compression scheme.
  size = aux_size*sizeof(INTEGER(p_vec)[0]);
  memcpy(INTEGER(p_vec), reinterpret_cast<char*>(header)+offset, size);

  // we have to convert j vector to p vector
  if (header->encoding == TRIPLET) {
    int* j_int = (INTEGER(p_vec));
    int* p_int = SparseArrayData::ConvertJtoPVector(j_int, header->nnz,
                                                    header->dims[1]);
    memcpy(INTEGER(p_vec), p_int,
           (header->dims[1]+1)*sizeof(INTEGER(p_vec)[0]));
    delete p_int;
  }

  Rcpp::Language create_spm_call("new", "dgCMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = header->dims[0];
  INTEGER(dim)[1] = header->dims[1];
  Rf_setAttrib(newobj, RSymbol_Dim, dim);
  Rf_setAttrib(newobj, RSymbol_i, i_vec);
  Rf_setAttrib(newobj, RSymbol_p, p_vec);
  Rf_setAttrib(newobj, RSymbol_x, x_vec);

  UNPROTECT(5);
  return newobj;
}
static SEXP DeserializeSparseTriplet(void *data) {
  sparse_triplet_header_t *header = reinterpret_cast
      <sparse_triplet_header_t*>(data);
  if (header == NULL) {
    throw PrestoWarningException("DeserializeSparseTriplet: header is null");
  }

  size_t offset, size;

  SEXP i_vec, j_vec, x_vec;
  PROTECT(i_vec = Rf_allocVector(INTSXP, header->nnz));
  offset = mapped_size(sizeof(*header));
  size = header->nnz*sizeof(INTEGER(i_vec)[0]);
  memcpy(INTEGER(i_vec), reinterpret_cast<char*>(header)+offset, size);

  PROTECT(x_vec = Rf_allocVector(REALSXP, header->nnz));
  offset += mapped_size(size);
  size = header->nnz*sizeof(REAL(x_vec)[0]);
  memcpy(REAL(x_vec), reinterpret_cast<char*>(header)+offset, size);

  PROTECT(j_vec = Rf_allocVector(INTSXP, header->nnz));
  offset += mapped_size(size);
  size = header->nnz*sizeof(INTEGER(j_vec)[0]);
  memcpy(INTEGER(j_vec), reinterpret_cast<char*>(header)+offset, size);

  Rcpp::Language create_spm_call("new", "dgTMatrix");
  SEXP newobj;
  PROTECT(newobj = Rf_eval(create_spm_call, R_GlobalEnv));

  SEXP dim;
  PROTECT(dim = Rf_allocVector(INTSXP, 2));
  INTEGER(dim)[0] = header->dims[0];
  INTEGER(dim)[1] = header->dims[1];
  setAttrib(newobj, RSymbol_Dim, dim);
  setAttrib(newobj, RSymbol_i, i_vec);
  setAttrib(newobj, RSymbol_j, j_vec);
  setAttrib(newobj, RSymbol_x, x_vec);

  UNPROTECT(5);
  return newobj;
}

static SEXP DeserializeDataFrame(void *data) {
  dframe_header_t *header = reinterpret_cast<dframe_header_t*>(data);
  SEXP df;
  PROTECT(df = allocVector(RAWSXP, header->size));
  memcpy(RAW(df),
         reinterpret_cast<unsigned char*>(data)+mapped_size(sizeof(*header)),
         header->size);
  Rcpp::Language unserialize_call("unserialize", df);
  SEXP dframe;
  PROTECT(dframe = Rf_eval(unserialize_call, R_GlobalEnv));
  UNPROTECT(2);
  return dframe;
}

static SEXP DeserializeList(void *data) {
  dlist_header_t *header = reinterpret_cast<dlist_header_t*>(data);
  SEXP df; 
  PROTECT(df = allocVector(RAWSXP, header->size));
  memcpy(RAW(df),
         reinterpret_cast<unsigned char*>(data)+mapped_size(sizeof(*header)),
         header->size);
  Rcpp::Language unserialize_call("unserialize", df);
  SEXP dlist;
  PROTECT(dlist = Rf_eval(unserialize_call, R_GlobalEnv));
  UNPROTECT(2);
  return dlist;
}

static SEXP DeserializeBinary(void *data, size_t size) {
  SEXP raw_data;  
  PROTECT(raw_data = Rf_allocVector(RAWSXP, size));
  memcpy(RAW(raw_data), 
         reinterpret_cast<unsigned char*>(data)+sizeof(int64_t), 
         size);
  Rcpp::Language unserialize_call("unserialize", raw_data);
  SEXP RObj;
  PROTECT(RObj = Rf_eval(unserialize_call, R_GlobalEnv));
  UNPROTECT(2);
  return RObj;
}

/** Write input memory data into R varaible and return the R variable.
 * Based on the input data type, we call sparse/dense deserialize module
 * @param data data to be written into R variable
 * @return an R variable with the input data
 */
SEXP Deserialize(void *data, size_t size) {
  int64_t type = *reinterpret_cast<int64_t*>(data);  

  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(j);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  switch (type) {
    case DENSE:
      return DeserializeDense(data);
      break;
    case SPARSE:
      return DeserializeSparse(data);
      break;
    case SPARSE_TRIPLET:
      return DeserializeSparseTriplet(data);
      break;
    case DATA_FRAME:
      return DeserializeDataFrame(data);
      break;
    case LIST:
      return DeserializeList(data);
      break;
    case BINARY:
      return DeserializeBinary(data, size);
      
    default:
      fprintf(stderr,
              "corrupt data type encountered during deserialization: %zd\n",
              type);
      return R_NilValue;
  }
}
}
