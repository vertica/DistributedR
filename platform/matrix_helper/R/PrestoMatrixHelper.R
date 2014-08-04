#####################################################################
#A scalable and high-performance platform for R.
#Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 2 of the License, or (at
#your option) any later version.

#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.  You should have received a
#copy of the GNU General Public License along with this program; if
#not, write to the Free Software Foundation, Inc., 59 Temple Place,
#Suite 330, Boston, MA 02111-1307 USA
#####################################################################

mmult.minplus <- function(weight_matrix, src_vector, out_vector=src_vector){
  nrc = dim(weight_matrix)
  if (class(weight_matrix) == "matrix"){ 
    if(length(src_vector) != nrc[1]){
      stop("the element number of source vector should be same as the number of matrix rows")
    }
    .C("mmult_dense_minplus",
      weightArray=as.integer(weight_matrix),
      numRow=as.integer(dim(weight_matrix)[1]),
      numCol=as.integer(dim(weight_matrix)[2]),
      source=as.integer(src_vector),
      dest=as.integer(out_vector),
      PACKAGE="MatrixHelper",
      NAOK=TRUE)$dest
  }else if (class(weight_matrix) == "dgCMatrix"){
    if(length(src_vector) <  max(nrc[1],nrc[2])){
# the dimension of a sparse matrix is at least max(# row, #col) * max(#rows, #cols), and the input vector should be larger than the size
      stop("Input source cannot be smaller than max(#row, # col) of input weight matrix")
    }
    .C("mmult_sparse_minplus",
      nz = as.integer(length(weight_matrix@x)),
      row = as.integer(weight_matrix@i),
      p = as.integer(weight_matrix@p),
      x = as.integer(weight_matrix@x),
      source = as.integer(src_vector),
      dest = as.integer(out_vector),
      PACKAGE="MatrixHelper",
      NAOK=TRUE)$dest
  }else{
    stop("Matrix type ", class(weight_matrix), " not supported\n")
  }
}

presto.spvm <- function(x, y){
  numColx = 0
  size = 0
  if (class(y) != "dgCMatrix" && class(y) != "dgTMatrix"){
    stop("input y should be sparse matrix. the current input: ", class(y))
  }
  numRowy = dim(y)[1]
  if (class(x) == "numeric"){
    numColx = length(x)
    size = as.integer(1)
  }else if(class(x) == "matrix"){
    numColx = dim(x)[2]
    size = as.integer(dim(x)[1])
  }else{
    stop("not supported datatype of x: ", class(x))
  }
  if (numColx != numRowy){
    stop("Dimension of x and y do not match for multiplication")
  }
  if (class(y) == "dgCMatrix")
    return(.Call("spvm", x, y, size))
  else
    return(.Call("spvm_triplet", x, y, size))
}

presto.spmv <- function(x, y){
  numRowy = 0
  size = 0
  if (class(x) != "dgCMatrix" && class(x) != "dgTMatrix"){
    stop("input x should be sparse matrix. the current input: ", class(x))
  }
  numColx = dim(x)[2]
  if (class(y) == "numeric"){
    numRowy = length(y)
    size = as.integer(1)
  }else if(class(y) == "matrix"){
    numRowy = dim(y)[1]
    size = as.integer(dim(y)[2])
  }else{
    stop("not supported datatype of y: ", class(y))
  }
  if (numColx != numRowy){
    stop("Dimension of x and y do not match for multiplication")
  }
  if (class(x) == "dgCMatrix")
    return(.Call("spmv", x, y, size))
  else
    return(.Call("spmv_triplet", x, y, size))
}

presto.rowSums <- function(x) {
  if (class(x) == "dgTMatrix" || class(x) == "dgCMatrix") {
    return(.Call("rowSums", x))
  } else {
    return(rowSums(x))
  }
}
