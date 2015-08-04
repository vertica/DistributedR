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

require(Rcpp)

NAMESPACE <- environment()

setClass("darray", representation(dim="numeric", blocks="numeric",
                                  sparse="logical", dimnames="list", distribution_policy="character"),
                   prototype(dim=c(0,0), sparse=FALSE, blocks=c(0,0)),
                   contains=("dobject"),
                   S3methods=TRUE)

setMethod("initialize", "darray",
          function(.Object, dim, blocks, sparse=FALSE, npartitions, subtype, distribution_policy) {
            .Object <- callNextMethod(.Object, dim=dim, blocks=blocks, npartitions=npartitions, subtype=subtype, 
                                      distribution_policy=distribution_policy)
            .Object@sparse = sparse
            .Object
          })

darray <- function(dim = NA, blocks = NA, sparse=FALSE, data=0, npartitions=NA, empty=FALSE, distribution_policy="roundrobin") {
  tryCatch({
   stype="STD"
   data_class<-class(data)
   if(data_class != "numeric" && data_class != "integer" && data_class != "logical") stop("Argument 'data' should be a number or logical value.")
   if(length(data)>1) stop("Argument 'data' should be a number, character, or logical value.")
   if(is.na(data) || data!=0){
      #For empty arrays, data does not make sense.
      if(!all(is.na(npartitions)) || empty) stop("Non-zero 'data' cannot be used with 'npartitions' or 'empty'.")
      #For sparse matrices we always initialize with 0 as they don't take space.
      if(sparse) stop("Initializing a sparse matrix with all non-zero elements makes it dense. Set 'data=0'.")
   }

   #Check if the user declared a flexible array, i.e., defined npartitions
   if(!all(is.na(npartitions))){
   #(TODO) Move most of these checks to dobject.R
   if(!is.na(dim) || !is.na(blocks)) stop("'dim' and 'blocks' cannot be declared when 'npartitions' is used")
        if(length(npartitions)==1) npartitions <- c(npartitions,1)
        if(length(npartitions)!=2) stop("length(npartitions) should be two")
        if (!is.numeric(npartitions)) stop("argument 'npartitions' should be numeric")
        else if (!all(round(npartitions)==npartitions)) stop("argument 'npartitions' should be an integer")

        blocks <- c(1, 1)
        dim <- npartitions
        empty=TRUE
        stype="FLEX_DECLARED"
   }else{
      if(empty) stype="UNINIT_DECLARED"
   }

   d <- new ("darray", dim, blocks, sparse, npartitions, subtype=stype, distribution_policy=distribution_policy)
   success <- FALSE

  if (!sparse) {
    success <- foreach(i,1:npartitions(d),
            initdata <- function(dhs = splits(d,i),
                                 val = data,
                                 fulldim = d@dim,
                                 blockdim = d@blocks,
                                 ii = i,
				 isempty = empty
                                 ) {
              #Empty means that we can leave the darray uninitialized. It saves space. The user is probably going to update it anyway.
	      if(isempty){
			dhs <- array(0,dim=c(0,0))              
              }else{
			dim=dobject.getdims(fulldim, blockdim, ii)
  	                dhs <- matrix(data=as.numeric(val), nrow=dim[1], ncol=dim[2])
              }
              update(dhs)
            }, progress=FALSE)
  } else {
    success <- foreach(i,1:npartitions(d),
            initdata <- function(dhs = splits(d,i),
                                 val = data,
                                 fulldim = d@dim,
                                 blockdim = d@blocks,
                                 ii = i,
				 isempty= empty
                                 ) {
			
              dim_d <- dobject.getdims(fulldim, blockdim, ii)
	      if(isempty) dim_d <-c(0,0)
              dhs <- new("dgCMatrix", i=as.integer({}),
                                      x=as.numeric({}), 
                                      p=as.integer(rep(val, dim_d[2] + 1)),
                                      Dim=as.integer(dim_d))
              update(dhs)
            }, progress=FALSE)
  }
  },error = handle_presto_exception)
  if(!success) { rm(d); gc(); NULL }
  else d
}

load.darray <- function(x, filename, triplet=TRUE, transpose=FALSE, progress=TRUE) {
tryCatch({  
  foreach(i, 1:length(splits(x)),
          loadsplit <- function(mat = splits(x, i),
                           fulldim = x@dim,
                           blockdim = x@blocks,
                           id = i,
                           tr = triplet,
                           fn = filename,
                           sp = x@sparse,
                           trans = transpose) {
            blocks <- ceiling(fulldim/blockdim)
            block.x <- (id-1)%/%blocks[2]+1
            block.y <- (id-1)%%blocks[2]+1
            i.offset <- as.integer((block.x-1)*blockdim[1])
            j.offset <- as.integer((block.y-1)*blockdim[2])

            splitname <- paste(fn, id-1, sep="")
            if(file.exists(splitname) == FALSE){
              stop("input file does not exist: ",splitname);
            }
            if (sp) {
#              mat@x[] <- 0
#              mat <- sparseMatrix(x=0, i=c(1), j=c(1), dims=c(1,1))
              mat <- .Call("ReadSparse", splitname, as.integer(dobject.getdims(fulldim, blockdim, id)), i.offset, j.offset, trans)#sparseMatrix(x=0, i=c(1), j=c(1), dims=blockdim)
            } else {
              mat[,] <- 0
#              mat <- array(data=0, dim=blockdim)
            
              if (tr) {
                data <- scan(file=splitname)
                len <- length(data)
                if (len > 0) {
                  indexarray <- array(dim=c(len/3,2))
                  if (!trans) {
                    indexarray[,1] <- data[seq(1,len,3)]-i.offset+1
                    indexarray[,2] <- data[seq(2,len,3)]-j.offset+1
                  } else {
                    indexarray[,1] <- data[seq(2,len,3)]-i.offset+1
                    indexarray[,2] <- data[seq(1,len,3)]-j.offset+1
                  }
                  mat[indexarray] <- data[seq(3,len,3)]
                }
              } else {
                data <- scan(file=splitname)
                mat[,] <- data
                if (trans) {
                  mat <- t(mat)
                }
              } 
            }     

            update(mat)
          }, progress=progress)
  },error = handle_presto_exception)
  return(TRUE)
}

# load a darray from a single file
# file format has to be edge list, no weights!
# input file must be pre-processed beforehand with tools/create_lookup!
load.darray.internal <- function(x, filename, hasweights=FALSE, zerobased=FALSE, progress=TRUE) {
  if (!x@sparse) {
    stop("load.darray.internal only works for sparse darrays")
  }

  if (x@blocks[1] != x@dim[1]) {  # 2D partitioning
    foreach(i, 1:length(splits(x)),
            loadsplit <- function(mat = splits(x, i),
                                  fulldim = x@dim,
                                  blockdim = x@blocks,
                                  id = i,
                                  fn = filename,
                                  zerobased = zerobased,
                                  hasweights = hasweights) {
              num.blocks <- ceiling(fulldim / blockdim)
              block.x <- floor((id-1) / num.blocks[2]) + 1
              block.y <- floor((id-1) %% num.blocks[2]) + 1
              col <- (block.y-1)*blockdim[2] + 1
              row <- (block.x-1)*blockdim[1] + 1
              current.blockdim <- blockdim
              if (col + current.blockdim[2] > fulldim[2]+1) {
                current.blockdim[2] <- fulldim[2] + 1 - col
              }
              if (row + current.blockdim[1] > fulldim[1]+1) {
                current.blockdim[1] <- fulldim[1] + 1 - row
              }
              mat <- .Call("ReadSparse2DPartition", fn, as.integer(current.blockdim), as.integer(col), as.integer(row), as.integer(fulldim[1]), zerobased, hasweights)
              update(mat, length(mat@x) == 0)
            })    
  } else {  # column partitioning
    foreach(i, 1:length(splits(x)),
            loadsplit <- function(mat = splits(x, i),
                                  fulldim = x@dim,
                                  blockdim = x@blocks,
                                  id = i,
                                  fn = filename,
                                  zerobased = zerobased) {
              col <- (id-1) * blockdim[2] + 1
              current.blockdim <- blockdim
              if (col + current.blockdim[2] > fulldim[2]+1) {
                current.blockdim[2] <- fulldim[2] + 1 - col
              }
              mat <- .Call("ReadSparseColPartition", fn, as.integer(current.blockdim), as.integer(col), zerobased)
              update(mat, length(mat@x) == 0)
            }, progress=progress)
  }
  return(TRUE)
}
#if (FALSE) {
setMethod("save", signature("darray"),
  function(x, list = character(), file = stop("'file' must be specified"), ascii = FALSE, version = NULL, envir = parent.frame(),
           compress = !ascii, compression_level, eval.promises = TRUE, precheck = TRUE){
  tryCatch({
    foreach(i, 1:length(splits(x)),
          savesplit <- function(mat = splits(x, i),
                                fulldim = x@dim,
                                blockdim = x@blocks,
                                id = i,
                                fn = filename,
                                sp = x@sparse) {
            blocks <- fulldim/blockdim
            block.x <- (id-1)%/%blocks[2]+1
            block.y <- (id-1)%%blocks[2]+1
            i.offset <- (block.x-1)*blockdim[1]
            j.offset <- (block.y-1)*blockdim[2]

            splitname <- paste(fn, id-1, sep="")
            if (sp) {
              triplet <- as(mat, "dgTMatrix")
              output <- array(dim=c(3,length(triplet@i)))
              output[1,] <- triplet@i+i.offset
              output[2,] <- triplet@j+j.offset
              output[3,] <- triplet@x
            } else {
              currentblockdim <- dim(mat)
              output <- array(dim=c(3,currentblockdim[1]*currentblockdim[2]))
              for (i in 1:currentblockdim[1]) {
                start <- (i-1)*currentblockdim[2]+1
                output[1,start:(start+currentblockdim[2]-1)] <- i+i.offset-1
                output[2,start:(start+currentblockdim[2]-1)] <- (1:currentblockdim[2])+j.offset-1
                output[3,start:(start+currentblockdim[2]-1)] <- mat[i,]
              }
            }
            write(output, file=splitname, ncolumns=3, append=FALSE)
          }, progress=progress)
  },error = handle_presto_exception)
  return(TRUE)
  }
)
#}
setMethod("*", signature("numeric", "darray"),
          da_scaling <- function(e1, e2) {
            if(is.invalid(e2)) stop("Operation not supported on empty arrays.")	
            c <- darray(npartitions=npartitions2D(e2), sparse=e2@sparse)    #(*) between numeric and sparse array returns sparse array
            success <- foreach(i,1:numSplits(e2),
              function(ys = splits(e2,i),
                       cs = splits(c,i),
                       s = e1) {
                cs <- ys*s
                update(cs)
              }, progress=FALSE)
            if(!success) { rm(c); gc(); NULL }
            else c
          }
        )
setMethod("*", signature("darray", "numeric"),
          function(e1, e2) {
            da_scaling(e2, e1)
          }
         )

setMethod("+", signature("darray", "numeric"),
          function(e1, e2) {
	   if(is.invalid(e1)) stop("Operation not supported on empty arrays.")
	   c<-darray(npartitions=npartitions2D(e1))     #(+) between numeric and sparse array returns dense array
           success <- foreach(i,1:npartitions(e1),
                function(xs = splits(e1,i),
                         ys = e2,
                         cs = splits(c, i)) {
                  cs <- xs + ys
                  update(cs)
                })
            if(!success) { rm(c); gc(); NULL }
            else c
          }
   )

setMethod("+", signature("numeric", "darray"),
          function(e1, e2) {
	  return (e2+e1)
 }
)

setMethod("+", signature("darray", "darray"),
          function(e1, e2) {
	   if(is.invalid(e1) || is.invalid(e2)) stop("Operation not supported on empty arrays.")
	   if(!all(dim(e1)==dim(e2))){
              stop("non-conformable arrays. Check dimensions of input arrays.")
            }
	   e1_psize<-partitionsize(e1)
	   e2_psize<-partitionsize(e2)

	   if(!all(e1_psize==e2_psize)){
              stop("non-conformable partitions of arrays. Partition sizes are not same for the arrays.")
           }
           
           #(+) between dense and sparse array can return either dense or sparse array
           # depending on number of non-zero elements after the operation
           # Storing results as dense array for mixed darray types
           sparse <- e1@sparse && e2@sparse
	   c<-darray(npartitions=npartitions2D(e1), sparse=sparse)
           success <- foreach(i,1:npartitions(e1),
                function(xs = splits(e1,i),
                         ys = splits(e2,i),
                         cs = splits(c, i),
                         sparse = sparse) {
                  cs <- xs + ys
                  #Result can be sparse sometimes for mixed darray types
                  if(inherits(cs, "dgCMatrix") && !sparse)
                    cs <- as(cs, "matrix")
                  update(cs)
                })
            if(!success) { rm(c); gc(); NULL }
            else c
          }
        )

setMethod("-", signature("darray", "darray"),
          function(e1, e2) {
	   if(is.invalid(e1) || is.invalid(e2)) stop("Operation not supported on empty arrays.")
	   if(!all(dim(e1)==dim(e2))){
              stop("non-conformable arrays. Check dimensions of input arrays.")
            }
	   e1_psize<-partitionsize(e1)
	   e2_psize<-partitionsize(e2)

	   if(!all(e1_psize==e2_psize)){
              stop("non-conformable partitions of arrays. Partition sizes are not same for the arrays.")
           }

           #(-) between dense and sparse array can return either dense or sparse array
           # depending on number of non-zero elements after the operation
           # Storing results as dense array for mixed darray types
           sparse <- e1@sparse && e2@sparse
	   c<-darray(npartitions=npartitions2D(e1), sparse=sparse)
           success <- foreach(i,1:npartitions(e1),
                function(xs = splits(e1,i),
                         ys = splits(e2,i),
                         cs = splits(c, i),
                         sparse = sparse) {
                  cs <- xs - ys
                  # Results can be sparse sometimes for mixed darray types
                  if(inherits(cs, "dgCMatrix") && !sparse)
                    cs <- as(cs, "matrix") 
                  update(cs)
                })
            if(!success) { rm(c); gc(); NULL }
            else c
          }
        )

setMethod("-", signature("darray", "numeric"),
          function(e1, e2) {
	  e2<-(-1*e2)
	  return (e1+e2)
 }
)

setMethod("-", signature("numeric", "darray"),
          function(e1, e2) {
	   if(is.invalid(e2)) stop("Operation not supported on empty arrays.")
	   c<-darray(npartitions=npartitions2D(e2))     #(-) between numeric and sparse array returns dense array
           success <- foreach(i,1:npartitions(e2),
                function(xs = splits(e2,i),
                         ys = e1,
                         cs = splits(c, i)) {
                  cs <-  ys-xs
                  update(cs)
                })
            if(!success) { rm(c); gc(); NULL }
            else c
          }
   )

#Prints the Frobenius norm
setMethod("norm", signature("darray", "missing"),
          function(x) {
	    if(is.invalid(x)) stop("Operation not supported on empty arrays.")
            norms <- darray(npartitions=npartitions2D(x), sparse=FALSE)
            foreach(i,1:length(splits(norms)),
                    localnorm <- function(v=splits(x,i),
                                          res=splits(norms,i)) {
                      res <- sum(v^2)
                      update(res)
                    }, progress=FALSE)
            res <- getpartition(norms)
            return(sqrt(sum(res)))
          }
        )

setMethod("%*%", signature("darray", "darray"),
  function(x, y) {
  if(is.invalid(x) || is.invalid(y)) stop("Operation not supported on empty arrays.")
  #Dimension of m*n times n*p is m*p
    if (dim(x)[2] != dim(y)[1]) {
      stop("Array sizes do not match for %*% operation")
    }
    c <- NA
    success <- FALSE
    if(npartitions2D(x)[2]==1 && npartitions2D(y)[1]!=1){ #x is row-partitioned but y is not column-partitioned
      c <- darray(npartitions=npartitions2D(x), sparse=(x@sparse && y@sparse))
      success <- foreach(i, 1:numSplits(x), function(row = splits(x,i),
                                          col = splits(y),
                                          tmp = splits(c,i)){
        tmp <- row %*% col
        update(tmp)
      }, progress=FALSE)
    }else if(npartitions2D(x)[2]!=1 && npartitions2D(y)[1]==1){ # x is not row-partitioned but y is column-partitioned
      c <- darray(npartitions=npartitions2D(y), sparse=(x@sparse && y@sparse))
      success <- foreach(i, 1:numSplits(y), function(row = splits(x),
                                          col = splits(y,i),
                                          tmp = splits(c,i)){
        tmp <- row %*% col
        update(tmp)
      }, progress=FALSE)      
    }else if(npartitions2D(x)[2]==1 && npartitions2D(y)[1]==1){ # x is row-partitioned and y is column-partitioned. best parallellism!
      c <- darray(npartitions=c(npartitions2D(x)[1],npartitions2D(y)[2]), sparse=(x@sparse && y@sparse))
      #Multiplication happens for each block of c (left to right), take x's row and y's column partition 
      #If we don't update all unitialized flex partitions in one foreach, error can occur (different partition error, as some partitions will be empty)
      nc<-npartitions2D(c)[2]
       success <- foreach(i, 1:npartitions(c), function(col = splits(y,((i-1)%%nc)+1),
       		  		             row = splits(x,floor((i-1)/nc)+1),
                                             tmp = splits(c,i)){
          tmp <- row %*% col
          update(tmp)
        }, progress=FALSE)
    }else{
      stop("Darray partition sizes do not conform to multiplication. x%*%y: Either x should be row-partitioned or y should be column-partitioned")
    }
    if(!success) { rm(c); gc(); NULL }
    else c
  })

# return subset of matrix given the block size and the index
# the block is counted from left to right. then top to down
# For example, (1,1)(1,2)(2,1)(2,2) is index of 1,2,3,4, respectively
get_sub_matrix <- function(in_mat, b_size, index){
  nrow = dim(in_mat)[1] #number of rows
  ncol = dim(in_mat)[2]
  nb_per_r = ceiling(ncol/b_size[2]) #number of blocks in a row
  if(index>nb_per_r*(ceiling(nrow/b_size[1]))){
    stop("index out of range")
  }
  b_row_idx = 1+(b_size[1])*(floor((index-1)/nb_per_r)) #begin row index of submatrix
  b_col_idx = 1+(b_size[2])*((index-1)%%nb_per_r)  #begin column index of submatrix
  e_row_idx = ifelse(b_row_idx+b_size[1]-1<nrow, b_row_idx+b_size[1]-1, nrow)
  e_col_idx = ifelse(b_col_idx+b_size[2]-1<ncol, b_col_idx+b_size[2]-1, ncol)
  return (matrix(in_mat[b_row_idx:e_row_idx, b_col_idx:e_col_idx], nrow=(e_row_idx-b_row_idx+1), ncol=(e_col_idx-b_col_idx+1)))
}


# input: an input matrix that will be filled into the darray
# blocks: block dimension of a created darray. if missing, block dim is calculated so that the darray is striped across executors

setGeneric("as.darray", function(input, blocks) standardGeneric("as.darray"))
#setMethod("as.darray", signature(input="matrix", blocks="missing"),
#  function(input, blocks) {
#    as.darray(input, dim(input))
#})

#If blocks is missing partition the matrix rowwise, and stripe the partitions across executors.
#Note that the number of partitions may not exactly equal the number of executors. 
#E.g, M=15x4 matrix, and nexecutors=6. No. of partitions of darray will be =5
setMethod("as.darray", signature(input="matrix", blocks="missing"),
  function(input, blocks) {
    ninst<-sum(distributedR_status()$Inst)
    blocks<-dim(input)
    blocks[1]<-ceiling(blocks[1]/ninst)
    as.darray(input, blocks)
})

setMethod("as.darray", signature(input="matrix", blocks="numeric"),
  function(input, blocks) {
    mdim <- dim(input)
    out_dobject <- darray(mdim, blocks, FALSE)

    if (! is.null(out_dobject)) {
      da_blocks = out_dobject@dim
      bdim = out_dobject@blocks
      if(bdim[1]>mdim[1] || bdim[2]>mdim[2] || da_blocks[1]!=mdim[1] || da_blocks[2]!=mdim[2]){
        ## check if input block dimension is larger than input matrix
        ## check if input darray dimension is same as input matrix dimension
        stop("input darray and matrix dimensions do not conform")
      }
      foreach(i, 1:numSplits(out_dobject), function(idx=i,
                                                 mtx=get_sub_matrix(input, out_dobject@blocks, i),
                                                 ds=splits(out_dobject,i)){
        ds <- mtx
        update(ds)
      }, progress=FALSE)

      if(is.null(dimnames(input)) == FALSE)
        dimnames(out_dobject) <- dimnames(input)
    }

    return (out_dobject)
})

