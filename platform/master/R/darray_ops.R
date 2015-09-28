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

#Implementation of common operators that can be applied to darrays
#The aim is to make darrays as transparent as basic R arrays

#Check is object is a darray
is.darray <- function(x)
    {
        if (class(x) == "darray")
            return(TRUE)
        else
            return(FALSE)
    }

#Error with legitimate message in case of splits typo
setMethod("split", signature("darray"), function(x,...)
    {   
        stop("\nFunction split() is not supported for darray object. \nCheck if you wanted to use function splits() instead")
    })

#Max of all elements in the array
setMethod("max", signature("darray"),function(x, na.rm = FALSE)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              temp <- darray(dim=c(1,numSplits(x)), blocks=c(1,1), sparse=FALSE)
              foreach(i,1:numSplits(x),
              localmax <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res <- matrix(max(v, na.rm=flag))

		  #R's max function has a weird bug for sparse matrices. When the matrix is a single row with a single negative element, 
                  #it may return the negative number instead of zero. We adjust for the error.
                  #E.g., a<-Matrix(c(-1,0,0,0,0), sparse=TRUE), max(t(a)) is -1. 
                  #But, a<-Matrix(c(-1,0,0,0,-2), sparse=TRUE), max(t(a)) is 0. 
		  if((class(v)=="dgCMatrix") && !is.na(res)) {
                     if(length(v)-nnzero(v)>0 && res<0)
		       res[1,1]<-0	     
		  }
                  update(res)
              }, progress=FALSE)
          res <- getpartition(temp)
          return(max(res))
      }
)

#Min of all elements in the array
setMethod("min", signature("darray"),function(x, na.rm=FALSE)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              temp <- darray(dim=c(1,numSplits(x)), blocks=c(1,1), sparse=FALSE)
              foreach(i,1:numSplits(x),
              localmin <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res <- matrix(min(v, na.rm=flag))
		  #R's min function has a weird bug for sparse matrices. When the matrix is a single row with a single positive element, 
                  #it may return the positive number instead of zero. We adjust for the error.
                  #E.g., a<-Matrix(c(1,0,0,0,0), sparse=TRUE), min(t(a)) is 1. 
                  #But, a<-Matrix(c(1,0,0,0,2), sparse=TRUE), min(t(a)) is 0. 

		  if((class(v)=="dgCMatrix") && !is.na(res)) {
                     if(length(v)-nnzero(v)>0 && res>0)
		       res[1,1]<-0		     
		  }
                  update(res)
              }, progress=FALSE)
          res <- getpartition(temp)
          return(min(res))
      }
)

#Sum of all elements in the array
setMethod("sum", signature("darray"),function(x, na.rm=FALSE)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              temp <- darray(dim=c(1,numSplits(x)), blocks=c(1,1), sparse=FALSE)
              foreach(i,1:numSplits(x),
              localsum <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res <- matrix(sum(v, na.rm=flag))
                  update(res)
              }, progress=FALSE)
          res <- getpartition(temp)
          return(sum(res))
      }
)


#Mean of a darray
setMethod("mean", signature("darray"),function(x, trim=0, na.rm=FALSE)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              if(trim!=0) stop("non-zero trim value is not supported\n")
              #Store sum and number of elements
              temp <- darray(dim=c(2,numSplits(x)), blocks=c(2,1), sparse=FALSE)
              foreach(i,1:numSplits(x),
              localmean <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res[1,1] <- sum(v, na.rm=flag)
		  #For really large numbers sum can overflow with integer addition.
		  if(is.na(res[1,1])) {res[1,1] <- sum(as.numeric(v), na.rm=flag)}
                  res[2,1] <- length(v)
                  if(flag){
                      res[2,1] <- res[2,1] - sum(is.na(v))
                  }
                  update(res)
              }, progress=FALSE)
          res <- getpartition(temp)
	  #First divide and then add to ensure that overflows can be better handled.
          return(sum(res[1,]/sum(res[2,])))
      }
)


#Column sums of a darray
setMethod("colSums", signature("darray"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              #For each partition we will store the column sums as
              #a vector of (1x ncols-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")
	      temp <- darray(npartitions=npartitions2D(x), sparse=FALSE)

              foreach(i,1:numSplits(x),
              localcsum <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res <- matrix(colSums(v, na.rm=flag),nrow=1)
                  update(res)
              }, progress=FALSE)
              res <- getpartition(temp)
              return(colSums(res, na.rm))
          }
)


#Row sums of a darray
setMethod("rowSums", signature("darray"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              #For each partition we will store the row sums as
              #a vector of (ncols-of-partition x 1)
              if(dims!=1) stop("only dims=1 is supported\n")
	      temp <- darray(npartitions=npartitions2D(x), sparse=FALSE)
	
              foreach(i,1:numSplits(x),
              localrsum <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm) {
                  res <- matrix(rowSums(v, na.rm=flag),ncol=1)
                  update(res)
              }, progress=FALSE)
              res <- getpartition(temp)
              return(rowSums(res, na.rm))
          }
)

#Column mean of a darray
setMethod("colMeans", signature("darray"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              #For each partition we will store the column sums and number of 
              #rows in a col, in vectors of (1x ncols-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")
	      temp <- darray(npartitions=npartitions2D(x), sparse=FALSE)
	      tempLen <- darray(npartitions=npartitions2D(x), sparse=FALSE)
	     
              foreach(i,1:numSplits(x),
                      localcmean <- function(v=splits(x,i),resS=splits(temp,i), resL=splits(tempLen,i),flag=na.rm) {
                          resS <- matrix(colSums(v, na.rm=flag),nrow=1)
                          resL <- matrix(as.numeric(nrow(v)), nrow=1, ncol=ncol(v))
                          if(flag){
                              resL <- resL-colSums(is.na(v))
                          }
                          update(resS)
                          update(resL)
                      }, progress=FALSE)
              res <- getpartition(temp)
              resL <- getpartition(tempLen)
              return(colSums(res, na.rm)/colSums(resL,na.rm))
          }
)


#Row mean of a darray
setMethod("rowMeans", signature("darray"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              #For each partition we will store the row sums and length as
              #a vector of (1 x nrows-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")
	      temp <- darray(npartitions=npartitions2D(x), sparse=FALSE)
	      tempLen <- darray(npartitions=npartitions2D(x), sparse=FALSE)

              foreach(i,1:numSplits(x),
                      localrmean <- function(v=splits(x,i),resS=splits(temp,i), resL=splits(tempLen,i), flag=na.rm) {
                          resS <- matrix(rowSums(v, na.rm=flag),ncol=1)
                          resL <- matrix(as.numeric(ncol(v)), ncol=1, nrow=nrow(v))
                          if(flag){
                              resL <- resL-rowSums(is.na(v))
                          }
                          update(resS)
                          update(resL)
                      }, progress=FALSE)
              res <- getpartition(temp)
              resL <- getpartition(tempLen)
              return(rowSums(res, na.rm)/rowSums(resL,na.rm))
          }
)

head.darray <- function(x, n=6L,...){
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              if(is.na(n)) return (0)
              n <- floor(n)   #round off fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- NULL
              #Make negative values positive
              if(n<0){
		  n <- dim(x)[1]+n
              }
              if(n<=0){
      		return (array(0,dim=c(0,ncol(x)), dimnames=dimnames(x)))
	      }

              #Define function that can return a subset of lines from a split
              headOfSplit <- function(x, id, nlines, bsize){
                  #Case 1: Obtain the full split
                  if(nlines>=bsize[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  #Case 2: Obtain only a small head of the split
                  temp <- darray(dim=c(nlines,bsize[2]), blocks=c(nlines,bsize[2]), sparse=x@sparse)
                  foreach(i,id:id,
                          localhead <- function(v=splits(x,i),res=splits(temp,1), nl=nlines) {
                              res <- head(v,nl)
                              update(res)
                          }, progress=FALSE)
                  return (getpartition(temp))
              }

	      psize<-partitionsize(x)
              nr <- 1
              nprocessed <- 0
              while((nr <= num.splits[1]) && (nprocessed < n)){
                 baseid <- 1 + ((nr-1)*num.splits[2])
                 temp <- headOfSplit(x, baseid, n-nprocessed, psize[baseid,])
                  #For each row stitch together column partitions (left to right)
                  nc <- 1
                  while(nc < num.splits[2]){
                      temp <- cbind2(temp, headOfSplit(x, baseid+nc, n-nprocessed, psize[baseid+nc,]))
                      nc <- nc+1
                  }
                  #Now bind the partitions via rows (from top to bottom)
		  if(is.null(res)){
		  	res<-temp
		  }else{
		        res <- rbind2(res, temp)
		  }
                  nr <- nr+1
                  nprocessed <- nprocessed+nrow(temp)
             }

              if(!is.null(dimnames(x)[[1]]) && length(dimnames(x)[[1]]) != 0 && length(dimnames(x)[[1]]) == nrow(res)) {
                 rownames(res) <- dimnames(x)[[1]]
              }   
              if(!is.null(dimnames(x)[[2]]) && length(dimnames(x)[[2]]) != 0 && length(dimnames(x)[[2]]) == ncol(res)) {
                 colnames(res) <- dimnames(x)[[2]]
              }

              return(res)
          }

#Tail of the darray
tail.darray <- function(x, n=6L,...) {
	      if(is.invalid(x)) stop("Operation not supported on empty arrays.")
              if(is.na(n)) return (0)
              n <- floor(n)   #round of fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- NULL
              if(n<0){
                  n <- dim(x)[1]+n
              }
              if(n<0) stop ("no. rows to be skipped is larger than array extent")
              if(n==0){
	        #Match the behavior to standard R
	        if(length(rownames(x))==0) return (tail(array(0,dim=c(1,1)),0))
		return (array(0,dim=c(0,ncol(x)), dimnames=dimnames(x)))
	       }

              #Define function to fetch subset of the partition data
              tailOfSplit <- function(x, id, nlines, bsize){
                  if(nlines>=bsize[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  temp <- darray(dim=c(nlines,bsize[2]), blocks=c(nlines,bsize[2]), sparse=x@sparse)
                  foreach(i,id:id,
                          localtail <- function(v=splits(x,i),res=splits(temp,1), nl=nlines) {
                              res <- tail(v,nl)
                              update(res)
                          }, progress=FALSE)
                  return(getpartition(temp))
                  
              }

	      psize<-partitionsize(x)
              nr <- num.splits[1]
              nprocessed <- 0
              #Lets build the result from the bottom
              while((nr >= 1) && (nprocessed < n)){
                  baseid <- 1 + ((nr-1)*num.splits[2])
                  temp <- tailOfSplit(x, baseid, n-nprocessed, psize[baseid,])
                  #For each row stitch together column partitions (left to right)
                  nc <- 1
                  while(nc < num.splits[2]){
                      temp <- cbind2(temp, tailOfSplit(x, baseid+nc, n-nprocessed, psize[baseid+nc,]))
                      nc <- nc+1
                  }
                  #Now bind the blocks via rows (from bottom to top)
		  if(is.null(res)){
		  	res<-temp
		  }else{
		        res <- rbind2(temp, res)
		  }
                  nr <- nr-1
                  nprocessed <- nprocessed+nrow(temp)
              }
  
              if(!is.null(dimnames(x)[[1]]) && length(dimnames(x)[[1]]) != 0 && length(dimnames(x)[[1]]) >= nrow(x)) {
                 rownames(res) <- dimnames(x)[[1]][(length(dimnames(x)[[1]])-nrow(res)+1):length(dimnames(x)[[1]])]
              } else {
  	         rownames(res) <- ((nrow(x)-nrow(res)+1):nrow(x))
	      }  

              if(!is.null(dimnames(x)[[2]]) && length(dimnames(x)[[2]]) != 0 && length(dimnames(x)[[2]]) == ncol(res)) {
                 colnames(res) <- dimnames(x)[[2]]
              }

              return(res)
          }


# Darrays can initially be empty or flexible sized. Just returning the number of rows declared in the dframe dimensions is not a good.
setMethod("nrow", signature("darray"), function(x)
    {
        rowPartitions <- seq(1,npartitions(x),by=x@npartitions[[2]])
        sum(partitionsize(x)[rowPartitions,1])
    })

setMethod("NROW", signature("darray"), function(x)
    {
        return (nrow(x))
    })

# Darrays can initially be empty or flexible sized. Just returning the number of cols declared in the dframe dimensions is not a good.
setMethod("ncol", signature("darray"), function(x)
    {
        colPartitions <- seq(1,x@npartitions[[2]])
        sum(partitionsize(x)[colPartitions,2])
    })

setMethod("NCOL", signature("darray"), function(x)
    {
        return (ncol(x))
    })

