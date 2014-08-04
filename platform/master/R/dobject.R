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

.get.shm.prefix <- function() {
  pm <- get_pm_object()
  master_addr <- pm$get_master_addr()
  master_port <- as.integer(pm$get_master_port())
  asc <- function(x) {strtoi(charToRaw(x),16L)}
  mID <- master_port + sum(unlist(lapply(strsplit(master_addr, "")[[1]], asc)))
  paste("R-shm-", as.character(mID), "-", sep="")
}

dobject_name <- function() {
  if (exists(".rand.dr.dobject.name", envir=baseenv()) == FALSE) {
    assign(".rand.dr.dobject.name", as.integer(runif(1)*.Machine$integer.max), envir=baseenv())
  }
  curId = get(".rand.dr.dobject.name", envir=baseenv())
  curId = curId %% .Machine$integer.max
  assign(".rand.dr.dobject.name", curId+1, envir=baseenv())
  paste(.get.shm.prefix(), curId, sep="")
}
setClass("dobject", representation(dim="numeric", blocks="numeric",
                                  name="character", dimnames="list", 
                                  dobject_ptr="Rcpp_DistributedObject", npartitions="numeric", subtype="character", split_distribution="character"), 
                   prototype(dim=c(0,0), blocks=c(0,0),
                             name="", dobject_ptr=NULL, npartitions=c(0,0), subtype="STD"),
                   S3methods=TRUE)

#subtype can be one of the three. It is used as metadata when performing checks related to updates
# (1)  STD    (standard dobject. Nothing special)
# (2)  UNINIT   (uninitialized object, e.g., empty darray or dframe. Currently unused.)
# (3)  FLEX_DECLARED  (object with flexible or unequal partition sizes, e.g., dlist or darrays)

setMethod("initialize", "dobject",
          function(.Object, dim=c(0,0), blocks=c(0,0), name=NA, npartitions = NA, subtype="STD", split_distribution="roundrobin") {
            pm <- get_pm_object()
            if(length(dim)!=2||length(blocks)!=2) stop("length(dim) and length(block) should be two")
            dim<-as.numeric(dim)
            blocks<-as.numeric(ceiling(blocks))
            if(all(blocks<=.Machine$integer.max) == FALSE) stop(paste("block size should be less than",.Machine$integer.max))
            if(class(dim)!="numeric"||class(blocks)!="numeric") stop("dim and blocks should be numeric value")
            if(!all(dim==floor(dim)))stop("dim should be integral values")
            if(dim[1]<=0||dim[2]<=0||blocks[1]<=0||blocks[2]<=0) stop("dim and blocks should be larger than 0")
            if(dim[1]<blocks[1]||dim[2]<blocks[2]) stop("blocks should be smaller than dim")
	    if(all(is.na(npartitions))) npartitions <-  c(ceiling(dim[1]/blocks[1]), ceiling(dim[2]/blocks[2]))
	    #Check if there are too many partitions. Stop if exceeds hard limit of 20K splits
            #nblocks <-  ceiling(dim[1]/blocks[1]) * ceiling(dim[2]/blocks[2])
	    nblocks <-  npartitions[1]*npartitions[2]
            nexecutors <- sum((distributedR_status())$Inst)
            
            if(nblocks > 20000){
                stop("Too many array partitions. Use fewer partitions. E.g., 1x-5x of #cores (",nexecutors,") instead of ",nblocks,".\n")
            }
            if(nblocks > 5*nexecutors){
                cat(paste("Warning: Too many array partitions may degrade performance. Use fewer partitions. E.g., 1x-5x of #cores (",nexecutors,") instead of ",nblocks,".\n",sep=""))
            }

            split_distribution_policy <- c("roundrobin", "random", "custom")
            split_distribution <- as.character(tolower(split_distribution))
            if(!split_distribution %in% split_distribution_policy)
              stop("Split can be distributed in Workers using following policies: roundrobin, random, custom\nPlease choose on of them")

            .Object@dimnames = list(NULL,NULL)
            .Object@dim = as.vector(dim)
            .Object@blocks = as.vector(blocks)
	    .Object@subtype = subtype
	    .Object@npartitions = npartitions
            .Object@split_distribution = split_distribution

            if (is.na(name)) {
              .Object@name = dobject_name()
            } else {
              .Object@name = paste(.get.shm.prefix(), name, sep="")
            }
            # dobj_ptr can be overwritten in the inherited class.
            .Object@dobject_ptr = new (DistributedObject, pm, as.character(class(.Object)), subtype, split_distribution)
            if(.Object@dobject_ptr$create(.Object@name, dim, blocks) == FALSE){stop("Fail to create dobject")}

            .Object
          })
#check if two input dobjects are the same (value-wise)
setMethod("==", signature("dobject", "dobject"),
          function(e1, e2) {
            if (all(dim(e1)==dim(e2))==FALSE){ #|| all(e1@blocks==e2@blocks)==FALSE){
              stop(paste("the dimension of two dobjects has to be the same", e1@dim, e2@dim, e1@blocks, e2@blocks))
            }
            ns <- numSplits(e1)
            comp <- darray(c(ns,1),c(1,1), FALSE, data = 0)
            foreach (i, 1:ns, function(sA = splits(e1, i),
                                       sB = splits(e2, i),
                                       sComp = splits(comp, i)){
              sComp[1] = all(sA==sB)
              update(sComp)
            })
            output <- getpartition(comp)
            all(output==1)
          }
         )
dimnames.dobject <- function(x) {
  list(x@dimnames[[1]], x@dimnames[[2]])
}

`dimnames<-.dobject` <- function(x, value) {
  if(!is.list(value) || length(value) != 2) stop("invalid type 'dimnames' given")

  value[[1]] <- as.character(value[[1]])
  value[[2]] <- as.character(value[[2]])
  if((is.null(value[[1]]) == FALSE && length(value[[1]]) != 0) && x@dim[1] != length(value[[1]])) stop("invalid row size 'dimnames' given")
  if((is.null(value[[2]]) == FALSE && length(value[[2]]) != 0) && x@dim[2] != length(value[[2]])) stop("invalid column size 'dimnames' given")
  x@dimnames[[1]] <- value[[1]]
  x@dimnames[[2]] <- value[[2]]
  x
}

# get (x,y) offsets of split with index
dobject.getoffsets <- function(fulldim, blockdim, index) {
  blocks <- ceiling(fulldim/blockdim)
  block.x <- (index-1)%/%blocks[2]+1
  block.y <- (index-1)%%blocks[2]+1
  return(c((block.x-1)*blockdim[1], (block.y-1)*blockdim[2]))
}
         
# get dims of split with index
dobject.getdims <- function(fulldim, blockdim, index) {
  res <- c(1,1)

  blocks <- ceiling(fulldim/blockdim)
  block.x <- (index-1)%/%blocks[2]+1
  block.y <- (index-1)%%blocks[2]+1

  if (block.x < blocks[1]) {
    res[1] <- blockdim[1]
  } else {
    if (fulldim[1]%%blockdim[1] == 0) {
      res[1] <- blockdim[1]
    } else {
      res[1] <- fulldim[1]%%blockdim[1]
    }
  }

  if (block.y < blocks[2]) {
    res[2] <- blockdim[2]
  } else {
    if (fulldim[2]%%blockdim[2] == 0) {
      res[2] <- blockdim[2]
    } else {
      res[2] <- fulldim[2]%%blockdim[2]
    }
  }
  return(as.integer(res))
}

setGeneric("getemptyflag", function(x) standardGeneric("getemptyflag"))
setMethod("getemptyflag", signature("dobject"),
  function(x) {
    return (x@empty)
})

#Partition size of an object. Here we define the case when index is not supplied since it is common to darray, dlist, and dframe.
setGeneric("partitionsize", function(x, index) standardGeneric("partitionsize"))

#Return size of all partitions. i'th row of the result corresponds to the i'th partition dimension
setMethod("partitionsize", signature("dobject","missing"),
  function(x, index) {
    if(is.null(x)) return (0)
    nparts<-npartitions(x)
    if(x@subtype == "FLEX_DECLARED"){
      #For flex objects let's go to the worker and figure out the current size
      temp<-darray(dim=c(nparts,2), blocks=c(1,2))
      foreach(i, 1:nparts, getSize<-function(y=splits(x,i), res=splits(temp,i)) {
	     res<-matrix(dim(y), nrow=1)
	     update(res)
        }, progress=FALSE)
      return (getpartition(temp))
   }else{
     #For non-flex objects we will reuse information from dimensions
     temp<-NULL
     for(i in 1:nparts){
     	 temp<-rbind(temp, dobject.getdims(x@dim,x@blocks,i))
     }
     return (temp)
   }
})

#Return the size of a partition
setMethod("partitionsize", signature("dobject","numeric"),
  function(x, index) {
    if(is.null(x)) return (0)
    index<-round(index)
    if(index<1) stop("Partition index should be a positive number")
    if(index>npartitions(x)) stop("Partition index should be less than total number of partitions (",npartitions(x),")")

    if(x@subtype == "FLEX_DECLARED"){
        #We don't store the sizes of flexible objects. Therefore, go to the worker and determine size
	temp<-darray(dim=c(1,2), blocks=c(1,2))
	foreach(i, 1:1, getSize<-function(y=splits(x,index), res=splits(temp,1)) {
	     res<-matrix(dim(y), nrow=1)
	     update(res)
        }, progress=FALSE)
	return (getpartition(temp))
    }else{
	#Just return the size as per the declared value
      return (matrix(dobject.getdims(x@dim,x@blocks,index),nrow=1))
    }
})

#Return the current dimension of the object. Useful primarily for arrays and data frames. 
#Calls the C++ code that tracks the current dimension. E.g., dimension of flexible arrays change after update.
setMethod("dim", signature("dobject"),
  function(x){
  x@dobject_ptr$dim()
})

setGeneric("clone", function(input,nrow=NA,ncol=NA,data=0,sparse=NA) standardGeneric("clone"))

#clones the size of the dobject, not the contents
#users can choose to specify the number of rows or columns (not both) in a partition
#Primarily used with flex objects where we want to keep the same flex dimension as that of the object
setMethod("clone", signature(input="dobject"),
  function(input, nrow=NA, ncol=NA, data=0, sparse=NA) {
  if(!is.na(nrow) && !is.na(ncol)) stop("Only one of nrow and ncol should be specified")
  if(!is.na(nrow)){
    if(input@npartitions[1]!=1) stop("Input object has varible number of rows in a partition. All rows will be cloned. Did you want to specify 'ncol' instead?")
    if(!is.numeric(nrow)) stop("nrow should be numeric")
    nrow<-as.integer(nrow)
    if(nrow<0) stop("nrow should be positive")
  }
  if(!is.na(ncol)){
   if(input@npartitions[2]!=1) stop("Input object has varible number of columns in a partition. All columns will be cloned. Did you want to specify 'nrow' instead?")
   if(!is.numeric(ncol)) stop("ncol should be numeric")
   ncol<-as.integer(ncol)
   if(ncol<0) stop("ncol should be positive")
  }

  if(is.na(sparse)) sparse <- input@sparse  
  if(sparse && (data!=0)) stop("Initializing a sparse matrix with all non-zero elements makes it dense. Set 'data=0'")
  if(class(input)=="darray"){
    do_dest = darray(npartitions=input@npartitions, sparse = sparse)
    foreach (i, 1:npartitions(input), function(sOrg  = splits(input, i),
                                             sDest = splits(do_dest, i), nr=nrow, nc=ncol, isSparse=sparse, v =data){
      if(is.na(nr)) nr <- nrow(sOrg)
      if(is.na(nc)) nc <- ncol(sOrg)

      if(isSparse){
  	    sDest <- new("dgCMatrix", i=as.integer({}),
                                      x=as.numeric({}),
                                      p=as.integer(rep(0, nc + 1)),
                                      Dim=as.integer(c(nr,nc))) 
      }else{
	    sDest <- array(v, dim=c(nr,nc))  
      }
      update(sDest)
    }, progress=FALSE)
    do_dest
   }else{
    stop("clone only supports darrays")
    }
  }
)

#performs deep-copy of input darray and return a darray
setMethod("clone", signature(input="dobject", nrow="missing", ncol="missing",data="missing",sparse="missing"),
  function(input) {
    if(class(input)!="darray"){stop("clone only supports darrays")}
    do_dest = darray(npartitions=input@npartitions, sparse = input@sparse)
    foreach (i, 1:numSplits(input), function(sOrg  = splits(input, i),
                                             sDest = splits(do_dest, i)){
      sDest <- sOrg
      update(sDest)
    }, progress=FALSE)
    do_dest
  }
)