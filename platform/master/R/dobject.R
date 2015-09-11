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
                                  dobject_ptr="Rcpp_DistributedObject", npartitions="numeric", subtype="character", distribution_policy="character"), 
                   prototype(dim=c(0,0), blocks=c(0,0),
                             name="", dobject_ptr=NULL, npartitions=c(0,0), subtype="STD", distribution_policy="roundrobin"),
                   S3methods=TRUE)

#subtype can be one of the three. It is used as metadata when performing checks related to updates
# (1)  STD    (standard dobject. Nothing special)
# (2)  UNINIT_DECLARED   (uninitialized object, e.g., empty darray)
# (3)  FLEX_DECLARED  (object with flexible or unequal partition sizes, e.g., dlist or darrays)

setMethod("initialize", "dobject",
          function(.Object, dim=c(0,0), blocks=c(0,0), name=NA, npartitions = NA, subtype="STD", distribution_policy="roundrobin") {
            pm <- get_pm_object()
            if(class(dim)!="numeric" && class(dim)!="integer") stop("dim should be numeric")
	    if(class(blocks)!="numeric" && class(blocks)!="integer") stop("blocks should be numeric")
            if(length(dim)!=2||length(blocks)!=2) stop("length(dim) and length(block) should be two")
            if(!all(dim==floor(dim)))stop("dim should be integral values")
            if(!all(blocks==floor(blocks)))stop("blocks should be integral values")
            if(all(blocks<=.Machine$integer.max) == FALSE) stop(paste("block size should be less than",.Machine$integer.max))

            if(dim[1]<=0||dim[2]<=0||blocks[1]<=0||blocks[2]<=0) stop("dim and blocks should be larger than 0")
            if(dim[1]<blocks[1]||dim[2]<blocks[2]) stop("blocks should be smaller than dim")
	    if(all(is.na(npartitions))) npartitions <-  c(ceiling(dim[1]/blocks[1]), ceiling(dim[2]/blocks[2]))
	    #Check if there are too many partitions. Stop if exceeds hard limit of 20K splits
            #nblocks <-  ceiling(dim[1]/blocks[1]) * ceiling(dim[2]/blocks[2])
	    nblocks <-  npartitions[1]*npartitions[2]
            nexecutors <- sum((distributedR_status())$Inst)

            if(nblocks > 100000 || nblocks > (nexecutors*(max(nexecutors,50)))){
                stop("Exceeded max limit of partitions on this cluster. Use fewer partitions. E.g., 1x-5x of #cores (",nexecutors,") instead of ",nblocks,".\n")
            }
            if(nblocks > 5*nexecutors){
                warning(paste("Too many partitions may degrade performance. Use fewer partitions. E.g., 1x-5x of #cores (",nexecutors,") instead of ",nblocks,".",sep=""))
            }

            supported_distributions <- c("roundrobin") #c("roundrobin", "random", "custom")
            distribution_policy <- as.character(tolower(distribution_policy))
            if(!distribution_policy %in% supported_distributions && distribution_policy!="custom" && distribution_policy!="ddc")
              stop("Invalid distribution policy")

            .Object@dimnames = list(NULL,NULL)
            .Object@dim = as.vector(dim)
            .Object@blocks = as.vector(blocks)
	    .Object@subtype = subtype
	    .Object@npartitions = npartitions
            .Object@distribution_policy = distribution_policy

            if (is.na(name)) {
              .Object@name = dobject_name()
            } else {
              .Object@name = paste(.get.shm.prefix(), name, sep="")
            }

            # dobj_ptr can be overwritten in the inherited class.
            .Object@dobject_ptr = new (DistributedObject, pm, as.character(class(.Object)), subtype, distribution_policy)
            if(.Object@dobject_ptr$create(.Object@name, dim, blocks) == FALSE){stop("Failed to create dobject")}

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
  if((is.null(value[[1]]) == FALSE && length(value[[1]]) != 0) && dim(x)[1] != length(value[[1]])) stop("invalid row size given in 'dimnames'")
  if((is.null(value[[2]]) == FALSE && length(value[[2]]) != 0) && dim(x)[2] != length(value[[2]])) stop("invalid column size given in 'dimnames'")
  x@dimnames[[1]] <- value[[1]]
  x@dimnames[[2]] <- value[[2]]
  x
}

# get (row,col) offsets of split with index
dobject.getoffsets <- function(fulldim, blockdim, index) {
  blocks <- ceiling(fulldim/blockdim)
  block.r <- (index-1)%/%blocks[2]+1
  block.c <- (index-1)%%blocks[2]+1
  return(c((block.r-1)*blockdim[1], (block.c-1)*blockdim[2]))
}
 
# get (r,c) offsets of partition with index
dobject.getPartitionOffsets <- function(obj, index) {
 ret<-NULL
 blocks <- npartitions2D(obj)
 block.r <- (index-1)%/%blocks[2]+1
 block.c <- (index-1)%%blocks[2]+1

 if(obj@subtype == "FLEX_DECLARED"){
 #For flex objects let's first figure out the parition sizes 
  psize<-partitionsize(obj)
  #For x side offset sum all partitions to the left
  coffset<-0
  if(block.c>1){coffset<-sum(psize[1:(block.c-1),2])}
  #For x side offset sum all partitions to the top
  roffset<-0
  i<-0
  while(i < (block.r-1)){
    roffset<-roffset+psize[1+(i*blocks[2]),1]
    i<-i+1
  }
  ret<-c(roffset, coffset)
 }else{
  fulldim<-dim(obj)
  blockdim<-obj@blocks
  ret<-c((block.r-1)*blockdim[1], (block.c-1)*blockdim[2])
 }
 return (ret)
}

# get (r,c) offsets of all partitions
dobject.getAllOffsets <- function(obj) {
  blocks <- npartitions2D(obj)
 ret<-NULL
 if(obj@subtype == "FLEX_DECLARED"){
  psize<-partitionsize(obj)
  coffsets<-0
  if(blocks[2]>1){coffsets<-c(0,cumsum(psize[1:(blocks[2]-1),2]))}
  roffsets<-0
  if(blocks[1]>1) {roffsets<-c(0,cumsum(psize[(1:(blocks[1]-1))*blocks[2],1]))}
  #return a matrix which has offsets for each partition
  ret<- (cbind(rep(roffsets,each=length(coffsets)),rep(coffsets,length(roffsets))))
 } else{
  fulldim<-dim(obj)
  blockdim<-obj@blocks
  coffsets<-0
  if(blocks[2]>1) { coffsets<-c(0,(1:(blocks[2]-1))*blockdim[2])}
  roffsets<-0
  if(blocks[1]>1) { roffsets<-c(0,(1:(blocks[1]-1))*blockdim[1])}
  ret<- (cbind(rep(roffsets,each=length(coffsets)),rep(coffsets,length(roffsets))))	
 }
  return (ret)
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
      return(x@dobject_ptr$parts_sizes())
    } else {
     #For non-flex objects we will reuse information from dimensions
     temp<-NULL
     fulldim<-dim(x)
     blockdim<-x@blocks
     nblocks<-npartitions2D(x)

     lastrsize<-blockdim[1]
     if (fulldim[1]%%blockdim[1] != 0) {
      lastrsize <- fulldim[1]%%blockdim[1]
     }
    
     lastcsize<-blockdim[2]
     if (fulldim[2]%%blockdim[2] != 0) {
      lastcsize <- fulldim[2]%%blockdim[2]
     }
     rsizes<-c(rep(blockdim[1],(nblocks[1]-1)),lastrsize)
     csizes<-c(rep(blockdim[2],(nblocks[2]-1)),lastcsize)
     temp<- (cbind(rep(rsizes,each=length(csizes)),rep(csizes,length(rsizes))))
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
      return(t(as.matrix(x@dobject_ptr$parts_sizes()[index,])))
    } else {
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
  if(is.invalid(input)) stop("Operation not supported on empty arrays and data-frames.")
  if(!is.na(nrow) && !is.na(ncol)) stop("Only one of nrow and ncol should be specified")
  data_class<-class(data)
  if(!is.na(nrow)){
    if(!is.numeric(nrow)) stop("nrow should be numeric")
    nrow<-as.integer(nrow)
    if(nrow<=0) stop("nrow should be positive")
  }
  if(!is.na(ncol)){
   if(!is.numeric(ncol)) stop("ncol should be numeric")
   ncol<-as.integer(ncol)
   if(ncol<=0) stop("ncol should be positive")
  }
  do_dest <- NULL
  pindex <- NULL
  nvalue<-dimnames(input)
 
  if(class(input)=="darray"){
    if(is.na(sparse)) sparse <- input@sparse  
    if(sparse && (data!=0)) stop("Initializing a sparse matrix with all non-zero elements makes it dense. Set 'data=0'")
    if(data_class != "numeric" && data_class != "integer" && data_class != "logical") stop("Argument 'data' should be a number or logical value.")
    if(length(data)>1) stop("Argument 'data' should be a number or logical value.")
    if(is.na(nrow) && is.na(ncol)){
       #Keep all the array, but change values to data
       if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
        do_dest = darray(dim=input@dim, blocks=input@blocks, sparse = sparse)
	}else{
        do_dest = darray(npartitions=input@npartitions, sparse = sparse)
	}
	pindex<-1:npartitions(input)
        #Set dimension names only if all rows and cols of input is copied
	if(!is.null(nvalue)){do_dest@dimnames<-nvalue}
    }else{ 
    	   if(is.na(nrow)){
	   	#Retain leftmost row partitions (i.e. left vertical stripe) and change the number of cols in these partitions.
		if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
		        do_dest = darray(dim=c(input@dim[1],ncol), blocks=c(input@blocks[1],ncol), sparse = sparse)
		}else{
			do_dest = darray(npartitions=c(input@npartitions[1],1), sparse = sparse)
		}
		pindex<-seq(1,npartitions(input),input@npartitions[2])
	    }else{
		#Retain top column partitions (i.e. top horizontal stripe) and change the number of rows in these partitions.
		if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
		        do_dest = darray(dim=c(nrow, input@dim[2]), blocks=c(nrow, input@blocks[2]), sparse = sparse)
		}else{
			do_dest = darray(npartitions=c(1,input@npartitions[2]), sparse = sparse) 
		}
		pindex<-1:input@npartitions[2]	
    	    }
    }

    foreach (i, 1:npartitions(do_dest), function(sOrg  = splits(input, pindex[i]),
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

   }else if(class(input) =="dframe"){
    if(data_class != "numeric" && data_class != "integer" && data_class != "logical" && data_class !="character") stop("Argument 'data' should be a number, character, or logical value.")
    if(length(data)>1) stop("Argument 'data' should be a number, character, or logical value.")
    if(!is.na(sparse)) stop("Argument 'sparse' cannot be used with dframes.")
    if(is.na(nrow) && is.na(ncol)){
       #Keep all the dframe, but change values to data
       if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
        do_dest = dframe(dim=input@dim, blocks=input@blocks)
       }else{
        do_dest = dframe(npartitions=input@npartitions)
	}
	pindex<- 1: npartitions(input)
    }else{
        if(is.na(nrow)){
		#Retain leftmost row partitions (i.e. left vertical stripe) and change the number of cols in these partitions.
		if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
			do_dest = dframe(dim=c(input@dim[1],ncol), blocks=c(input@blocks[1],ncol))
        	}else{
			do_dest = dframe(npartitions=c(input@npartitions[1],1))
		}
		pindex<-seq(1,npartitions(input),input@npartitions[2])
	}else{
		#Retain top column partitions (i.e. top horizontal stripe) and change the number of rows in these partitions.
		if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
			do_dest = dframe(dim=c(nrow, input@dim[2]), blocks=c(nrow, input@blocks[2]))
        	}else{
			do_dest = dframe(npartitions=c(1,input@npartitions[2]))
		}
		pindex<-1:input@npartitions[2]	
    	}
    }

    foreach (i, 1:npartitions(do_dest), function(sOrg  = splits(input, pindex[i]),
                                             sDest = splits(do_dest, i), nr=nrow, nc=ncol, v =data){
      if(is.na(nr)) nr <- nrow(sOrg)
      if(is.na(nc)) nc <- ncol(sOrg)
      X1<-matrix(v, nrow=nr, ncol=nc)
      sDest <- data.frame(X1)
      update(sDest)
    }, progress=FALSE)
    #Set dimension names only if all rows and cols of input is copied
    if(!is.null(nvalue) && is.na(nrow) && is.na(ncol)){dimnames(do_dest)<-nvalue }
   }else{
    stop("clone only supports darrays and dframes")
   }
  do_dest
  }
)

#performs deep-copy of input object and return a dobject
setMethod("clone", signature(input="dobject", nrow="missing", ncol="missing",data="missing",sparse="missing"),
  function(input) {
    if(is.invalid(input)) stop("Operation not supported on empty arrays and data-frames.")
    do_dest<-NULL
    if(class(input)=="darray"){ 
       if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
        do_dest = darray(dim=input@dim, blocks=input@blocks, sparse = input@sparse)
	}else{
        do_dest = darray(npartitions=input@npartitions, sparse = input@sparse)
	}
    } else if(class(input)=="dframe"){ 
       if(input@subtype=="STD" || input@subtype=="UNINIT_DECLARED"){
        do_dest = dframe(dim=input@dim, blocks=input@blocks)
       }else{
        do_dest = dframe(npartitions=input@npartitions)
	}
    } else {stop("clone only supports darrays and dframes")}

    foreach (i, 1:numSplits(input), function(sOrg  = splits(input, i),
                                             sDest = splits(do_dest, i)){
      sDest <- sOrg
      update(sDest)
    }, progress=FALSE)
    nvalue<-dimnames(input)
    if(!is.null(nvalue)){do_dest@dimnames<-nvalue}
    do_dest
  }
)

#Check if the distributed object is invalid. An object is invalid if the user has not yet written contents to the 
#object which was declared empty (via argument 'empty) or a flexible dobject (via npartitions)

setGeneric("is.invalid", function(x) standardGeneric("is.invalid"))
setMethod("is.invalid", signature("dobject"),
   function(x) {
        x@dobject_ptr$is_object_invalid()
   })
