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
       
dframe <- function(dim=NA, blocks=NA, npartitions=NA, distribution_policy="roundrobin") {
  tryCatch({
  #stype="UNINIT_DECLARED"
  stype="STD"
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
  }

  d <- new ("dframe", dim, blocks, npartitions, subtype=stype, distribution_policy=distribution_policy)

  nExecutors <- sum(distributedR_status()$Inst)  

  indices <- function(i,npartitions,nExecutors){
    as.list(seq(from=i,to=npartitions,by=nExecutors))
  }

  range = 1:(min(npartitions(d),nExecutors))

  success <- foreach(i,range,
          initdata <- function(dhs = splits(d,indices(i,npartitions(d),nExecutors)),
	  	      		 indexes = indices(i,npartitions(d),nExecutors),
                                 fulldim = d@dim,
                                 blockdim = d@blocks,
                                 ii = i,
				 type= stype) {
	    if(type=="FLEX_DECLARED"){
		dhs <- lapply(1:length(dhs),function(x) {data.frame()}) #Empty data frame
	    }else{
                getdim <- function(x) {
                  dobject.getdims(fulldim, blockdim, indexes[[x]])
                }
                dhs <- lapply(1:length(dhs),function(x) {
                dim <- getdim(x)
                data.frame(matrix(data=0, nrow=dim[1], ncol=dim[2]))})
	    }
            update(dhs)
          }, progress=FALSE)
  },error = handle_presto_exception)
  if(!success) { rm(d); gc(); NULL }
  else d
}

setClass("dframe", representation(dim="numeric", blocks="numeric", dimnames="list", distribution_policy="character"), 
                   prototype(dim=c(0,0), blocks=c(0,0)),
                   contains=("dobject"),
                   S3methods=TRUE)

setMethod("initialize", "dframe",
          function(.Object, dim, blocks, npartitions, subtype, distribution_policy) {
             .Object <- callNextMethod(.Object, dim=dim, blocks=blocks, npartitions=npartitions, subtype=subtype, distribution_policy=distribution_policy)
#            .Object@names = NA
#            .Object@row.names = NA
            .Object
          })

`dimnames<-.dframe` <- function(x, value) {
  if(!is.list(value) || length(value) != 2) stop("invalid type 'dimnames' given")
  value[[1]] <- as.character(value[[1]])
  value[[2]] <- as.character(value[[2]])
  x@dimnames[[1]] <- value[[1]]
  x@dimnames[[2]] <- value[[2]]
  
  if((is.null(value[[1]]) == FALSE && length(value[[1]]) != 0) && dim(x)[1] != length(value[[1]])) stop("invalid row size given in 'dimnames'")
  if((is.null(value[[2]]) == FALSE && length(value[[2]]) != 0) && dim(x)[2] != length(value[[2]])) stop("invalid column size given in 'dimnames'")
  poffsets<-dobject.getAllOffsets(x)
  psize<-partitionsize(x)

  getnames <- function(i, type) {
     if(type=="row" && length(value[[1]])!=0)
         return(value[[1]][(poffsets[i,1]+1):(poffsets[i,1]+psize[i,1])])
     else if(type=="column" && length(value[[2]])!=0)
         return(value[[2]][(poffsets[i,2]+1):(poffsets[i,2]+psize[i,2])])
     else
         return(NA)
  }

  tryCatch({
    foreach(i,1:numSplits(x),
              function(dfs = splits(x,i),
		       rnames = getnames(i, "row"),
		       cnames = getnames(i, "column")) {
              if(is.null(cnames) == FALSE && is.na(cnames) == FALSE)
                 if(length(cnames)==length(names(dfs))) names(dfs) <- cnames
              if(is.null(rnames) == FALSE && is.na(rnames) == FALSE)
                 if(length(rnames)==length(row.names(dfs))) row.names(dfs) <- rnames
              update(dfs)
            }, progress=FALSE)
  },error = handle_presto_exception)
  x
}

          
setGeneric("dframe_set_values", function(df, data) standardGeneric("dframe_set_values"))
setMethod("dframe_set_values", signature("dframe", "numeric"),
  function(df, data) {
    tryCatch({
      foreach(i,1:numSplits(df),
              initdata <- function(dhs = splits(df,i),
                                   fulldim = df@dim,
                                   blockdim = df@blocks,
                                   ii = i,
                                   data=data
                                   ) {
                dim=dobject.getdims(fulldim, blockdim, ii)
                dhs <- data.frame(matrix(data,dim[1],dim[2]))
                update(dhs)
              }, progress=FALSE)
      },error = handle_presto_exception)
      df
  })

setMethod("+", signature("dframe", "numeric"),
	     function(e1,e2) {
	     if(is.invalid(e1)) stop("Operation not supported on empty dframes")
	     c <- dframe(npartitions=npartitions2D(e1))
	     success <- foreach(i, 1:npartitions(e1),
	     function(a = splits(e1,i),
		      b = e2,
		      cs = splits(c,i)){
		cs <- a+b
		update(cs)
		})
	     if(!success) {rm(c); gc(); NULL}
	     else c
})

setMethod("+", signature("numeric", "dframe"),
	       function(e1, e2) {
	       return (e2+e1)
})

setMethod("+", signature("dframe","dframe"),
	       function(e1, e2){
	       if(is.invalid(e1) || is.invalid(e2)) stop("Operation not suported on empty arrays.")
	       if(!all(dim(e1)==dim(e2))){
		stop("non-conformable dframes. check dimensions of the input dframes")
	       }
		
	       e1_psize<-partitionsize(e1)
	       e2_psize<-partitionsize(e2)		
	       if(!all(e1_psize==e2_psize)){
                stop("non-conformable partitions of dframes. Partition sizes are not same for the dframes.")
               }

	       c<-dframe(npartition=npartitions2D(e1))
	       success <- foreach(i, 1:npartitions(e1), 
	       	       	  	     function(a = splits(e1,i),
				     	      b = splits(e2,i),
					      cs = splits(c,i)){
			cs <- a+b
			update(cs)
		})
		if(!success) { rm(c); gc(); NULL }
		else c
})

setMethod("-", signature("dframe", "numeric"),
	     function(e1,e2) {
	     e2 <- (-1*e2)
	     return(e2 + e1)	      
})

setMethod("-", signature("numeric", "dframe"),
	     function(e1,e2) {
	      if(is.invalid(e2)) stop("Operation not supported on empty dframes")
	      c <- dframe(npartitions=npartitions2D(e2))
	      success <- foreach(i, 1:npartitions(e2),
	             function(a = splits(e2,i),
	       		b = e1,
			cs = splits(c,i)){
		cs <- b-a
		update(cs)
	      })
	      if(!success) {rm(c); gc(); NULL}
	      else c   
})

setMethod("-", signature("dframe","dframe"),
	       function(e1, e2){
	       if(is.invalid(e1) || is.invalid(e2)) stop("Operation not suported on empty arrays.")
	       if(!all(dim(e1)==dim(e2))){
		stop("non-conformable dframes. check dimensions of the input dframes")
		}
		
		e1_psize<-partitionsize(e1)
		e2_psize<-partitionsize(e2)		
		if(!all(e1_psize==e2_psize)){
             	 stop("non-conformable partitions of dframes. Partition sizes are not same for the dframes.")
           	}

		c<-dframe(npartition=npartitions2D(e1))
		success <- foreach(i, 1:npartitions(e1), 
			function(a = splits(e1,i),
				 b = splits(e2,i),
				 cs = splits(c,i)){
			cs <- a-b
			update(cs)
		})
		
		if(!success) { rm(c); gc(); NULL }
		else c
})

# return subset of data.frame given the block size and index
# the block is counted from left to right and then top to down
get_sub_dataframe <- function(dataframe, block, index){

  nrow = dim(dataframe)[1]
  ncol = dim(dataframe)[2]
  blocks_per_row = ceiling(ncol/block[2])
  if(index > blocks_per_row*(ceiling(nrow/block[1]))){
         stop("index out of range")
  }
  b_row_idx = 1+(block[1])*(floor((index-1)/blocks_per_row)) #begin row index of submatrix
  b_col_idx = 1+(block[2])*((index-1)%%blocks_per_row)  #begin column index of submatrix
  e_row_idx = ifelse(b_row_idx+block[1]-1<nrow, b_row_idx+block[1]-1, nrow)
  e_col_idx = ifelse(b_col_idx+block[2]-1<ncol, b_col_idx+block[2]-1, ncol)

  return (data.frame(dataframe[b_row_idx:e_row_idx, b_col_idx:e_col_idx]))
}

setGeneric("as.dframe", function(input, blocks) standardGeneric("as.dframe"))

setMethod("as.dframe", signature(input="data.frame", blocks="missing"),
  function(input, blocks) {
    ninst<-sum(distributedR_status()$Inst)
    blocks<-dim(input)
    blocks[1]<-ceiling(blocks[1]/ninst)
    as.dframe(input, blocks)
})

setMethod("as.dframe", signature(input="data.frame",blocks="numeric"),
 function(input,blocks){
 
  out_dobject <- dframe(dim(input),blocks)

  if(! is.null(out_dobject)){
      foreach(i, 1:npartitions(out_dobject), copy<-function(idx=i,
						df=get_sub_dataframe(input,blocks,i),
						ddf=splits(out_dobject,i)){
    ddf<- df
    update(ddf)
      },progress=FALSE)
 
   if(is.null(dimnames(input)) == FALSE)
       dimnames(out_dobject) <- dimnames(input)
   }

   return (out_dobject)
})

setMethod("as.dframe", signature(input="matrix", blocks="numeric"),
function(input, blocks){
 df <- data.frame(input)
 return (as.dframe(df,blocks))
})

setMethod("as.dframe",signature(input="matrix", blocks="missing"), 
function(input, blocks){
    ninst<-sum(distributedR_status()$Inst)
    blocks<-dim(input)
    blocks[1]<-ceiling(blocks[1]/ninst)
    as.dframe(input, blocks)
})
