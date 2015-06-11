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

#Implementation of common operators that can be applied to dframes
#The aim is to make dframes as transparent as basic R data.frame
#Indrajit Roy and Kyungyong Lee (August, 2013)

is.dframe <- function(x)
    {
        if (class(x) == "dframe")
            return(TRUE)
        else
            return(FALSE)
    }

# Error with legitimate message in case of splits typo
setMethod("split", signature("dframe"), function(x,...)
    {   
        stop("\nFunction split() is not supported for dframe object. \nCheck if you wanted to use function splits() instead")
    })

.dframe_aggregation <- function (x, AGGR_FUN, na.rm = FALSE) {
  if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
  temp <- dframe(dim=c(1,npartitions(x)), blocks=c(1,1))
  foreach(i,1:npartitions(x),
    local_aggr <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm, AGGR_FUN=AGGR_FUN) {
      tryCatch({
        if (all(is.na(v)) == TRUE && all(sapply(v, is.nan)) == FALSE)
           res <<- data.frame(NA)
        else
           res <<- data.frame(AGGR_FUN(v, na.rm=flag))
      }, error=function(e){res <<- data.frame(as.character(e), stringsAsFactors=FALSE)})
      update(res)
    }, progress=FALSE)
  res <- getpartition(temp);
  if (all(lapply(res, is.character) == FALSE)== TRUE) {
    if(all(is.na(res)) == TRUE && all(sapply(res, is.nan)) == FALSE)
      return (NA)
    else
      return(AGGR_FUN(res))
  } else {
    stop(as.character(res[names(which(sapply(res, function(x) any(is.character(x)))))[1]]))
  }
}


#Max of all elements in the array
setMethod("max", signature("dframe"),function(x, na.rm = FALSE) return (.dframe_aggregation(x, max, na.rm)))

#Min of all elements in the array
setMethod("min", signature("dframe"),function(x, na.rm = FALSE) return (.dframe_aggregation(x, min, na.rm)))

#Sum of all elements in the array
setMethod("sum", signature("dframe"),function(x, na.rm = FALSE) return (.dframe_aggregation(x, sum, na.rm)))

#Column sums of a darray
# colSums, rowSums, colMeans, rowMeans of dframe will be added.

#Check if the columns of the dframe are either numeric, logical, or integer
is_dframe_numeric<-function(x){
  temp <- darray(dim=c(1,npartitions(x)), blocks=c(1,1), sparse=FALSE)
  foreach(i,1:npartitions(x),
	 localcheck <- function(v=splits(x,i),res=splits(temp,i)) {
	 allowed_types<-c("numeric","logical","integer")
	 types_present<-lapply(v,class)
         res<-matrix(all((types_present %in% allowed_types)==TRUE), nrow=1)
           update(res)
         }, progress=FALSE)
   return (!any(getpartition(temp)==FALSE))	      	   
}

#Column sums of a darray
setMethod("colSums", signature("dframe"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              #For each partition we will store the column sums as
              #a vector of (1x ncols-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")
	      
	      #First check if all columns are numeric
	      if(!is_dframe_numeric(x)) stop("colSums only defined on a data frame with numeric, integer, or logical variables")

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
#Row sums of a dframe
setMethod("rowSums", signature("dframe"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              #For each partition we will store the row sums as
              #a vector of (ncols-of-partition x 1)
              if(dims!=1) stop("only dims=1 is supported\n")

	      #First check if all columns are numeric
	      if(!is_dframe_numeric(x)) stop("rowSums only defined on a data frame with numeric, integer, or logical variables")	 
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

#Column mean of a dframe
setMethod("colMeans", signature("dframe"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              #For each partition we will store the column sums and number of 
              #rows in a col, in vectors of (1x ncols-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")

	      #First check if all columns are numeric
	      if(!is_dframe_numeric(x)) stop("colMeans only defined on a data frame with numeric, integer, or logical variables")

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

#Row mean of a dframe
setMethod("rowMeans", signature("dframe"),function(x, na.rm=FALSE, dims=1)
          {
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              #For each partition we will store the row sums and length as
              #a vector of (1 x nrows-of-partition)
              if(dims!=1) stop("only dims=1 is supported\n")

	      #First check if all columns are numeric
	      if(!is_dframe_numeric(x)) stop("rowMeans only defined on a data frame with numeric, integer, or logical variables")	 
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

#Head of the darray
head.dframe <- function(x, n=6L,...){
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              if(is.na(n)) return (0)
              n <- floor(n)   #round off fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- NULL
              #Make negative values positive
              if(n<0){
                  n <- x@dim[1]+n
             }
              if(n<=0) return (0)

              #Define function that can return a subset of lines from a split
              headOfSplit <- function(x, id, nlines, bsize){
                  #Case 1: Obtain the full split
                  if(nlines>=bsize[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  #Case 2: Obtain only a small head of the split
                  temp <- dframe(dim=c(nlines,bsize[2]), blocks=c(nlines,bsize[2]))
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

              if (length(x@dimnames[[1]]) < length(row.names(res))) {
                row.names(res) <- NULL
              }             
              return(res)
          }

#Tail of the darray
tail.dframe <- function(x, n=6L,...) {
	      if(is.invalid(x)) stop("Operation not supported on empty data-frames.")
              if(is.na(n)) return (0)
              n <- floor(n)   #round of fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- NULL
              if(n<0){
                  n <- x@dim[1]+n
              }
              if(n<0) stop ("no. rows to be skipped is larger than array extent")
              if(n==0) return (0)

              #Define function to fetch subset of the partition data
              tailOfSplit <- function(x, id, nlines, bsize){
                  if(nlines>=bsize[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  temp <- dframe(dim=c(nlines,bsize[2]), blocks=c(nlines,bsize[2]))
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

              if (length(x@dimnames[[1]]) < length(row.names(res))) {
                row.names(res) <- NULL
              }
              return(res)
          }

# Intially a dframe could be empty. Just returning the number of rows declared in the dframe dimensions is not a good.
setMethod("nrow", signature("dframe"), function(x)
    {
        #Data may be block partitioned. We only need to look at the leftmost partitions.
        nc<-ceiling(x@dim[2]/x@blocks[2])
        nr<-ceiling(x@dim[1]/x@blocks[1])
        temp <- darray(dim=c(nr,1), blocks=c(1,1), sparse=FALSE)
        #Generate index numbers for the leftmost partitions.
        lparts<-seq(1,npartitions(x),nc)
        foreach(i,1:npartitions(temp),
              localnrow <- function(v=splits(x,lparts[i]),res=splits(temp,i)) {
                  res <- matrix(nrow(v))
                  update(res)
              }, progress=FALSE)
        res <- getpartition(temp)
        return(sum(as.numeric(res)))
    })

setMethod("NROW", signature("dframe"), function(x)
    {
        return (nrow(x))
    })

# Intially a dframe could be empty. Just returning the number of cols declared in the dframe dimensions is not a good.
setMethod("ncol", signature("dframe"), function(x)
    {
        #Data may be block partitioned. We only need to look at first row of partitions, i.e., first set of partitions from left to right
        nc<-ceiling(x@dim[2]/x@blocks[2])
        nr<-ceiling(x@dim[1]/x@blocks[1])
        temp <- darray(dim=c(nc,1), blocks=c(1,1), sparse=FALSE)
        #Generate index numbers for the topmost row of partitions.
        foreach(i,1:npartitions(temp),
              localnrow <- function(v=splits(x,i),res=splits(temp,i)) {
                  res <- matrix(ncol(v))
                  update(res)
              }, progress=FALSE)
        res <- getpartition(temp)
        return(sum(as.numeric(res)))
    })

setMethod("NCOL", signature("dframe"), function(x)
    {
        return (ncol(x))
    })

## Converts in-place the specified categorical columns of a dframe to factor, considering the labels on all partitions
# DF: The input dframe
# colName: A vector of the name of the interested categorical columns
# colID: When colName is not available, column positions can be specified using a numerical vector
# trace: When it is FALSE (default) the progress of the foreach will be hidden
factor.dframe <- function(DF, colName, colID, trace=FALSE) {

    Labels <- .findLabels(DF, colName, colID, trace=trace)

    if(length(Labels$columns) != 0) {
        # distributing Levels among all the partitions and creating the factors accordingly
        foreach(i, 1:npartitions(DF), progress=trace, function(DFi=splits(DF,i), Levels=Labels$Levels, columns=Labels$columns) {
            DFi[,columns] <- lapply(columns, function(x) factor(DFi[,x], levels=Levels[[which(columns==x)]]))
            update(DFi)
        })
    }
    # no need to return the modified dframe because it is passed by reference
}

## Creates a clone of the input dframe with the specified categorical columns converted to factor, considering the labels on all partitions
# DF: The input dframe
# colName: A vector of the name of the interested categorical columns
# colID: When colName is not available, column positions can be specified using a numerical vector
# trace: When it is FALSE (default) the progress of the foreach will be hidden
as.factor.dframe <- function(DF, colName, colID, trace=FALSE) {

    Labels <- .findLabels(DF, colName, colID, trace=trace)
    outputDF <- clone(DF)
    if(length(Labels$columns) != 0) {
        # distributing Levels among all the partitions and creating the factors accordingly
        foreach(i, 1:npartitions(outputDF), progress=trace, function(DFi=splits(outputDF,i), Levels=Labels$Levels, columns=Labels$columns) {
            DFi[,columns] <- lapply(columns, function(x) factor(DFi[,x], levels=Levels[[which(columns==x)]]))
            update(DFi)
        })
    }
    outputDF
}

## Finds the list of the labels on the categorical columns of a dframe
# DF: The input dframe
# colName: A vector of the name of the interested categorical columns
# colID: When colName is not available, column positions can be specified using a numerical vector
levels.dframe <- function(DF, colName, colID, trace=FALSE) {

    Labels <- .findLabels(DF, colName, colID, trace=trace)

    Labels
}

# An intermediate function to avoid code redundancy
.findLabels <- function(DF, colName, colID, trace=FALSE) {

    if(!is.dframe(DF) || is.invalid(DF)) stop("DF must be a valid dframe", call.=FALSE)
    if(any (partitionsize(DF)[,2] != ncol(DF)) ) stop("DF must be partitioned row-wise")

    nparts <- npartitions(DF)

    if(!missing(colName) && !missing(colID))
        warning("colID is ignored when colName is specified", call.=FALSE)

    if(!missing(colName)) {
        if(! is.character(colName) || NCOL(colName)!=1)
            stop("colName must be a charactor array", call.=FALSE)
        if(! all(colName %in% colnames(DF)))
            stop("there are names in colName that are not available in the column names of DF", call.=FALSE)
        columns <- unique(colName)
    } else if(!missing(colID)) {
        if(! is.numeric(colID) || NCOL(colID)!=1)
            stop("colID must be a numeric array", call.=FALSE)
        if(any( (colID > ncol(DF)) | (colID < 1) ))
            stop("Numbers in colID must be between 1 and the number of columns in DF", call.=FALSE)
        columns <- unique(floor(colID))
    } else { # when missing both colName and colID, all the columns of types 'character', 'factor', and 'logical' will be selected
        dcolID <- dlist(nparts)
        # all the partitions of DF must have the same type by definition
        foreach(i, 1:1, progress=trace, function(DFi=splits(DF,i), li=splits(dcolID,i)) {
            li <- list(which (sapply(1:ncol(DFi), function(x) is.factor(DFi[,x]) || is.character(DFi[,x]) || is.logical(DFi[,x]))))
            update(li)
        })
        columns <- unlist(getpartition(dcolID,1))
        if(length(columns) == 0) {
            warning("No change occured because no column of types 'character', 'factor', or 'logical' was found")
            return(list(Levels=list(), columns=columns))
        }
        if(trace) 
            cat(paste("Number of columns found of types 'character', 'factor', or 'logical':", length(columns),"\n"))
    }
    
    # finding different categories on each partition
    dlabels <- dlist(nparts)
    foreach(i, 1:nparts, progress=trace, function(DFi=splits(DF,i), li=splits(dlabels,i), columns=columns) {
        if(! all( sapply(columns, function(x) is.character(DFi[,x])) | sapply(columns, function(x) is.factor(DFi[,x])) | sapply(columns, function(x) is.logical(DFi[,x])) )) {
            ll <- "all of the specified columns must be of type 'character', 'factor', or 'logical'"
            class(ll) <- "err_message"
            li <- list(ll)
        } else {
            li <- lapply(columns, function(x) levels(factor(DFi[,x])))
        }
        update(li)
    })
    
    Labels <- getpartition(dlabels)
    if(any(sapply(Labels, function(x) inherits(x, "err_message"))))
        stop("all of the specified columns must be of type 'character', 'factor', or 'logical'", call.=FALSE)

    ncolumns <- length(columns)
    Levels <- lapply(1:ncolumns, function(i) unique(unlist(Labels[seq(i,ncolumns * nparts,ncolumns)])))

    list(Levels=Levels, columns=columns)
}

## Converts in-place the specified categorical columns of a dframe from factor to their labels of type character
# DF: The input dframe
# colName: A vector of the name of the interested categorical columns
# colID: When colName is not available, column positions can be specified using a numerical vector
# trace: When it is FALSE (default) the progress of the foreach will be hidden
unfactor.dframe <- function(DF, colName, colID, trace=FALSE) {

    if(!is.dframe(DF) || is.invalid(DF)) stop("DF must be a valid dframe", call.=FALSE)
    if(any (partitionsize(DF)[,2] != ncol(DF)) ) stop("DF must be partitioned row-wise")

    nparts <- npartitions(DF)

    if(!missing(colName) && !missing(colID))
        warning("colID is ignored when colName is specified", call.=FALSE)

    if(!missing(colName)) {
        if(! is.character(colName) || NCOL(colName)!=1)
            stop("colName must be a charactor array", call.=FALSE)
        if(! all(colName %in% colnames(DF)))
            stop("there are names in colName that are not available in the column names of DF", call.=FALSE)
        columns <- unique(colName)
    } else if(!missing(colID)) {
        if(! is.numeric(colID) || NCOL(colID)!=1)
            stop("colID must be a numeric array", call.=FALSE)
        if(any( (colID > ncol(DF)) | (colID < 1) ))
            stop("Numbers in colID must be between 1 and the number of columns in DF", call.=FALSE)
        columns <- unique(floor(colID))
    } else { # when missing both colName and colID, all the columns of types 'character', 'factor', and 'logical' will be selected
        dcolID <- dlist(nparts)
        # all the partitions of DF must have the same type by definition
        foreach(i, 1:1, progress=trace, function(DFi=splits(DF,i), li=splits(dcolID,i)) {
            li <- list(which (sapply(1:ncol(DFi), function(x) is.factor(DFi[,x]) || is.character(DFi[,x]) || is.logical(DFi[,x]))))
            update(li)
        })
        columns <- unlist(getpartition(dcolID,1))
        if(length(columns) == 0) {
            warning("No change occured because no column of types 'character', 'factor', or 'logical' was found")
        }
        if(trace) 
            cat(paste("Number of columns found of types 'character', 'factor', or 'logical':", length(columns),"\n"))
    }

    if(length(columns) != 0) {
        # finding different categories on each partition
        derr <- dlist(nparts)
        foreach(i, 1:nparts, progress=trace, function(DFi=splits(DF,i), li=splits(derr,i), columns=columns) {
            if(! all( sapply(columns, function(x) is.character(DFi[,x])) | sapply(columns, function(x) is.factor(DFi[,x])) )) {
                ll <- "all of the specified columns must be of type 'character', 'factor', or 'logical'"
                class(ll) <- "err_message"
                li <- list(ll)
            } else {
                li <- list("no error")
                DFi[,columns] <- lapply(columns, function(x) as.character(DFi[,x]))
            }
            update(li)
            update(DFi)
        })
        
        errors <- getpartition(derr)
        if(any(sapply(errors, function(x) inherits(x, "err_message"))))
            stop("all of the specified columns must be of type 'character', 'factor', or 'logical'", call.=FALSE)
        # no need to return the modified dframe because it is passed by reference
    }
}

