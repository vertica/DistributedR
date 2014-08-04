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
  temp <- dframe(dim=c(1,npartitions(x)), blocks=c(1,1))
  foreach(i,1:npartitions(x),
    local_aggr <- function(v=splits(x,i),res=splits(temp,i), flag=na.rm, AGGR_FUN=AGGR_FUN) {
      tryCatch({
        res <<- data.frame(AGGR_FUN(v, na.rm=flag))
      }, error=function(e){res <<- data.frame(as.character(e), stringsAsFactors=FALSE)})
      update(res)
    }, progress=FALSE)
  res <- getpartition(temp);
  if (all(lapply(res, is.character) == FALSE)== TRUE) {
    return(AGGR_FUN(res))
  } else {
    return (as.character(res[names(which(sapply(res, function(x) any(is.character(x)))))[1]]))
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

#Head of the darray
head.dframe <- function(x, n=6L,...){
              if(is.na(n)) return (0)
              n <- floor(n)   #round off fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- 0
              #Make negative values positive
              if(n<0){
                  n <- x@dim[1]+n
             }
              if(n<=0) return (0)

              #Define function that can return a subset of lines from a split
              headOfSplit <- function(x, id, nlines){
                  #Case 1: Obtain the full split
                  if(nlines>=x@blocks[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  #Case 1: Obtain only a small head of the split
                  temp <- dframe(dim=c(nlines,x@blocks[2]), blocks=c(nlines,x@blocks[2]))
                  foreach(i,id:id,
                          localhead <- function(v=splits(x,i),res=splits(temp,1), nl=nlines) {
                              res <- head(v,nl)
                              update(res)
                          }, progress=FALSE)
                  return (getpartition(temp))
              }
              
              nrsplits <- min(ceiling(n/x@blocks[1]), num.splits[1])
              nr <- 1
              nprocessed <- 0
              while(nr <= nrsplits){
                 baseid <- 1 + ((nr-1)*num.splits[2])
                 temp <- headOfSplit(x, baseid, n-nprocessed)
                  #For each row stitch together column partitions (left to right)
                  nc <- 1
                  while(nc < num.splits[2]){
                      temp <- cbind(temp, headOfSplit(x, baseid+nc, n-nprocessed))
                      nc <- nc+1
                  }
                  #Now bind the partitions via rows (from top to bottom)
                  if(nr==1){
                      res <- temp
                  }else{
                      res <- rbind(res, temp)
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
              if(is.na(n)) return (0)
              n <- floor(n)   #round of fractions
              num.splits <- ceiling(x@dim/x@blocks)
              res <- 0
              if(n<0){
                  n <- x@dim[1]+n
              }
              if(n<0) stop ("no. rows to be skipped is larger than array extent")
              if(n==0) return (0)

              #Define function to fetch subset of the partition data
              tailOfSplit <- function(x, id, nlines){
                  if(nlines>=x@blocks[1]){
                      return (getpartition(x,id))
                  }
                  if(nlines<=0) stop("trying to fetch non-positive number of rows")
                  temp <- dframe(dim=c(nlines,x@blocks[2]), blocks=c(nlines,x@blocks[2]))
                  foreach(i,id:id,
                          localtail <- function(v=splits(x,i),res=splits(temp,1), nl=nlines) {
                              res <- tail(v,nl)
                              update(res)
                          }, progress=FALSE)
                  return(getpartition(temp))
                  
              }
              
              #Rows that should be skipped from the top
              nskip <- max(x@dim[1]-n,0)
              #Starting row block in a 2D view
              rid <- 1+floor(nskip/x@blocks[1])
              nr <- num.splits[1]
              nprocessed <- 0
              #Lets build the result from the bottom
              while(nr >= rid){
                  baseid <- 1 + ((nr-1)*num.splits[2])
                  temp <- tailOfSplit(x, baseid, n-nprocessed)
                  #For each row stitch together column partitions (left to right)
                  nc <- 1
                  while(nc < num.splits[2]){
                      tos <- tailOfSplit(x, baseid+nc, n-nprocessed)
                      temp <- cbind(temp, tos)
                      #temp <- cbind(temp, getpartition(x, baseid+nc))
                      nc <- nc+1
                  }
                  #Now bind the blocks via rows (from bottom to top)
                  if(nr==num.splits[1]){
                      res <- temp
                  }else{
                      res <- rbind(temp, res)
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
        return(sum(res))
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
        return(sum(res))
    })

setMethod("NCOL", signature("dframe"), function(x)
    {
        return (ncol(x))
    })
