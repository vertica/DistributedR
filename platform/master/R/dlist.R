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

##
# dlist.R : Definition of distributed lists
# @partitions : Number of parts list should be partitioned into
# block size is always (1*1) while dimension is is (#dlist partitions*1)
##

require(Rcpp)

NAMESPACE <- environment()
       
dlist <- function(partitions) {
  if (missing(partitions)) stop("argument \"partitions\" is missing, with no default")
  if (length(partitions) > 1) stop("argument 'partitions' should be of length one")
  if (!is.numeric(partitions)) stop("argument 'partitions' should be numeric") 
  else if (round(partitions)!=partitions) stop("argument 'partitions' should be an integer")
  
  blocks <- c(1, 1)
  dim <- c(partitions, 1)
  tryCatch({
  d <- new ("dlist", dim, blocks)

  foreach(i,1:npartitions(d),
          initdata <- function(dhs = splits(d,i)) {
            dhs <- list()
            update(dhs)
          }, progress=FALSE)
  },error = handle_presto_exception)
  d
}

setClass("dlist", representation(dim="numeric", blocks="numeric", dimnames="list"), 
                   prototype(dim=c(0,0), blocks=c(0,0)),
                   contains=("dobject"),
                   S3methods=TRUE)

setMethod("initialize", "dlist",
          function(.Object, dim, blocks) { 
             .Object <- callNextMethod(.Object, dim=dim, blocks=blocks)
            .Object
          })

# Error with legitimate message in case of splits typo
setMethod("split", signature("dlist"), function(x,...) {   
        stop("\nFunction split() is not supported for dlist object. \nCheck if you wanted to use function splits() instead")
    })

#Check is object is a darray
is.dlist <- function(x) {   
        if (class(x) == "dlist")
            return(TRUE)
        else
            return(FALSE)
    } 
          
setGeneric("dlist_set_split", function(dList, index, ldata) standardGeneric("dlist_set_split"))
setMethod("dlist_set_split", signature("dlist", "numeric", "vector"),
  function(dList, index, ldata) {
    tryCatch({
      foreach(i,index,
              initdata <- function(dhs = splits(dList,i),
                                   ldata = list(ldata)
                                   ) {
                dhs <- ldata
                update(dhs)
              }, progress=FALSE)
      },error = handle_presto_exception)
      dList
  })


#For a list R returns null as the dimension. We may change it in future.
setMethod("partitionsize", signature("dlist","numeric"),
  function(x, index) {
    return (NULL)
})

setMethod("partitionsize", signature("dlist","missing"),
  function(x, index) {
    return (NULL)
})
