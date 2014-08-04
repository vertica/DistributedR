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
       
dframe <- function(dim, blocks) {
  tryCatch({
  d <- new ("dframe", dim, blocks)
  
  foreach(i,1:npartitions(d),
          initdata <- function(dhs = splits(d,i)) {
            dhs <- data.frame()
            update(dhs)
          }, progress=FALSE)
  },error = handle_presto_exception)
  d
}

setClass("dframe", representation(dim="numeric", blocks="numeric", dimnames="list"), 
                   prototype(dim=c(0,0), blocks=c(0,0)),
                   contains=("dobject"),
                   S3methods=TRUE)

setMethod("initialize", "dframe",
          function(.Object, dim, blocks) {
             .Object <- callNextMethod(.Object, dim=dim, blocks=blocks)
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
  
  if((is.null(value[[1]]) == FALSE && length(value[[1]]) != 0) && x@dim[1] != length(value[[1]])) stop("invalid row size 'dimnames' given")
  if((is.null(value[[2]]) == FALSE && length(value[[2]]) != 0) && x@dim[2] != length(value[[2]])) stop("invalid column size 'dimnames' given")
  tryCatch({
    foreach(i,1:numSplits(x),
              function(dfs = splits(x,i),
                       rnames = value[[1]][((dobject.getoffsets(x@dim, x@blocks, i))[1]+1):((dobject.getoffsets(x@dim, x@blocks, i))[1]+dobject.getdims(x@dim, x@blocks, i)[1])],
                       cnames = value[[2]][((dobject.getoffsets(x@dim, x@blocks, i))[2]+1):((dobject.getoffsets(x@dim, x@blocks, i))[2]+dobject.getdims(x@dim, x@blocks, i)[2])]) {
              if(is.null(cnames) == FALSE && is.na(cnames) == FALSE && length(cnames)==length(names(dfs))) names(dfs) <- cnames
              if(is.null(rnames) == FALSE && is.na(rnames) == FALSE && length(rnames)==length(row.names(dfs))) row.names(dfs) <- rnames
              update(dfs)
            }, progress=TRUE)
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


