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

library(distributedR)

presto.page.rank <- function (inGraph, numRow=0, rowPerBlock=0, sparse=TRUE, iter=-1, dmp=0.85, qerr=0.001, normalized=FALSE){
  curTime = proc.time()[3]
  if(class(inGraph) != "character" && class(inGraph) != "darray_handle"){
    stop("input matrix "+class(inGraph)+" is not supported")
  }
  if(class(inGraph) == "character"){ #input graph is kept at a file with the inGraph path
    path <- inGraph
    if(numRow==0 || rowPerBlock ==0){
      stop("PrestoPageRank: If a transition matrix is loaded from a file, number of rows of the matrix and rows per block has to be specified explicitly")
    }
    inGraph <- darray(dim=c(numRow,numRow), blocks=c(rowPerBlock, numRow), sparse=sparse)  #generate darray for calculation
    cat("darray is created with full dimensions ", inGraph@dim, " block dimensions: ", inGraph@blocks, " elapsed time: ", proc.time()[3]-curTime,"\n");
    curTime = proc.time()[3]
    load.darray(inGraph, path)
    cat("loaded data to distributed arrays. elapsed time: ", proc.time()[3]-curTime,"\n")
  }else if(class(inGraph)=="darray_handle"){  #inGraph is an darray
    numRow = inGraph@dim[1]
    rowPerBlock = inGraph@blocks[1]
  }
  if(inGraph@dim[2] != inGraph@blocks[2]){
    stop("Input darray has to be row-partitioned for efficiency reason.")
  }
  if(normalized == FALSE){   # if input transition matrix is not normalized, do normalize it!!
    bc_sum <- darray(c(numSplits(inGraph), inGraph@dim[2]), c(1,inGraph@dim[2]), sparse=FALSE)  # darray of column-wise sum of each block
    foreach(i, 1:numSplits(inGraph), function(ds=splits(inGraph,i),
                                              csum=splits(bc_sum,i)){   # calculate column-sum of each block
      csum <- matrix(colSums(ds), nrow=1)
      update(csum)
    })
    csum_da <- darray(c(1, inGraph@dim[2]),c(1,inGraph@dim[2]), sparse=FALSE)
    foreach(i, 1:1, function(ds=splits(bc_sum),
                             cs=splits(csum_da)){  # aggregate column sum of all blocks
      cs <- matrix(nrow=1, data=colSums(ds))
      update(cs)
    })
    foreach(i, 1:numSplits(inGraph), function(ds=splits(inGraph,i),
                                              cs=splits(csum_da)){ #divide a column with the sum for normalization
      cs_denm<-as.numeric(cs)
      cs_denm<-ifelse(cs_denm==0, 1, cs_denm)
      ds = t(t(ds)/cs_denm) 
      update(ds)
    })
  }

  xold <- darray(dim=c(numRow,1), blocks=c(numRow,1), sparse=FALSE, data=1/numRow)
  zvec <- darray(c(numRow,1), c(numRow,1), sparse=FALSE, data=as.numeric((1-dmp)/numRow))
  curIter <- 1
  merged <- FALSE
  cat(proc.time(),"\n")
  repeat{
    if((iter>0 && curIter>iter) || (merged == TRUE)){
      cat("PageRank completed in ", curIter," iterations\n")
#      print(getpartition(xold))
      break
    }
    cat("iteration: ", curIter, "\n")
    curTime = proc.time()[3]
    xnew <- dmp*(inGraph %*% xold) + zvec  
    cat("xnew computed. elapsed time: ", proc.time()[3]-curTime, "\n")
    curTime = proc.time()[3]
    diff <- xnew - xold
    cat("diff calculation completed. elapsed time: ", proc.time()[3]-curTime, "\n")
    curTime = proc.time()[3]
    sq_norm <- distributedR::norm(diff)
    cat("norm calculation completed. 2-norm: ", sq_norm," elapsed time: ", proc.time()[3]-curTime, "\n")
    if(sq_norm < qerr){
      merged = TRUE
    }
    curIter <- curIter + 1
    xold <- xnew
  }  
  xold
}
