# Copyright [2013] Hewlett-Packard Development Company, L.P.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#########################################################
#
#  File hpdpagerank.R
#  Distributed pagerank 
#  
#
#########################################################

#   dgraph: A darray which represents an adjacency matrix of a graph. It will be altered by this function.
#   niter: The maximum number of iterations
#   eps: The calculation is considered as complete if the difference of PageRank values between iterations change less than this value for every vertex.
#   damping: The damping factor
#   personalized: Optional personalization vector (of type darray). When it is NULL, a constant value of 1/N will be used where N is the number of vertices.
#   weights: Optional edge weights (of type darray). When it is NULL, a constant value of 1 will be used.
#   trace: boolean, to show the progress
#   na_action: the desired behaviour in the case of infinite data in dgraph
#   OutDegree: optional darray which keeps the out-degree of the vertices in a darray with one split and dimension (nVertices,1)
hpdpagerank <- function(dgraph, niter = 1000, eps = 0.001, damping=0.85, personalized=NULL, weights=NULL, 
                        trace=FALSE, na_action = c("pass","exclude","fail")) {
  OutDegree <- NULL # reserved for the future
  startTotalTime <- proc.time()
  ### Validating the inputs
  if (trace) {
    cat("Validating the inputs\n")
    starttime<-proc.time()
  }
  directed = TRUE # the first version treats any graph as a directed graph
  ### Argument checks
  if (!is.darray(dgraph)) stop("dgraph argument must be of type darray")
  if (is.invalid(dgraph)) stop("'dgraph' should not be an empty darray")
  nVertices <- nrow(dgraph) # number of vertices in the graph
  if (nVertices != ncol(dgraph)) { stop("The input adjacency matrix must be square") }
  if (nVertices != dgraph@blocks[1]) { stop("The input adjacency matrix must be partitioned column-wise") }
  # Checking missed value
  switch(match.arg(na_action),
      "fail" = {
          anyMiss <- .naCheckPageRank(dgraph, trace)
          if(anyMiss > 0)
              stop("missing values in dgraph")
      },
      "exclude" = {
          anyMiss <- .naCheckPageRank(dgraph, trace, cover=TRUE)
          if(anyMiss > 0)
              warning(anyMiss, " edges are excluded because of missed value")
      },
      "pass" = {
          # do nothing    
      },
      {
          stop("only 'pass', 'exclude', and 'fail' are valid values for na_action")
      }
  )
  # Checking personalized
  if (!is.null(personalized)) {
      if(!is.darray(personalized)) stop("personalized argument must be a darray")
      if(personalized@sparse) stop("personalized argument must be a dense darray")
      if(any(dim(personalized) != c(1,nVertices)) || personalized@blocks[2] != dgraph@blocks[2])
        stop("dimensions and partitions in personalized should be compatible with dgraph")
      if(.naCheckPageRank(personalized, trace) > 0) stop("missed values in personalized darray!") 
  }
  # Checking weights
  if (!is.null(weights)) {
    if(!is.darray(weights)) stop("weights argument must be a darray")
    if(weights@sparse != dgraph@sparse) stop("weights should be similar to dgraph for sparse feature")
    if(any(dim(weights) != dim(dgraph)) || any(weights@blocks != dgraph@blocks))
      stop("weights should have the same dimension and partitioning as dgraph")
    if(.naCheckPageRank(weights, trace) > 0) stop("missed values in weights darray!") 
  }
  # Checking OutDegree
  if (!is.null(OutDegree)) {
    if(!is.darray(OutDegree)) stop("OutDegree argument must be a darray")
    if(OutDegree@sparse) stop("OutDegree argument must be a dense darray")
    if(any(dim(OutDegree) != c(nVertices,1)) || any(OutDegree@blocks != c(nVertices,1)))
      stop("OutDegree should be a darray with single split and dimension '(nVertices,1)'")
    if(.naCheckPageRank(OutDegree, trace) > 0) stop("missed values in OutDegree darray!")    
  }

  nparts <- npartitions(dgraph)
  blockSize <- dgraph@blocks[2]

  niter <- as.numeric(niter)
  if (! niter > 0 )
    stop("Invalid iteration count, it should be a positive number")
  eps <- as.numeric(eps)
  if (! eps > 0 )
    stop("Invalid value for 'eps', it should be a positive number")
  damping <- as.numeric(damping)
  if (damping <= 0 || damping >= 1)
    stop("Invalid damping factor, it should be between 0 and 1")

  if (trace) {    # end of timing step
    endtime <- proc.time()
    #print(distributedR_status())
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  ### Initialization
  if (trace) {
    cat("Initialization PR and OutDegree\n")
    starttime<-proc.time()
  }
  maxdiff <- eps
  # initial values for PR is 1/nVertices like most literature (it is 1-damping in igraph package)
  PR <- darray(dim=c(1,nVertices), blocks=c(1,blockSize), sparse=FALSE, data=1-damping)
  PR_new <- darray(dim=c(1,nVertices), blocks=c(1,blockSize), sparse=FALSE)

  if (trace) {    # end of timing step
    endtime <- proc.time()
    #print(distributedR_status())
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  #Number of outgoing edges calculated in each partition
  if(is.null(OutDegree) || !is.null(weights)) {  
      nWorkers <- NROW(distributedR_status())  #number of workers
      OutDegree <- darray(dim=c(nVertices,nWorkers), blocks=c(nVertices,1), data=0)  #OutDegree with a temporary partition at each worker

      #Let's get the number of outgoing edges in each partition
      if (trace) {
        cat("Calculating the number of outgoing edges in each partition\n")
        starttime<-proc.time()
      }
      env <- environment()
      env$index <- 0
      if (is.null(weights)) { # when there is no weight on the edges
        #In parallel perform rowsums
        while(env$index < nparts){
            foreach(j, 1:min(nWorkers, nparts - env$index), progress=trace, sumarray<-function(dg=splits(dgraph,j+env$index), tp=splits(OutDegree,j)){
                if(class(dg) == "matrix")
                    tp <- tp + rowSums(dg)
                else
                    tp <- tp + .Call("rowSums", dg, PACKAGE="MatrixHelper")
                update(tp)
            }, scheduler=1)
            env$index <- env$index + nWorkers
        } #while

      } else {  # when there are weights on the edges
        #In parallel perform rowsums
        while(env$index < nparts){
            foreach(j, 1:min(nWorkers, nparts - env$index), progress=trace, 
                    sumarray<-function(dg=splits(dgraph,j+env$index), tp=splits(OutDegree,j), wi=splits(weights,j+env$index)){
                if(class(dg) == "matrix")
                    tp <- tp + rowSums(dg * wi)
                else
                    tp <- tp + .Call("rowSums", dg * wi, PACKAGE="MatrixHelper")
                update(tp)
            }, scheduler=1)
            env$index <- env$index + nWorkers
        } #while

      } # if-else
      #Finally sum up the OutDegree vector. Final value will be stored in the first partition
      if(nWorkers > 1) {
          for(iWorker in 2:nWorkers){
              env$index <- iWorker
              foreach(j, 1, progress=trace, sumarray<-function(tpi=splits(OutDegree,env$index), tp1=splits(OutDegree,1)){
                  tp1 <- tp1 + tpi
                  update(tp1)    
              }, scheduler=1)
          }
      } # if
      if (trace) {    # end of timing step
        endtime <- proc.time()
        #print(distributedR_status())
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
      }

  } # end of if(is.null(OutDegree))

  # the array that keeps the track of the convergence
  diffArray <- darray(dim=c(1,nparts), blocks=c(1,1), sparse=FALSE)

  ### iterations
  iteration_counter <- 1
  if (trace)  iterations_start <- proc.time()
  while (niter >= iteration_counter && maxdiff >= eps) {
    if (trace) {
      cat("Iteration: ",iteration_counter,"\n")
      starttime <- proc.time()
    }
    iteration_counter <- iteration_counter + 1
    # the new PR (PageRank vector) is calculated in each iteration
    if(is.null(weights) && is.null(personalized)) {
        foreach(i, 1:nparts, progress=trace, function(dg=splits(dgraph,i), PR_newi=splits(PR_new,i), prOld=splits(PR),
                   damping=damping, TPs=splits(OutDegree,1)) {
            if(class(dg) == "matrix") {
                .Call("pagerank_vm", PR_newi, prOld, dg, TPs, damping, NULL, NULL, PACKAGE="MatrixHelper")
            } else {
                .Call("pagerank_spvm", PR_newi, prOld, dg, TPs, damping, NULL, NULL, PACKAGE="MatrixHelper")
            }
            update(PR_newi)
        }, scheduler=1)
    } else if(!is.null(weights) && is.null(personalized)) {
        foreach(i, 1:nparts, progress=trace, function(dg=splits(dgraph,i), PR_newi=splits(PR_new,i), prOld=splits(PR),
                   damping=damping, TPs=splits(OutDegree,1), wi=splits(weights,i)) {
            if(class(dg) == "matrix") {
                .Call("pagerank_vm", PR_newi, prOld, dg, TPs, damping, NULL, wi, PACKAGE="MatrixHelper")
            } else {
                .Call("pagerank_spvm", PR_newi, prOld, dg, TPs, damping, NULL, wi, PACKAGE="MatrixHelper")
            }
            update(PR_newi)
        }, scheduler=1)
    } else if(is.null(weights) && !is.null(personalized)) {
        foreach(i, 1:nparts, progress=trace, function(dg=splits(dgraph,i), PR_newi=splits(PR_new,i), prOld=splits(PR),
                   damping=damping, TPs=splits(OutDegree,1), peri=splits(personalized,i)) {
            if(class(dg) == "matrix") {
                .Call("pagerank_vm", PR_newi, prOld, dg, TPs, damping, peri, NULL, PACKAGE="MatrixHelper")
            } else {
                .Call("pagerank_spvm", PR_newi, prOld, dg, TPs, damping, peri, NULL, PACKAGE="MatrixHelper")
            }
            update(PR_newi)
        }, scheduler=1)
    } else {
        foreach(i, 1:nparts, progress=trace, function(dg=splits(dgraph,i), PR_newi=splits(PR_new,i), prOld=splits(PR),
                   damping=damping, TPs=splits(OutDegree,1), wi=splits(weights,i), peri=splits(personalized,i)) {
            if(class(dg) == "matrix") {
                .Call("pagerank_vm", PR_newi, prOld, dg, TPs, damping, peri, wi, PACKAGE="MatrixHelper")
            } else {
                .Call("pagerank_spvm", PR_newi, prOld, dg, TPs, damping, peri, wi, PACKAGE="MatrixHelper")
            }
            update(PR_newi)
        }, scheduler=1)
    }

    # normalizing the PR_new vector and finding maxdiff
    sumPR <- sum(PR_new)
    foreach(i, 1:nparts, progress=trace, function(PR_newi=splits(PR_new,i), PRi=splits(PR,i), diff=splits(diffArray,i), sumPR=sumPR){
      PR_newi <- PR_newi / sumPR
      diff <- max(abs(PR_newi - PRi))
      update(PR_newi)
      update(diff)
    })
    maxdiff <- max(diffArray)
    if(is.na(maxdiff)) stop("An error occured in the calculation most likely because of missed values in dgraph. You may use na_action='exclude' to overwrite the missed values with 0.")
    # swaping the PR vectors
    tempPR <- PR
    PR <- PR_new
    PR_new <- tempPR

    if (trace) {    # end of timing step
      endtime <- proc.time()
      #print(distributedR_status())
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
    
  } # while

  if (trace) {
    iterations_finish <- proc.time()
    iterations_totalTime <- iterations_finish[3] - iterations_start[3]
  }

  if (trace) {
    endTotalTime <- proc.time()
    totalTime <- endTotalTime - startTotalTime
    cat("*****************************\n")
    cat("Total running time:",(totalTime)[3],"sec\n")
    iterationTime = iterations_totalTime / (iteration_counter -1)
    cat("Running time of each iteration on average:",iterationTime,"sec\n")
  }

  PR
}

## .naCheckPageRank checks any missed value (NA, NaN, Inf) in X
#   X: the input darray
#   trace: boolean, to show the progress 
#   cover: when it is TRUE, the missed values will be replaced with 0
#   it returns the number of missed values
.naCheckPageRank <- function(X, trace, cover = FALSE) {
  if (trace) {
    cat("Checking for missed values\n")
    starttime<-proc.time()
  }
  nparts <- npartitions(X)
  tempArray = darray(dim=c(1,nparts), blocks=c(1,1), data=0)

  foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), xi=splits(X,i), cover=cover){
    missed <- !is.finite(xi)
    if(any(missed)) {
      tempArrayi <- matrix(sum(missed))
      update(tempArrayi)
      if(cover) {
        xi[missed] <- 0
        update(xi)
      }
    }
  })
  found <- sum(tempArray)
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  found
}

## finding the top ranked page
hpdwhich.max <- function(PR, trace=FALSE) {
  if(!is.darray(PR))  stop("PR must be of type darray")
  if(nrow(PR) != 1) stop("PR must have a single row")

  nVertices <- ncol(PR)
  nparts <- npartitions(PR)
  blockSize <- PR@blocks[2]

  dntopValue <- darray(c(nparts,1),c(1,1))  # top value from each partition of PR
  dntopIndex <- darray(c(nparts,1),c(1,1))  # the index (vertex ID) of top value from each partition of PR
  
  if (trace) {
    cat("Finding the top of each partition\n")
    starttime<-proc.time()
  }
  foreach(i, 1:nparts, progress=trace, function(pri=splits(PR,i), dntkV=splits(dntopValue,i), dntkI=splits(dntopIndex,i), idx=i, blockSize=blockSize){
    offset <- blockSize * (idx-1)
    dntkV <- max(pri)
    dntkI <- which.max(pri) + offset
    update(dntkV)
    update(dntkI)
  })
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  indices <- getpartition(dntopIndex)
  values <-  getpartition(dntopValue)

  indices[which.max(values)]
}

