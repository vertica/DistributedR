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
#  File hpdkmeans.R
#  Distributed version of kmeans 
#
#  This code is a distributed version of kmeans function available in R stats package.
#
#  
#########################################################

.hpkmeans.env <- new.env()
.hpkmeans.env$iterations_totalTime <- 0

#   X: is a darray of samples. Each row represents a sample.
#   centers: the matrix of centers or their number
#   iter.max: maximum number of iterations
#   nstart: number of tries
#   algorithm: choice of algorithm
#   sampling_threshold: determining between centralized and distributed sampling for choosing random centers
#   trace: boolean, to show the progress
#   na_action: the desired behaviour in the case of infinite data
#   completeModel: when it is FALSE (default), the function does not return cluster label of samples
hpdkmeans <-
function(X, centers, iter.max = 10, nstart = 1,
#         algorithm = c("Lloyd", "Forgy", "Hartigan-Wong", "MacQueen"), 
        sampling_threshold = 1e6, trace = FALSE, na_action = c("exclude","fail"), completeModel = FALSE)
{
    startTotalTime <- proc.time()

    # validating the input arguments
    if(is.null(X)) stop("'X' is a required argument")
    if(!is.darray(X)) stop("'X' should be of type darray")
    if(is.invalid(X)) stop("'X' should not be an empty darray")
    nSample <- nrow(X)    # number of samples
    if(is.na(nSample)) stop("invalid nrow(X)")
    p <- ncol(X)    # number of predictors
    if(is.na(p)) stop("invalid ncol(X)")
    if(p != partitionsize(X)[1,2]) stop("'X' should be partitioned row-wise")
    if(missing(centers)) stop("'centers' must be a number or a matrix")
    if(is.darray(centers) || is.dframe(centers) || is.dlist(centers))
        stop("'centers' must be a number or a matrix")
#A    nmeth <- switch(match.arg(algorithm),
#A                    "Hartigan-Wong" = 1,
#A                    "Lloyd" = 2, "Forgy" = 2,
#A                    "MacQueen" = 3)
    nmeth <- 2
    if(nmeth != 2) stop("The specified algorithm is not supported")
    
    nstart <- as.integer(nstart)
    if(nstart <= 0) stop("nstart should be a positive integer number")
    
    nparts <- npartitions(X)  # number of partitions (blocks)

    mask <- NULL
    switch(match.arg(na_action),
        "fail" = {
            anyMiss <- .naCheckKmeans(X, trace=trace)            
            if(anyMiss > 0)
                stop(anyMiss, " sample(s) have missed values")
        },
        "exclude" = {
            mask <- clone(X, ncol=1, sparse=FALSE)
            anyMiss <- .naCheckKmeans(X, trace=trace, mask)
            if(anyMiss > 0) {
                if(anyMiss == nSample)
                    stop("all the samples have missed values")
                else
                    warning(anyMiss, " sample(s) are excluded because of missed value")
            }
            else
                mask <- NULL # to avoid extra overhead
        },
#A        "pass" = {
#A            # do nothing            
#A        },
        {
            stop("only 'exclude' and 'fail' are valid values for na_action")
        }
    )
    
    initialCenters <- FALSE

    if(length(centers) == 1L) { # the number of centers is given
#O        if (centers == 1) nmeth <- 3
        k <- as.integer(centers)
        if(is.na(k)) stop("'invalid value of 'k'")
        if(nSample < k)
            stop("more cluster centers than data points")

        centers <- .pickCenters(X, k, sampling_threshold, trace)

    } else {    # initial centers are given
        if(! all(is.finite(centers))    )
            stop("only finite values for centers are acceptable")
        centers <- as.matrix(centers)
        if(any(duplicated(centers)))
            stop("initial centers are not distinct")
        initialCenters <- TRUE
        k <- nrow(centers)
        if(nSample < k)
            stop("more cluster centers than data points")
    }

    iter.max <- as.integer(iter.max)
    if(is.na(iter.max) || iter.max < 1) stop("'iter.max' must be positive")
    if(ncol(X) != ncol(centers))
        stop("must have same number of columns in 'X' and 'centers'")

    # calculating the norm of all samples for approximating distance (||a|| - ||b|| <= ||a - b||)
    if(trace) {
        cat("Calculating the norm of samples\n")
        starttime<-proc.time()
    }
    Norms <- clone(X, ncol=1, sparse=FALSE)
    if(is.null(mask)) {
        foreach(i, 1:npartitions(X), progress=trace, function(Xi=splits(X,i), Ni=splits(Norms,i)) {
            library(HPdcluster)
            if(class(Xi) == "matrix")
                .Call("calculate_norm", Xi, Ni, PACKAGE="HPdcluster")
            else
                .Call("calculate_norm", as.matrix(Xi), Ni, PACKAGE="HPdcluster")
            update(Ni)
        })
    } else {
        foreach(i, 1:npartitions(X), progress=trace, function(Xi=splits(X,i), Ni=splits(Norms,i), maski=splits(mask,i)) {
            library(HPdcluster)
            good <- maski > 0
            if(class(Xi) == "matrix")
                Ni[good,] <- .Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="HPdcluster")
            else
                Ni[good,] <- .Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="HPdcluster")
            update(Ni)
        })
    }
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }

#O  Z <- do_one(nmeth)
    Z <- .do_oneSet(nmeth, X, Norms, k, centers, iter.max, trace, mask, completeModel=completeModel)
    if(completeModel)
        best <- sum(Z$wss)

    if(nstart >= 2 && !initialCenters && completeModel) {
	    for(i in 2:nstart) {
	        centers <- .pickCenters(X, k, sampling_threshold, trace)
	        ZZ <- .do_oneSet(nmeth, X, Norms, k, centers, iter.max, trace, mask, completeModel=completeModel)
	        if((z <- sum(ZZ$wss)) < best) {
		        Z <- ZZ
		        best <- z
	        }
	    }
    }

    centers <- Z$centers
    dimnames(centers) <- list(1L:k, colnames(X))
    cluster <- Z$cluster
    if(completeModel && !is.null(rn <- rownames(X))) {
        rownames(cluster) <- rn
    }

#O    totss <- sum(scale(x, scale = FALSE)^2)
    if(completeModel)
        totss <- .d.totss(X, trace, mask)

    if (trace) {
        endTotalTime <- proc.time()
        totalTime <- endTotalTime - startTotalTime
        cat("*****************************\n")
        cat("Total running time:",(totalTime)[3],"sec\n")
        iterationTime = .hpkmeans.env$iterations_totalTime / Z$iter
        cat("Running time of each iteration on average:",iterationTime,"sec\n")
    }

    if(completeModel) 
        structure(list(cluster = cluster, centers = centers, totss = totss,
                       withinss = Z$wss, tot.withinss = best,
                       betweenss = totss - best, 
                       size = Z$size, iter = Z$iter),
	              class = c("hpdkmeans","kmeans"))
    else
        structure(list(centers = centers, totss = NA,
                       withinss = NA, tot.withinss = NA,
                       betweenss = NA, 
                       size = Z$size, iter = Z$iter),
	              class = c("hpdkmeans","kmeans"))
}

## modelled on print methods in the cluster package
print.hpdkmeans <- function(x, ...)
{
    cat("K-means clustering with ", length(x$size), " clusters of sizes ",
        paste(x$size, collapse=", "), "\n", sep="")
    cat("\nCluster means:\n")
    print(x$centers, ...)
#O    cat("\nClustering vector:\n")
#O    print(x$cluster, ...)
    if(! is.null(x$withinss)) {
        cat("\nWithin cluster sum of squares by cluster:\n")
        print(x$withinss, ...)
        cat(sprintf(" (between_SS / total_SS = %5.1f %%)\n",
		    100 * x$betweenss/x$totss))
    }
    cat("\nAvailable components:\n")
    print(names(x))
    invisible(x)
}

fitted.hpdkmeans <- function(object, method = c("centers", "classes"), ...)
{
	method <- match.arg(method)
	if (method == "centers") object$centers[object$cl, , drop=FALSE]
	else object$cl
}

##################################################
#        Functions Added by Arash                #
##################################################

## Calculating totss
.d.totss <- function(X, trace = TRUE, mask = NULL)
{
    # validating the input arguments
    if(is.null(X)) stop("'X' is a required argument")
    if(!is.darray(X)) stop("'X' should be of type darray")
    if(!is.logical(trace)) stop("'trace' should be logical")
    if(!is.null(mask) && !is.darray(mask)) stop("'mask' should be of type darray")

    nparts <- npartitions(X)
    if(trace) {
        cat("Calculating the total sum of squares\n")
        starttime<-proc.time()
    }

    ss <- darray(dim=c(1,nparts),blocks=c(1,1))
    if(is.null(mask)) {
        center <- colMeans(X, na.rm=TRUE)
        foreach(i, 1:nparts, progress=trace, d.sweep <- function(Xi=splits(X,i),
                ssi=splits(ss,i), center=center){
            scaledXi <- t(Xi) - center
            ssi <- sum(scaledXi^2)
            update(ssi)
        })
    } else { # we need to ignore the entire sample when it contains NA 
        center_darray <- darray(dim=c(ncol(X),nparts), blocks=c(ncol(X),1))
        foreach(i, 1:nparts, progress=trace, function(Xi=splits(X,i),
                maski=splits(mask,i), center_darrayi=splits(center_darray,i)) {
            center_darrayi <- colSums(Xi[maski > 0,])
            update(center_darrayi)
        })
        center <- rowSums(center_darray) / sum(mask)

        foreach(i, 1:nparts, progress=trace, d.sweep <- function(Xi=splits(X,i),
                maski=splits(mask,i), ssi=splits(ss,i), center=center){
            scaledXi <- t(Xi[maski > 0,]) - center
            ssi <- sum(scaledXi^2)
            update(ssi)
        })
    }
    totss <- sum(ss)
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }

    totss
}

## .naCheckKmeans checks any missed value (NA, NaN, Inf) in X
.naCheckKmeans <- function(X, trace, mask = NULL) {
    nparts <- npartitions(X)
    tempArray = darray(dim=c(1,nparts), blocks=c(1,1), data=0)

    if (trace) {
        cat("Checking for missed values\n")
        starttime<-proc.time()
    }
    if(is.null(mask)) {
        foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), xi=splits(X,i)){
            missed <- !is.finite(rowSums(xi))
            if(any(missed)) {
               tempArrayi[1,1] <- sum(missed)
               update(tempArrayi)
           }
        })
    } else {
        foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), xi=splits(X,i), maski=splits(mask,i)){            
            maski[,1] <- as.numeric(is.finite(rowSums(xi)))
            tempArrayi <- nrow(maski) - sum(maski)
            update(maski)
            update(tempArrayi)
        })
    }
    found <- sum(tempArray)

    if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }
    
    found
} 

## Randomly finds centers
.pickCenters <- function (X, k, sampling_threshold, trace) {
    nSample <- nrow(X)    # number of samples
    p <- as.integer(ncol(X))    # number of predictors
    nparts <- npartitions(X)  # number of partitions (blocks)
    blockSizes <- partitionsize(X)[,1]
    ## we need to randomly select centers from samples distributed amon partitions of X
    ## all the samples should have the same chance to be picked as a center
    ## we need to avoid duplicates here
    maxIterCenterFinding <- 10
    nCenterAttempts <- 0
    dupplicateCenters <- TRUE
    while(dupplicateCenters && (nCenterAttempts < maxIterCenterFinding)) {
        ## k and p are not expected to be huge
        d.centers <- darray(dim=c(k*nparts,p), blocks=c(k,p), data=NA)
        if ( blockSizes[1] > sampling_threshold || nSample > 1e9) {
            selectedBlocks <- sample.int(nparts, k, replace=TRUE)
            if(trace) {
                cat("Picking randomly selected centers (distributed sampling)\n")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, centerDist <- function(idx=i, Xi=splits(X,i), selectedBlocks=selectedBlocks
                    , d.centersi=splits(d.centers,i)){
                nselect <- sum(selectedBlocks == idx)
                if(nselect > 0) {
                  index <- sample.int(nrow(Xi), nselect)
                  if(class(Xi) == "matrix")
                      d.centersi[1:nselect,] <- Xi[index,]
                  else    # When X is sparse, class(Xi) == "dgCMatrix"
                      d.centersi[1:nselect,] <- as.matrix(Xi[index,])
                  update(d.centersi)
                }
            })                
            centers <- na.omit(getpartition(d.centers))
            ## As we assume no NA/NaN/Inf in X the number of centers are correct
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
        } else {                                                      # centralized sampling
            indexOfCenters <- sample.int(nSample, k)  # It is not suitable for a big value of nSample
            if(trace) {
                cat("Picking randomly selected centers (centralized sampling)\n")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, centerCent <- function(idx=i, Xi=splits(X,i), indexOfCenters=indexOfCenters
                    , d.centersi=splits(d.centers,i), blockSizes=blockSizes){
                offset <- 0
                if(idx > 1)
                    offset <- sum(blockSizes[1:(idx -1)])
                index <- (indexOfCenters > offset) & (indexOfCenters <= nrow(Xi) + offset)
                if(any(index)) {
                    if(class(Xi) == "matrix")
                        d.centersi[1:sum(index),] <- Xi[indexOfCenters[index] - offset,]
                    else    # When X is sparse, class(Xi) == "dgCMatrix"
                        d.centersi[1:sum(index),] <- as.matrix(Xi[indexOfCenters[index] - offset,])
                    update(d.centersi)
                }
            })
            centers <- na.omit(getpartition(d.centers))

            ## As we assume no NA/NaN/Inf in X the number of centers are correct
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
        }

        d.centers = NULL    # lets GC remove it
        nCenterAttempts <- nCenterAttempts + 1
        # dupplicateCenters becomes TRUE when there is a duplicate or the number of picked centers is smaller than desired
        dupplicateCenters <- any(duplicated(centers)) || (nrow(centers) < k)
    }
    if(dupplicateCenters)
        stop("Unsuccefully tried  ", sQuote(maxIterCenterFinding), 
            " times to randomly pick distinct centers. Please specify appropriate centers manually.")
    centers
}

## Main clustersing function (a complete set of iterations for given centers)
.do_oneSet <- function (nmeth, X, Norms, k, centers, iter.max, trace, mask = NULL, completeModel=FALSE) {
    nSample <- nrow(X)    # number of samples
    p <- as.integer(ncol(X))    # number of features
    nparts <- npartitions(X)  # number of partitions (blocks)
    # Create two arrays which hold the intermediate sum of points and total no. of points belonging to a cluster
    # We create k.p matrix as it's easier to operate on
    sumOfCluster <- darray(dim=c(k*p, nparts), blocks=c(k*p, 1), FALSE)
    numOfPoints <- darray(dim=c(k, nparts), blocks=c(k,1), FALSE)

    #Create an array that maps points to their clusters
    if(completeModel)
        cluster <- clone(X, ncol=1, data=NA, sparse=FALSE)
    else
        cluster <- darray(npartitions=npartitions(X))

    iteration_counter <- 1
    .hpkmeans.env$iterations_totalTime <- 0

    if(is.null(mask)) {
        # The main loop for the convergence (mask == NULL)
        for(iter in 1:iter.max) {
            if(trace) {
                cat("Making Clusters; iteration: ",iter,"\n")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, kmeansFunc <- function(Xi=splits(X,i), Ni=splits(Norms,i),
                    centers=centers, sumOfClusteri=splits(sumOfCluster,i), numOfPointsi=splits(numOfPoints,i),
                    clusteri=splits(cluster,i), nmeth=nmeth, completeModel=completeModel){
                library(HPdcluster)
                cl = integer(nrow(Xi))
                nc = integer(nrow(centers))
                if(class(Xi) == "matrix")
                    .Call("hpdkmeans_Lloyd", Xi, Ni, centers, cl, nc, PACKAGE="HPdcluster")
                else    # When X is sparse, class(Xi) == "dgCMatrix"
                    .Call("hpdkmeans_Lloyd", as.matrix(Xi), Ni, centers, cl, nc, PACKAGE="HPdcluster")

                # to take care of empty clusters
                centers[is.nan(centers)] <- 0
                sumOfClusteri <- matrix(centers * nc)
                numOfPointsi <- matrix(nc)
                if(completeModel)
                    clusteri <- matrix(cl)
                
                update(sumOfClusteri)
                update(numOfPointsi)
                if(completeModel)
                    update(clusteri)
            })
            size <- rowSums(numOfPoints)    # the number of points in every cluster, the size of each cluster
            cent <- rowSums(sumOfCluster)   # sum of all centers
            newCenters <- matrix(cent,nrow=k,ncol=p) / matrix(size,nrow=k,ncol=p)
            newCenters[size == 0,] <- centers[size == 0,] # some of the clusters might be empty
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                .hpkmeans.env$iterations_totalTime <- .hpkmeans.env$iterations_totalTime + spentTime[3]
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
            if(all(centers == newCenters))
                break   # converged
            else {
                centers = newCenters
                iteration_counter <- iteration_counter + 1
            }
        }
        # The end of the main loop (mask == NULL)
    } else {
        # The main loop for the convergence (mask != NULL)
        for(iter in 1:iter.max) {
            if(trace) {
                cat("Making Clusters\n")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, kmeansFunc <- function(Xi=splits(X,i), maski=splits(mask,i), Ni=splits(Norms,i),
                    centers=centers, sumOfClusteri=splits(sumOfCluster,i), numOfPointsi=splits(numOfPoints,i),
                    clusteri=splits(cluster,i), nmeth=nmeth, completeModel=completeModel){
                library(HPdcluster)
                good <- maski > 0
                m <- as.integer(sum(maski))
                cl <- integer(m)
                nc <- integer(nrow(centers))
                if(class(Xi) == "matrix")
                    .Call("hpdkmeans_Lloyd", Xi[good,], Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
                else    # When X is sparse, class(Xi) == "dgCMatrix"
                    .Call("hpdkmeans_Lloyd", as.matrix(Xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
                # to take care of empty clusters
                centers[is.nan(centers)] <- 0
                sumOfClusteri <- matrix(centers * nc)
                numOfPointsi <- matrix(nc)
                if(completeModel)
                    clusteri[good,] <- matrix(cl)
                
                update(sumOfClusteri)
                update(numOfPointsi)
                if(completeModel)
                    update(clusteri)
            })
            size <- rowSums(numOfPoints)    # the number of points in every cluster, the size of each cluster
            cent <- rowSums(sumOfCluster)   # sum of all centers
            newCenters <- matrix(cent,nrow=k,ncol=p) / matrix(size,nrow=k,ncol=p)
            newCenters[size == 0,] <- centers[size == 0,] # some of the clusters might be empty
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                .hpkmeans.env$iterations_totalTime <- .hpkmeans.env$iterations_totalTime + spentTime[3]
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
            if(all(centers == newCenters))
                break   # converged
            else {
                centers = newCenters
                iteration_counter <- iteration_counter + 1
            }
        }
        # The end of the main loop (mask != NULL)
    }

    if(iteration_counter > iter.max)
        warning(sprintf(ngettext(iter.max, "did not converge in %d iteration",
            "did not converge in %d iterations"), iter.max), call.=FALSE, domain = NA)
    if(any(size == 0))
        warning("empty cluster: try a better set of initial centers", call.=FALSE)

    if(completeModel) {
        #Create a darray for wss
        dwss <- darray(dim=c(k, nparts), blocks=c(k,1), sparse=FALSE, empty=TRUE)
        if(trace) {
            cat("Calculating wss\n")
            starttime<-proc.time()
        }

        if(is.null(mask)) {
            foreach(i, 1:nparts, progress=trace, wssFunction <- function(Xi=splits(X,i),
                    dwssi=splits(dwss,i), clusteri=splits(cluster,i), centers=centers){
                library(HPdcluster)
	    		if(class(Xi) == "matrix")
	    			dwssi <- .Call("calculate_wss", Xi, centers, clusteri, PACKAGE="HPdcluster")
	    		else
	    			dwssi <- .Call("calculate_wss", as.matrix(Xi), centers, clusteri, PACKAGE="HPdcluster")
                update(dwssi)
            })
        } else {
            foreach(i, 1:nparts, progress=trace, wssFunction <- function(Xi=splits(X,i), maski=splits(mask,i),
                    dwssi=splits(dwss,i), clusteri=splits(cluster,i), centers=centers){
                library(HPdcluster)
                good <- maski > 0
	    		if(class(Xi) == "matrix")
	    			dwssi <- .Call("calculate_wss", Xi[good,], centers, clusteri[good,], PACKAGE="HPdcluster")
	    		else
	    			dwssi <- .Call("calculate_wss", as.matrix(Xi[good,]), centers, clusteri[good,], PACKAGE="HPdcluster")
                update(dwssi)
            })
        }

        wss <- rowSums(dwss)
        if (trace) {    # timing end
            endtime <- proc.time()
            spentTime <- endtime-starttime
            cat("Spent time:",(spentTime)[3],"sec\n")
        }
    } else # if(completeModel)
        wss <- NA

    list(cluster=cluster, centers=centers, wss=wss, size=size, iter=iteration_counter)
}

## Applying the cluster centers learned from hpdkmeans to label samples of a new dataset
# newdata: it is the new dataset. It must be of type either darray or matrix.
# centers: cluster centers that will be used for labeling
# trace: when it is TRUE, displays the progress
hpdapply <- function(newdata, centers, trace=FALSE) {
  darrayInput <- TRUE
  if(!is.darray(newdata)) {
    if(is.matrix(newdata) && is.numeric(newdata)) {
      darrayInput <- FALSE
    } else {
      stop("newdata must be of type either darray or numeric matrix")
    }
  }

  if(!is.matrix(centers) || !is.numeric(centers))
    stop("centers must be a numeric matrix")
  if(ncol(newdata) != ncol(centers))
    stop("newdata and centers should have the same number of features")

  nSample <- nrow(newdata)    # number of samples
  p <- as.integer(ncol(newdata))    # number of features

  if(darrayInput) { # newdata is a darray
    nparts <- npartitions(newdata)  # number of partitions (blocks)
    if(p != partitionsize(newdata)[1,2]) stop("newdata should always be row-wise partitioned")

    # samples with missed values should not be used
    mask <- clone(newdata, ncol=1, sparse=FALSE)
    anyMiss <- .naCheckKmeans(newdata, trace=trace, mask)
    if(anyMiss > 0) {
        if(anyMiss == nSample)
            stop("all the samples have missed values")
        else
            warning(anyMiss, " sample(s) are excluded because of missed value")
    } else
        mask <- NULL # to avoid extra overhead

    #Create an array that maps points to their cluster labels
    cluster <- clone(newdata, ncol=1, data=NA, sparse=FALSE)

    # Create a norm darray to fulfil the requirement of "hpdkmeans_Lloyd"
    if(trace) {
        cat("Calculating the norm of samples\n")
        starttime<-proc.time()
    }
    Norms <- clone(newdata, ncol=1, sparse=FALSE)
    if(is.null(mask)) {
        foreach(i, 1:npartitions(newdata), progress=trace, function(Xi=splits(newdata,i), Ni=splits(Norms,i)) {
            library(HPdcluster)
            if(class(Xi) == "matrix")
                .Call("calculate_norm", Xi, Ni, PACKAGE="HPdcluster")
            else
                .Call("calculate_norm", as.matrix(Xi), Ni, PACKAGE="HPdcluster")
            update(Ni)
        })
    } else {
        foreach(i, 1:npartitions(newdata), progress=trace, function(Xi=splits(newdata,i), maski=splits(mask,i), Ni=splits(Norms,i)) {
            library(HPdcluster)
            good <- maski > 0
            if(class(Xi) == "matrix")
                Ni[good,] <- .Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="HPdcluster")
            else
                Ni[good,] <- .Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="HPdcluster")
            update(Ni)
        })
    }
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }


    if(trace) {
      cat("Finding labels\n")
      starttime<-proc.time()
    }
    if(is.null(mask)) { # there is no missed value in newdata
        foreach(i, 1:nparts, progress=trace, function(xi=splits(newdata,i), centers=centers, clusteri=splits(cluster,i), Ni=splits(Norms,i) ){
          library(HPdcluster)
          cl = integer(nrow(xi))
          nc = integer(nrow(centers))
          if(class(xi) == "matrix")
            .Call("hpdkmeans_Lloyd", xi, Ni, centers, cl, nc, PACKAGE="HPdcluster")
          else    # When newdata is sparse, class(xi) == "dgCMatrix"
            .Call("hpdkmeans_Lloyd", as.matrix(xi), Ni, centers, cl, nc, PACKAGE="HPdcluster")
        
          clusteri <- matrix(cl)
                
          update(clusteri)
        })
    } else { # some of the samples should be ignored because of missed values
        foreach(i, 1:nparts, progress=trace, function(xi=splits(newdata,i), centers=centers, clusteri=splits(cluster,i), maski=splits(mask,i), Ni=splits(Norms,i) ){
          library(HPdcluster)
          good <- maski > 0
          m <- as.integer(sum(maski))
          cl <- integer(m)
          nc = integer(nrow(centers))
          if(m == 1)
            .Call("hpdkmeans_Lloyd", matrix(xi[good,],1), matrix(Ni[good,],1), centers, cl, nc, PACKAGE="HPdcluster")
          else    
            if(class(xi) == "matrix")
                .Call("hpdkmeans_Lloyd", xi[good,], Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
            else # When newdata is sparse, class(xi) == "dgCMatrix"
                .Call("hpdkmeans_Lloyd", as.matrix(xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
        
          clusteri[good,] <- matrix(cl)
                
          update(clusteri)
        })
    }
    if (trace) {    # timing end
      endtime <- proc.time()
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
  } else { # newdata is a matrix

    if(trace) {
      cat("Finding labels\n")
      starttime <- proc.time()
    }
    # samples with missed values should not be used
    mask <- is.finite(rowSums(newdata))
    missingSamples <- sum(!mask)
    if(missingSamples == 0)  mask <- NULL # avoid extra overhead of mask
    else {
        cluster <- matrix(NA, nrow=nSample, ncol=1)
        nSample <- nSample - missingSamples
        if(nSample == 0)   stop("all the samples in newdata contain missed values")
    }

    centersTemp <- matrix(centers, nrow=nrow(centers)) # the copy will be modified by the function
    normsTemp <- matrix(0, nrow=nrow(newdata), ncol=1) # a norm matrix for the newdata

    cl = integer(nSample)
    nc = integer(nrow(centers))
    if(missingSamples == 0) {
        .Call("calculate_norm", newdata, normsTemp, PACKAGE="HPdcluster")
        .Call("hpdkmeans_Lloyd", newdata, normsTemp, centersTemp, cl, nc, PACKAGE="HPdcluster")
        cluster <- matrix(cl,nrow=nSample, ncol=1)
    } else {
        if(nSample == 1) {
            normsTemp[mask,] <- .Call("calculate_norm", matrix(newdata[mask,],1), normsTemp[mask,], PACKAGE="HPdcluster")
            .Call("hpdkmeans_Lloyd", matrix(newdata[mask,],1), normsTemp[mask,], centersTemp, cl, nc, PACKAGE="HPdcluster")
        } else {
            normsTemp[mask,] <- .Call("calculate_norm", newdata[mask,], normsTemp[mask,], PACKAGE="HPdcluster")
            .Call("hpdkmeans_Lloyd", newdata[mask,], normsTemp[mask,], centersTemp, cl, nc, PACKAGE="HPdcluster")
        }
        cluster[mask,1] <- cl
    }
    if (trace) {    # timing end
      endtime <- proc.time()
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
  }

  cluster
}

## A supplementary function for deployment
# inputModel: it is the model that is going to be prepared for deployment
deploy.hpdkmeans <- function(inputModel) {
    if(is.null(inputModel$centers))
        stop("the model does not contain centers and cannot be used for prediction")
    distributed_objects <- sapply(inputModel, is.darray)
    if(any(distributed_objects))
        inputModel <- inputModel[-(which(distributed_objects, arr.ind=TRUE))]
    class(inputModel) <- c("hpdkmeans","kmeans")
    inputModel
}

