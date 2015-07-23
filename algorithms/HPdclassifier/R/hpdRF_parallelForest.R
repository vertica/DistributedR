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
#  File hpdRF_parallelForest.R
#
#  This code is a distributed version based on randomForest function available in randomForest package.
#  Its based on the technique of parallel creation of sub-forests.
#
#########################################################
"hpdRF_parallelForest" <- function(x, nExecutor, ...)   UseMethod("hpdRF_parallelForest")

"hpdRF_parallelForest.formula" <-
    function(formula, data = NULL, ..., ntree=500, na.action = na.fail, nExecutor, trace=FALSE, completeModel=FALSE, setSeed) {
### formula interface for hpdRF_parallelForest.
### code gratefully copied from randomForest.formula (package randomForest_4.6-10).
###
    nExecutor <- round(nExecutor)
    if(nExecutor <= 0)
        stop("nExecutor should be a positive integer number")
    if (!inherits(formula, "formula"))
        stop("method is only for formula objects")
    m <- match.call(expand.dots = FALSE)
    ## Catch xtest and ytest in arguments.
    if (any(c("xtest", "ytest") %in% names(m)))
        stop("xtest/ytest not supported through the formula interface")
    names(m)[2] <- "formula"
    if(!is.null(data))
        if (is.matrix(eval(m$data, parent.frame())))
            m$data <- as.data.frame(data)
    m$... <- NULL
    m$na.action <- na.action
    m[[1]] <- as.name("model.frame")
    m$ntree <- NULL
    m$nExecutor <- NULL
    m$trace <- NULL
    m$setSeed <- NULL # the argument setSeed is only for test purpose
    m$completeModel <- NULL

    if(!is.null(data)) {
        if(is.dframe(data)) {
            ret <- hpdRF_parallelForest.default(data, ..., ntree=ntree, nExecutor=nExecutor,trace=trace,setSeed=setSeed, completeModel=completeModel, formula=formula, na.action=na.action)
        } else {
            m <- eval(m, parent.frame())
            y <- model.response(m)
            Terms <- attr(m, "terms")
            attr(Terms, "intercept") <- 0
            attr(y, "na.action") <- attr(m, "na.action")
            ## Drop any "negative" terms in the formula.
            ## test with:
            ## randomForest(Fertility~.-Catholic+I(Catholic<50),data=swiss,mtry=2)
            m <- model.frame(terms(reformulate(attributes(Terms)$term.labels)),
                             data.frame(m))
            ## if (!is.null(y)) m <- m[, -1, drop=FALSE]
            for (i in seq(along=m)) {
                if (is.ordered(m[[i]])) m[[i]] <- as.numeric(m[[i]])
            }
            ret <- hpdRF_parallelForest.default(m, y, ..., ntree=ntree, nExecutor=nExecutor,trace=trace,setSeed=setSeed, completeModel=completeModel)
            ret$terms <- Terms
            if (!is.null(attr(y, "na.action")) && completeModel) {
                attr(ret$predicted, "na.action") <- ret$na.action <- attr(y, "na.action")
	        }
        }
    }

    cl <- match.call()
    cl[[1]] <- as.name("hpdRF_parallelForest")
    ret$call <- cl
    class(ret) <- c("hpdRF_parallelForest.formula", "hpdRF_parallelForest", "hpdrandomForest", "randomForest.formula", "randomForest")
    return(ret)
} # "hpdRF_parallelForest.formula"

## x, y, xtest, ytest should have all follow one of these cases:
## Case 1- compatible to their types in randomForest
## Case 2- They are all (in the case of existance) of type darray
## Case 3- x is of type dframe, and there is a formula. y is null; xtest and ytest are not supported at this case
"hpdRF_parallelForest.default"  <-
    function(x, y=NULL,  xtest=NULL, ytest=NULL, ntree=500,
             mtry=if (!is.null(y) && !is.factor(y) && !is.dframe(y))
             max(floor(ncol(x)/3), 1) else floor(sqrt(ncol(x))),
             replace=TRUE, classwt=NULL, cutoff, strata,
             sampsize = if (replace) nrow(x) else ceiling(.632*nrow(x)),
             nodesize = if (!is.null(y) && !is.factor(y) && !is.dframe(y)) 5 else 1,
             maxnodes=NULL,importance=FALSE, localImp=FALSE, nPerm=1,
             proximity=FALSE,
             norm.votes=TRUE, keep.forest=TRUE,
             nExecutor, trace=FALSE, completeModel=FALSE, ..., setSeed, formula, na.action=na.fail) {

    startTotalTime <- proc.time()

    m <- match.call(expand.dots = FALSE)
    # validating the inputs
    ntree <- round(ntree)
    nExecutor <- round(nExecutor)
    if(nExecutor <= 0 || nExecutor > ntree)
        stop("nExecutor should be a positive integer number and smaller than 'ntree'")

    nSamples <- NROW(x)
    if (nSamples == 0) stop("data (x) has 0 rows")
    Stratify <- length(sampsize) > 1
    if ((!Stratify) && sampsize > nSamples) stop("sampsize too large")

    if(is.dframe(x)) { # when x is dframe and y formula
        if (missing(formula))
            formula <- ~.
        if (!is.null(y))
            stop("when x is of type dframe, the interface with formula should be used")
        if (!is.null(xtest) || !is.null(ytest))
            stop("xtest/ytest are not supported when x is a dframe")

        allNames <- colnames(x)
        varNames <- all.vars(formula)
        if("." %in% varNames)
            varNames <- varNames[- which(varNames == ".")]
        if(! all(varNames %in% allNames))
            stop("there are variable names in the formula which are not present in the column names of 'x'")
        if(length(all.vars(formula)) != length(all.vars(formula[[2]]))) { # there is a response
            response <- all.vars(formula[[2]])
            if( length(response) > 1 || "." %in% response)
                stop("only one response is allowed in the formula")
            features <- all.vars(formula[[3]])
            if("." %in% features) nFeatures <- length(colnames(x)) -1
            else nFeatures <- length(features)
        } else { # there is no response (unsupervised)
            nFeatures <- NCOL(x)
            keep.forest <- FALSE
            sampsize <- sampsize * 2
        }
    } else {
        nFeatures <- NCOL(x)

        if (!is.null(xtest)) {
            if (is.null(y))
                stop("xtest cannot be used for unsupervised mode")
            if (nFeatures != ncol(xtest))
                stop("x and xtest must have same number of columns")
            if (nrow(xtest) == 0)
                stop("assigned xtest is empty")
        }
        if(!is.null(y)) {
            if(NCOL(y) != 1)
                stop("y should have a single column")
            if(NROW(y) != nSamples)
                stop("length of response must be the same as predictors")
            if(is.data.frame(y))    y <- y[,1]
        } else { # there is no response (unsupervised)
            keep.forest <- FALSE
            sampsize <- sampsize * 2
        }
        if(!is.null(ytest)) {
            if(NCOL(ytest) != 1)
                stop("ytest should have a single column")
            if (!is.factor(ytest) && NROW(ytest) == 0)
                stop("assigned ytest is empty")
            if(is.data.frame(ytest)) ytest <- ytest[,1]
            if(is.null(xtest)) 
                stop("xtest is not available")
            if(NROW(ytest) != NROW(xtest))
                stop("length of ytest must be the same as xtest")
        }
    } # if-else

    ## Make sure mtry is in reasonable range.
    if (mtry < 1 || mtry > nFeatures)
        warning("invalid mtry: reset to within valid range")
    mtry <- max(1, min(nFeatures, round(mtry)))

    if (nodesize <= 0)  stop("nodesize must be a positive integer")
    # the forced argument for the internal randomForest functions
    do.trace <- FALSE
    keep.inbag=FALSE
    corr.bias=FALSE # remove it from the interface because it is said it is experimental

    # this list helps to pass the value of input arguments to the workers even when they are assigned variables  
    if(trace) {
        cat("Listing the input data\n")
        starttime <- proc.time()
    }
    if(proximity) warning("Calculating and storing proximity matrix is very memory inefficient.")
    # it is better to apply norm.votes after combine
    inputData <- list(ntree=ntree, mtry=mtry, 
            replace=replace, classwt=classwt, sampsize=sampsize,
            nodesize=nodesize, maxnodes=maxnodes, importance=importance, localImp=localImp,
            nPerm=nPerm, proximity=proximity,
            keep.forest=keep.forest, corr.bias=corr.bias, nExecutor=nExecutor)
    # these arguments don't have default values in the original signature of the function
    if (!missing(cutoff))
        inputData$cutoff <- cutoff
    if (!missing(strata))
        inputData$strata <- strata
#A    if (!missing(proximity))
#A        inputData$proximity <- proximity
#A    if (!missing(oob.prox))
#A        inputData$oob.prox <- oob.prox
    
    if (!missing(setSeed)) {
        # setting seed only for test purpose
        inputData$setSeed <- rep(setSeed, nExecutor)
    } else {
        # setting the seed to improve randomness of executors
        inputData$setSeed <- runif(nExecutor, 1,10000)
    }

    if(trace) {
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
        cat("Parallel execution\n")
        starttime <- proc.time()
    }
    # the ouptput dlist
    outdl <- dlist(nExecutor)

    if (is.matrix(x) || is.data.frame(x)) {
    ## Case 1- compatible to their types in randomForest
        # validating xtest
        if(!is.null(xtest)) {
            if(is.darray(xtest) || is.dframe(xtest) || is.dlist(xtest))
                stop("The type of 'xtest' should be consistent with 'x'")
        }
        # validating y
        if(!is.null(y)) {
            if(is.darray(y) || is.dframe(y) || is.dlist(y))
                stop("'y' cannot be a distributed type when 'x' is not")
        }
        # validating ytest
        if(!is.null(ytest)) {
            if(is.darray(ytest) || is.dframe(ytest) || is.dlist(ytest) || is.null(y))
                stop("The type of 'ytest' should be consistent with 'y'")
        }

        # Each argument of foreach function is limited to 2GB
        # parallel creation of the sub-forests
        foreach(i, 1:nExecutor, progress=trace, trainModel <- function(oli=splits(outdl,i), inputD=inputData, x=x, 
                y=if(is.null(y)) TRUE else y, xtest=if(is.null(xtest)) TRUE else xtest,
                ytest=if(is.null(ytest)) TRUE else ytest, idx=i, .tryCatchWE=.tryCatchWE, completeModel=completeModel) {
            library(randomForest)
            inputD$x <- x
            if(!is.logical(y)) {
                if(is.character(y))
                    inputD$y <- factor(y)
                else
                    inputD$y <- y
            }
            if(!is.logical(xtest))
                inputD$xtest <- xtest
            if(!is.logical(ytest)) {
                if(is.character(ytest))
                    inputD$ytest <- factor(ytest)
                else
                    inputD$ytest <- ytest
            }
            # determining number of trees for this sub-forest
            quotient <- inputD$ntree %/% inputD$nExecutor
            remainder <- inputD$ntree %% inputD$nExecutor
            if( idx <= remainder) inputD$ntree <- quotient + 1
            else inputD$ntree <- quotient

            set.seed(inputD$setSeed[idx])

            oli <- .tryCatchWE( do.call("randomForest", inputD) )

            if( inherits(oli[[1]], "randomForest") ) { # when there is no error
                # y is the same for all trees
                if(idx != 1) 
                    oli[[1]]$y <- NULL
                if(oli[[1]]$type == "classification") {
                    # confusion will be calculated after combine
                    oli[[1]]$confusion <- NULL
                } else if(oli[[1]]$type == "unsupervised") {
                    # votes for unsupervised mode can be removed
                    oli[[1]]$votes <- NULL
                }
                    
                if(! completeModel) {
                    oli[[1]]$oob.times <- NULL
                    oli[[1]]$test <- NULL
                    oli[[1]]$proximity <- NULL
                } # not completeModel
            }

            update(oli)
        }, scheduler=1)
    } else if (is.darray(x)) {
    ## Case 2- They are all (in the case of existance) of type darray
        if(is.invalid(x)) stop("'x' should not be an empty darray")
        if(x@sparse)
            stop("Sparse darray is not supported for x")
        # validating xtest
        if(!is.null(xtest)) {
            if(!is.darray(xtest))
                stop("The type of 'xtest' should be consistent with 'x'")
            if(is.invalid(xtest)) stop("'xtest' should not be an empty darray")
            if(xtest@sparse)
                stop("Sparse darray is not supported for xtest")
        } else # splits of this darray will have 0 columns and 0 rows which can be indication of its being NULL insdide foreach 
            xtest <- darray(c(1,1),c(1,1),data=NA) 
        # validating y
        if(!is.null(y)) {
            if(!is.darray(y))
                stop("The type of 'y' should be consistent with 'x'")
            if(is.invalid(y)) stop("'y' should not be an empty darray")
            if(y@sparse)
                stop("Sparse darray is not supported for y")
            if(Stratify) stop("sampsize should be of length one")
        } else # splits of this darray will have 0 columns and 0 rows which can be indication of its being NULL insdide foreach 
            y <- darray(c(1,1),c(1,1),data=NA) 
        # validating ytest
        if(!is.null(ytest)) {
            if(is.null(y))
                stop("The type of 'ytest' should be consistent with 'y'")
            if(!is.darray(ytest))
                stop("The type of 'ytest' should be consistent with 'y'")
            if(is.invalid(ytest)) stop("'ytest' should not be an empty darray")
            if(ytest@sparse)
                stop("Sparse darray is not supported for ytest")
        } else # splits of this darray will have 0 columns and 0 rows which can be indication of its being NULL insdide foreach 
            ytest <- darray(c(1,1),c(1,1),data=NA) 

        # Each argument of foreach function is limited to 2GB
        # parallel creation of the sub-forests
        foreach(i, 1:nExecutor, progress=trace, trainModel <- function(oli=splits(outdl,i), inputD=inputData, x=splits(x), 
                y=splits(y), xtest=splits(xtest), ytest=splits(ytest), idx=i, .tryCatchWE=.tryCatchWE, completeModel=completeModel,
                xcoln=colnames(x), xtestcoln=colnames(xtest)) { # this line can be omitted after the problem of splits in passing colnames is resolved
            library(randomForest)
            colnames(x) <- xcoln
            inputD$x <- x
            if(! all(is.na(y)))
                inputD$y <- y[,1]
            if(! all(is.na(xtest))) {
                colnames(xtest) <- xtestcoln
                inputD$xtest <- xtest
            }
            if(! all(is.na(ytest)))
                inputD$ytest <- ytest[,1]
                
            # determining number of trees for this sub-forest
            quotient <- inputD$ntree %/% inputD$nExecutor
            remainder <- inputD$ntree %% inputD$nExecutor
            if( idx <= remainder) inputD$ntree <- quotient + 1
            else inputD$ntree <- quotient

            set.seed(inputD$setSeed[idx])

            oli <- .tryCatchWE( do.call("randomForest", inputD) )

            if( inherits(oli[[1]], "randomForest") ) { # when there is no error
                # y is the same for all trees
                if(idx != 1) 
                    oli[[1]]$y <- NULL
                if(oli[[1]]$type == "classification") {
                    # confusion will be calculated after combine
                    oli[[1]]$confusion <- NULL
                } else if(oli[[1]]$type == "unsupervised") {
                    # votes for unsupervised mode can be removed
                    oli[[1]]$votes <- NULL
                }
                    
                if(! completeModel) {
                    oli[[1]]$oob.times <- NULL
                    oli[[1]]$test <- NULL
                    oli[[1]]$proximity <- NULL
                } # not completeModel
            }

            update(oli)
        }, scheduler=1)
    } else if (is.dframe(x)) {
    ## Case 3- x is of type dframe; y, xtest, and ytest are not supported at this case
        # validating xtest
            # it is already checked that xtest is NULL
        # validating y
            # it is already checked that y is NULL 
        # validating ytest
            # it is already checked that ytest is NULL
        # Each argument of foreach function is limited to 2GB
        # parallel creation of the sub-forests
        foreach(i, 1:nExecutor, progress=trace, trainModel <- function(oli=splits(outdl,i), inputD=inputData, x=splits(x), 
                formula=formula, idx=i, .tryCatchWE=.tryCatchWE, na.action=na.action, completeModel=completeModel) {
            library(randomForest)
            
            nsamples1 <- nrow(x)
            x <- na.action(x)
            nsamples.delta <- nsamples1 - nrow(x)
            
            if(length(all.vars(formula)) != length(all.vars(formula[[2]]))) { # there is a response
                inputD$sampsize <- inputD$sampsize - nsamples.delta
                xnames <- all.vars(formula[[3]])
                yname <- all.vars(formula[[2]])
                if("." %in% xnames) {
                    allNames <- colnames(x)
                    names(allNames) <- allNames
                    xnames <- allNames[- which(names(allNames) == yname)]
                }
                inputD$x <- x[xnames]   # x is of type data.frame
                if(is.character(x[,yname])) {# y will be either a numeric vector or a factor
                    inputD$y <- factor(x[,yname])
                    yCategories <- levels(inputD$y)
                    if("" %in% yCategories) stop("Found an empty category in the response")
                } else
                    inputD$y <- x[,yname]
            } else { # there is no response (clustering)
                inputD$sampsize <- inputD$sampsize - 2 * nsamples.delta
                inputD$x <- x   # x is of type data.frame
            }

            # determining number of trees for this sub-forest
            quotient <- inputD$ntree %/% inputD$nExecutor
            remainder <- inputD$ntree %% inputD$nExecutor
            if( idx <= remainder) inputD$ntree <- quotient + 1
            else inputD$ntree <- quotient

            set.seed(inputD$setSeed[idx])

            oli <- .tryCatchWE( do.call("randomForest", inputD) )

            if( inherits(oli[[1]], "randomForest") ) { # when there is no error
                # y is the same for all trees
                if(idx != 1) 
                    oli[[1]]$y <- NULL
                if(oli[[1]]$type == "classification") {
                    # confusion will be calculated after combine
                    oli[[1]]$confusion <- NULL
                } else if(oli[[1]]$type == "unsupervised") {
                    # votes for unsupervised mode can be removed
                    oli[[1]]$votes <- NULL
                }
                    
                if(! completeModel) {
                    oli[[1]]$oob.times <- NULL
                    oli[[1]]$test <- NULL
                    oli[[1]]$proximity <- NULL
                } # not completeModel
            }

            update(oli)
        }, scheduler=1)
    } else {
    ## Not supported type
        stop("the only supported structures for x are: 'matrix', 'data.frame', 'darray', and 'dframe'. When x is a 'dframe', the formula interface should be used.")
    }
    
    if(trace) {
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
        cat("Gathering the distributed sub-forests\n")
        starttime <- proc.time()
    }
    
    rflist <- getpartition(outdl) # collecting all sub-forests and warnings

    if(trace) {
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
        cat("Combining the sub-forests\n")
        starttime <- proc.time()
    }

    if(! inherits(rflist[[1]], "randomForest") ) # if there is any error
        stop(rflist[[1]][[1]])

    if( length(rflist[[2]]) > 0) { # if there is any warning message
        for(i in 1:length(rflist[[2]]) )
            warning(rflist[[2]][[i]])
    }
    
    # removing all the warnings from the list
    warnings <- seq(length(rflist), 2, -2)
    for(i in  warnings) rflist[[i]] <- NULL

    # preserving err.rate, mse, and rsq
    if(rflist[[1]]$type == "classification") {
        err.rate <- do.call("rbind", lapply(rflist, function(x) x$err.rate))
        if(!is.null(xtest))
            err.rate.test <- do.call("rbind", lapply(rflist, function(x) x$test$err.rate))            
    } else if(rflist[[1]]$type == "regression") {
        mse <- do.call("c", lapply(rflist, function(x) x$mse))
        rsq <- do.call("c", lapply(rflist, function(x) x$rsq))
        if(!is.null(xtest)) {
            mse.test <- do.call("c", lapply(rflist, function(x) x$test$mse))
            rsq.test <- do.call("c", lapply(rflist, function(x) x$test$rsq))
        }
    }

    if (!missing(setSeed)) {
        set.seed(setSeed)   # setting seed before calling combine function
    }

    rf <- do.call("combine", rflist)
    rf$call <- m
    class(rf) <- c("hpdRF_parallelForest", "hpdrandomForest", "randomForest")

    if(trace) {
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
        cat("Post processing\n")
        starttime <- proc.time()
    }

    
    # adding combined err.rate, mse, and rsq
    if(rf$type == "classification") {
        rf$err.rate <- err.rate
        if(completeModel) {
            if (norm.votes)
                rf$votes <- t(apply(rf$votes, 1, function(x) x/sum(x)))
            class(rf$votes) <- c(class(rf$votes), "votes")
        
            if(!is.null(rf$test)) {
                rf$test$err.rate <- err.rate.test
                if (norm.votes)
                    rf$test$votes <- t(apply(rf$test$votes, 1, function(x) x/sum(x)))
                class(rf$test$votes) <- c(class(rf$test$votes), "votes")
            }
        } # completeModel=TRUE
        # calculating confusion matrix
        classLabels=levels(rf$y)
        con <- table(observed = rf$y, predicted = rf$predicted)[classLabels, classLabels]
        rf$confusion <- cbind(con, class.error = 1 - diag(con)/rowSums(con))

    } else if(rf$type == "regression") {
        rf$mse <- mse
        rf$rsq <- rsq
        if(!is.null(rf$test)) {
            rf$test$mse <- mse.test
            rf$test$rsq <- rsq.test
        }
        if(completeModel) {
            # correct calculation of predict and oob.times for regression
            oob.times <- 0
            predicted <- 0
            for(i in 1:length(rflist)) {
                oob.times <- oob.times + rflist[[i]]$oob.times
                # when rflist[[i]]$oob.times==0, then rflist[[i]]$predicted==NA
                predicted <- predicted + ifelse(is.na(rflist[[i]]$predicted), 0, rflist[[i]]$predicted) * rflist[[i]]$oob.times
            }
            rf$oob.times <- oob.times
            rf$predicted <- predicted / oob.times
        } # completeModel=TRUE
    }
    
    # Saving the terms
    if(rf$type == "classification" || rf$type == "regression") {
        if(is.dframe(x)) {
            yname <- all.vars(formula[[2]])
            xnames <- all.vars(formula[[3]])
            if("." %in% xnames) {
                allNames <- colnames(x)
                names(allNames) <- allNames
                xnames <- allNames[- which(names(allNames) == yname)]
            }
        } else {
            yname <- names(y)
            if(is.null(yname)) yname <- colnames(y)
            xnames <- names(x)
            if(is.null(xnames)) xnames <- colnames(x)
        }
        if(!is.null(yname) && !is.null(xnames) && length(yname)==1 ) {
            rf$terms <- terms(as.formula(paste(yname, paste(xnames, collapse=" + "), sep=" ~ ")))
            environment(rf$terms) <- globalenv()
        }
    }
    
    # we do not provide these feature
    rf$inbag <- NULL    # keep.inbag=FALSE
    rf$coefs <- NULL    # corr.bias=FALSE

    if(! completeModel) {
        rf$y <- NULL
        rf$oob.times <- NULL
        rf$votes <- NULL
        rf$predicted <- NULL
        rf$test <- NULL
        rf$proximity <- NULL
    } # not completeModel
    if (trace) {
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")

        endTotalTime <- proc.time()
        totalTime <- endTotalTime - startTotalTime
        cat("*****************************\n")
        cat("Total running time:",(totalTime)[3],"sec\n")
    }

    rf
} # "hpdRF_parallelForest.default"

##' We want to catch *and* save both errors and warnings, and in the case of
##' a warning, also keep the computed result.
##'
##' @title tryCatch both warnings and errors
##' @param expr
##' @return a list with 'value' and 'warnings', where 
##'  'value' may be an error caught.
##' @author Modified version of a piece of code written by Martin Maechler
.tryCatchWE <- function(expr)
{
    list_of_Warnings <- list()
    w.handler <- function(w){ # warning handler
        list_of_Warnings[[length(list_of_Warnings)+1]] <<- w
        invokeRestart("muffleWarning")
    }
    list(withCallingHandlers(tryCatch(expr, error = function(e) e),
                                     warning = w.handler),
         warnings = list_of_Warnings)
}

## A supplementary function for deployment
# inputModel: it is the model that is going to be prepared for deployment
deploy.hpdRF_parallelForest <- function(inputModel) {
    if(is.null(inputModel$forest))
        stop("The model does not contain a forest and cannot be used for prediction")
    # clearing environment
    environment(inputModel$terms) <- globalenv()
    # removing unnecessary elements
    inputModel$y <- NULL
    inputModel$oob.times <- NULL
    inputModel$votes <- NULL
    inputModel$predicted <- NULL
    inputModel$importanceSD <- NULL
    inputModel$localImportance <- NULL
    inputModel$proximity <- NULL
    inputModel$test <- NULL
    inputModel$proximity <- NULL
    
    inputModel
}

predict.hpdRF_parallelForest <- function (object, newdata, trace=FALSE) {
    # validating arguments
    if (!inherits(object, "randomForest"))
        stop("object not of class randomForest")
    if (object$type != "classification" && object$type != "regression")
        stop("only objects of type 'classification' and 'regression' are supported")
    if (is.null(object$forest)) stop("No forest component in the object")
    if (inherits(object, "randomForest.formula"))
        class(object) <- c("randomForest.formula", "randomForest")
    else
        class(object) <- "randomForest"

    if (!is.darray(newdata) && !is.dframe(newdata)) {
        output <- predict(object, newdata)
    } else {
        nparts <- npartitions(newdata)
        nSamples <- NROW(newdata)
        if(nSamples == 0) stop("No sample found in the newdata")

        if((object$type == "classification") || is.dframe(newdata)) { # the output will be a dframe; either because the output is categorical or to be consistent with the input
            have.dframe = TRUE
            if(is.darray(newdata)) {
                output <- dframe(npartitions=npartitions(newdata), distribution_policy=newdata@distribution_policy)
            } else
        	    output <- clone(newdata,ncol = 1)
        } else { # the output will be a darray because it would be regression and the input type is darray
            have.dframe = FALSE
            output <- clone(newdata,ncol = 1)
        }

        errorList <- dlist(nparts) # to track any error
        foreach(i, 1:nparts, progress=trace, function(object=object, new=splits(newdata,i), out=splits(output,i), 
                erri=splits(errorList,i), have.dframe=have.dframe, coln=colnames(newdata)){
            library(randomForest)
            
            result <- tryCatch({
                colnames(new) <- coln
                out <- predict(object,new)
                if(have.dframe) out <- data.frame(out)
                update(out)
              }, error = function(err) {
                erri <- list(err)
                update(erri)
              }
            )
        })

        anyError <- getpartition(errorList)
        if(length(anyError) > 0)
            stop(anyError[[1]])
    }
    output
}


