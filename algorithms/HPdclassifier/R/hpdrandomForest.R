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
#  File hpdrandomForest.R
#  Distributed version of randomForest 
#
#  This code is a distributed version based on randomForest function available in randomForest package.
#
#
#########################################################
"hpdrandomForest" <-
function(x, nExecutor, ...)
  UseMethod("hpdrandomForest")

"hpdrandomForest.formula" <-
    function(formula, data = NULL, ..., ntree=500, na.action = na.fail, nExecutor, trace=FALSE, completeModel=FALSE, setSeed) {
### formula interface for hpdrandomForest.
### code gratefully copied from randomForest.formula (package randomForest_4.6-7).
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
            ret <- hpdrandomForest.default(data, ..., ntree=ntree, nExecutor=nExecutor,trace=trace,setSeed=setSeed, completeModel=completeModel, formula=formula)
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
            ret <- hpdrandomForest.default(m, y, ..., ntree=ntree, nExecutor=nExecutor,trace=trace,setSeed=setSeed, completeModel=completeModel)
            ret$terms <- Terms
            if (completeModel && !is.null(attr(y, "na.action"))) {
                attr(ret$predicted, "na.action") <- ret$na.action <- attr(y, "na.action")
	        }
        }
    }

    cl <- match.call()
    cl[[1]] <- as.name("hpdrandomForest")
    ret$call <- cl
    class(ret) <- c("hpdrandomForest.formula", "hpdrandomForest", "randomForest.formula", "randomForest")
    return(ret)
} # "hpdrandomForest.formula"

## x, y, xtest, ytest should have all follow one of these cases:
## Case 1- compatible to their types in randomForest
## Case 2- They are all (in the case of existance) of type darray
## Case 3- x is of type dframe, and there is a formula. y is null; xtest and ytest are not supported at this case
"hpdrandomForest.default"  <-
    function(x, y=NULL,  xtest=NULL, ytest=NULL, ntree=500,
             mtry=if (!is.null(y) && !is.factor(y) && !is.dframe(y))
             max(floor(ncol(x)/3), 1) else floor(sqrt(ncol(x))),
             replace=TRUE, classwt=NULL, cutoff, strata,
             sampsize = if (replace) nrow(x) else ceiling(.632*nrow(x)),
             nodesize = if (!is.null(y) && !is.factor(y) && !is.dframe(y)) 5 else 1,
             maxnodes=NULL,importance=FALSE, localImp=FALSE, nPerm=1,
             proximity=FALSE,
             norm.votes=TRUE, keep.forest=TRUE,
             nExecutor, trace=FALSE, completeModel=FALSE, ..., setSeed, formula) {

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
    if(trace)
        cat("Listing the input data\n")
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

    if(trace)
        cat("Parallel execution\n")
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
                ytest=if(is.null(ytest)) TRUE else ytest, idx=i, .local_randomForest.default=.local_randomForest.default) {
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

            oli <- tryCatch({
                        list(do.call(".local_randomForest.default", inputD))
                    }, error = function(e){
                        oli <- list(error=e)
                    })

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
            }

            update(oli)
        })
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
                y=splits(y), xtest=splits(xtest), ytest=splits(ytest), idx=i, .local_randomForest.default=.local_randomForest.default, 
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

            oli <- tryCatch({
                        list(do.call(".local_randomForest.default", inputD))
                    }, error = function(e){
                        oli <- list(error=e)
                    })

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
            }

            update(oli)
        })
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
                formula=formula, idx=i, .local_randomForest.default=.local_randomForest.default) {
            library(randomForest)
            
            if(length(all.vars(formula)) != length(all.vars(formula[[2]]))) { # there is a response
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
                inputD$x <- x   # x is of type data.frame
            }

            # determining number of trees for this sub-forest
            quotient <- inputD$ntree %/% inputD$nExecutor
            remainder <- inputD$ntree %% inputD$nExecutor
            if( idx <= remainder) inputD$ntree <- quotient + 1
            else inputD$ntree <- quotient

            set.seed(inputD$setSeed[idx])

            oli <- tryCatch({
                        list(do.call(".local_randomForest.default", inputD))
                    }, error = function(e){
                        oli <- list(error=e)
                    })

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
            }

            update(oli)
        })
    } else {
    ## Not supported type
        stop("the only supported structures for x are: 'matrix', 'data.frame', 'darray', and 'dframe'. When x is a 'dframe', the formula interface should be used.")
    }
    
    if(trace)
        cat("Combining the distributed forests\n")
    
    rflist <- getpartition(outdl) # collecting all sub-forests

    if(! inherits(rflist[[1]], "randomForest") ) # if there is any error
        stop(rflist[[1]][[1]])

    if( length(rflist[[1]]$warnings) > 0) { # if there is any warning message
        for(i in 1:length(rflist[[1]]$warnings) )
            warning(rflist[[1]]$warnings[[i]])
    }
    rflist[[1]]$warnings <- NULL

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
    class(rf) <- c("hpdrandomForest", "randomForest")

    # adding combined err.rate, mse, and rsq
    if(rf$type == "classification") {
        rf$err.rate <- err.rate
        if (norm.votes)
            rf$votes <- t(apply(rf$votes, 1, function(x) x/sum(x)))
        class(rf$votes) <- c(class(rf$votes), "votes")

        if(!is.null(rf$test)) {
            rf$test$err.rate <- err.rate.test
            if (norm.votes)
                rf$test$votes <- t(apply(rf$test$votes, 1, function(x) x/sum(x)))
            class(rf$test$votes) <- c(class(rf$test$votes), "votes")
        }
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
    
    # we do not provide this feature
    rf$inbag <- NULL    # keep.inbag=FALSE
    rf$coefs <- NULL    # corr.bias=FALSE

    if(! completeModel) {
        rf$y <- NULL
        rf$oob.times <- NULL
        rf$votes <- NULL
        rf$predicted <- NULL
        rf$test <- NULL
        rf$proximity <- NULL
    }

    rf
} # "hpdrandomForest.default"

###################################################################################
### code gratefully copied from randomForest.default (package randomForest_4.6-10).
## all stop commands are removed. Therefore, the error message is returned instead of crashing the distributedR environment.
.local_randomForest.default <-
    function(x, y=NULL,  xtest=NULL, ytest=NULL, ntree=500,
             mtry=if (!is.null(y) && !is.factor(y))
             max(floor(ncol(x)/3), 1) else floor(sqrt(ncol(x))),
             replace=TRUE, classwt=NULL, cutoff, strata,
             sampsize = if (replace) nrow(x) else ceiling(.632*nrow(x)),
             nodesize = if (!is.null(y) && !is.factor(y)) 5 else 1,
             maxnodes=NULL,
             importance=FALSE, localImp=FALSE, nPerm=1,
             proximity=FALSE, oob.prox=proximity,
             norm.votes=FALSE, do.trace=FALSE,
             keep.forest=!is.null(y) && is.null(xtest), corr.bias=FALSE,
             keep.inbag=FALSE, ...) {

    # proximity is very memory inefficient
    # proximity <- FALSE
    # oob.prox <- FALSE
    ## mylevels() returns levels if given a factor, otherwise 0.
    mylevels <- function(x) if (is.factor(x)) levels(x) else 0
    ## a list for collecting all warning messages
    warningMsg <- list()

    addclass <- is.null(y)
    classRF <- addclass || is.factor(y)
    if (!classRF && length(unique(y)) <= 5) {
        warningMsg[[length(warningMsg)+1]] <- "The response has five or fewer unique values.  Are you sure you want to do regression?"
    }
    if (classRF && !addclass && length(unique(y)) < 2)
        return("Need at least two classes to do classification.")
    n <- nrow(x)
    p <- ncol(x)
    if (n == 0) stop("data (x) has 0 rows")
    x.row.names <- rownames(x)
    x.col.names <- if (is.null(colnames(x))) 1:ncol(x) else colnames(x)

    ## overcome R's lazy evaluation:
    keep.forest <- keep.forest

    testdat <- !is.null(xtest)
    if (testdat) {
        if (ncol(x) != ncol(xtest))
            return("x and xtest must have same number of columns")
        ntest <- nrow(xtest)
        xts.row.names <- rownames(xtest)
    }

    ## Make sure mtry is in reasonable range.
    if (mtry < 1 || mtry > p)
        warningMsg[[length(warningMsg)+1]] <- "invalid mtry: reset to within valid range"
    mtry <- max(1, min(p, round(mtry)))
    if (!is.null(y)) {
        if (length(y) != n) return("length of response must be the same as predictors")
        addclass <- FALSE
    } else {
        if (!addclass) addclass <- TRUE
        y <- factor(c(rep(1, n), rep(2, n)))
        x <- rbind(x, x)
    }

    ## Check for NAs.
    if (any(is.na(x))) return("NA not permitted in predictors")
    if (testdat && any(is.na(xtest))) return("NA not permitted in xtest")
    if (any(is.na(y))) return("NA not permitted in response")
    if (!is.null(ytest) && any(is.na(ytest))) return("NA not permitted in ytest")

    if (is.data.frame(x)) {
        xlevels <- lapply(x, mylevels)
        ncat <- sapply(xlevels, length)
        ## Treat ordered factors as numerics.
        ncat <- ifelse(sapply(x, is.ordered), 1, ncat)
        x <- data.matrix(x)
        if(testdat) {
            if(!is.data.frame(xtest))
                return("xtest must be data frame if x is")
            xfactor <- which(sapply(xtest, is.factor))
            if (length(xfactor) > 0) {
                for (i in xfactor) {
                    if (any(! levels(xtest[[i]]) %in% xlevels[[i]]))
                        return("New factor levels in xtest not present in x")
                    xtest[[i]] <-
                        factor(xlevels[[i]][match(xtest[[i]], xlevels[[i]])],
                               levels=xlevels[[i]])
                }
            }
            xtest <- data.matrix(xtest)
        }
    } else {
        ncat <- rep(1, p)
        xlevels <- as.list(rep(0, p))
    }
    maxcat <- max(ncat)
    if (maxcat > 53)
        return("Can not handle categorical predictors with more than 53 categories.")

    if (classRF) {
        nclass <- length(levels(y))
        ## Check for empty classes:
        if (any(table(y) == 0)) return("Can't have empty classes in y.")
        if (!is.null(ytest)) {
            if (!is.factor(ytest)) return("ytest must be a factor")
            if (!all(levels(y) == levels(ytest)))
                return("y and ytest must have the same levels")
        }
        if (missing(cutoff)) {
            cutoff <- rep(1 / nclass, nclass)
        } else {
            if (sum(cutoff) > 1 || sum(cutoff) < 0 || !all(cutoff > 0) ||
                length(cutoff) != nclass) {
                return("Incorrect cutoff specified.")
            }
            if (!is.null(names(cutoff))) {
                if (!all(names(cutoff) %in% levels(y))) {
                    return("Wrong name(s) for cutoff")
                }
                cutoff <- cutoff[levels(y)]
            }
        }
        if (!is.null(classwt)) {
            if (length(classwt) != nclass)
                return("length of classwt not equal to number of classes")
            ## If classwt has names, match to class labels.
            if (!is.null(names(classwt))) {
                if (!all(names(classwt) %in% levels(y))) {
                    return("Wrong name(s) for classwt")
                }
                classwt <- classwt[levels(y)]
            }
            if (any(classwt <= 0)) return("classwt must be positive")
            ipi <- 1
        } else {
            classwt <- rep(1, nclass)
            ipi <- 0
        }
    } else addclass <- FALSE

    if (missing(proximity)) proximity <- addclass
    if (proximity) {
        prox <- matrix(0.0, n, n)
        proxts <- if (testdat) matrix(0, ntest, ntest + n) else double(1)
    } else {
        prox <- proxts <- double(1)
    }

    if (localImp) {
        importance <- TRUE
        impmat <- matrix(0, p, n)
    } else impmat <- double(1)

    if (importance) {
        if (nPerm < 1) nPerm <- as.integer(1) else nPerm <- as.integer(nPerm)
        if (classRF) {
            impout <- matrix(0.0, p, nclass + 2)
            impSD <- matrix(0.0, p, nclass + 1)
        } else {
            impout <- matrix(0.0, p, 2)
            impSD <- double(p)
            names(impSD) <- x.col.names
        }
    } else {
        impout <- double(p)
        impSD <- double(1)
    }

    nsample <- if (addclass) 2 * n else n
    Stratify <- length(sampsize) > 1
    if ((!Stratify) && sampsize > nrow(x)) return("sampsize too large")
    if (Stratify && (!classRF)) return("sampsize should be of length one")
    if (classRF) {
        if (Stratify) {
            if (missing(strata)) strata <- y
            if (!is.factor(strata)) strata <- as.factor(strata)
            nsum <- sum(sampsize)
            if (length(sampsize) > nlevels(strata))
                return("sampsize has too many elements.")
            if (any(sampsize <= 0) || nsum == 0)
                return("Bad sampsize specification")
            ## If sampsize has names, match to class labels.
            if (!is.null(names(sampsize))) {
                sampsize <- sampsize[levels(strata)]
            }
            if (any(sampsize > table(strata)))
              return("sampsize can not be larger than class frequency")
        } else {
            nsum <- sampsize
        }
        nrnodes <- 2 * trunc(nsum / nodesize) + 1
    } else {
        ## For regression trees, need to do this to get maximal trees.
        nrnodes <- 2 * trunc(sampsize/max(1, nodesize - 4)) + 1
    }
    if (!is.null(maxnodes)) {
        ## convert # of terminal nodes to total # of nodes
        maxnodes <- 2 * maxnodes - 1
        if (maxnodes > nrnodes) warningMsg[[length(warningMsg)+1]] <- "maxnodes exceeds its max value."
        nrnodes <- min(c(nrnodes, max(c(maxnodes, 1))))
    }
    ## Compiled code expects variables in rows and observations in columns.
    x <- t(x)
    storage.mode(x) <- "double"
    if (testdat) {
        xtest <- t(xtest)
        storage.mode(xtest) <- "double"
        if (is.null(ytest)) {
            ytest <- labelts <- 0
        } else {
            labelts <- TRUE
        }
    } else {
        xtest <- double(1)
        ytest <- double(1)
        ntest <- 1
        labelts <- FALSE
    }
    nt <- if (keep.forest) ntree else 1

    if (classRF) {
        cwt <- classwt
        threshold <- cutoff
        error.test <- if (labelts) double((nclass+1) * ntree) else double(1)
        rfout <- .C("classRF",
                    x = x,
                    xdim = as.integer(c(p, n)),
                    y = as.integer(y),
                    nclass = as.integer(nclass),
                    ncat = as.integer(ncat),
                    maxcat = as.integer(maxcat),
                    sampsize = as.integer(sampsize),
                    strata = if (Stratify) as.integer(strata) else integer(1),
                    Options = as.integer(c(addclass,
                    importance,
                    localImp,
                    proximity,
                    oob.prox,
                    do.trace,
                    keep.forest,
                    replace,
                    Stratify,
                    keep.inbag)),
                    ntree = as.integer(ntree),
                    mtry = as.integer(mtry),
                    ipi = as.integer(ipi),
                    classwt = as.double(cwt),
                    cutoff = as.double(threshold),
                    nodesize = as.integer(nodesize),
                    outcl = integer(nsample),
                    counttr = integer(nclass * nsample),
                    prox = prox,
                    impout = impout,
                    impSD = impSD,
                    impmat = impmat,
                    nrnodes = as.integer(nrnodes),
                    ndbigtree = integer(ntree),
                    nodestatus = integer(nt * nrnodes),
                    bestvar = integer(nt * nrnodes),
                    treemap = integer(nt * 2 * nrnodes),
                    nodepred = integer(nt * nrnodes),
                    xbestsplit = double(nt * nrnodes),
                    errtr = double((nclass+1) * ntree),
                    testdat = as.integer(testdat),
                    xts = as.double(xtest),
                    clts = as.integer(ytest),
                    nts = as.integer(ntest),
                    countts = double(nclass * ntest),
                    outclts = as.integer(numeric(ntest)),
                    labelts = as.integer(labelts),
                    proxts = proxts,
                    errts = error.test,
                    inbag = if (keep.inbag)
                    matrix(integer(n * ntree), n) else integer(n),
                    DUP=FALSE,
                    PACKAGE="randomForest")[-1]
        if (keep.forest) {
            ## deal with the random forest outputs
            max.nodes <- max(rfout$ndbigtree)
            treemap <- aperm(array(rfout$treemap, dim = c(2, nrnodes, ntree)),
                             c(2, 1, 3))[1:max.nodes, , , drop=FALSE]
        }
        if (!addclass) {
            ## Turn the predicted class into a factor like y.
            out.class <- factor(rfout$outcl, levels=1:nclass,
                                labels=levels(y))
            names(out.class) <- x.row.names
            con <- table(observed = y,
                         predicted = out.class)[levels(y), levels(y)]
            con <- cbind(con, class.error = 1 - diag(con)/rowSums(con))
        }
        out.votes <- t(matrix(rfout$counttr, nclass, nsample))[1:n, ]
        oob.times <- rowSums(out.votes)
        if (norm.votes)
            out.votes <- t(apply(out.votes, 1, function(x) x/sum(x)))
        dimnames(out.votes) <- list(x.row.names, levels(y))
        class(out.votes) <- c(class(out.votes), "votes")
        if (testdat) {
            out.class.ts <- factor(rfout$outclts, levels=1:nclass,
                                   labels=levels(y))
            names(out.class.ts) <- xts.row.names
            out.votes.ts <- t(matrix(rfout$countts, nclass, ntest))
            dimnames(out.votes.ts) <- list(xts.row.names, levels(y))
            if (norm.votes)
                out.votes.ts <- t(apply(out.votes.ts, 1,
                                        function(x) x/sum(x)))
            class(out.votes.ts) <- c(class(out.votes.ts), "votes")
            if (labelts) {
                testcon <- table(observed = ytest,
                                 predicted = out.class.ts)[levels(y), levels(y)]
                testcon <- cbind(testcon,
                                 class.error = 1 - diag(testcon)/rowSums(testcon))
            }
        }
        cl <- match.call()
        cl[[1]] <- as.name("randomForest")
        out <- list(call = cl,
                    type = if (addclass) "unsupervised" else "classification",
                    predicted = if (addclass) NULL else out.class,
                    err.rate = if (addclass) NULL else t(matrix(rfout$errtr,
                    nclass+1, ntree,
                    dimnames=list(c("OOB", levels(y)), NULL))),
                    confusion = if (addclass) NULL else con,
                    votes = out.votes,
                    oob.times = oob.times,
                    classes = levels(y),
                    importance = if (importance)
                    matrix(rfout$impout, p, nclass+2,
                           dimnames = list(x.col.names,
                           c(levels(y), "MeanDecreaseAccuracy",
                             "MeanDecreaseGini")))
                    else matrix(rfout$impout, ncol=1,
                                dimnames=list(x.col.names, "MeanDecreaseGini")),
                    importanceSD = if (importance)
                    matrix(rfout$impSD, p, nclass + 1,
                           dimnames = list(x.col.names,
                           c(levels(y), "MeanDecreaseAccuracy")))
                    else NULL,
                    localImportance = if (localImp)
                    matrix(rfout$impmat, p, n,
                           dimnames = list(x.col.names,x.row.names)) else NULL,
                    proximity = if (proximity) matrix(rfout$prox, n, n,
                    dimnames = list(x.row.names, x.row.names)) else NULL,
                    ntree = ntree,
                    mtry = mtry,
                    forest = if (!keep.forest) NULL else {
                        list(ndbigtree = rfout$ndbigtree,
                             nodestatus = matrix(rfout$nodestatus,
                             ncol = ntree)[1:max.nodes,, drop=FALSE],
                             bestvar = matrix(rfout$bestvar, ncol = ntree)[1:max.nodes,, drop=FALSE],
                             treemap = treemap,
                             nodepred = matrix(rfout$nodepred,
                             ncol = ntree)[1:max.nodes,, drop=FALSE],
                             xbestsplit = matrix(rfout$xbestsplit,
                             ncol = ntree)[1:max.nodes,, drop=FALSE],
                             pid = rfout$classwt, cutoff=cutoff, ncat=ncat,
                             maxcat = maxcat,
                             nrnodes = max.nodes, ntree = ntree,
                             nclass = nclass, xlevels=xlevels)
                    },
                    y = if (addclass) NULL else y,
                    test = if(!testdat) NULL else list(
                    predicted = out.class.ts,
                    err.rate = if (labelts) t(matrix(rfout$errts, nclass+1,
                    ntree,
                dimnames=list(c("Test", levels(y)), NULL))) else NULL,
                    confusion = if (labelts) testcon else NULL,
                    votes = out.votes.ts,
                    proximity = if(proximity) matrix(rfout$proxts, nrow=ntest,
                    dimnames = list(xts.row.names, c(xts.row.names,
                    x.row.names))) else NULL),
                    inbag = if (keep.inbag) rfout$inbag else NULL)
    } else {
		ymean <- mean(y)
		y <- y - ymean
		ytest <- ytest - ymean
        rfout <- .C("regRF",
                    x,
                    as.double(y),
                    as.integer(c(n, p)),
                    as.integer(sampsize),
                    as.integer(nodesize),
                    as.integer(nrnodes),
                    as.integer(ntree),
                    as.integer(mtry),
                    as.integer(c(importance, localImp, nPerm)),
                    as.integer(ncat),
                    as.integer(maxcat),
                    as.integer(do.trace),
                    as.integer(proximity),
                    as.integer(oob.prox),
                    as.integer(corr.bias),
                    ypred = double(n),
                    impout = impout,
                    impmat = impmat,
                    impSD = impSD,
                    prox = prox,
                    ndbigtree = integer(ntree),
                    nodestatus = matrix(integer(nrnodes * nt), ncol=nt),
                    leftDaughter = matrix(integer(nrnodes * nt), ncol=nt),
                    rightDaughter = matrix(integer(nrnodes * nt), ncol=nt),
                    nodepred = matrix(double(nrnodes * nt), ncol=nt),
                    bestvar = matrix(integer(nrnodes * nt), ncol=nt),
                    xbestsplit = matrix(double(nrnodes * nt), ncol=nt),
                    mse = double(ntree),
                    keep = as.integer(c(keep.forest, keep.inbag)),
                    replace = as.integer(replace),
                    testdat = as.integer(testdat),
                    xts = xtest,
                    ntest = as.integer(ntest),
                    yts = as.double(ytest),
                    labelts = as.integer(labelts),
                    ytestpred = double(ntest),
                    proxts = proxts,
                    msets = double(if (labelts) ntree else 1),
                    coef = double(2),
                    oob.times = integer(n),
                    inbag = if (keep.inbag)
                    matrix(integer(n * ntree), n) else integer(1),
                    DUP=FALSE,
                    PACKAGE="randomForest")[c(16:28, 36:41)]
        ## Format the forest component, if present.
        if (keep.forest) {
            max.nodes <- max(rfout$ndbigtree)
            rfout$nodestatus <-
                rfout$nodestatus[1:max.nodes, , drop=FALSE]
            rfout$bestvar <-
                rfout$bestvar[1:max.nodes, , drop=FALSE]
            rfout$nodepred <-
                rfout$nodepred[1:max.nodes, , drop=FALSE] + ymean
            rfout$xbestsplit <-
                rfout$xbestsplit[1:max.nodes, , drop=FALSE]
            rfout$leftDaughter <-
                rfout$leftDaughter[1:max.nodes, , drop=FALSE]
            rfout$rightDaughter <-
                rfout$rightDaughter[1:max.nodes, , drop=FALSE]
        }
        cl <- match.call()
        cl[[1]] <- as.name("randomForest")
        ## Make sure those obs. that have not been OOB get NA as prediction.
        ypred <- rfout$ypred
        if (any(rfout$oob.times < 1)) {
            ypred[rfout$oob.times == 0] <- NA
        }
        out <- list(call = cl,
                    type = "regression",
                    predicted = structure(ypred + ymean, names=x.row.names),
                    mse = rfout$mse,
                    rsq = 1 - rfout$mse / (var(y) * (n-1) / n),
                    oob.times = rfout$oob.times,
                    importance = if (importance) matrix(rfout$impout, p, 2,
                    dimnames=list(x.col.names,
                                  c("%IncMSE","IncNodePurity"))) else
                        matrix(rfout$impout, ncol=1,
                               dimnames=list(x.col.names, "IncNodePurity")),
                    importanceSD=if (importance) rfout$impSD else NULL,
                    localImportance = if (localImp)
                    matrix(rfout$impmat, p, n, dimnames=list(x.col.names,
                                               x.row.names)) else NULL,
                    proximity = if (proximity) matrix(rfout$prox, n, n,
                    dimnames = list(x.row.names, x.row.names)) else NULL,
                   ntree = ntree,
                    mtry = mtry,
                    forest = if (keep.forest)
                    c(rfout[c("ndbigtree", "nodestatus", "leftDaughter",
                              "rightDaughter", "nodepred", "bestvar",
                              "xbestsplit")],
                      list(ncat = ncat), list(nrnodes=max.nodes),
                      list(ntree=ntree), list(xlevels=xlevels)) else NULL,
                    coefs = if (corr.bias) rfout$coef else NULL,
                    y = y + ymean,
                    test = if(testdat) {
                        list(predicted = structure(rfout$ytestpred + ymean,
                             names=xts.row.names),
                             mse = if(labelts) rfout$msets else NULL,
                             rsq = if(labelts) 1 - rfout$msets /
                                        (var(ytest) * (n-1) / n) else NULL,
                             proximity = if (proximity)
                             matrix(rfout$proxts / ntree, nrow = ntest,
                                    dimnames = list(xts.row.names,
                                    c(xts.row.names,
                                    x.row.names))) else NULL)
                    } else NULL,
                    inbag = if (keep.inbag)
                    matrix(rfout$inbag, nrow(rfout$inbag),
                           dimnames=list(x.row.names, NULL)) else NULL)
    }
    class(out) <- "randomForest"
    out$warnings <- warningMsg
    # out$proximity <- NULL # proximity is very memory inefficient
    return(out)
}
