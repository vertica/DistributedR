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
#  File hpdglm.R
#  Distributed version of glm 
#
#  This code is a distributed version of glm function availabel in R stats package.
#  

#########################################################
## A copy of glm.R license:
#
#  Copyright (C) 1995-2012 The R Core Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  A copy of the GNU General Public License is available at
#  http://www.r-project.org/Licenses/


## responses: The darray that contains the vector of responses. It is single column, with values 0 or 1 for binomial regression.
## predictors: The darray that contains the vector of predictors. It may have many columns, but the number of rows and its number of blocks should be the same as responses.
## weights: It is an optional darray for weights on the samples. It has a single column. The number of rows and its number of blocks should be the same as responses.
## na_action: It is an optional argument which indicates what should happen when the data contain unsuitable values; e.g., NA, NaN, or Inf. The default is set to 'exclude'. The other option is 'fail'.
## start: Starting values for coefficients. It is optional.
## etastart: Starting values for parameter 'eta' which is used for computing deviance. It is optional.
## mustart: Starting values for mu 'parameter' which is used for computing deviance. It is optional.
## offset: An optional darray which can be used to specify an _a priori_ known component to be included in the linear predictor during fitting.
## control: An optional list of controlling arguments. The optional elements of the list and their default values are: epsilon = 1e-8, maxit = 25, trace = FALSE, rigorous = FALSE.
## method: This argument can be used for the future improvement. The only available fitting method at the moment is 'hpdglm.fit.Newton'. In the future, if we have new developed algorithms, this algorith can be used to switch between them. 
## completeModel: When it is FALSE (default), the output values that are in darray structure will be discarded.

hpdglm <- function(responses, predictors, family = gaussian, weights = NULL,
		na_action="exclude", start = NULL, etastart = NULL, mustart = NULL, offset = NULL,		
		control = list(...), method = "hpdglm.fit.Newton", completeModel = FALSE, ...)
{

    startTotalTime <- proc.time()

    call <- match.call()

    ## check the type of responses and predictors
    if( is.null(responses) || is.null(predictors) )
        stop("'responses' and 'predictors' are required arguments")
    if(! is.darray(responses) || ! is.darray(predictors))
        stop("'responses' and 'predictors' should be distributed arrays")
    if(is.invalid(responses) || is.invalid(predictors))
        stop("Neither 'responses' nor 'predictors' should be empty darrays")
    if(responses@sparse || predictors@sparse)
        stop("Sparse darray is not supported for 'responses' and 'predictors'")
    if(ncol(responses) != 1)
        stop("Multivariate is not supported")

    ## family
    if(is.character(family))
        family <- get(family, mode = "function", envir = parent.frame())
    if(is.function(family)) family <- family()
    if(is.null(family$family))
	    stop("'family' not recognized")
    if(!( (family$family == "binomial" && family$link == "logit") || (family$family == "gaussian" && family$link == "identity") ||
         (family$family == "poisson" && family$link == "log") ))
        stop("Only binomial(logit), poisson(log), and gaussian(identity) are supported for now")
    ## control parameters
    control <- do.call("hpdglm.control", control)
    trace <- ifelse(control$trace, TRUE, FALSE)
    nparts <- npartitions(responses)

    ## check weights
    binaryWeights <- TRUE
    if(!is.null(weights)){
        if(! is.darray(weights))
            stop("'weights' should be of type darray")
        if(weights@sparse)
            stop("Sparse darray is not supported for 'weights'")

        if( any(partitionsize(weights) != partitionsize(responses)) )
            stop(gettextf("'weights' should have the same partitioning pattern as 'responses', and single column"), domain = NA)
        ## check that there is no negative value for weights
        if (trace) {
            message("Checking weights")
            starttime <- proc.time()
        }
        testArray1 <- darray(dim=c(1, npartitions(weights)), c(1,1), data=0)
        testArray2 <- darray(dim=c(1, npartitions(weights)), c(1,1), data=0)
        foreach(i, 1:npartitions(weights), progress=trace, function(weightsi=splits(weights,i), 
                testArray1i=splits(testArray1,i), testArray2i=splits(testArray2,i)){
            condition <- all(weightsi >= 0)
            testArray1i <- as.numeric(condition)                
            update(testArray1i)
            if(condition) {
                testArray2i <- as.numeric(all(weightsi == 0 || weightsi == 1))
                update(testArray2i)
            }
        })
        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }

        if( ! all( getpartition(testArray1) == 1 ) )
        	stop("all weights should be non-negative")
        if( ! all( getpartition(testArray2) == 1 ) )
        	binaryWeights <- FALSE
    }
    ## check etastart
    if(!is.null(etastart)) {
        if(! is.darray(etastart))
            stop("'etastart' should be of type darray")
        if(etastart@sparse)
            stop("Sparse darray is not supported for 'etastart'")
        if( any(partitionsize(etastart) != partitionsize(responses)) )
            stop(gettextf("'etastart' should have the same pattern as 'respnses'"), domain = NA)
    }
    ## check mustart
    if(!is.null(mustart)) {
        if(! is.darray(mustart))
            stop("'mustart' should be of type darray")
        if(mustart@sparse)
            stop("Sparse darray is not supported for 'mustart'")
        if( any(partitionsize(mustart) != partitionsize(responses)) )
            stop(gettextf("'mustart' should have the same partitioning pattern as 'respnses'"), domain = NA)
        if(family$link == "logit" || family$link == "log") {
            testArray <- darray(dim=c(1, nparts), c(1,1), data=0)
            if (trace) {
                message("Checking mustart")
                starttime <- proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(mustarti=splits(mustart,i), 
                    testArrayi=splits(testArray,i)){
                condition <- all(mustarti > 0 & mustarti < 1)
                testArrayi <- as.numeric(condition)                
                update(testArrayi)
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            
            if( ! all( getpartition(testArray) == 1 ) )
            	stop("mustart has values out of valid range (0,1)")
        }        
    }

    ## check offset
    if(!is.null(offset)) {
        if(! is.darray(offset))
            stop("'offset' should be of type darray")
        if( any(partitionsize(offset) != partitionsize(responses)) )
            stop(gettextf("'offset' should have the same pattern as 'responses'"), domain = NA)
    }

    ## The call to method (hpdglm.fit)
    if (trace) {
        ps <- distributedR_status()
        message("Number of executors:",ps$Inst)
        message("System memory usage before fitting (MB): ",ps$MemUsed)
        message("Memory used to store darrays before fitting (MB):",ps$DarrayUsed)
        startFitTime <- proc.time()
    }
    fit <- eval(call(if(is.function(method)) "method" else "hpdglm.fit",
                     predictors, responses, weights = weights, start = start,
                     etastart = etastart, mustart = mustart,
                     offset = offset, family = family, control = control,
                     intercept = TRUE, method = method, binaryWeights = binaryWeights, 
                     na_action=na_action, completeModel=completeModel))
                     
    if (trace) {
        endFitTime <- proc.time()
        message("Fitting time:",(endFitTime - startFitTime)[3]," sec")
        ps <- distributedR_status()
        message("System memory usage after fitting (MB):",ps$MemUsed)
        message("Memory used to store darrays after fitting (MB):",ps$DarrayUsed)
    }

    if(! is.null(colnames(predictors)))
        rownames(fit$coefficients) <- c("(Intercept)", colnames(predictors))
    ## adding some parameters to fit
    fit <- c(fit, list(call = call, offset = offset, control = control, method = method))

    class(fit) <- c("hpdglm", "glm", "lm")

    if (trace) {
        endTotalTime <- proc.time()
        totalTime <- endTotalTime - startTotalTime
        message("*****************************")
        message("Total running time:",(totalTime)[3]," sec")
    }

    fit
}

hpdglm.control <- function(epsilon = 1e-8, maxit = 25, trace = FALSE, rigorous = FALSE)
{
    if(!is.numeric(epsilon) || epsilon <= 0)
	    stop("value of 'epsilon' must be > 0")
    if(!is.numeric(maxit) || maxit <= 0)
	    stop("maximum number of iterations must be > 0")
    list(epsilon = epsilon, maxit = maxit, trace = trace, rigorous = rigorous)
}

hpdglm.fit <-
    function (X, Y, weights = NULL, start = NULL,
	      etastart = NULL, mustart = NULL, offset = NULL,
	      family = gaussian(), control = list(), intercept = TRUE, method = "hpdglm.fit.Newton", 
          binaryWeights = TRUE, na_action="exclude", completeModel=FALSE)
{
    control <- do.call("hpdglm.control", control)
    conv <- FALSE       # converged
    nobs <- NROW(Y)     # number of observations
    nvars <- ncol(X) + 1    # adding 1 to the number of variances because of the intercept
    nparts <- npartitions(X)  # number of partitions (blocks)
    testArray <- darray(dim=c(1, nparts), c(1,1)) # this darray is used on different parts to reduce some results from other darrays    

    if (nparts != npartitions(Y) || !all(partitionsize(X)[,1] == partitionsize(Y)[,1]))
        stop("'predictors' and 'responses' should be partitioned row-wise with the same pattern. If the samples are loaded from a database, you can use the db2darrays() function to load the darrays with the same partitioning pattern.", call. = FALSE)

    EMPTY <- nvars < 2      # EMPTY=TRUE indicates no variable in X
    if(EMPTY) {
            stop("Empty model is not supported", call. = FALSE)
    }

    trace <- ifelse(control$trace, TRUE, FALSE)

    if (trace) {
        starttime_preLoop<-proc.time()
    }

    ## define weights
    if (is.null(weights)) {
	    weights <- clone(X, ncol=1, data=1)
        # .naCheck function may alter weights
        nOmit <- .naCheck(X, Y, weights, trace)
        ngoodObs <- nobs - nOmit
    } else {
        # .naCheck function may alter weights
        nOmit <- .naCheck(X, Y, weights, trace)
        if (trace) {
            message("Finding number of good observations")
            starttime<-proc.time()
        }
        foreach(i, 1:nparts, progress=trace, function(weightsi=splits(weights,i), testArrayi=splits(testArray,i)) {
            testArrayi <- as.numeric(sum(weightsi > 0))
            update(testArrayi)
        })
        ngoodObs <- sum(testArray)
        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }
    }
    
    # decision about na_action
    switch(na_action,
        "exclude" = {
            # do nothing            
        },
        "fail" = {
            if(nOmit > 0)
                stop("missing values in samples")
        },
        {
            stop("only 'exclude' and 'fail' are valid values for na_action")
        }
    )

    ## stop if not enough parameters    
    if (ngoodObs < nvars)
        stop(paste("Number of variables is", nvars, ", but only", ngoodObs, "observations (samples with NA/NaN/Inf values are excluded)"))

    ## define offset
    isInitialOffset <- TRUE
    if (is.null(offset)) {
	    offset <- clone(X, ncol = 1, sparse = TRUE, data = 0)
        isInitialOffset <- FALSE
    }

    ## get family functions:
    variance <- family$variance # parall safe for binomial, gaussian, poisson
    linkinv  <- family$linkinv # parall safe for logit, identity, log, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
    if (!is.function(variance) || !is.function(linkinv) )
	    stop("'family' argument seems not to be a valid family object", call. = FALSE)
    dev.resids <- family$dev.resids # parall safe for binomial, gaussian, poisson
#O    aic <- family$aic # may need to be parallized (its distributed version implemented for binomial, poisson, and gaussian)
    mu.eta <- family$mu.eta # parall safe for logit, identity, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
    unless.null <- function(x, if.null) if(is.null(x)) if.null else x
    valideta <- unless.null(family$valideta, function(eta) TRUE) # parall safe for logit, identity, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
    validmu  <- unless.null(family$validmu,  function(mu) TRUE) # parall safe for binomial, gaussian, poisson

    if(is.null(mustart)) {
        ## calculates mustart and may change y and weights
        ## intitialize is modified to support darray
        eval(.hpdglm.fit.initialize(family))
    } else {
        mukeep <- mustart
        eval(.hpdglm.fit.initialize(family))
        mustart <- mukeep
    }
    
    coefold <- NULL
    ## Setting eta
    if(!is.null(etastart)) 
        eta <- etastart
    else {
        eta <- clone(X, ncol = 1, data = 0)
        if(!is.null(start)) {
            if (length(start) != nvars)
                stop(paste("length of 'start' should equal", nvars, "and correspond to initial coefs"))
            else {
                coefold <- start
                if (trace) {
                    message("Initializing eta")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), offseti=splits(offset,i), Xi=splits(X,i),
                         st=as.matrix(start), weightsi=splits(weights,i)){
                    good <- weightsi > 0
                    if(all(good)) {
                        etai = as.matrix(offseti) + (Xi %*% st[-1,] + st[1,1])
                        update(etai)
                    } else if(any(good)) {
                        if(sum(good) == 1)
                            etai[good,] = as.matrix(offseti[good]) + cbind(1,matrix(Xi[good,],1,)) %*% st
                        else
                            etai[good,] = as.matrix(offseti[good]) + cbind(1,Xi[good,]) %*% st
                        update(etai)
                    }
                })
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
             }
        } else {
            ## mustart is already initialized
            if (trace) {
                message("Initializing eta")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, mustartFunction <- function(mustarti=splits(mustart,i), etai=splits(eta,i),
                     func=family$linkfun, weightsi=splits(weights,i)){
                good <- weightsi > 0
                if(all(good)) {
                    etai <- func(mustarti)
                    update(etai)
                } else if(any(good)) {
                    etai[good,] <- func(mustarti[good,])
                    update(etai)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
        }
    }
    ## Setting mu
    if(is.null(etastart)) {
        mu <- mustart
    } else {
        mu <- clone(X, ncol = 1, data = 0)
        if (trace) {
            message("Updating mu")
            starttime<-proc.time()
        }
        foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), mui=splits(mu,i), func=linkinv, weightsi=splits(weights,i)){
            good <- weightsi > 0
            if(all(good)) {
                mui <- func(etai)
                update(mui)
            } else if(any(good)) {
                mui[good,] <- func(etai[good,])
                update(mui)
            }
        })

        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }
    }

    if(control$rigorous) {
        ## validating mu and eta
        if (trace) {
            message("Validating mu and eta")
            starttime<-proc.time()
        }
        foreach(i, 1:nparts, progress=trace, function(mui=splits(mu,i), etai=splits(eta,i), testArrayi=splits(testArray,i), validmu=validmu,
                 valideta=valideta, unless.null=unless.null, weightsi=splits(weights,i)){
            good <- weightsi > 0
            if(all(good)) {
                testArrayi <- as.numeric(validmu(mui) && valideta(etai))
            } else if(any(good)) 
                testArrayi <- as.numeric(validmu(mui[good,]) && valideta(etai[good,]))
            else
                testArrayi <- as.numeric(1)
            update(testArrayi)
        })
        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }
        if( ! all( getpartition(testArray) == 1 ) )
            stop("cannot find valid starting values: please specify some", call. = FALSE)
    }

    ## calculate initial deviance and coefficient
    devArray <- darray(dim=c(nparts,1), blocks=c(1,1), data=0)
    if (trace) {
        message("Calculating initial deviance")
        starttime<-proc.time()
    }
    foreach(i, 1:nparts, progress=trace, function(devArrayi=splits(devArray,i), Yi=splits(Y,i), mui=splits(mu,i),
             weightsi=splits(weights,i), dev.resids=dev.resids){
        good <- weightsi > 0
        if(all(good))
            devArrayi <- sum(dev.resids(Yi, mui, weightsi))
        else if(any(good)) 
            devArrayi <- sum(dev.resids(Yi[good,1], mui[good,1], weightsi[good,1]))
        else
            devArrayi <- as.numeric(0)
        update(devArrayi)
    })
    devold <- sum(devArray)
    if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        message("Spent time: ",(spentTime)[3]," sec")
    }

    dev <- devold   # the value of deviance
    boundary <- conv <- FALSE   # indicators for boundary situation and convergence
    if (is.null(start))
        start <- matrix(0,nvars)
    
    ## it should be faster if some darrays are allocated out of the iteration
    if(! binaryWeights || isInitialOffset) {
        mu.eta.val <- clone(X, ncol = ncol(eta), data = 0)
        dGood <- clone(X, ncol = ncol(weights), data = 0)
        Z <- clone(X, ncol = ncol(Y), data = 0) # all darrays can be created once out of the loop
        W <- clone(X, ncol = ncol(weights), data = 0)
    }

    if (trace) {
        stoptime_preLoop<-proc.time()
        preLoopTime <- stoptime_preLoop-starttime_preLoop
        message("***** Pre-Loop time:",(preLoopTime)[3],"sec *****")

        ps <- distributedR_status()
        message("System memory usage before L.S. iteration (MB):",ps$MemUsed)
        message("Memory used to store darrays before L.S. iteration (MB):",ps$DarrayUsed)
        startItTime <- proc.time()
    }

    ##------------- THE Iteratively Reweighting L.S. iteration -----------
    for (iter in 1L:control$maxit) {

        if(control$rigorous) {
            if (trace) {
                message("Checking any NAs or 0s in V(mu)")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(mui=splits(mu,i), weightsi=splits(weights,i), testArrayi=splits(testArray,i), variance=variance){
                good <- weightsi > 0
                if(any(good)) {
                    varmu <- variance(mui[good])
                    testArrayi <- as.numeric(any(is.na(varmu)) || any(varmu == 0))
                } else
                    testArrayi <- as.numeric(0)
                update(testArrayi)
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            if( any( getpartition(testArray) != 0 ) )
                stop("NAs or 0s in V(mu)")
        }

        if(! binaryWeights || isInitialOffset) {
            ## The routine when weights are not binary or there is an initial offset. It is more time consuming.
            if (trace) {
                message("Calculating mu.eta.val")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), mu.eta.val.i=splits(mu.eta.val,i), weightsi=splits(weights,i),
                     testArrayi=splits(testArray,i), mu.eta=mu.eta){
                good <- (weightsi > 0)
                if(all(good)) {
                    mu.eta.val.i <- mu.eta(etai)
                    testArrayi <- as.numeric(any(is.na(mu.eta.val.i)))
                    update(mu.eta.val.i)
                    update(testArrayi)
                } else if(any(good)) {
                    mu.eta.val.i[good,] <- mu.eta(etai[good,])
                    testArrayi <- as.numeric(any(is.na(mu.eta.val.i[good,])))
                    update(mu.eta.val.i)
                    update(testArrayi)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            if( any( getpartition(testArray) == 1 ) )
                stop("NAs in d(mu)/d(eta)")

            ## drop observations for which w will be zero
            if (trace) {
                message("Calculating dGood")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(dGoodi=splits(dGood,i), weightsi=splits(weights,i), mu.eta.val.i=splits(mu.eta.val,i),
                    testArrayi=splits(testArray,i)){            
                dGoodi <- matrix(as.numeric((weightsi > 0) & (mu.eta.val.i != 0)), nrow(dGoodi), ncol(dGoodi))
                testArrayi <- as.numeric(any(dGoodi == 1))
                update(dGoodi)
                update(testArrayi)
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            if (all(getpartition(testArray) == 0)) {
                conv <- FALSE
                warning("no observations informative at iteration ", iter)
                break
            }

#O            z <- (eta - offset)[good] + (y - mu)[good]/mu.eta.val[good]       
 
            if (trace) {
                message("Calculating Z")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(Zi=splits(Z,i), Yi=splits(Y,i), mui=splits(mu,i), etai=splits(eta,i), offseti=splits(offset,i), 
                    mu.eta.val.i=splits(mu.eta.val,i), dGoodi=splits(dGood,i), colSize = NCOL(Y) ){
                good <- (dGoodi == 1)
                if(all(good)) {
                    Zi = (etai - as.matrix(offseti)) + (Yi - mui)/mu.eta.val.i
                    update(Zi)
                } else if(any(good)) {
                    Zi[good,] = (etai[good,] - as.matrix(offseti[good])) + (Yi[good,1] - mui[good,])/mu.eta.val.i[good,]
                    update(Zi)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            
#O            w <- sqrt((weights[good] * mu.eta.val[good]^2)/variance(mu)[good])

            if (trace) {
                message("Calculating W")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(Wi=splits(W,i), weightsi=splits(weights,i), dGoodi=splits(dGood,i),
                     mu.eta.val.i=splits(mu.eta.val,i), mui=splits(mu,i), variance=variance){
                good <- (dGoodi == 1)
                if(all(good)) {
                    Wi <- sqrt((weightsi * mu.eta.val.i^2)/variance(mui))
                    update(Wi)
                } else if(any(good)) {
                    Wi[good,] <- sqrt((weightsi[good,] * mu.eta.val.i[good,]^2)/variance(mui[good,]))
                    update(Wi)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            ## call Fortran code via C wrapper
#O            if(!is.loaded('DisCdqrls'))dyn.load('./dlm.so')
#O            fit <- .Call('DisCdqrls', x[good, , drop = FALSE] * w, z * w,
#O                         min(1e-7, control$epsilon/1000))

            if(method == "hpdglm.fit.Newton")
                coefficients <- .LSPSolution_Newton(Z, X, start, gaussian(identity), W, trace)
            else
                stop("Unknown method")

        } # !binaryWeights
        else {
            ## Faster routine when the weights are binary and there is no initial offset
            if (trace) {
                startNewtonTime <- proc.time()
            }
            W <- weights # in this routine W and weights are the same
            if(method == "hpdglm.fit.Newton")
                coefficients <- .LSPSolution_Newton(Y, X, start, family, weights, trace)
            else
                stop("Unknown method")
            if (trace) {
                endNewtonTime <- proc.time()
                message("NewtonTime:", (endNewtonTime - startNewtonTime)[3], " sec")
            }
        } # binaryWeights

        if (any(!is.finite(coefficients))) {
            conv <- FALSE
            warning(paste("non-finite coefficients at iteration", iter))
            break
        }
        ## calculate updated values of eta and mu with the new coef:
        start <- coefficients
        if (trace) {
            message("Calculate updated values of eta and mu")
            starttime<-proc.time()
        }
        foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), Xi=splits(X,i), start=start, mui=splits(mu,i), offseti=splits(offset,i),
                 devArrayi=splits(devArray,i), Yi=splits(Y,i), weightsi=splits(weights,i), linkinv=linkinv, dev.resids=dev.resids){
            good <- weightsi > 0
            if(all(good)) {
                etai <- (Xi %*% start[-1,] + start[1,1]) + as.matrix(offseti)
                mui <- linkinv(etai)
                devArrayi <- sum(dev.resids(Yi, mui, weightsi))
                update(etai)
                update(mui)
                update(devArrayi)
            } else if(any(good)) {
                if(sum(good) == 1) {
                    etai[good,] <- cbind(1,matrix(Xi[good,],1,)) %*% start + as.matrix(offseti[good])
                } else {
                    etai[good,] <- cbind(1,Xi[good,]) %*% start + as.matrix(offseti[good])
                }
                mui[good,] <- linkinv(etai[good,])
                devArrayi <- sum(dev.resids(Yi[good,1], mui[good,], weightsi[good,]))
                update(etai)
                update(mui)
                update(devArrayi)
            }
        })             
        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }
        # calculating new devience
        dev <- sum(devArray)

        if (control$trace)
            message("Deviance = ", dev, " Iterations -", iter)
        ## check for divergence
        boundary <- FALSE
        if (!is.finite(dev)) {
            if(is.null(coefold))
                stop("no valid set of coefficients has been found: please supply starting values", call. = FALSE)
            warning("step size truncated due to divergence", call. = FALSE)
            ii <- 1
            while (!is.finite(dev)) {
                if (ii > control$maxit)
                    stop("inner loop 1; cannot correct step size", call. = FALSE)
                ii <- ii + 1
                start <- (start + coefold)/2
                ## calculating deviance
                if (trace) {
                    message("Calculating deviance")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), Xi=splits(X,i), st=start, offseti=splits(offset,i), mui=splits(mu,i), 
                        devArrayi=splits(devArray,i), Yi=splits(Y,i), weightsi=splits(weights,i), linkinv=linkinv, dev.resids=dev.resids){
                    good <- weightsi > 0
                    if(all(good)) {
                        etai <- (Xi %*% st[-1,] + st[1,1]) + as.matrix(offseti)
                        mui <- linkinv(etai)
                        devArrayi <- sum(dev.resids(Yi, mui, weightsi))
                        update(etai)
                        update(mui)
                        update(devArrayi)
                    } else if(any(good)) {
                        if(sum(good) == 1)
                            etai[good,] <- cbind(1,matrix(Xi[good,],1,)) %*% st + as.matrix(offseti[good])
                        else
                            etai[good,] <- cbind(1,Xi[good,]) %*% st + as.matrix(offseti[good])
                        mui[good,] <- linkinv(etai[good,])
                        devArrayi <- sum(dev.resids(Yi[good,1], mui[good,], weightsi[good,]))
                        update(etai)
                        update(mui)
                        update(devArrayi)
                    }
                })
                dev <- sum(devArray)
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
            }
            boundary <- TRUE
            if (control$trace)
                if (trace) message("Step halved: new deviance =", dev, "\n", "coefficients", coefficients)
        }

        if(control$rigorous) {
            ## check for fitted values outside domain.
            ## validating mu and eta    
            if (trace) {
                message("Revalidating mu and eta")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(mui=splits(mu,i), etai=splits(eta,i), testArrayi=splits(testArray,i), 
                    validmu=validmu, valideta=valideta, unless.null=unless.null, weightsi=splits(weights,i)){
                good <- weightsi > 0
                if(all(good)) {
                    testArrayi <- as.numeric(validmu(mui) && valideta(etai))
                } else if(any(good))
                    testArrayi <- as.numeric(validmu(mui[good,]) && valideta(etai[good,]))
                else
                    testArrayi <- as.numeric(1)
                update(testArrayi)
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            if( ! all( getpartition(testArray) == 1 ) ) {
                notValid <- TRUE
                if(is.null(coefold))
                    stop("no valid set of coefficients has been found: please supply starting values", call. = FALSE)
                warning("step size truncated: out of bounds", call. = FALSE)
                ii <- 1
                while (notValid) {
                    if (ii > control$maxit)
                        stop("inner loop 2; cannot correct step size", call. = FALSE)
                    ii <- ii + 1
                    start <- (start + coefold)/2
                    if (trace) {
                        message("Updating eta and mu")
                        starttime<-proc.time()
                    }
                    foreach(i, 1:nparts, progress=trace, function(etai=splits(eta,i), Xi=splits(X,i), offseti=splits(offset,i), 
                            mui=splits(mu,i), start=start, linkinv=linkinv, weightsi=splits(weights,i)){
                        good <- weightsi > 0
                        if(all(good)) {
                            etai <- cbind(1,Xi) %*% start + as.matrix(offseti)
                            mui <- linkinv(etai)
                            update(etai)
                            update(mui)
                        } else if(any(good)) {
                            if(sum(good) == 1)
                                etai[good,] <- cbind(1,matrix(Xi[good,],1,)) %*% start + as.matrix(offseti[good])
                            else
                                etai[good,] <- cbind(1,Xi[good,]) %*% start + as.matrix(offseti[good])
                            mui[good,] <- linkinv(etai[good,])
                            update(etai)
                            update(mui)
                        }
                    })
                    if (trace) {    # end of timing step
                        endtime <- proc.time()
                        spentTime <- endtime-starttime
                        message("Spent time: ",(spentTime)[3]," sec")
                    }
                    ## finding the condition for while loop
                    if (trace) {
                        message("Validating eta and mu")
                        starttime<-proc.time()
                    }
                    foreach(i, 1:nparts, progress=trace, function(mui=splits(mu,i), etai=splits(eta,i), testArrayi=splits(testArray,i), 
                            validmu=validmu, valideta=valideta, unless.null=unless.null, weightsi=splits(weights,i)){
                        good <- weightsi > 0
                        if(all(good)) {
                            testArrayi <- as.numeric(validmu(mui)) * as.numeric(valideta(etai))
                        } else if(any(good))
                            testArrayi <- as.numeric(validmu(mui[good,])) * as.numeric(valideta(etai[good,]))
                        else
                            testArrayi <- as.numeric(1)
                        update(testArrayi)
                    })
                    if (trace) {    # end of timing step
                        endtime <- proc.time()
                        spentTime <- endtime-starttime
                        message("Spent time: ",(spentTime)[3]," sec")
                    }
                    if( all( getpartition(testArray) == 1 ) )
                        notValid <- FALSE
                }
                boundary <- TRUE
                ## calculating deviance
                if (trace) {
                    message("Calculating deviance")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(mui=splits(mu,i), devArrayi=splits(devArray,i), Yi=splits(Y,i), 
                        weightsi=splits(weights,i), dev.resids=dev.resids){
                    good <- weightsi > 0
                    if(all(good)) {
                        devArrayi <- sum(dev.resids(Yi, mui, weightsi))
                        update(devArrayi)
                    } else if(any(good)) {
                        devArrayi <- sum(dev.resids(Yi[good,1], mui[good,], weightsi[good,]))
                        update(devArrayi)
                    }
                })
                dev <- sum(devArray)
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
                if (control$trace)
                    message("Step halved: new deviance =", dev)
            }
        } ## rigorous

        ## check for convergence
        if (abs(dev - devold)/(0.1 + abs(dev)) < control$epsilon) {
            conv <- TRUE
            coef <- start
            break
        } else {
            devold <- dev
            coef <- coefold <- start
        }

    } ##-------------- end IRLS iteration -------------------------------
    if (trace) {
        endItTime <- proc.time()
        message("***** LS Iteration time:",(endItTime - startItTime)[3],"sec *****")
        ps <- distributedR_status()
        message("System memory usage after LS iteration (MB):",ps$MemUsed)
        message("Memory used to store darrays after LS iteration (MB):",ps$DarrayUsed)

        starttime_postLoop<-proc.time()
    }

    if(! is.null(colnames(X)))
        rownames(coef) <- c("(Intercept)", colnames(X)) 

    if(!conv && iter==1)
        stop("hpdglm.fit failed to converge at the first iteration.")
    if (!conv)
        warning("hpdglm.fit: algorithm did not converge", call. = FALSE)

    if (boundary)
        warning("hpdglm.fit: algorithm stopped at boundary value", call. = FALSE)

    ## calculate df
    nulldf <- ngoodObs - as.integer(intercept)

#O   rank <- if(EMPTY) 0 else fit$rank
    ## The accurate rank cannot be found through our approach. So it is assumed equal to nvars.
    rank <- if(EMPTY) 0 else min(nvars, ngoodObs)
    resdf  <- ngoodObs - rank

    # cov-matrix will be more accurate if we use initial weight when it is binary
    if(binaryWeights && isInitialOffset)
        W = weights

    eps <- 10*.Machine$double.eps

    if(completeModel) {

        if(control$rigorous) {
            if (family$family == "binomial") {
                if (trace) {
                    message("Checking the quality of the result")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(testArrayi=splits(testArray,i), mui=splits(mu,i), weightsi=splits(weights,i), eps=eps) {
                    good <- weightsi > 0
                    if(all(good)) {
                        testArrayi <- as.numeric(any(mui > 1 -eps) || any(mui < eps))
                    } else if(any(good))
                        testArrayi <- as.numeric(any(mui[good,] > 1 -eps) || any(mui[good,] < eps))
                    else
                        testArrayi <- as.numeric(0)
                    update(testArrayi)
                })            
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
                if(any(getpartition(testArray) == 1))
                    warning("hpdglm.fit: fitted probabilities numerically 0 or 1 occurred", call. = FALSE)
            }

            if (family$family == "poisson") {
                if (trace) {
                    message("Checking the quality of the result")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(testArrayi=splits(testArray,i), mui=splits(mu,i), weightsi=splits(weights,i), eps=eps) {
                    good <- weightsi > 0
                    if(all(good)) {
                        testArrayi <- as.numeric(any(mui < eps))
                    } else if(any(good))
                        testArrayi <- as.numeric(any(mui[good,] < eps))
                    else
                        testArrayi <- as.numeric(0)
                    update(testArrayi)
                })            
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
                if(any(getpartition(testArray) == 1))
                    warning("hpdglm.fit: fitted rates numerically 0 occurred", call. = FALSE)
            }
        }

        ## update by accurate calculation
        residuals <- clone(X, ncol = ncol(Y), data = NA)
        if (trace) {
            message("Calculating residuals")
            starttime<-proc.time()
        }
        foreach(i, 1:nparts, progress=trace, function(resi=splits(residuals,i), Yi=splits(Y,i), mui=splits(mu,i), etai=splits(eta,i),
                 mu.eta=mu.eta, weightsi=splits(weights,i)){
            good <- weightsi > 0
            if(all(good)) {
                resi <- (Yi - mui)/mu.eta(etai)
                update(resi)
            } else if(any(good)) {
                resi[good,] <- (Yi[good,1] - mui[good,])/mu.eta(etai[good,])
                update(resi)
            }
        })
        if (trace) {    # end of timing step
            endtime <- proc.time()
            spentTime <- endtime-starttime
            message("Spent time: ",(spentTime)[3]," sec")
        }

        ## calculate null deviance -- corrected in glm() if offset and intercept
        nullArray <- darray(dim=c(1,nparts),blocks=c(1,1), data=0)
        if (intercept) {
            sumMul <- darray(dim=c(1,nparts),blocks=c(1,1), data=0)
            sumWeight <- darray(dim=c(1,nparts),blocks=c(1,1), data=0)
            if (trace) {
                message("Calculating wtdmu")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(sumMuli=splits(sumMul,i), sumWeighti=splits(sumWeight,i), Yi=splits(Y,i), weightsi=splits(weights,i)) {
                good <- weightsi > 0
                if(all(good)) {
                    sumMuli <- sum(weightsi * Yi)
                    sumWeighti <- sum(weightsi)
                    update(sumMuli)
                    update(sumWeighti)
                } else if(any(good)) {
                    sumMuli <- sum(weightsi[good,] * Yi[good,1])
                    sumWeighti <- sum(weightsi[good,])
                    update(sumMuli)
                    update(sumWeighti)
                }
            })
            wtdmu <- sum(sumMul)/sum(sumWeight)
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            if (trace) {
                message("Calculating null deviance")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(nullArrayi=splits(nullArray,i), Yi=splits(Y,i), wtdmu=wtdmu, 
                    weightsi=splits(weights,i), dev.resids=dev.resids){
                good <- weightsi > 0
                if(all(good)) {
                    nullArrayi <- sum(dev.resids(Yi, wtdmu, weightsi))
                    update(nullArrayi)
                } else if(any(good)) {
                    nullArrayi <- sum(dev.resids(Yi[good,1], wtdmu, weightsi[good,]))
                    update(nullArrayi)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
        } else {  
            if (trace) {
                message("Calculating null deviance")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(nullArrayi=splits(nullArray,i), Yi=splits(Y,i), weightsi=splits(weights,i),
                     offseti=splits(offset,i), dev.resids=dev.resids, linkinv=linkinv){
                good <- weightsi > 0
                if(all(good)) {
                    nullArrayi <- sum(dev.resids(Yi, linkinv(as.matrix(offseti)), weightsi))
                    update(nullArrayi)
                } else if(any(good)) {
                    nullArrayi <- sum(dev.resids(Yi[good,1], linkinv(as.matrix(offseti[good])), weightsi[good,]))
                    update(nullArrayi)
                }
            })
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
        }
        nulldev <- sum(nullArray)

        ## calculate AIC
        aic.model <- .d.aic(family, Y, mu, weights, dev, trace, ngoodObs) + 2 * rank

        ## effects, Rmat, qr are not computed through our approach in comparison to glm
        
        fit <- list(coefficients = coef, d.residuals = residuals, d.fitted.values = mu,
            family = family, d.linear.predictors = eta, deviance = dev, aic = aic.model,
            null.deviance = nulldev, iter = iter, weights = W,
            prior.weights = weights, df.residual = resdf, df.null = nulldf,
            converged = conv, boundary = boundary, responses = Y, predictors = X)

    } else { # test
        fit <- list(coefficients = coef, d.fitted.values = mu, 
            family = family, d.linear.predictors = eta, deviance = dev, aic = NA,
            null.deviance = NA, iter = iter, weights = W,
            prior.weights = weights, df.residual = resdf, df.null = nulldf,
            converged = conv, boundary = boundary, responses = Y, predictors = X)
    }
    
    if(nOmit > 0)
        fit$na_action <- list(type="exclude", numbers=nOmit)

    class(fit) <- c("hpdglm", "glm", "lm")

    if (trace) {
        endtime_postLoop <- proc.time()
        message("***** Post-Loop time:",(endtime_postLoop - starttime_postLoop)[3],"sec *****")
    }
    fit
} # end of hpdglm.fit function

## A supplementary function for deployment
# inputModel: it is the model that is going to be prepared for deployment
deploy.hpdglm <- function(inputModel) {
    # clearing the environments
    environment(inputModel$family$validmu) <- globalenv()
    environment(inputModel$family$aic) <- globalenv()
    environment(inputModel$family$dev.resids) <- globalenv()
    environment(inputModel$family$variance) <- globalenv()

    if(is.null(inputModel$coefficients) || !length(inputModel$coefficients))
        stop("the model does not contain coefficients and cannot be used for prediction")
    fitSummary <- summary.hpdglm(inputModel)
    distributed_objects <- sapply(fitSummary, is.darray)
    if(any(distributed_objects))
        fitSummary <- fitSummary[-(which(distributed_objects, arr.ind=TRUE))]
    inputModel$summary <- fitSummary
    class(inputModel$summary) <- "summary.hpdglm"

    distributed_objects <- sapply(inputModel, is.darray)
    if(any(distributed_objects))
        inputModel <- inputModel[-(which(distributed_objects, arr.ind=TRUE))]
    class(inputModel) <- c("hpdglm", "glm", "lm")

    inputModel
}

print.hpdglm <- function(x, digits= max(3, getOption("digits") - 3), ...)
{
    cat("\nCall:  ",
	paste(deparse(x$call), sep="\n", collapse = "\n"), "\n\n", sep="")
    if(length(coef(x))) {
        cat("Coefficients")
        if(is.character(co <- x$contrasts))
            cat("  [contrasts: ",
                apply(cbind(names(co),co), 1L, paste, collapse="="), "]")
        cat(":\n")
        print.default(format(x$coefficients, digits=digits),
                      print.gap = 2, quote = FALSE)
    } else cat("No coefficients\n\n")
    cat("\nDegrees of Freedom:", x$df.null, "Total (i.e. Null); ",
        x$df.residual, "Residual\n")
    if(nzchar(mess <- naprint(x$na.action))) cat("  (",mess, ")\n", sep="")
    cat("Null Deviance:	   ",	format(signif(x$null.deviance, digits)),
	"\nResidual Deviance:", format(signif(x$deviance, digits)),
	"\tAIC:", format(signif(x$aic, digits)), "\n")
    invisible(x)
}

summary.hpdglm <- function(object, dispersion = NULL,
			correlation = FALSE, symbolic.cor = FALSE, trace=FALSE, ...)
{
    est.disp <- FALSE
    df.r <- object$df.residual
    completeModel <- !is.null(object$d.residuals)

    if(completeModel) { # runs only in complete mode
        if(is.null(dispersion)){	# calculate dispersion if needed
	        if(object$family$family %in% c("poisson", "binomial"))
                dispersion <- 1
	        else if(df.r > 0) {
                est.disp <- TRUE
    #O    		if(any(object$weights==0))
    #O    		    warning("observations with zero weight not used for calculating dispersion")
    #O		    sum((object$weights*object$residuals^2)[object$weights > 0])/ df.r
                w <- object$weights
                tempArray <- darray(dim=c(1,npartitions(w)), blocks=c(1,1), data=0)
                foreach(i, 1:npartitions(w), progress=trace, function(wi=splits(w,i), ri=splits(object$d.residuals,i), tempArrayi=splits(tempArray,i), df.r=df.r){
                    good <- wi > 0
                    if(all(good)) {
                        tempArrayi <- sum((wi * ri^2))/ df.r
                        update(tempArrayi)
                    } else if(any(good)) {
                        tempArrayi <- sum((wi * ri^2)[good,])/ df.r
                        update(tempArrayi)
                    }
                })
                dispersion <- sum(tempArray)
	        } else {
                est.disp <- TRUE
                dispersion <- NaN
            }
        }
    } # runs only in complete mode
    ## calculate scaled and unscaled covariance matrix

    aliased <- is.na(coef(object))  # used in print method
#O    p <- object$rank  rank is not available for current approach
    p <- length(object$coefficients)

    if (p > 0) {
        if(!completeModel) { # incomplete mode
            coef.table <- cbind(object$coefficients, NA, NA, NA)
            dimnames(coef.table) <- list(rownames(object$coefficients), c("Estimate", "Std. Error", "t value","Pr(>|t|)"))
            covmat.unscaled <- covmat <- matrix(, 0L, 0L)           
        } else { # complete mode

            coef.p <- object$coefficients
            covmat.unscaled <- .covariantMatrix(object, trace=trace)
            covmat <- dispersion * covmat.unscaled
            var.cf <- diag(covmat)

            ## calculate coef table

            s.err <- sqrt(var.cf)
            tvalue <- coef.p/s.err

            dn <- c("Estimate", "Std. Error")
            if(!est.disp) { # known dispersion
                pvalue <- 2*pnorm(-abs(tvalue))
                coef.table <- cbind(coef.p, s.err, tvalue, pvalue)
                dimnames(coef.table) <- list(rownames(coef.p),
                                             c(dn, "z value","Pr(>|z|)"))
            } else if(df.r > 0) {
                pvalue <- 2*pt(-abs(tvalue), df.r)
                coef.table <- cbind(coef.p, s.err, tvalue, pvalue)
                dimnames(coef.table) <- list(rownames(coef.p),
                                             c(dn, "t value","Pr(>|t|)"))
            } else { # df.r == 0
                coef.table <- cbind(coef.p, NaN, NaN, NaN)
                dimnames(coef.table) <- list(rownames(coef.p),
                                             c(dn, "t value","Pr(>|t|)"))
            }
        }          
        df.f <- p

    } else {
        coef.table <- matrix(, 0L, 4L)
        dimnames(coef.table) <-
            list(NULL, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"))
        covmat.unscaled <- covmat <- matrix(, 0L, 0L)
        df.f <- length(aliased)
    }
    ## return answer

    ## these need not all exist, e.g. na.action.
    keep <- match(c("call","family","deviance", "aic",
		      "df.residual","null.deviance","df.null",
              "iter", "na_action"), names(object), 0L)
    if(completeModel) { # complete mode
        deviance.resid <- residuals.hpdglm(object, type = "deviance", trace=FALSE)
    } else { # incomplete mode
        deviance.resid <- NA
    }
    ans <- c(object[keep],
	     list(deviance.resid = deviance.resid,
		  coefficients = coef.table,
                  aliased = aliased,
		  dispersion = dispersion,
#O		  df = c(object$rank, df.r, df.f),
		  df = c(p, df.r, df.f),
		  cov.unscaled = covmat.unscaled,
		  cov.scaled = covmat))

    if(correlation && p > 0 && completeModel) {
    	dd <- sqrt(diag(covmat.unscaled))
	    ans$correlation <-
	    covmat.unscaled/outer(dd,dd)
	    ans$symbolic.cor <- symbolic.cor
    }

    # it was better if it was quantiles
    if(completeModel) { # incomplete mode
        ans$minMax <- drop(cbind(min(ans$deviance.resid, na.rm=TRUE),max(ans$deviance.resid, na.rm=TRUE)))
    } else { # incomplete mode
        ans$minMax <- c(NA,NA)
    }
    names(ans$minMax) <- c("Min", "Max")

    class(ans) <- "summary.hpdglm"
    return(ans)
}

print.summary.hpdglm <-
    function (x, digits = max(3, getOption("digits") - 3),
	      symbolic.cor = x$symbolic.cor,
	      signif.stars = getOption("show.signif.stars"), ...)
{
    cat("\nCall:\n",
	paste(deparse(x$call), sep="\n", collapse = "\n"), "\n\n", sep="")

#O    if(x$df.residual > 5) {
#O  	x$deviance.resid <- quantile(x$deviance.resid,na.rm=TRUE)
#O  	names(x$deviance.resid) <- c("Min", "1Q", "Median", "3Q", "Max")
#O    	x$deviance.resid <- drop(cbind(min(x$deviance.resid, na.rm=TRUE),max(x$deviance.resid, na.rm=TRUE)))
#O    	names(x$deviance.resid) <- c("Min", "Max")
#O    }

    xx <- zapsmall(x$minMax, digits + 1)
    cat("Deviance Residuals: \n")
    print.default(xx, digits=digits, na.print = "", print.gap = 2)

    if(length(x$aliased) == 0L) {
        cat("\nNo Coefficients\n")
    } else {
        ## df component added in 1.8.0
        ## partial matching problem here.
        df <- if ("df" %in% names(x)) x[["df"]] else NULL
        if (!is.null(df) && (nsingular <- df[3L] - df[1L]))
            cat("\nCoefficients: (", nsingular,
                " not defined because of singularities)\n", sep = "")
        else cat("\nCoefficients:\n")
        coefs <- x$coefficients
        if(!is.null(aliased <- x$aliased) && any(aliased)) {
            cn <- names(aliased)
            coefs <- matrix(NA, length(aliased), 4L,
                            dimnames=list(cn, colnames(coefs)))
            coefs[!aliased, ] <- x$coefficients
        }
        printCoefmat(coefs, digits=digits, signif.stars=signif.stars,
                     na.print="NA", ...)
    }
    
    cat("\n(Dispersion parameter for ", x$family$family,
	" family taken to be ", format(x$dispersion), ")\n\n",
	apply(cbind(paste(format(c("Null","Residual"), justify="right"),
                          "deviance:"),
		    format(unlist(x[c("null.deviance","deviance")]),
			   digits= max(5, digits+1)), " on",
		    format(unlist(x[c("df.null","df.residual")])),
		    " degrees of freedom\n"),
	      1L, paste, collapse=" "), sep="")
    if(nzchar(mess <- naprint(x$na.action))) cat("  (",mess, ")\n", sep="")
    cat("AIC: ", format(x$aic, digits= max(4, digits+1)),"\n\n",
	"Number of Fisher Scoring iterations: ", x$iter,
	"\n", sep="")

    correl <- x$correlation
    if(!is.null(correl)) {
# looks most sensible not to give NAs for undefined coefficients
#         if(!is.null(aliased) && any(aliased)) {
#             nc <- length(aliased)
#             correl <- matrix(NA, nc, nc, dimnames = list(cn, cn))
#             correl[!aliased, !aliased] <- x$correl
#         }
	  p <- NCOL(correl)
	  if(p > 1) {
	      cat("\nCorrelation of Coefficients:\n")
	    if(is.logical(symbolic.cor) && symbolic.cor) {# NULL < 1.7.0 objects
		  print(symnum(correl, abbr.colnames = NULL))
	    } else {
		  correl <- format(round(correl, 2), nsmall = 2, digits = digits)
  		  correl[!lower.tri(correl)] <- ""
		  print(correl[-1, -p, drop=FALSE], quote = FALSE)
	    }
	  }
    }
    cat("\n")
    invisible(x)
}


## GLM Methods for Generic Functions :

residuals.hpdglm <-
    function(object,
	     type = c("deviance", "pearson", "working", "response", "partial"), trace=FALSE,
	     ...)
{
    if(is.null(object$d.residuals))
        stop("This function is only available for complete models.")
    type <- match.arg(type)
    Y <- object$responses
    r <- object$d.residuals
    mu	<- object$d.fitted.values
    wts <- object$prior.weights
    nparts <- npartitions(mu)

    switch(type,
        deviance=,pearson=,response=
        if(is.null(Y)) {
           mu.eta <- object$family$mu.eta
           eta <- object$d.linear.predictors
           Y <- clone(mu, ncol = ncol(mu))
           message("Building Y")
           foreach(i, 1:nparts, function(Yi=splits(Y,i), mui=splits(mu,i), ri=splits(r,i), etai=splits(eta,i)){
                Yi[,1] <- mui + ri * mu.eta(etai)
                update(Yi)
           })
        }
    )
    
    switch(type,
        deviance = if(object$df.residual > 0) {
            res <- clone(mu, ncol = ncol(mu), data = NA)
            d.res <- clone(mu, ncol = ncol(mu), data = 0)
            if(trace)
                message("Building residuals")
            foreach(i, 1:nparts, progress=trace, function(resi=splits(res,i), d.resi=splits(d.res,i), Yi=splits(Y,i), mui=splits(mu,i),
                     wtsi=splits(wts,i), func=(object$family$dev.resids)){
                good <- wtsi > 0
                if(all(good)) {
                    d.resi <- sqrt(pmax(func(Yi, mui, wtsi), 0))
                    resi <- ifelse(Yi > mui, d.resi, -d.resi)
                    update(d.resi)
                    update(resi)
                } else if(any(good)) {
                    d.resi[good,] <- sqrt(pmax(func(Yi[good,], mui[good,], wtsi[good,]), 0))
                    resi[good,] <- ifelse(Yi[good,1] > mui[good,], d.resi[good,], -d.resi[good,])
                    update(d.resi)
                    update(resi)
                }
            })
   	    },
        pearson = {
            res <- clone(mu, ncol = ncol(mu), data = NA)
            if(trace)
                message("Building residuals")
            foreach(i, 1:nparts, progress=trace, function(resi=splits(res,i), Yi=splits(Y,i), mui=splits(mu,i), wtsi=splits(wts,i),
                     func=object$family$variance){
                resi <- (Yi[,1]-mui)*sqrt(wtsi)/sqrt(func(mui))
                update(resi)
            })
        },
        working = {
            res <- r
        },
	    response = {
            res <- Y - mu
        },
        partial = {
            res <- r
        }
    )
#O    if(!is.null(object$na.action))
#O        res <- naresid(object$na.action, res)
#O    if (type == "partial") ## need to avoid doing naresid() twice.
#O        res <- res+predict(object, type="terms")
    res
}

weights.hpdglm <- function(object, type = c("prior", "working"), ...)
{
    type <- match.arg(type)
    res <- if(type == "prior") object$prior.weights else object$weights
    if(is.null(object$na.action)) res
    else naresid(object$na.action, res)
}


#########################################################
#        Functions specifically added for hpdglm        #
#########################################################

## Distributed version of family.initilize
.hpdglm.fit.initialize <- function (family) {
    switch( family$family,
        "binomial" = {
            ex1 <- expression({
                mustart <- clone(X, ncol = 1, data = 0)
                    ## anything, e.g. NA/NaN, for cases with zero weight is OK.
                    if (trace) {
                        message("Initilizing mustart")
                        starttime<-proc.time()
                    }
                    errArray <- darray(dim=c(nparts,1), blocks=c(1,1), data=0)
                    foreach(i, 1:nparts, progress=trace, function(Yi=splits(Y,i), weightsi=splits(weights,i), errArrayi=splits(errArray,i)){
                        good <- weightsi > 0
                        if(all(good)) {
                            if (any(Yi < 0 | Yi > 1)) {
                                errArrayi <- 1
                                update(errArrayi)
                            }
                        } else if(any(good))
                            if (any(Yi[good,] < 0 | Yi[good,] > 1)) {
                                errArrayi <- 1
                                update(errArrayi)
                            }
                    })
                    if( sum(errArray) > 0 )
                        stop("y values must be 0 <= y <= 1")
                    foreach(i, 1:nparts, progress=trace, function(Yi=splits(Y,i), weightsi=splits(weights,i), mus=splits(mustart,i)){
                        good <- weightsi > 0
                        if(all(good)) {
                            mus <- (weightsi * Yi + 0.5)/(weightsi + 1)
                            update(mus)
                        } else if(any(good)) {
                            mus[good,] <- (weightsi[good,] * Yi[good,] + 0.5)/(weightsi[good,] + 1)
                            update(mus)
                        }
                    })
                    if (trace) {    # end of timing step
                        endtime <- proc.time()
                        spentTime <- endtime-starttime
                        message("Spent time: ",(spentTime)[3]," sec")
                    }
#O                  m <- weights * y
#O                  if(any(abs(m - round(m)) > 1e-3))
#O                    warning("non-integer #successes in a binomial glm!")
            })
        },
        "quasibinomial" = {
            ex1 <- expression({
                stop(sQuote(family$family), " family not supported yet")
            })
        },
        "poisson" = {
            ex1 <- expression({
                if (trace) {
                    message("Initilizing")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(Yi=splits(Y,i), testArrayi=splits(testArray,i), weightsi=splits(weights,i)) {
                    good <- weightsi > 0
                    if(all(good)) {
                        testArrayi <- as.numeric(any(Yi < 0))
                    } else {
                        testArrayi <- as.numeric(any(Yi[good,] < 0))
                    }
                    update(testArrayi)
                })
                if(sum(testArray) > 0)
            	    stop("negative values not allowed for the Poisson family")
                    
                mustart <- clone(X, ncol = 1, data = 0)
                foreach(i, 1:nparts, progress=trace, function(Yi=splits(Y,i), mustarti=splits(mustart,i), weightsi=splits(weights,i)) {
                    good <- weightsi > 0
                    if(all(good)) {
                        mustarti <- Yi + 0.1
                    } else {
                        mustarti[good,] <- Yi[good,] + 0.1
                    }
                    update(mustarti)
                })
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
            })
        },
        "quasipoisson" = {
            ex1 <- expression({
                stop(sQuote(family$family), " family not supported yet")
            })
        },
        "gaussian" = {
            ex1 <- expression({
                mustart <- clone(X, ncol = 1, data = 0)
                if (trace) {
                    message("Initilizing mustart")
                    starttime<-proc.time()
                }
                foreach(i, 1:nparts, progress=trace, function(testArrayi=splits(testArray,i), Yi=splits(Y,i), mustarti=splits(mustart,i),
                         weightsi=splits(weights,i), link=family$link ) {
                    good <- weightsi > 0
                    if(all(good)) {
                        testArrayi <- as.numeric((link == "inverse" && any(Yi == 0)) || (link == "log" && any(Yi <= 0)))
                        mustarti <- Yi
                        update(mustarti)
                    } else if(any(good)) {
                        testArrayi <- as.numeric((link == "inverse" && any(Yi == 0)) || (link == "log" && any(Yi <= 0)))
                        mustarti[good,] <- Yi[good,]
                        update(mustarti)
                    } else {
                        testArrayi <- as.numeric(0)
                    }
                    update(testArrayi)
                })                
                if (trace) {    # end of timing step
                    endtime <- proc.time()
                    spentTime <- endtime-starttime
                    message("Spent time: ",(spentTime)[3]," sec")
                }
                if(is.null(etastart) && is.null(start) && is.null(mustart) &&
                          (any(getpartition(testArray) == 1)))
                               stop("cannot find valid starting values: please specify some")
            })
        },
        "Gamma" = {
            ex1 <- expression({
                stop(sQuote(family$family), " family not supported yet")
            })
        },
        "inverse.gaussian" = {
            ex1 <- expression({
                stop(sQuote(family$family), " family not supported yet")
            })
        },
        "quasi" = {
            ex1 <- expression({
                stop(sQuote(family$family), " family not supported yet")
            })
        },
        ## else :
        ex1 <- expression({ stop(sQuote(family$family), " family not recognised") })
    )# end switch(.)
    ex1
}

## Solution provided for Least Square Problem (LSP)
## Ref for the algorithm: http://www.cs.purdue.edu/homes/alanqi/Courses/ML-11/CS59000-ML-13.pdf
.LSPSolution_Newton <- function (Y, X, globalTheta, family, weights, trace=TRUE) {

    nobservation <- NROW(X)
    npredictors <- NCOL(X)
    npart <- npartitions(X)

    # Stores the partial gradiants distributed across workers: (npredictors+1 x 1) matrix
    distGrad <- darray(dim=c(npredictors+1,npart), blocks=c(npredictors+1,1), FALSE, data=0)
    # Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
    distHessian <- darray(dim=c((npredictors+1)*(npredictors+1), npart), blocks=c((npredictors+1)*(npredictors+1),1), FALSE, data=0)

    # Calculate gradient and hessian locally
    if (trace) {
        message("Calculating local gradient and hessian")
        starttime<-proc.time()
    }
    foreach(i, 1:npartitions(X), progress=trace, sigmoid <- function(x = splits(X,i), y=splits(Y,i), hess=splits(distHessian,i),
             g=splits(distGrad,i), theta=globalTheta, m=nobservation, myIdx=i, weightsi=splits(weights,i), family=family) {
      good <- (weightsi > 0)

          if(sum(good) == 1)
              x <- cbind(1, matrix(x[good,],1,)) * weightsi[good,]
          else
              x <- cbind(1, x[good,]) * weightsi[good,]

          trans <- t(x)   # transpose of matrix x

          switch(family$family,
              "binomial"={
                # dim(h) = dim(y)
                h <- family$linkinv(x %*% theta)
                #Gradient calcualation
                if(all(good))
                    g <- 1/m * (trans %*% (h - y * weightsi))  
                else
                    g <- 1/m * (trans %*% (h - y[good,1] * weightsi[good,1]))  
                #Hessian calcualation
                hsum <- as.numeric(h * (1 - h))
                hess <- matrix(1/m * (trans %*% (hsum * x ))) # converted into a single column matrix
              },
              "gaussian"={
                if(all(good))
                    g <- 1/m * trans %*% (y * weightsi)
                else
                    g <- 1/m * trans %*% (y[good,1] * weightsi[good,1])
                hess <- matrix(1/m * trans %*% x) # converted into a single column matrix
              },
              "poisson"={
                h <- family$linkinv(x %*% theta)
                #Gradient calcualation
                if(all(good))
                    g <- 1/m * (trans %*% (h - y * weightsi))
                else
                    g <- 1/m * (trans %*% (h - y[good,1] * weightsi[good,1]))
                #Hessian calcualation
                hsum <- as.numeric(h)
                hess <- matrix(1/m * (trans %*% (hsum * x ))) # converted into a single column matrix                
              },
              stop("The specified family is not supported for now")
          )
              
          update(g)
          update(hess)
    })
    if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        message("Spent time: ",(spentTime)[3]," sec")
    }

    #Fetch all gradients and sum them up to get one vector: 1 x (npredictors+1)
    grad<-rowSums(distGrad)

    #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
    #We should idealy use reduce() when number of partitions is large
    hessian<-matrix(rowSums(distHessian),nrow=npredictors+1) 
    
    switch(family$family,
        "binomial"={
          globalTheta = globalTheta - solve(hessian) %*% matrix(grad, ncol=1)
        },
        "gaussian"={
          globalTheta = solve(hessian) %*% matrix(grad, ncol=1)
        },
        "poisson"={
          globalTheta = globalTheta - solve(hessian) %*% matrix(grad, ncol=1)
        },
        stop("The specified family is not supported for now")
    )
}

## Calculating the co-variant matrix
.covariantMatrix <- function (object, trace=TRUE) {
    X <- object$predictors
    nobs <- NROW(X)
    nparts <- npartitions(X)
    npredictors <- NCOL(X)
    W <- object$weights   # samples with weight==0 should not be considered in the calculation

    switch( object$family$family,
        "binomial" = {               
            #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
            distHessian <- darray(dim=c((npredictors+1)*(npredictors+1), nparts), blocks=c((npredictors+1)*(npredictors+1),1), FALSE, data=0)

            if (trace) message("Calculating the covariant matrix")
            foreach(i, 1:nparts, progress=trace, errorCalc <- function(Xi = splits(X,i), hess=splits(distHessian,i), theta=object$coefficients,
                     Wi=splits(W,i), linkinv=object$family$linkinv) {
                good <- Wi > 0
                if(all(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi,1,)) * as.numeric(Wi)
                    else
                        x <- cbind(1,Xi) * as.numeric(Wi)

                    z <- x %*% theta
                    h <- linkinv(z)

                    #Hessian calcualation for the covariance
                    hsum <- as.numeric(h * (1 - h))
                    hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix

                    update(hess)
                } else if(any(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
                    else
                        x <- cbind(1,Xi[good,]) * Wi[good,1]

                    z <- x %*% theta
                    h <- linkinv(z)

                    #Hessian calcualation for the covariance
                    hsum <- as.numeric(h * (1 - h))
                    hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix

                    update(hess)
                }
            })

            #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
            hessian <- matrix(rowSums(distHessian),nrow=npredictors+1)
            covmat <- solve(hessian)
        },
        "quasibinomial" = {
            stop(sQuote(family$family), " family not supported yet")
        },
        "poisson" = {
            #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
            distHessian <- darray(dim=c((npredictors+1)*(npredictors+1), nparts), blocks=c((npredictors+1)*(npredictors+1),1), FALSE, data=0)

            if (trace) message("Calculating the covariant matrix")
            foreach(i, 1:nparts, progress=trace, errorCalc <- function(Xi = splits(X,i), hess=splits(distHessian,i), theta=object$coefficients,
                     Wi=splits(W,i), linkinv=object$family$linkinv) {
                good <- Wi > 0
                if(all(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi,1,)) * as.numeric(Wi)
                    else
                        x <- cbind(1,Xi) * as.numeric(Wi)

                    z <- x %*% theta
                    h <- linkinv(z)

                    #Hessian calcualation for the covariance
                    hsum <- as.numeric(h)
                    hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix

                    update(hess)
                } else if(any(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
                    else
                        x <- cbind(1,Xi[good,]) * Wi[good,1]

                    z <- x %*% theta
                    h <- linkinv(z)

                    #Hessian calcualation for the covariance
                    hsum <- as.numeric(h)
                    hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix

                    update(hess)
                }
            })

            #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
            hessian <- matrix(rowSums(distHessian),nrow=npredictors+1)
            covmat <- solve(hessian)
        },
        "quasipoisson" = {
            stop(sQuote(family$family), " family not supported yet")
        },
        "gaussian" = {
            #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
            distHessian <- darray(dim=c((npredictors+1)*(npredictors+1), nparts), blocks=c((npredictors+1)*(npredictors+1),1), FALSE, data=0)

            if (trace) message("Calculating the covariant matrix")
            foreach(i, 1:nparts, progress=trace, errorCalc <- function(Xi = splits(X,i), hess=splits(distHessian,i), theta=object$coefficients,
                     Wi=splits(W,i)) {
                good <- Wi > 0
                if(all(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi,1,)) * as.numeric(Wi)
                    else
                        x <- cbind(1,Xi) * as.numeric(Wi)

                    #Hessian calcualation for the covariance
                    hess <- matrix(t(x) %*% x) # converted into a single column matrix 
                    update(hess)
                } else if(any(good)) {
                    if(sum(good) == 1)
                        x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
                    else
                        x <- cbind(1,Xi[good,]) * Wi[good,1]

                    #Hessian calcualation for the covariance
                    hess <- matrix(t(x) %*% x) # converted into a single column matrix 
                    update(hess)
                }
            })

            #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
            hessian <- matrix(rowSums(distHessian),nrow=npredictors+1)
            covmat <- solve(hessian)
        },
        "Gamma" = {
            stop(sQuote(family$family), " family not supported yet")
        },
        "inverse.gaussian" = {
            stop(sQuote(family$family), " family not supported yet")
        },
        "quasi" = {
            stop(sQuote(family$family), " family not supported yet")
        },
        ## else :
        stop(sQuote(family$family), " family not recognised")
    )# end switch(.)
    covmat
}

## prediction given a model and newdata
## object: A built model of type hpdglm.
## newdata: A darray or a matrix containing predictors of new samples
## type: The type of prediction required which can be "link" or "response"
## na.action: A function to determine what should be done with missing values (reserved for the future improvement)  
## mask: A darray with a single column, and 0 or 1 as the value of its elements.
##      It indicates which samples (rows) should be considered in the calculation
predict.hpdglm <-
  function(object, newdata, type = c("link", "response"), na.action = na.pass, mask=NULL, trace=TRUE, ...)
{
    if(missing(newdata) || is.null(newdata)) {
        stop("newdata argument is required!")
    }
    if(!is.darray(newdata) && !is.matrix(newdata))
        stop("newdata should be of type darray or matrix")
    type <- match.arg(type)
    coef <- object$coefficients
    if(ncol(newdata) != length(coef)-1)
        stop("the number of predictors in newdata should fit the number of coefficients in the model")
    
    ## newdata can be either a darray or a normal array
    if(is.darray(newdata)){  # newdata is a darray
        nparts <- npartitions(newdata)
        pred.col <- NCOL(coef)
        pred.row <- NROW(newdata)       
        pred <- clone(newdata, ncol = pred.col, data = NA)

        if(is.null(mask)) {     # no mask
            if (trace) message("Calculating link prediction")
            foreach(i, 1:nparts, progress=trace, function(newdatai=splits(newdata,i), predi=splits(pred,i), coef=coef){
                predi <- cbind(1,newdatai) %*% coef
                update(predi)
            })

            switch(type,
	            response = {
                    if (trace) message("Calculating response prediction")
                    foreach(i, 1:nparts, progress=trace, function(predi=splits(pred,i), func=family(object)$linkinv){
                        predi <- func(predi)    
                        update(predi)
                    })                    
                },
	            link = )
        } else {                # with mask
            if(class(newdata) != class(mask))
                stop("newdata and mask should be of the same type")
            if( nrow(mask) != pred.row || ncol(mask) != 1 || any(partitionsize(mask)[,1] != partitionsize(newdata)[,1]))
                stop("'mask' must have the same partitioning pattern as newdata")
            if (trace) message("Calculating link prediction")
            foreach(i, 1:nparts, progress=trace, function(newdatai=splits(newdata,i), predi=splits(pred,i), coef=coef, maski=splits(mask,i)){
                good <- maski > 0
                if(all(good)) {
                    predi <- cbind(1,newdatai) %*% coef
                    update(predi)
                } else if(any(good)) {
                    if(sum(good) == 1)
                        predi[good,] <- cbind(1,matrix(newdatai[good,],1,)) %*% coef
                    else
                        predi[good,] <- cbind(1,newdatai[good,]) %*% coef
                    update(predi)
                }
            })

            switch(type,
	            response = {
                    if (trace) message("Calculating response prediction")
                    foreach(i, 1:nparts, progress=trace, function(predi=splits(pred,i), func=family(object)$linkinv, maski=splits(mask,i)){
                        good <- maski > 0
                        if(all(good)) {
                            predi <- func(predi)    
                            update(predi)
                        } else if(any(good)) {
                            predi[good,] <- func(predi[good,])    
                            update(predi)
                        }
                    })                    
                },
	            link = )
        }
    } else {                # newdata is a normal array
        if(is.null(mask)) { # no mask
            if (trace) message("Calculating link prediction")
            pred <- cbind(1,newdata) %*% coef
            switch(type,
	            response = {
                    if (trace) message("Calculating response prediction")
                    pred <- family(object)$linkinv(pred)
                },
	            link = )
        } else {            # with mask
            if(class(newdata) != class(mask))
                stop("newdata and mask should be of the same type")
            if( nrow(mask) != NROW(newdata) || ncol(mask) != 1 )
                stop("'mask' must have a single column and the same number of rows as newdata")
            pred <- matrix(NA, NROW(newdata), 1)
            if (trace) message("Calculating link prediction")
            pred[mask > 0,] <- cbind(1,newdata[mask > 0,]) %*% coef
            switch(type,
	            response = {
                    if (trace) message("Calculating response prediction")
                    pred[mask > 0,] <- family(object)$linkinv(pred[mask > 0,])
                },
	            link = )
        }
    }
    pred
}

## it overwrites generic fitted function and will be used in validation
fitted.hpdglm <- function (object, ...) {
    object$d.fitted.values
}

## distributed version of aic
.d.aic <- function(family, y, mu, wt, dev, trace=TRUE, nobs) {
    nparts <- npartitions(y)
    tempArray = darray(dim=c(1,nparts), blocks=c(1,1), data=0)
    switch(family$family,
        "binomial" = {            
            if (trace) {
                message("Calculating aic")
                starttime<-proc.time()
            }

            m <- wt

            foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), mi=splits(m,i), yi=splits(y,i),
                     mui=splits(mu,i), wti=splits(wt,i)){
                good <- wti > 0
                if(all(good)) {
                    tempArrayi <- -2*sum(ifelse(mi > 0, (wti/mi), 0)*dbinom(round(mi*yi), round(mi), mui, log=TRUE))
                    update(tempArrayi)
                } else if(any(good)) {
                    tempArrayi <- -2*sum(ifelse(mi[good,] > 0, (wti[good,]/mi[good,]), 0)*dbinom(round(mi[good,]*yi[good,1]), round(mi[good,]), mui[good,], log=TRUE))
                    update(tempArrayi)
                }
            })
            aic <- sum(tempArray)
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
        },
        "gaussian" = {
            if (trace) {
                message("Calculating aic")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), wti=splits(wt,i)){
                good <- wti > 0
                if(all(good)) {
                    tempArrayi <- sum(log(wti))
                    update(tempArrayi)
                } else if(any(good)) {
                    tempArrayi <- sum(log(wti[good,]))
                    update(tempArrayi)
                }
            })            
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
            aic <- nobs*(log(dev/nobs*2*pi)+1)+2 - sum(tempArray)             
        },
        "poisson" = {
            if (trace) {
                message("Calculating aic")
                starttime<-proc.time()
            }
            foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), mui=splits(mu,i), yi=splits(y,i),
                     wti=splits(wt,i)){
                good <- wti > 0
                if(all(good)) {
                    tempArrayi <- -2*sum(dpois(yi, mui, log=TRUE) * wti)
                    update(tempArrayi)
                } else if(any(good)) {
                    tempArrayi <- -2*sum(dpois(yi[good,], mui[good,], log=TRUE) * wti[good,])
                    update(tempArrayi)
                }
            })
            aic <- sum(tempArray)            
            if (trace) {    # end of timing step
                endtime <- proc.time()
                spentTime <- endtime-starttime
                message("Spent time: ",(spentTime)[3]," sec")
            }
        },
        stop(sQuote(family$family), " family not supported yet")
    )

    return(aic)
}

# .naCheck checks any missed value (NA, NaN, or Inf) in X, and Y
# This function may alter weights
.naCheck <- function(X, Y, weights, trace) {
    nparts <- npartitions(Y)
    tempArray = darray(dim=c(1,nparts), blocks=c(1,1), data=0)

    if (trace) {
        message("Checking for missed values")
        starttime<-proc.time()
    }
    foreach(i, 1:nparts, progress=trace, function(tempArrayi=splits(tempArray,i), wi=splits(weights,i), xi=splits(X,i), yi=splits(Y,i)){
        yTest <- !is.finite(yi)
        xTest <- matrix(rowSums(!is.finite(xi)),nrow(yi),ncol(yi)) != 0
        test <- yTest | xTest
        if(any(test)) {
            tempArrayi <- as.numeric(sum(test))
            wi[test] <- 0
            update(tempArrayi)
            update(wi)
        }
    })
    nOmits <- sum(tempArray) # number of excluded samples

    if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        message("Spent time: ",(spentTime)[3]," sec")
    }
    
    nOmits
}
