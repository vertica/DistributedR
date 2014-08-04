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
# It is mostly inspired by bootfuns.q of R package boot
#
# Aarsh Fard (afard@vertica.com)
# 

#########################################################
## A copy of bootfuns.q license:
#
# part of R package boot
# copyright (C) 1997-2001 Angelo J. Canty
# corrections (C) 1997-2011 B. D. Ripley
#
# Unlimited distribution is permitted

########################################### Split-sample-Validation #####################################################
# responses, predictors: darrays containing data
# hpdglmfit: An object of class hpdglm containing the result of a fitted model
# cost: An optional cost function for validation
# percent: the percent of data which should be set aside for validation
# sampling_threshold: threshold for the method of sampling (centralized or distributed). It should be always smaller than 1e9
v.hpdglm <- function(responses, predictors, hpdglmfit, cost=hpdCost, percent=30, sampling_threshold = 1e6)
{
    ## validation estimate of error for hpdglm prediction with a specific percent as validation set.
    ## cost is a function of two arguments: the observed values and the the predicted values.
    call <- match.call()

    ## check the type of responses and predictors
    if( is.null(responses) || is.null(predictors) )
        stop("'responses' and 'responses' are required arguments")
    if(! is.darray(responses) || ! is.darray(predictors))
        stop("'responses' and 'predictors' should be distributed arrays")
    nparts <- npartitions(responses)  # number of partitions (blocks)
    nSample <- nrow(responses)       # number of samples

    blockSize <- responses@blocks[1]

    if (!exists(".Random.seed", envir=.GlobalEnv, inherits = FALSE)) runif(1)
    seed <- get(".Random.seed", envir=.GlobalEnv, inherits = FALSE)

    if ( blockSize > 1e9 )
        stop("sampling does not work for 'blockSize > 1e9'")
    if ((percent >= 100) || (percent <= 0))
        stop("'percent' outside allowable range: 0 < percent < 100")

    mask <- darray(dim=c(nSample,1), blocks=c(blockSize,1), data=1)
    validationSize <- ceiling(nSample * percent / 100)   

    if ( blockSize > sampling_threshold || nSample > 1e9) {       # distributed sampling
        cat("Updating mask through distributed sampling\n")
        foreach(i, 1:nparts, samplingFunction <- function(maski=splits(mask,i), vSize=round(validationSize/nparts)){
            vSamples <- sample.int(nrow(maski), vSize)
            maski[vSamples] <- 0
            update(maski)
        })
    } else {                                                       # centralized sampling
        validationSamples <- sample.int(nSample, validationSize)
        cat("Updating mask through centralized sampling\n")

        foreach(i, 1:nparts, function(maski=splits(mask,i), validationSamples=validationSamples, idx=i, blockSize=blockSize){
            start <- (idx-1) * blockSize
            end <- nrow(maski) + start      # the last block might be smaller      
            filter <- validationSamples[(validationSamples > start & validationSamples <= end)] - start
            maski[filter] <- 0
            update(maski)
        })
    }

    hpdglm.y <- hpdglmfit$responses
    if(is.null(hpdglmfit$na_action)) {   # when there is no missed data in initial samples
        cost.0 <- cost(hpdglm.y, fitted(hpdglmfit))
    } else {            # when there are missed data in initial samples
        cost.0 <- cost(hpdglm.y, fitted(hpdglmfit), mask=hpdglmfit$prior.weights)
    }

    Call <- hpdglmfit$call
    ## we want data and weights (mask) from here but family from the parent.
    Call$responses <- responses
    Call$predictors <- predictors
    Call$weights <- mask
    d.hpdglm <- eval.parent(Call)

    ## in the process of building model, mask will be updated to filter samples with missed data    
    cost.i <- cost(responses, predict(d.hpdglm, predictors, type = "response", mask=mask) ,mask=mask)
    if(is.null(hpdglmfit$na_action)) {   # when there is no missed data in initial samples
        cost.0 <- cost.0 - cost(responses, predict(d.hpdglm, predictors, type = "response"))
    } else {            # when there are missed data in initial samples
        cost.0 <- cost.0 - cost(responses, predict(d.hpdglm, predictors, type = "response", 
                    mask=hpdglmfit$prior.weights), mask=hpdglmfit$prior.weights)
        missedPercent <- hpdglmfit$na_action$numbers * 100 / nSample
        warning(format(missedPercent,digits=4),
            " percent of samples excluded because of missed data. Big percentage can affect the acuracy of the validation.")
    }

    list(call = call, percent = percent,
         delta = as.numeric(c(cost.i, cost.i + cost.0)),  # drop any names
         seed = seed)
}

########################################### Cross-Validation #####################################################
# responses, predictors: darrays containing data
# hpdglmfit: An object of class hpdglm containing the result of a fitted model
# cost: An optional cost function for validation
# K: number of folds in cross validation
# sampling_threshold: threshold for the method of sampling (centralized or distributed). It should be alwas smaller than 1e9
cv.hpdglm <- function(responses, predictors, hpdglmfit, cost=hpdCost, K=10, sampling_threshold = 1e6)
{
    # cross-validation estimate of error for hpdglm prediction with K groups.
    # cost is a function of two arguments: the observed values and the predicted values.
    call <- match.call()

    ## check the type of responses and predictors
    if( is.null(responses) || is.null(predictors) )
        stop("'responses' and 'responses' are required arguments")
    if(! is.darray(responses) || ! is.darray(predictors))
        stop("'responses' and 'predictors' should be distributed arrays")
    nparts <- npartitions(responses)  # number of partitions (blocks)
    nSample <- nrow(responses)       # number of samples

    blockSize <- responses@blocks[1]

    if (!exists(".Random.seed", envir=.GlobalEnv, inherits = FALSE)) runif(1)
    seed <- get(".Random.seed", envir=.GlobalEnv, inherits = FALSE)

    if ( blockSize > 1e9 )
        stop("sampling does not work for 'blockSize > 1e9'")

    Kmax <- ifelse(nSample < 10000, nSample, 100L)      ## limits for number of folders
    if ((K > Kmax) || (K <= 1))
        stop("'K' outside allowable range")
    
    
    if ( blockSize > sampling_threshold || nSample > 1e9 || nSample/K > sampling_threshold) {       # distributed sampling
        ## folds keeps the fold tags for each sample
        folds <- darray(dim=c(nSample,1), blocks=c(blockSize,1))
        cat("Distributed sampling\n")
        foreach(i, 1:nparts, function(foldsi=splits(folds,i), K=K, .sample0=.sample0){
            fSize <- ceiling(nrow(foldsi)/K)
            s <- .sample0(rep(1L:K, fSize), nrow(foldsi))
            foldsi <- matrix(as.numeric(s), nrow(foldsi), ncol(foldsi))
            update(foldsi)
        })                        
    } else {                                                      # centralized sampling
        cat("Centralized sampling\n")
        fSize <- ceiling(nSample/K)
        folds <- .sample0(rep(1L:K, fSize), nSample)
    }

    mask <- darray(dim=c(nSample,1), blocks=c(blockSize,1), data=1)

    hpdglm.y <- hpdglmfit$responses
    if(is.null(hpdglmfit$na_action)) {   # when there is no missed data in initial samples
        cost.0 <- cost(hpdglm.y, fitted(hpdglmfit))
    } else {            # when there are missed data in initial samples
        cost.0 <- cost(hpdglm.y, fitted(hpdglmfit), mask=hpdglmfit$prior.weights)
    }
    CV <- 0
    Call <- hpdglmfit$call
    Call$responses <- responses
    Call$predictors <- predictors

    for(iteration in 1:K) {
        cat("*** Fold number ",iteration," ***\n")
        if (is.darray(folds)) {       # distributed masking
            cat("Updating mask\n")
            foreach(i, 1:nparts, function(maski=splits(mask,i), foldsi=splits(folds,i), iteration=iteration){
                maski <- ifelse(foldsi == iteration, 0, 1)
                update(maski)
            })
         } else {                     # centralized masking
            cat("Updating mask\n")
            foreach(i, 1:nparts, function(maski=splits(mask,i), folds=folds, iteration=iteration, idx=i, blockSize=blockSize){
                start <- (idx-1) * blockSize + 1
                end <- nrow(maski) + start     # the last block might be smaller
                foldsi <- matrix(folds[start:end], nrow(maski), ncol(maski))
                maski <- ifelse(foldsi == iteration, 0, 1)
                update(maski)
            })
        }

        ## we want data and weights (mask) from here but family from the parent.
        Call$weights <- mask
        d.hpdglm <- eval.parent(Call)
        p.alpha <- 1/K
        ## in the process of building model, mask will be updated to filter samples with missed data
        cost.i <- cost(responses, predict(d.hpdglm, predictors, type = "response", mask=mask) ,mask=mask)
        CV <- CV + p.alpha * cost.i
        if(is.null(hpdglmfit$na_action)) {   # when there is no missed data in initial samples
            cost.0 <- cost.0 - p.alpha * cost(responses, predict(d.hpdglm, predictors, type = "response"))
        } else {            # when there are missed data in initial samples
            cost.0 <- cost.0 - p.alpha * cost(responses, predict(d.hpdglm, predictors, type = "response", 
                    mask=hpdglmfit$prior.weights), mask=hpdglmfit$prior.weights)
            missedPercent <- hpdglmfit$na_action$numbers * 100 / nSample
        }
    } # iteration
    if(!is.null(hpdglmfit$na_action))
        warning(format(missedPercent,digits=4),
            " percent of samples excluded because of missed data. Big percentage can affect the acuracy of the validation.")

    list(call = call, K = K,
         delta = as.numeric(c(CV, CV + cost.0)),  # drop any names
         seed = seed)
}


## making elements of a darray to a power. It changes the input X.
d.pow <- function (X, power) {
    if (! is.darray(X))
        stop("d.pow needs a darray as input")
    if (! is.numeric(power))
        stop("power should be a number")
    foreach(i, 1:npartitions(X), function(Xi=splits(X,i), power=power) {
        Xi <- Xi ^ power
        update(Xi)
    })
    X
}

## defualt distributed cost function
## y and yhat are of type darray
## mask indicates which samples (rows) should be considered in the calculation
## mask should be a darray with a single column, and 0 or 1 as the value of its elements
## equivalent sequential: mean((y[mask==1,]-yhat[mask==1,])^2)
hpdCost <- function (y, yhat, mask=NULL) {
    cat("Calculating cost\n")
    if(is.null(mask)) {
        res <- mean(d.pow(y-yhat, 2))
        return (res )
    }

    nparts <- npartitions(y)
    dcost <- darray(dim=c(1,nparts), blocks=c(1,1))
    foreach(i, 1:nparts, function(yi=splits(y,i), yhati=splits(yhat,i), maski=splits(mask,i), dcosti=splits(dcost,i)){
        good <- maski > 0
        if(any(good)) {
            y_filter <- yi[good,]
            yhat_filter <- yhati[good,]
            dcosti <- sum((y_filter - yhat_filter)^2)
            update(dcosti)
        }
    })
    cost <- sum(dcost) / (sum(mask) * ncol(y))
    return(cost)
}

.sample0 <- function(x, ...) x[sample.int(length(x), ...)]

