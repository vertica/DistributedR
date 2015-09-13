#####################################################################################
# Copyright [2013] Hewlett-Packard Development Company, L.P.                        # 
#                                                                                   #
# This program is free software; you can redistribute it and/or                     #
# modify it under the terms of the GNU General Public License                       #
# as published by the Free Software Foundation; either version 2                    #
# of the License, or (at your option) any later version.                            #
#                                                                                   #
# This program is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                    #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                      #
# GNU General Public License for more details.                                      #
#                                                                                   #
# You should have received a copy of the GNU General Public License                 #
# along with this program; if not, write to the Free Software                       #
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.    #
#####################################################################################

##################################### [Classification only] Calculating Confusion Matrix ####################################
# observed: the response observed in the test data
# predicted: the predicted value for response
confusionMatrix <- function(observed, predicted) {
    if (NROW(observed) != NROW(predicted)) stop("The number of values in 'observed' should be the same as the number of values in 'predicted'")
    if (NCOL(observed) != 1) stop("'observed' should have a single value per sample")
    if (NCOL(predicted) != 1) stop("'predicted' should have a single value per sample")
    if (is.darray(observed) || is.darray(predicted)) stop("darray is an invalid format for either 'observed' or 'predicted'")
    if (is.dlist(observed) || is.dlist(predicted) ) stop("dlist is not a valid type for the inputs")

    if (is.dframe(observed)) {
        oLabels <- levels.dframe(observed, colID=1)[[1]][[1]]
    } else { # the data is not distributed
        if (!is.data.frame(observed) && !is.character(observed) && !is.factor(observed))
            stop("the supported types for 'observed' are dframe, dataframe, matrix, character and factor")
        if(is.data.frame(observed))    observed <- factor(observed[,1])
        else if(is.character(observed)) observed <- factor(observed)
        oLabels <- levels(observed)
    }

    # it is assumed that an appropriate predict function is generated 'provided' input
    if (is.dframe(observed)) {        # dframe
        if(!is.dframe(predicted))   stop("'predicted' must be a dframe when observed is a dframe")
        if(any(partitionsize(observed) != partitionsize(predicted)))
            stop("'predicted' must have the same partitioning as 'observed'")
        pLabels <- levels.dframe(predicted, colID=1)[[1]][[1]]
        if(!all (pLabels %in% oLabels))
            stop("there are categories in 'predicted' that are not available in 'observed'")

        dconf <- dlist(npartitions(observed))
        foreach(i, 1:npartitions(observed), progress=FALSE, function(confi=splits(dconf,i), yi=splits(observed,i), y.predi=splits(predicted,i), oLabels=oLabels) {
            yt <- factor(yi[,1], levels=oLabels) # make it simple factor
            yp <- factor(y.predi[,1], levels=oLabels)

            confusion <- table(observed = yt, predicted = yp)[oLabels, oLabels]
            
            confi <- list(confusion) # ordering the rows and columns
            update(confi)
        })

        confList <- getpartition(dconf)
        confusion <- Reduce('+', confList)
    } else {                   # non dframe
        if (!is.data.frame(predicted) && !is.character(predicted) && !is.factor(predicted))
            stop("'predicted' must be an ordinary R object when 'observed' is the same")
        if(is.data.frame(predicted))
            predicted <- factor(predicted[,1])
        else if(is.character(predicted))
            predicted <- factor(predicted)
        pLabels <- levels(predicted)
        if(!all (pLabels %in% oLabels))
            stop("there are categories in 'predicted' that are not available in 'observed'")

        predicted <- factor(predicted, levels=oLabels) # extends its levels
        confusion <- table(observed = observed, predicted = predicted)[oLabels, oLabels]
    }
    confusion
}


##################################### [Classification only] Calculating Error Rate ####################################
# observed: the response observed in the test data
# predicted: the predicted value for response
errorRate <- function(observed, predicted) {
    confusion <- confusionMatrix(observed, predicted)
    # calculating the error rate per class
    class.error <- 1 - diag(confusion)/rowSums(confusion)
    nCorrectPrediction <- sum(diag(confusion))
    nPredictions <- sum(confusion)
    err.rate <- (nPredictions - nCorrectPrediction) / nPredictions
    names(err.rate) <- "err.rate"

    c(err.rate, class.error)
}


##################################### [Regration only] Calculating Mean of squared residuals ####################################
# observed: the response observed in the test data
# predicted: the predicted value for response
# na.rm: logical.  Should missing values (including ‘NaN’) be removed?
"meanSquared" <- function(observed, predicted, na.rm=FALSE) {
    if (NROW(observed) != NROW(predicted)) stop("The number of values in 'observed' should be the same as the number of values in 'predicted'")
    if (NCOL(observed) != 1) stop("'observed' should have a single value per sample")
    if (NCOL(predicted) != 1) stop("'predicted' should have a single value per sample")
    if (is.dlist(observed) || is.dlist(predicted) ) stop("dlist is not a valid type for the inputs")

    # it is assumed that 'predicted' is provided by an appropriate predict function
    if (is.dframe(observed) || is.darray(observed)) {        # dframe or darray
        if(!(is.dframe(predicted) || is.darray(predicted) ))   stop("'predicted' must be a distributed object when 'observed' is")
        if(any(partitionsize(observed) != partitionsize(predicted) ))
            stop("'predicted' must have the same partitioning as 'observed'")

        dmse <- darray(dim=c(npartitions(observed),1), blocks=c(1,1), data=0)
        nValidSamples <- clone(dmse)
        foreach(i, 1:npartitions(observed), progress=FALSE, function(dmsei=splits(dmse,i), yi=splits(observed,i), y.predi=splits(predicted,i), dn=splits(nValidSamples,i), na.rm=na.rm) {
            diff <- (yi - y.predi)
            dmsei <- matrix(sum(diff ^2, na.rm=na.rm))
            if(na.rm)
                dn <- matrix(sum(!is.na(diff)))
            else
                dn <- matrix(NROW(diff))
            update(dmsei)
            update(dn)
        })

        mse <- sum(dmse) / sum(nValidSamples)
    } else {                   # not distributed
        if (!is.data.frame(predicted) && !is.numeric(predicted))
            stop("'predicted' must be an ordinary R object when 'observed' is")
        
        if(is.data.frame(observed) || is.matrix(observed))
            observed <- observed[,1] # make it a simple array
        if(is.data.frame(predicted) || is.matrix(predicted))
            predicted <- predicted[,1] # make it a simple array

        mse <- mean((observed - predicted)^2, na.rm=na.rm)
    }
    
    mse
}


##################################### [Regration only] Calculating “R-squared”: 1 - mse / Var(observed) ####################################
# observed: the response observed in the test data
# predicted: the predicted value for response
# na.rm: logical.  Should missing values (including ‘NaN’) be removed?
"rSquared" <- function(observed, predicted, na.rm=FALSE) {
    # many input validation will be done inside "meanSquared.default"
    mse <- meanSquared(observed, predicted, na.rm=na.rm)
    if(is.na(mse)) return(NA)
    
    if (is.dframe(observed) || is.darray(observed)) {        # dframe or darray
        observed_mean <- colMeans(observed, na.rm=na.rm)
        dy_var <- darray(dim=c(npartitions(observed),1), blocks=c(1,1), data=0)
        nValidSamples <- clone(dy_var)
        foreach(i, 1:npartitions(observed), progress=FALSE, function(dy_vari=splits(dy_var,i), yi=splits(observed,i), observed_mean=observed_mean, dn=splits(nValidSamples,i), na.rm=na.rm) {
            diff <- (yi - observed_mean)
            dy_vari <- sum(diff ^2, na.rm=na.rm)
            if(na.rm)
                dn <- matrix(sum(!is.na(diff)))
            else
                dn <- matrix(NROW(diff))

            update(dy_vari)
            update(dn)
        })
        # in the randomForst package the population-variance is used instead of sample-variance
        observed_var <- sum(dy_var)/ (sum(nValidSamples) -1)
        rsq <- 1 - mse / observed_var
    } else {                   # not distributed
        if(is.data.frame(observed) || is.matrix(observed))
            observed <- observed[,1] # make it a simple array

        # in the randomForst package the population-variance is used instead of sample-variance
        rsq <- 1 - mse / var(observed, na.rm=na.rm)
    }
    
    rsq
}

