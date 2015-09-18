####################################################################################
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
####################################################################################

####################################################################################
#  File hpdegbm.R
#  Distributed version of gbm 

#  This code is a distributed version of gbm function availabel in R gbm package.

#  myModel <- hpdegbm (.....)
#  GBM_model <- myModel[[1]] # trained GBM sub-models
#  best.iter <- myModel[[2]] # best iteration of each sub-models

####################################################################################
## A copy of gbm R package license:
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

####################################################################################




####################################################################################
###                Model training function of distributed GBM
####################################################################################
hpdegbm <- function(
       X_train,
       Y_train, 
       nExecutor,                                     
       distribution,
       n.trees = 1000, 
       interaction.depth = 1, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.50, #0.5-0.8,
       offset = NULL, 
       misc = NULL, 
       w = NULL,
       var.monotone = NULL,
       nTrain = NULL,
       train.fraction = NULL,
       keep.data = FALSE,
       verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       var.names = NULL,
       #response.name = "y",
       group = NULL,
       trace = FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       completeModel = FALSE,
       samplingFlag = TRUE,  
       nClass,
       sampleThresh=100) # default system parameters are defined here

# X_train: a dframe, darray, data frame, or data matrix containing the predictor variables
# Y_train: a vector of outputs
# distribution: support Gaussian, AdaBoost, bernoulli, multinomial in DR version 1.2
# n.trees: the total numner of tress to fit 
# interactive.depth: the maximum depth of variable interactions
# n.minobsinnode: minimum number of obervations in the tress terminal nodes
# shrinkage: learning rate. Typical vaule: [0.001, 1]
# bag.fraction: the fraction of the training set observations randomly selected for the next tree in the expansion
# verbose: If TRUE, gbm prints out progress and performance indicators
# trace: If TRUE, print out the training time
# completeModel: If TRUE, add training time in the model 
# samplingFlag: If true, call distributed sampling
# nClass: a parameter used to determine the sampling ratio. For classification, it is the class number 
# sampleThresh: sample threshold

### start of hpdegbm function
{
   # parameters checking
   if(missing(X_train))
	stop("'X_train' is a required argument")

   if(missing(Y_train))
	stop("'Y_train' is a required argument")

   if(!is.dframe(X_train) & !is.data.frame(X_train) & !is.darray(X_train) & !is.matrix(X_train)) {
       stop("'X_train' must be a dframe or data.frame or darray or matrix")
   } else {
          nSamples <- nrow(X_train)
          if (nSamples == 0) stop("X_train has 0 rows")
   }

   if(!is.dframe(Y_train) & !is.data.frame(Y_train) & !is.darray(Y_train) & !is.matrix(Y_train) & !is.vector(Y_train)) 
       stop("'Y_train' must be a dframe or data.frame or darray or matrix or numeric vector")

   if ( (is.vector(Y_train))  & !(nrow(X_train) == length(Y_train)) ) 
       stop("'Y_train' must be non-empty and have same number of rows as 'X_train'")

   if ( ((distribution=="bernoulli") || (distribution=="adaboost") || (distribution=="gaussian")) && (is.dframe(Y_train)) )
      stop("'Y_train' cannot be dframe for regression and binary classification")


   if (missing(nExecutor))   
       nExecutor <- sum(distributedR_status()$Inst)

   if (!( (is.numeric(nExecutor)) && (length(nExecutor)==1) && (nExecutor%%1 == 0) && (nExecutor > 0) )) 
        stop("'nExecutor' must be a positive integer number")

   if(missing(distribution))
	stop("'distribution' is a required argument")

   if ( !(is.na(distribution)) && (!(distribution=="gaussian")) && (!(distribution=="bernoulli")) && (!(distribution=="adaboost")) && (!(distribution=="multinomial"))  )
       stop("'distribution' must be gaussian or bernoulli or adaboost or multinomial")

  if (!( (is.numeric(n.trees)) && (length(n.trees)==1) && (n.trees%%1 == 0) && (n.trees > 0) )) 
        stop("'n.trees' must be a positive integer")

   if (!( (is.numeric(interaction.depth)) && (length(interaction.depth)==1) && (interaction.depth%%1 == 0) && (interaction.depth > 0) )) 
        stop("'interaction.depth' must be a positive integer")

   if (!( (is.numeric(n.minobsinnode)) && (length(n.minobsinnode)==1) && (n.minobsinnode%%1 == 0) && (n.minobsinnode > 0) )) 
        stop("'n.minobsinnode' must be a positive integer")

   if ( !( (is.numeric(shrinkage)) && (length(shrinkage)==1) && (shrinkage >= 0.001) && (shrinkage <= 1) ))
        stop("'shrinkage' must be a number in the interval [0.001,1]")

   if (!( (is.numeric(bag.fraction)) && (length(bag.fraction)==1) && (bag.fraction <= 1) && (bag.fraction > 0) )) 
        stop("'bag.fraction' must be a number in the interval (0,1]")

   if(trace) {
        cat("Start model training\n")
        starttime <- Sys.time()
    }

   if (!((samplingFlag == TRUE) | (samplingFlag == FALSE)))
         stop("'samplingFlag' must be TRUE or FALSE")

   if ( !(distribution=="gaussian") & (missing(nClass)) & ((is.dframe(X_train))) | ((is.darray(X_train))) )
	stop("'nClass' is a required argument for X_train as dframe or darray and non-gaussian distribution")


   if (!( (is.numeric(nClass)) && (length(nClass)==1) && (nClass%%1 == 0) && (nClass > 0) )) 
        stop("'nClass' must be a positive integer")

  
   if (!( (is.numeric(sampleThresh)) && (length(sampleThresh)==1) && (sampleThresh > 0) ))
        stop("'sampleThresh' must be a positive number")

   if (distribution == "gaussian")
       nClass = 1

   # store trained gbm model
   dl_GBM_model <- dlist(nExecutor)
   dbest.iter <- darray(c(nExecutor,1), c(1,1))  
  
   # model training
   if ((!is.dframe(X_train)) & (!is.darray(X_train))) { # for small data, load the whole data into every core
      # system parameters are transfered into foreach
      foreach(i, 1:nExecutor, function(dGBM_modeli=splits(dl_GBM_model,i), best.iter=splits(dbest.iter,i), x=X_train,y=Y_train, 
                  n.trees=n.trees, distribution=distribution, interaction.depth=interaction.depth, n.minobsinnode=n.minobsinnode, 
                  shrinkage=shrinkage, bag.fraction=bag.fraction, .tryCatchWE=.tryCatchWE) {
      library(gbm)

      if (distribution=="multinomial") {
         y <- unlist(y) #  convert it back to "factor" for multinomial distribution
      }

      # apply gbm.fit for GBM modeling: local GBM model
      dGBM_model <- gbm.fit(x, y,  
         offset = NULL, 
         misc = NULL, 
         n.trees = n.trees, 
         distribution = distribution,
         w = NULL,
         var.monotone = NULL,
         interaction.depth = interaction.depth, 
         n.minobsinnode = n.minobsinnode,
         shrinkage = shrinkage, 
         bag.fraction = bag.fraction, 
         nTrain = NULL,
         train.fraction = NULL, 
         keep.data = FALSE,
         verbose = TRUE,
         var.names = NULL,
         response.name = "y",
         group = NULL)

       # estimate the best iteration
       best.iter0 <- gbm.perf(dGBM_model, 
              plot.it = FALSE, 
              oobag.curve = FALSE, 
              overlay = FALSE, 
              method="OOB")

       if (best.iter0 <50) best.iter0 <- 50
       best.iter <- as.matrix(best.iter0)


       # convert dGBM_model to a list
       dGBM_modeli <- list(dGBM_model) 
  
       update(dGBM_modeli)
       update(best.iter)
      })
    } else{  ### For big data: load each partition into every core
     # X_train: dframe/darray
     # Y_train: dframe/darray
     # if nExecutor > npartition_train, distributed sampling can generate nExecutor partitions. nExecutor=npartition_of_sampled X_train. Or use one partition multiple times by i%%npartition_train+1
     npartition_train <- npartitions(X_train)

     ### distributed sampling: hpdsampling
     if (samplingFlag == TRUE) {
        sampledXY <- hpdsampling(X_train,Y_train, nClass, sampleThresh)
        sX_train <- sampledXY[[1]]
        sY_train <- sampledXY[[2]]
     } else {
        sX_train <- X_train
        sY_train <- Y_train
     }

     foreach(i, 1:nExecutor, function(dGBM_modeli=splits(dl_GBM_model,i), best.iter=splits(dbest.iter,i), x=splits(sX_train,i%%npartition_train+1),y=splits(sY_train,i%%npartition_train+1),
                  n.trees=n.trees, distribution=distribution, interaction.depth=interaction.depth, n.minobsinnode=n.minobsinnode, 
                  shrinkage=shrinkage, bag.fraction=bag.fraction, .tryCatchWE=.tryCatchWE) {
         library(gbm)

         if (distribution=="multinomial") {
            y <- unlist(y) #  convert it back to "factor" for multinomial distribution
         }

         ### apply gbm.fit for GBM modeling: local GBM model
         dGBM_model <- gbm.fit(x, y,  
            offset = NULL, 
            misc = NULL, 
            distribution = distribution,
            w = NULL,
            var.monotone = NULL,
            n.trees = n.trees, 
            interaction.depth = interaction.depth, 
            n.minobsinnode = n.minobsinnode,
            shrinkage = shrinkage,
            bag.fraction = bag.fraction, 
            nTrain = NULL,
            train.fraction = NULL,
            keep.data = FALSE,
            verbose = TRUE,
            var.names = NULL,
            #response.name = "y",
            group = NULL)

          best.iter0 <- gbm.perf(dGBM_model, 
              plot.it = FALSE, 
              oobag.curve = FALSE, 
              overlay = FALSE, 
              method="OOB")

          if (best.iter0 <50) best.iter0 <- 50

          best.iter <- as.matrix(best.iter0)

          dGBM_modeli <- list(dGBM_model) 
  
          update(dGBM_modeli)
          update(best.iter)
        })
    }
  
    if(trace) {
	timing_info <- Sys.time() - timing_info
	print(timing_info)
    }

    # output of hpdegbm: GBM model and best iteration estiamtion
    GBM_model1 <- getpartition(dl_GBM_model)
    best.iter1 <- getpartition(dbest.iter)

   # if (completeModel) {
   #    finalModel <- list(hpdegbm_Model="GBM_model1",bestIterations="best.iter1", trainingTime="timing_info")
   # } else {
   #    finalModel <- list(hpdegbm_Model="GBM_model1",bestIteration="best.iter1")
   # }

    if (completeModel) {
       finalModel <- list(GBM_model1,best.iter1, timing_info)
    } else {
       finalModel <- list(GBM_model1, best.iter1)
    }

    cl <- match.call()
    cl[[1]] <- as.name("hpdegbm")
    finalModel$call <- cl
    class(finalModel) <- c("hpdegbm", "gbm")

    finalModel$model <- finalModel[[1]]
    finalModel$distribution <- finalModel[[1]][[1]]$distribution
    finalModel$n.trees <- n.trees
    finalModel$numGBMModel <- nExecutor
    finalModel$bestIterations <- finalModel[[2]]

    return (finalModel)

} # end of hpdegbm for model training


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


print.hpdegbm <- function(x, ...)
{
    print(x$call)
    cat(paste("\n Number of GBM models: ", x$numGBMModel, "\n"))
    cat(paste("distribution: ", x$distribution, "\n"))    
    cat(paste("n.trees: ", x$ n.trees, "\n"))
    cat(paste("best iterations of GBM models: ", x$bestIterations, "\n"))
    #print(x$model[[1]]) 
}





