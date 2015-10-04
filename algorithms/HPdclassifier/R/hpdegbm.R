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
       bag.fraction = 0.50, #0.5-0.8
       samplingFlag = TRUE,  
       sampleThresh=100,
       trace = FALSE) 
# X_train: a dframe, darray, data frame, or data matrix containing the predictor variables
# Y_train: a vector of outputs
# distribution: support Gaussian, AdaBoost, bernoulli, multinomial in DR version 1.2
# n.trees: the total number of trees to fit 
# interactive.depth: the maximum depth of variable interactions
# n.minobsinnode: minimum number of obervations in the tress terminal nodes
# shrinkage: learning rate. Typical vaule: [0.001, 1]
# bag.fraction: the fraction of the training set observations randomly selected for the next tree in the expansion
# verbose: If TRUE, gbm prints out progress and performance indicators
# trace: If TRUE, print out the training time
# samplingFlag: If true, call distributed sampling
# sampleThresh: sample threshold

### start of hpdegbm function
{
  ##########################################################################
  # Argument checks
  ##########################################################################
  if(missing(X_train))
    stop("'X_train' is a required argument")

  if(missing(Y_train))
    stop("'Y_train' is a required argument")

  if(!is.dframe(X_train) & !is.data.frame(X_train) & 
     !is.darray(X_train) & !is.matrix(X_train)) {
    stop("'X_train' must be a dframe or data.frame or darray or matrix")
  } 

  if (nrow(X_train) == 0) {
    stop("'X_train' must be non-empty")
  }

  if(!(.isdarrayorframe(Y_train) || is.data.frame(Y_train) ||
       is.matrix(Y_train) || is.vector(Y_train)) || 
     (is.vector(Y_train) && is.na(Y_train))) 
    stop("'Y_train' must be a dframe or data.frame or darray or matrix or numeric vector")

  if (is.vector(Y_train) && length(Y_train) == 0)
    stop("'Y_train' must be non-empty")

  if (!is.vector(Y_train) && nrow(Y_train) == 0)
    stop("'Y_train' must be non-empty")

  if (is.vector(Y_train) && nrow(X_train) != length(Y_train)) 
    stop("'Y_train' must have the same number of rows as 'X_train'")

  if (!is.vector(Y_train) && nrow(X_train) != nrow(Y_train))
    stop("'Y_train' must have the same number of rows as 'X_train'")

  if ((.isdarrayorframe(X_train) && !.isdarrayorframe(Y_train)) ||
      (.isdarrayorframe(Y_train) && !.isdarrayorframe(X_train)))
    stop("Either both 'X_train' and 'Y_train' must be dobjects, or neither can be dobjects")

  if (.isdarrayorframe(X_train) && !.isRowPartitioned(X_train))
    stop("'X_train' must be partitioned row-wise")

  if (.isdarrayorframe(Y_train) && !.isRowPartitioned(Y_train))
    stop("'Y_train' must be partitioned row-wise")

  # If it's a vector, no need to check this
  if ((.isdarrayorframe(Y_train) || is.matrix(Y_train) ||
       is.data.frame(Y_train))  && ncol(Y_train) != 1)
    stop("'Y_train' must only have one column")

  if (.isdarrayorframe(X_train) &&  .isdarrayorframe(Y_train)) {
    partsize1 <- partitionsize(X_train)
    partsize2 <- partitionsize(Y_train)

    if (!all(dim(partsize1) == dim(partsize2)) || !all(partsize1[,1] ==
                                                       partsize2[,1]))
      stop("'X_train' and 'Y_train' must have the same number of partitions, and corresponding partitions must have the same size")
  }

  if (missing(nExecutor)) {
    if (.isdarrayorframe(X_train))
      nExecutor <- npartitions(X_train) 
    else 
      nExecutor <- sum(distributedR_status()$Inst)
  }

  if (.isdarrayorframe(X_train) && !samplingFlag &&
      (nExecutor > npartitions(X_train))) {
    nExecutor <- npartitions(X_train)
    warning(paste("When samplingFlag is FALSE, 'nExecutor' may not be greater than
            the number of input partitions. Setting nExecutor =", nExecutor))
  }

  if (!.isPositiveInteger(nExecutor)) 
    stop("'nExecutor' must be a positive integer")

  if(missing(distribution))
    stop("'distribution' is a required argument")

  if (is.na(distribution) || !(distribution == "gaussian"  || 
                               distribution == "bernoulli" || 
                               distribution == "adaboost"  || 
                               distribution == "multinomial"))
    stop("'distribution' must be gaussian or bernoulli or adaboost or multinomial")

  if (!.isPositiveInteger(n.trees)) 
    stop("'n.trees' must be a positive integer")

  if (!.isPositiveInteger(interaction.depth)) 
    stop("'interaction.depth' must be a positive integer")

  if (!.isPositiveInteger(n.minobsinnode)) 
    stop("'n.minobsinnode' must be a positive integer")

  if (!(is.numeric(shrinkage) && length(shrinkage) == 1 && 
        shrinkage >= 0.001 && shrinkage <= 1))
    stop("'shrinkage' must be a number in the interval [0.001,1]")

  if (!(is.numeric(bag.fraction) && length(bag.fraction) == 1 && 
        bag.fraction <= 1 && bag.fraction > 0)) 
    stop("'bag.fraction' must be a number in the interval (0,1]")

  if(trace) {
    message("Start model training")
    starttime <- proc.time()
  }

  if (!(samplingFlag == TRUE || samplingFlag == FALSE))
    stop("'samplingFlag' must be TRUE or FALSE")
 
  if (!(is.numeric(sampleThresh) && length(sampleThresh) == 1 && sampleThresh > 0))
    stop("'sampleThresh' must be a positive number")

  if (samplingFlag && .isdarrayorframe(Y_train)) {
    # Used to compute the number of samples needed per partition
    nClass <- if (distribution == "gaussian") {
                1
              } else if (distribution == "adaboost" || distribution == "bernoulli") {
                2  
              } else if (distribution == "multinomial") {
                if (trace) {
                  message("Counting number of classes in response...")
                }
                # Count the number of classes in Y_train
                if (is.dframe(Y_train)) {
                  length(levels.dframe(Y_train)$Levels[[1]])
                } else if (is.darray(Y_train)) {
                  classCounts <- dframe(npartition = npartitions(Y_train))
                  foreach(i, 1:npartitions(Y_train), 
                          function(ys = splits(Y_train, i), 
                                   ccs = splits(classCounts, i)) {
                    ccs <- as.data.frame(names(table(ys[,1])))
                    update(ccs)
                  })
                  length(unique(getpartition(classCounts)[,1]))
                } 
              }
    if (trace) {
      message(paste0("Counted ", nClass,  " classes in Y_train"))
    }
  }
  ##########################################################################
  # Model training
  ##########################################################################

   # store trained gbm model
   dl_GBM_model <- dlist(nExecutor)
   dbest.iter <- darray(c(nExecutor,1), c(1,1))  
  
   # For small data, build nExecutor models on the whole dataset
   if (!is.dframe(X_train) && !is.darray(X_train)) {       
     foreach(i, 1:nExecutor, function(dGBM_modeli       = splits(dl_GBM_model,i), 
                                      best.iter         = splits(dbest.iter,i),
                                      x                 = X_train,
                                      y                 = Y_train, 
                                      n.trees           = n.trees,
                                      distribution      = distribution,
                                      interaction.depth = interaction.depth,
                                      n.minobsinnode    = n.minobsinnode,
                                      shrinkage         = shrinkage,
                                      bag.fraction      = bag.fraction,
                                      buildLocalModel   = .buildLocalGbmModel) {
        buildLocalModel(dGBM_modeli, best.iter, x, y, n.trees, distribution,
                        interaction.depth, n.minobsinnode, shrinkage,
                        bag.fraction)

      }, progress = trace)
    } else {  
      # For big data: Sample the data into nExecutor partitions and build a
      # model on each
      
      # Distributed sampling
      if (samplingFlag == TRUE) {
        nTrain   <- nrow(X_train)
        nFeature <- ncol(X_train)
         
        # Use a heuristic to compute a good sampleRatio, but only take at most
        # 90% of the input data per output partition in order to maintain
        # variety among the samples on which models are trained
        sampleRatio <- min(0.9, (sampleThresh * nClass * nFeature) / nTrain)
        if (trace) {
          message(paste0("Beginning distributed sampling, with samplingRatio = ",
                         sampleRatio))
          samplingStart <- proc.time()
        }

        # Perform distributed sampling. The outputs contain as many models as
        # executors to be built
        sampledXY <- hpdsample(X_train, Y_train, nSamplePartitions = nExecutor,
                               samplingRatio = sampleRatio)
        if (trace) {
          samplingTime <- (proc.time() - samplingStart)["elapsed"]
          message(paste0("Distributed sampling complete, took ", 
                         samplingTime, "s"))

        }
        sX_train <- sampledXY[[1]]
        sY_train <- sampledXY[[2]]
      } else { # Don't sample the data
        sX_train <- X_train
        sY_train <- Y_train
      }

      if (trace) {
        message("Checking to ensure all partitions contain all classes")
      }

      # Check which classes are in each partition of Y_train. Each partition
      # must contain all of the classes
      if (distribution != "gaussian") {
        npartition_Y <- npartitions(sY_train)
        # Will store the class names contained in each partition
        dpartClasses  <- dlist(npartition = npartition_Y) 

        foreach(i, 1:npartition_Y, function(y   = splits(sY_train, i),
                                            pcs = splits(dpartClasses, i)) {
          tab <- table(y)
          # Store names of classes with non-zero count in this partition
          pcs <- list(names(tab[which(tab != 0)]))
          update(pcs)
        }, progress = trace)

        pclasses <- getpartition(dpartClasses)

        if (!all(sapply(pclasses, function(cs) all(cs == pclasses[[1]])))) {
          if (samplingFlag) 
            stop("Bad sampling: Some partitions do not contain all classes. Please try again")
          else 
            stop("Some partitions do not contain all classes. Try using samplingFlag == TRUE")
        }
      }
      if (trace) {
        message("Building local models")
      }
 
      # Build local model on each partition of sampled data
      foreach(i, 1:nExecutor, function(dGBM_modeli       = splits(dl_GBM_model,i), 
                                       best.iter         = splits(dbest.iter,i), 
                                       x                 = splits(sX_train, i),
                                       y                 = splits(sY_train, i),
                                       n.trees           = n.trees,
                                       distribution      = distribution,
                                       interaction.depth = interaction.depth,
                                       n.minobsinnode    = n.minobsinnode,
                                       shrinkage         = shrinkage,
                                       bag.fraction      = bag.fraction,
                                       buildLocalModel   = .buildLocalGbmModel) {
        buildLocalModel(dGBM_modeli, best.iter, x, y, n.trees, distribution,
                        interaction.depth, n.minobsinnode, shrinkage,
                        bag.fraction)
      }, progress = trace)
    }

    # output of hpdegbm: GBM model and best iteration estiamtion
    GBM_model1 <- getpartition(dl_GBM_model)
    best.iter1 <- getpartition(dbest.iter)

    if(trace) {
      timing_info <- (proc.time() - starttime)["elapsed"]
      message(paste0("Total execution time: ", timing_info, 's'))
    }

    finalModel <- list()
  
    cl      <- match.call()
    cl[[1]] <- as.name("hpdegbm")

    finalModel$call           <- cl
    finalModel$model          <- GBM_model1 
    finalModel$distribution   <- distribution 
    finalModel$n.trees        <- n.trees
    finalModel$numGBMModel    <- nExecutor
    finalModel$bestIterations <- best.iter1
    finalModel$featureNames   <- colnames(X_train)
    class(finalModel) <- c("hpdegbm", "gbm")

    return (finalModel)
} # end of hpdegbm for model training

print.hpdegbm <- function(x, ...) {
  print(x$call)
  cat(paste("Number of GBM models: ", x$numGBMModel, "\n"))
  cat(paste("Distribution: ", x$distribution, "\n"))    
  cat(paste("Number of trees: ", x$n.trees, "\n"))
  cat(paste("Best iterations of GBM models: ", "\n"))
  print(x$bestIterations)
  cat(paste("Feature names: ", "\n"))
  print(x$featureNames)
}

# Helper function to build a gbm model on a partition
.buildLocalGbmModel <- function(dGBM_modeli, best.iter, x, y, n.trees,
                                distribution, interaction.depth,
                                n.minobsinnode, shrinkage, bag.fraction) {
  library(gbm)

  # gbm.fit requires that y be a vector
  if (is.data.frame(y))
    y <- y[,1]

  # Apply gbm.fit for GBM modeling: local GBM model
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
    verbose = FALSE,
    var.names = NULL,
    group = NULL)

  best.iter0 <- gbm.perf(dGBM_model, 
      plot.it = FALSE, 
      oobag.curve = FALSE, 
      overlay = FALSE, 
      method="OOB")

  best.iter <- max(50, as.matrix(best.iter0))

  dGBM_modeli <- list(dGBM_model) 

  update(dGBM_modeli)
  update(best.iter)
}

# Used for error checking. Returns TRUE if x is a positive integer
.isPositiveInteger <- function(x) 
  return (is.numeric(x) && length(x) == 1 && x%%1 == 0 && x > 0)

# Used for error checking. Returns TRUE if x is a darray or a dframe
.isdarrayorframe <- function(x) return (is.darray(x) || is.dframe(x))

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

.isRowPartitioned <- function(d) {
  # Tells whether d is row-wise partitioned or not
  #
  # Args:
  #   d: A darray/dframe
  #
  # Returns:
  #   A boolean that is TRUE if d is row-wise partitioned and FALSE otherwise
  return (all(partitionsize(d)[,2] == ncol(d)))
}
