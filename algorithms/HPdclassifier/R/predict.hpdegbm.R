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


predict.hpdegbm <- function(object, newdata, trace = FALSE) {
  # Outputs a prediction for every row in newdata using an hpdegbm model.
  # Prediction may be either distributed or centralized, depending on the type
  # of newdata
  #
  # Args:
  #   model:     An object of class hpdegbm, an ensemble of gbm models
  #   best.iter: Best n.trees for each sub-model
  #   newdata:   Testing data in data.frame/matrix/dframe/darray. When newdata
  #              is a dobject, prediction is done in a distributed manner
  #
  # Returns:
  #   A vector/darray/dframe of predictions, one for each row of newdata. The
  #   output is a darray/dframe only if the input is also a darray/dframe

  # Check function arguments
  if (missing(object))
    stop("'object' is a required argument")
 
  if (is.null(object$n.trees) || is.null(object$distribution) || 
      is.null(object$bestIterations))
    stop("The input hpdegbm model is invalid")
  
  if (missing(newdata))
    stop("'newdata' is a required argument")

  if (!is.dframe(newdata) && !is.darray(newdata) && !is.data.frame(newdata) && !is.matrix(newdata))
     stop("'newdata' must be a dframe or darray or data.frame or matrix")

  if (!is.null(object$featureNames) && !is.null(colnames(newdata)) &&
      (length(colnames(newdata)) != length(object$featureNames) ||
       !all(colnames(newdata) == object$featureNames)))
    stop("'newdata' column names must be the same as those used to train the model")
 
  # Prediction
  results <- 
    if (is.darray(newdata) || is.dframe(newdata)) { # Distributed prediction
      # dobject to contain predictions. Each partition contains the
      # predictions from the corresponding partition of newdata
      preds <- if (object$distribution == "multinomial") 
                 dframe(npartition = npartitions(newdata))
               else 
                 darray(npartition = npartitions(newdata))

      # Make predictions on each partition of newdata
      foreach(i, 1:npartitions(newdata), function(ps = splits(preds, i),
                                                  nds = splits(newdata, i),
                                                  object = object,
                                                  ensemblePredict =
                                                    .ensemblePredict) {
        library(gbm)
        out <- ensemblePredict(object, nds)
        # Adjust the type of the predictions according to the type of preds
        ps <- if(is.data.frame(ps)) as.data.frame(out) 
              else as.matrix(out)
        update(ps)
      })
      if (object$distribution == "multinomial") {
        # Adjust local factors to be same as global factors for the case where
        # one partition doesn't contain predictions of all classes
        factor.dframe(preds)
      }
      preds
    } else { # Centralized prediction
      .ensemblePredict(object, newdata)
    }
  return(results)
} # End of predict.hpdegbm


.combinerFn <- function(ensemble, modelPreds) { 
  # Function that combines the predictions from different models into one
  # prediction based on the gbm distribution
  #
  # Args:
  #   ensemble:   An ensemble of models, should be the output of hpdegbm
  #   modelPreds: A list of predictions from the different models in the
  #               ensemble 
  # Returns:
  #   The aggregate prediction to be output by the ensemble
  distribution <- ensemble$distribution 
  if (distribution == "bernoulli" || distribution == "adaboost") {
    # For each prediction, choose the median of the probabilities output by each model
    apply(do.call(cbind, modelPreds), 1, median)
  } else if (distribution == "multinomial") {
    # Convert predictions from gbm.fit, which are probabilities for each class,
    # into hard predictions (class names)
    hardModelPreds <- lapply(modelPreds, function(mPred) apply(mPred, 1, function(row) {
      i <- which.max(row)
      colnames(mPred)[i]
    }))
    # Find the most commonly predicted class (voting)
    apply(do.call(cbind, hardModelPreds), 1, function(row)
          names(which.max(table(row))))
  } else if (distribution == "gaussian") {
    Reduce('+', modelPreds)/length(modelPreds)
  } else {
    stop(paste0("Invalid distribution: '", distribution, "'"))
  }
}

.ensemblePredict <- function(ensemble, newdata) {
  # Makes a prediction with each model in the ensemble and aggregates the
  # results to give a final prediction output for each row of newdata
  #
  # Args:
  #   ensemble: An object of class hpdegbm
  #   newdata:  A matrix/data.frame of predictor variables 
  # Returns:
  #   A prediction for each row of newdata
  localPreds <- lapply(seq_along(ensemble$model), function(i) {
                       predict(ensemble$model[[i]], newdata, 
                               n.trees = ensemble$bestIterations[[i]],
                               type = "response")})
  return (.combinerFn(ensemble, localPreds))
}
