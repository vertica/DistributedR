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


####################################################################################
###                     Data-distributed and model-centralized prediction function of distributed GBM
####################################################################################
predict.hpdegbm <- function(object, newdata, trace = FALSE) {
  # model: assembled GBM model
  # best.iter: best iteration of sub-models
  # newdata: testing data in dframe/darray
  # type="link" (gaussian, bernoulli, adaboost) or "response" (multinomial)

  ############### data-distributed and model-centralized  prediction #################
  # suitable for models whose size can fit into the memory
  # train data: npartition_train/nExecutor_train
  # test data : npartition_test/nExecutor_test. maybe different from train data

  # extract GBM models and corresponding best iterations (n.trees)
  model     <- object$model
  best.iter <- object$bestIterations

  # extract distribution from trained model
  firstModelDistr <- object$distribution
  distributionGBM <- firstModelDistr[[1]] 

  # check function arguments
  if(missing(object))
    stop("'object' is a required argument")
 
  if (is.null(object$n.trees) || is.null(object$distribution) || 
      is.null(object$bestIterations))
    stop("The input hpdegbm model is invalid")
  
  if(missing(newdata))
    stop("'newdata' is a required argument")

  if(!is.dframe(newdata) && !is.darray(newdata) && !is.data.frame(newdata) && !is.matrix(newdata))
     stop("'newdata' must be a dframe or darray or data.frame or matrix")
 
  # prediction for distributed big data 
  if((is.dframe(newdata)) || (is.darray(newdata))) {
     npartition_test <- npartitions(newdata)  
     nExecutor_test  <- npartition_test

     nTest <- nrow(newdata)

     if (distributionGBM == "multinomial") {
        daPredict <- dframe(dim=c(nTest,1), blocks=c(ceiling(nTest/npartition_test),1))
     } else {
        daPredict <- darray(dim=c(nTest,1), blocks=c(ceiling(nTest/npartition_test),1))
     }

     # distributed prediction
     foreach(i, 1:nExecutor_test, function(GBM_model       = model, 
                                           best.iter       = best.iter, 
                                           predi           = splits(daPredict,i),  
                                           data2           = splits(newdata,i),
                                           distributionGBM = distributionGBM, 
                                           type            = "link") { 
        library(gbm)

        # Gaussian distribution for regression
        if (distributionGBM == "gaussian") {
           # Prediction by the first sub-model: AdaBoost, Bernoulli distribution for binary classification
           GBM_modeli <- GBM_model[[1]]
           best.iteri <- best.iter[1,1]
           predi <- predict(GBM_modeli, data2, best.iteri, type="link")  

           # fusion with other sub-models
           npartition_train <- nrow(best.iter)
        if (npartition_train > 1) {
           for (k in 2: npartition_train) {
               GBM_modelk <- GBM_model[[k]]
               best.iterk <- best.iter[k,1]
               predi <- predi + predict(GBM_modelk, data2, best.iterk, type="link")
           }
        } 
           predi <- predi/npartition_train # average of regressions from sub-models
        }

         # bernoulli or AdaBoost distribution for binary classification
        if ((distributionGBM == "bernoulli") | (distributionGBM == "adaboost") ) {
           # Prediction by the first sub-model: AdaBoost, Bernoulli distribution for binary classification
           GBM_modeli <- GBM_model[[1]]
           best.iteri <- best.iter[1,1]
           predi < - sign(predict(GBM_modeli, data2, best.iteri, type)) # AdaBoost/Bernoulli 

           # fusion with other sub-models
           npartition_train <- nrow(best.iter)
        if (npartition_train > 1) {
             for (k in 2: npartition_train) {
                 GBM_modelk <- GBM_model[[k]]
                 best.iterk <- best.iter[k,1]
                 predi <- predi + sign(predict(GBM_modelk, data2, best.iterk, type))
             } 
           }
           predi <- sign(predi)
         }


         # multinomial distribution for multi-class classification
         if (distributionGBM == "multinomial") {
           # Prediction by the first sub-model
           # X: dframe/darray; Y: dframe (factor vector originally)/darray
           GBM_modeli <- GBM_model[[1]]
           best.iteri <- best.iter[1,1]
           pred0 <- predict(GBM_modeli, data2, best.iteri, type="response") # fuzzy classification of multi-class

           # fusion with other sub-models
           npartition_train <- nrow(best.iter)
          if (npartition_train > 1) {
             for (k in 2: npartition_train) {
                 GBM_modelk <- GBM_model[[k]]
                 best.iterk <- best.iter[k,1]
                 pred0 <- pred0 + (predict(GBM_modelk, data2, best.iterk, type="response")) # fusion of sub-models
             }
          } 
          predi <- apply(pred0,1,which.max)
          # predi <- as.matrix(predi) # multi-lass classification: output {1,2, classID}
          predi <- as.data.frame(sapply(predi, function(x) colnames(pred0)[x])) # multi-class classification: output is a factor vector
         }

          update(predi)
     }) 

    # Predictions <- getpartition(daPredict)
     Predictions <- daPredict
   } else{
     # centralized prediction for small data (newdata). newdata is data.frame or matrix
     nTest <- nrow(newdata)
     GBM_model=model

     # centralized prediction: newdata is a data.frame or matrix
     library(gbm)

     # Gaussian distribution for regression
     if (distributionGBM == "gaussian") {
        # Prediction by the first sub-model: gaussian distribution for regression
        GBM_modeli <- GBM_model[[1]]
        best.iteri <- best.iter[1,1]
        predi <- predict(GBM_modeli, newdata, best.iteri, type="link") 

        # fusion with other sub-models
        npartition_train <- nrow(best.iter)
        if (npartition_train > 1) {
            for (k in 2: npartition_train) {
                GBM_modelk <- GBM_model[[k]]
                best.iterk <- best.iter[k,1]
                predi <- predi + predict(GBM_modelk, newdata, best.iterk, type="link")
            } 
        }
        predi <- predi/npartition_train # average of regressions from sub-models
      }

    # bernoulli or AdaBoost distribution for binary classification
    if ((distributionGBM == "bernoulli") | (distributionGBM == "adaboost")) {
        # Prediction by the first sub-model: AdaBoost, Bernoulli distribution for binary classification
        GBM_modeli <- GBM_model[[1]]
        best.iteri <- best.iter[1,1]
        predi <- sign(predict(GBM_modeli, newdata, best.iteri, type="link") ) # AdaBoost/Bernoulli 

        # fusion with other sub-models
        npartition_train <- nrow(best.iter)
        if (npartition_train > 1) {
            for (k in 2: npartition_train) {
                GBM_modelk <- GBM_model[[k]]
                best.iterk <- best.iter[k,1]
                predi <- predi + sign(predict(GBM_modelk, newdata, best.iterk, type="link"))
            } 
        }
        predi <- sign(predi)
      }


     # multinomial distribution for multi-class classification
     if (distributionGBM == "multinomial") {
        # Prediction by the first sub-model
        # X: dframe/darray; Y: dframe (factor vector originally)/darray
        GBM_modeli <- GBM_model[[1]]
        best.iteri <- best.iter[1,1]
        pred0 <- predict(GBM_modeli, newdata, best.iteri, type="response") # fuzzy classification of multi-class

        # fusion with other sub-models
        npartition_train <- nrow(best.iter)
        if (npartition_train > 1) {
            for (k in 2: npartition_train) {
                GBM_modelk <- GBM_model[[k]]
                best.iterk <- best.iter[k,1]
                pred0 <- pred0 + (predict(GBM_modelk, newdata, best.iterk, type="response")) # fusion of sub-models
            }
        } 
        predi <- apply(pred0,1,which.max) # output: {1,2,3,...}
      }
      
      if (distributionGBM == "multinomial") {
             Predictions <- sapply(predi, function(x) colnames(pred0)[x])
      } else {
             Predictions <- predi
      }
  } # end of else

  return(Predictions)
} # end of predict.hpdegbm





