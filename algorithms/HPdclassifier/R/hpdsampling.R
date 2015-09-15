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
#  File hpdsampling.R
#  This code is a distributed version of sample availabel in R gbm package.

####################################################################################



####################################################################################
#                  Distributed sampling
####################################################################################
hpdsampling <- function(dfX,daY, nClass, sampleThresh=100)
{
  #dfX: predictor variables in dframe/darray
  #daY: vector of outcomes in darray (dvector/dfactor is not supported yet in 2015-7-01)
  #nClass: number of classes
  # sampleThresh = 200: sample size planning factor
  #function output: dfRX, daRY                 

  ####################################################################################
  # check function arguments
   if(missing(dfX))
	stop("'dfX' is a required argument")

   if(missing(daY))
	stop("'daY' is a required argument")

   if(missing(nClass))
	stop("'nClass' is a required argument")

   if(nrow(dfX) != nrow(daY))
  	stop("'daY' must have same number of rows as 'dfX'")

   if(npartitions(dfX) != npartitions(daY))
	stop("'daY' must have same number of partitions as 'dfX'")


   if(!is.dframe(dfX) && !is.darray(dfX))
       stop("'dfX' must be a dframe or darray")

   # for binary classification/regression, daY is a darray. For multinomial distribution (multi-classification), daY is a dframe (factor vecotr originally)
   if(!is.dframe(daY) && !is.darray(daY))
       stop("'daY' must be a dframe or darray")

   if (missing(nClass)) 
   	stop("'nClass' is a required argument")

   if (!( (is.numeric(nClass)) && (length(nClass)==1) && (nClass%%1 == 0) && (nClass > 0) )) 
        stop("'nClass' must be a positive integer")

  
   if (!( (is.numeric(sampleThresh)) && (length(sampleThresh)==1) && (sampleThresh > 0) ))
        stop("'sampleThresh' must be a positive number")


   # sample size planning
   # Ns : size of sub-chunks. #Ns <- 1*ceiling((nTrain/npartition)/npartition) # size of sampled sub-chunk
   npartition <- npartitions(dfX)
   nTrain   <- nrow(dfX)
   p <- ncol(dfX) # p: number of variables
   
   sampleRatio <- (nTrain/(npartition * p * nClass)) / sampleThresh 
   if (sampleRatio > 1) {
      Ns <- ceiling((nTrain/npartition)/npartition) 
   }  else {
      Ns <- ceiling((1/sampleRatio) * (nTrain/npartition)/npartition)
   } 

   if (Ns > (nTrain/npartition))
      Ns <- ceiling(0.9*nTrain/npartition)

   ### distributed sampling of X_train, Y_train
   if (is.darray(dfX)) {
       dfSX <- darray(c(Ns*npartition,p), blocks=c(Ns,p))  
       dfRX <- darray(c(Ns*npartition*npartition,p), blocks=c(Ns*npartition,p)) 
   } else {
       dfSX <- dframe(c(Ns*npartition,p), blocks=c(Ns,p))  
       dfRX <- dframe(c(Ns*npartition*npartition,p), blocks=c(Ns*npartition,p))
   } 

   if (is.darray(daY)) {
        daSY <- darray(c(Ns*npartition,1), blocks=c(Ns,1)) 
        daRY <- darray(c(Ns*npartition*npartition,1), blocks=c(Ns*npartition,1))
   } else {
        daSY <- dframe(c(Ns*npartition,1), blocks=c(Ns,1)) 
        daRY <- dframe(c(Ns*npartition*npartition,1), blocks=c(Ns*npartition,1))
   }

  for ( k in 1: npartition) { # npartition
      # distributed sampling: The input and output have the same npartition 
      foreach(i, 1:npartition, function(X_train2=splits(dfX,i),Y_train2=splits(daY,i),SX_train=splits(dfSX,i),SY_train=splits(daSY,i), Ns=Ns ) {
             index <- sample(1:nrow(X_train2), Ns)
             SX_train <- X_train2[index,]

             if ((is.matrix(Y_train2)) | (is.numeric(Y_train2))) {
                 SY_train <- Y_train2[index,]
             } else {
                 SY_train <- as.data.frame(Y_train2[index,])
             }

          update(SX_train)
          update(SY_train)
       })

      # assemble sampled sub-chunks to form one partition of final training data in dframe/darray
      ASX <- getpartition(dfSX)
      ASY <- getpartition(daSY)

      # move assembled sampled chunk to one partition of final training data
      # For regression and binary classification, the response is a numeric vector
      # For multi-class classification, the response is a factor vector
      foreach(l,k:k, function(RX_train=splits(dfRX,l),RY_train=splits(daRY,l), ASX=ASX, ASY=ASY ) {
          RX_train <- ASX

          if (is.numeric(RY_train)) {
             RY_train <- as.array(ASY)
          } else {
              RY_train <- as.data.frame(ASY)
          }

          update(RX_train)
          update(RY_train)
       })
  } # end of for
  

  sampledXY <- list(dfRX, daRY)
  return(sampledXY) 

} # end of distributed sampling

# recover sampled dfRX and daRY
#X_train <- sampledXY[[1]]
#Y_train <- sampledXY[[2]]







