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
hpdsampling <- function(dfX,daY, Ns, npartition)
{
  #dfX: predictor variables in dframe/darray
  #daY: vector of outcomes in darray (dvector/dfactor is not supported yet in2015-7-01
  #npartition: now the sampled data have the same npartition as input. In the future, the input and output may have different partitions
  #Ns : size of sub-chunks. #Ns <- 1*ceiling((nTrain/npartition)/npartition) # size of sampled sub-chunk
  #function output: dfRX, daRY                 

  ####################################################################################
  # check function arguments
   if(missing(dfX))
	stop("'dfX' is a required argument")

   if(missing(daY))
	stop("'Y_train' is a required argument")

   if(missing(Ns))
	stop("'Ns' is a required argument")

   if(missing(npartition))
	stop("'npartition' is a required argument")

   if(!is.dframe(dfX) && !is.darray(dfX))
       stop("'dfX' must be a dframe or darray")

   # for binary classification/regression, daY is a darray. For multinomial distribution (multi-classification), daY is a dframe (factor vecotr originally)
   if(!is.dframe(daY) && !is.darray(daY))
       stop("'daY' must be a dframe or darray")

    if (npartition <= 0)
       stop("'npartition' must be a positive number")

    if (Ns <= 0)
       stop("'Ns' must be a positive number")



  ### distributed sampling of X_train, Y_train
  p <- ncol(dfX) # p: number of variables
  dfSX <- dframe(c(Ns*npartition,p), blocks=c(Ns,p))  
  daSY <- darray(c(Ns*npartition,1), blocks=c(Ns,1))

  dfRX <- dframe(c(Ns*npartition*npartition,p), blocks=c(Ns*npartition,p))  
  daRY <- darray(c(Ns*npartition*npartition,1), blocks=c(Ns*npartition,1))

  for ( k in 1: npartition) { # npartition
      # distributed sampling: The input and output have the same npartition 
      foreach(i, 1:npartition, function(X_train2=splits(dfX,i),Y_train2=splits(daY,i),SX_train=splits(dfSX,i),SY_train=splits(daSY,i), Ns=Ns ) {
          index <- sample(1:nrow(X_train2), Ns)
          SX_train <- X_train2[index,]
          SY_train <- Y_train2[index,]

          update(SX_train)
          update(SY_train)
       })

      # assemble sampled sub-chunks to form one partition of final training data in dframe/darray
      ASX <- getpartition(dfSX)
      ASY <- getpartition(daSY)

      # move assembled sampled chunk to one partition of final training data
      foreach(l,k:k, function(RX_train=splits(dfRX,l),RY_train=splits(daRY,l), ASX=ASX, ASY=ASY ) {
          RX_train <- ASX
          RY_train <- as.array(ASY)

          update(RX_train)
          update(RY_train)
       })
  } # end of for
  
  ####return one dframe and one darray from a defined R function
  sampledXY <- list(dfRX, daRY)
  return(sampledXY) # dframe or darray

} # end of distributed sampling

# recover sampled dfRX and daRY
#X_train <- sampledXY[[1]]
#Y_train <- sampledXY[[2]]







