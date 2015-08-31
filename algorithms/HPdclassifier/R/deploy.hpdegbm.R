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
###                deploy.hpdegbm
####################################################################################
             
########################### hpdegbm V1.0 deploy
# inputModel: it is the model that is going to be prepared for deployment
  #GBM_model1 <- getpartition(dl_GBM_model)
  #best.iter1 <- getpartition(dbest.iter)
  #finalModel <- list(GBM_model1,best.iter1)

deploy.hpdegbm <- function(inputModel) {
    if(is.null(inputModel[[1]]))
        stop("The model does not contain ensemble of GBM sub-model and cannot be used for prediction")

    if(is.null(inputModel[[2]]))
        stop("The model does not contain best iterations of GBM sub-model and cannot be used for prediction")


    inputModel
}






