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
#  File predictHPdRF.R
#
#  This code provides prediction of distributed test data using random forest.
#
#
#########################################################

"predictHPdRF" <- function (object, newdata, trace=FALSE) {
    # validating arguments
    if (!inherits(object, "randomForest"))
        stop("object not of class randomForest")
    if (object$type != "classification" && object$type != "regression")
        stop("only objects of type 'classification' and 'regression' are supported")
    if (is.null(object$forest)) stop("No forest component in the object")
    if (!is.darray(newdata) && !is.dframe(newdata))
        stop("newdata should be of type darray or dframe")

    nparts <- npartitions(newdata)
    nSamples <- NROW(newdata)
    if(nSamples == 0) stop("No sample found in the newdata")

    if((object$type == "classification") || is.dframe(newdata)) { # the output will be a dframe; either because the output is categorical or to be consistent with the input
        have.dframe = TRUE
        if(is.darray(newdata)) {
            output <- dframe(npartitions=npartitions(newdata), distribution_policy=newdata@distribution_policy)
        } else
    	    output <- clone(newdata,ncol = 1)
    } else { # the output will be a darray because it would be regression and the input type is darray
        have.dframe = FALSE
        output <- clone(newdata,ncol = 1)
    }

    errorList <- dlist(nparts) # to track any error
    foreach(i, 1:nparts, progress=trace, function(object=object, new=splits(newdata,i), out=splits(output,i), 
            erri=splits(errorList,i), have.dframe=have.dframe, coln=colnames(newdata)){
        library(randomForest)
        
        result <- tryCatch({
            colnames(new) <- coln
            out <- predict(object,new)
            if(have.dframe) out <- data.frame(out)
            update(out)
          }, error = function(err) {
            erri <- list(err)
            update(erri)
          }
        )
    })

    anyError <- getpartition(errorList)
    if(length(anyError) > 0)
        stop(anyError[[1]])

    output
}
