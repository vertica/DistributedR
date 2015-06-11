\name{hpdglm}
\alias{hpdglm}
\alias{print.hpdglm}
\alias{hpdglm.fit}
\alias{fitted.hpdglm}
\alias{weights.hpdglm}
\alias{deviance.hpdglm}

\title{
Distributed Generalized Linear Models
}
\description{
hpdglm function is intended to be a distributed alternative for glm function.
}
\usage{
hpdglm(responses, predictors, family=gaussian, weights=NULL, 
na_action="exclude", start=NULL, etastart=NULL, mustart=NULL,
offset=NULL, control=list(...), method="hpdglm.fit.Newton",
completeModel=FALSE, ...)
}
\arguments{
  \item{responses}{
    the darray that contains the vector of responses.
  }
  \item{predictors}{
    the darray that contains the vector of predictors. hpdglm() cannot accept a predictor with constant value. Moreover, a categorical predictor should be decoded (converted to several predictors) before applying hpdglm().
  }
  \item{family}{
    it specifies the family function for regression. The supported family-links at the time of this writing are gaussian(identity), binomial(logit), and poisson(log). The mentioned links are the default ones for their families; so, specifying them is optional. The default family is Gaussian.
  }
  \item{weights}{
    it is an optional darray of 'prior weights' to be used in the fitting process. It has a single column. The number of rows and its number of blocks should be the same as responses. The values should not be negative (greater than or equal to zero). Weight zero on a sample makes it be ignored.
  }
  \item{na_action}{
    it indicates what should happen when the data contain missed values. Values of NA, NaN, and Inf in samples are treated as missed values. There are two options for this argument exclude and fail. When exclude is selected (the default choice), the weight of any sample with missed values will become zero, and that sample will be ignored in the fitting process. In the darray which will be created for residuals, the value corresponding to these samples will be NA. When fail is selected, the function will stop in the case of any missed value in the dataset.
  }
  \item{start}{
    starting values for coefficients. It is optional.
  }
  \item{etastart}{
    starting values for parameter 'eta' which is used for computing deviance. It should be of type darray. It is optional.
  }
  \item{mustart}{
    starting values for mu 'parameter' which is used for computing deviance. It should be of type darray. It is optional.
  }
  \item{offset}{
    an optional darray which can be used to specify an _a priori_ known component to be included in the linear predictor during fitting.
  }
  \item{control}{
    an optional list of controlling arguments. The optional elements of the list and their default values are: epsilon = 1e-8, maxit = 25, trace = FALSE, rigorous = FALSE.
  }
  \item{method}{
    this argument reserved for the future improvement. The only available fitting method at the moment is "hpdglm.fit.Newton". In the future, if we have new developed algorithms, this argument can be used to switch between them.
  }
  \item{completeModel}{
    when it is FALSE (default), calculation of several output values that are not required for prediction are skipped. Therefore, the function can perform faster.
  }
  \item{\dots}{
    arguments to be used to form the default
    \code{control} argument if it is not supplied directly.
  }
}
\details{
    predictors and responses must align with each other (have the same number of rows and similar partitioning). Models created either in complete or incomplete mode can be used for prediction. The only motivation behind completeModel=FALSE is performance. Indeed, caluculation of several values, which are not required for prediction, are skipped.
}
\value{
  \item{coefficients }{calculated coefficients}
  \item{d.residuals }{(available only when completeModel=TRUE; otherwise it is NULL) the working residuals, that is the residuals in the final iteration of the IWLS fit. Since cases with zero weights are omitted, their working residuals are NA. It is of type darray.}
  \item{d.fitted.values}{the fitted mean values, obtained by transforming the linear predictors by the inverse of the link function. It is of type darray.}
  \item{family}{the family function used for regression}
  \item{d.linear.predictors}{the linear fit on link scale. It is of type darray.}
  \item{deviance}{up to a constant, minus twice the maximized log-likelihood.}
  \item{aic}{(available only when completeModel=TRUE; otherwise it is NA) a version of Akaike's An Information Criterion, minus twice the maximized log-likelihood plus twice the number of parameters, computed by the aic component of the family.}
  \item{null.deviance}{(available only when completeModel=TRUE; otherwise it is NA) the deviance for the null model, comparable with deviance.}
  \item{iter}{the number of iterations of IWLS used.}
  \item{prior.weights}{the weights initially supplied. All of its values are 1 if no initial weights used. It is of type darray. The value of weight will become 0 for the samples with invalid data (NA, NaN, Inf).}
  \item{weights}{the working weights, that is the weights in the final iteration of the IWLS fit. It is of type darray. In order to save memory and execution time, no new darray will be created for weights when the initial weights are all 0 or 1, and it will simply be a reference to prior.weights.}
  \item{df.residual}{the residual degrees of freedom.}
  \item{df.null}{the residual degrees of freedom for the null model.}
  \item{converged}{logical. Was the IWLS algorithm judged to have converged?}
  \item{boundary}{logical. Is the fitted value on the boundary of the attainable values?}
  \item{responses}{the darray of responses.}
  \item{predictors}{the darray of predictors.}
  \item{na_action}{this item exists only when a few samples are excluded because of missed data. It is a list containing type "exclude" and the number of excluded samples.}
  \item{call}{the matched call.}
  \item{offset}{the offset darray used.}
  \item{control}{the value of the control argument used.}
  \item{method}{the name of the fitter function used, currently always "hpdglm.fit.Newton".}
}
\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    ## Example for linear regression
    library(HPdregression)
    distributedR_start()

    require(MASS)
    # creating the darray of response
    Y <- as.darray(data.matrix(Boston["medv"]))
    # creating the darray of predictors
    X <- as.darray(data.matrix(Boston[c("rad","crim","ptratio","dis")]))
    # building linear regression model
    reg <- hpdglm(Y,X, completeModel=TRUE)
    summary(reg)

    ## Example for logistic regression
    Y <- as.darray(data.matrix(mtcars["am"]))
    X <- as.darray(data.matrix(mtcars[c("wt","hp")]))

    # building logistic regression model
    myModel <- hpdglm(Y, X, binomial, completeModel=TRUE)
    summary(myModel)
    
    ## Example for poisson regression
    Y <- as.darray(data.matrix(mtcars["carb"]))
    X <- as.darray(data.matrix(mtcars[-which(colnames(mtcars)=="carb")]))
    # building linear regression model
    reg <- hpdglm(Y,X, poisson, completeModel=TRUE)
    summary(reg)
 }    
}

\keyword{ Generalized Linear Regression }
\keyword{ Distributed R }
