\name{residuals.hpdglm}
\alias{residuals.hpdglm}
\alias{residuals}
\alias{resid}

\title{
    Extract Residuals of an hpdglm Model
}
\description{
    This function extracts model residuals of an hpdglm model in a darray. The abbreviated function is resid.
}
\usage{
residuals.hpdglm(object, type = c("deviance", "pearson", 
                 "working", "response", "partial"), ...)
}
\arguments{
  \item{object}{an hpdglm model}
  \item{type}{can be "deviance", "pearson", "working", "response", or "partial".}
}
\value{
    darray of residuals
}

\examples{
 \dontrun{
    require(distributedR)
    distributedR_start()
    Y <- as.darray(as.matrix(mtcars$am),
                   c(ceiling(length(mtcars$am)/4),1))
    X <- as.darray(as.matrix(cbind(mtcars$wt,mtcars$hp)),
                   c(ceiling(length(mtcars$hp)/4),2))

    myModel <- hpdglm(responses=Y, predictors=X, family= binomial(logit))
    res <- resid(myModel)
 }
}

\keyword{ hpdglm model }
\keyword{ Distributed R }
