\name{hpdglm.control}
\alias{hpdglm.control}
\title{
    Auxiliary for Controlling hpdglm Fitting
}
\description{
  Auxiliary function for \code{\link{hpdglm}} fitting.
  Typically only used internally by \code{\link{hpdglm.fit}}, but may be
  used to construct a \code{control} argument to either function.
}
\usage{
hpdglm.control(epsilon = 1e-08, maxit = 25, trace = FALSE, 
               rigorous = FALSE)
}
\arguments{
  \item{epsilon}{
    It is used to adjust desired accuracy of the result.
  }
  \item{maxit}{
    It is the maximu, number of iterations before achieving the desired accuracy.
  }
  \item{trace}{
    When this argument is true, intermediate steps of the progress are displayed.
  }
  \item{rigorous}{
    When this argument is true, some extra checks are performed during fitting procedure. For example, mu and eta may be validating in each iteration to check if the fitted values are outside of the domain. Usually these checks are time consuming; therefore, the default value for this argument is FALSE.}
  }

\value{
    A list with components named as the arguments.
}

\examples{
 \dontrun{
    require(distributedR)
    distributedR_start()
    Y <- as.darray(as.matrix(mtcars$am),c(ceiling(length(mtcars$am)/4),1))
    X <- as.darray(as.matrix(cbind(mtcars$wt,mtcars$hp)),
                   c(ceiling(length(mtcars$hp)/4),2))

    myModel <- hpdglm(Y, X, binomial, control=list(epsilon=1e-02, maxit=5,
                      trace=FALSE, rigorous=TRUE)
 }
}

\keyword{ hpdglm model }
\keyword{ Distributed R }
