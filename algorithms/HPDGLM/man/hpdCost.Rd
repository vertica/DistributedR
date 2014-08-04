\name{hpdCost}
\alias{hpdCost}
\title{
    Prediction Cost
}
\description{
    The defualt distributed cost function for v.hpdglm and cv.hpdglm
}
\usage{
hpdCost(y, yhat, mask = NULL)
}

\arguments{
  \item{y}{
    darray of the real response
  }
  \item{yhat}{
    darray of the predicted response
  }
  \item{mask}{
    mask indicates which samples (rows) should be considered in the calculation. It should be a darray with a single column, and 0 or 1 as the value of its elements.
  }
}
\details{
     equivalent sequential: mean((y[mask==1,]-yhat[mask==1,])^2)
}
\value{
    a real value corresponding to the cost
}

\author{
Arash Fard
}

\keyword{ prediction }
\keyword{ distributed model }
