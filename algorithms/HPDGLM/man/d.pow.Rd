\name{d.pow}
\alias{d.pow}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
    Power Function for darray
}
\description{
    It makes the elements of a darray to a power
}
\usage{
d.pow(X, power)
}
\arguments{
  \item{X}{
    a darray
  }
  \item{power}{
    a numerical value
  }
}
\value{
    the input darray which has its elements have raised to a power.
}

\author{
    Arash Fard
}

\examples{
 \dontrun{
    require(distributedR)
    distributedR_start()
    Y <- as.darray(as.matrix(faithful$eruptions),
                   c(ceiling(length(faithful$eruption)/4),1))
    Y <- d.pow(Y, 2)
 }
}

\keyword{ Distributed R }
