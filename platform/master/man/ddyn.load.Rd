\name{ddyn.load}
\alias{ddyn.load}
\title{ddyn.load}

\description{Load \code{so} library on all Executors}

\usage{
    ddyn.load(x,trace=FALSE)
}

\arguments{
    \item{x}{A vector of packages that their \code{so} libraries need to be loaded on all executors}
    \item{trace}{when it is FALSE (default) the progress of the foreach will be hidden.}
}

\details{
    Use this function to load the specified list of libraries on all of the running executors.
}

\author{HP Vertica Development Team}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
  Learning and Graph Processing with Sparse Matrices. \emph{EuroSys'13},
  197--210.
  \item Homepage: http://www.vertica.com/distributedr
%  \item Mailing list: presto-dev@external.groups.hp.com
}
}

\seealso{
\code{\link{ddyn.unload}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
ddyn.load(c("HPdcluster"))
}
}
