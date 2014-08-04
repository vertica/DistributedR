\name{is.darray}
\alias{is.darray}
\title{is.darray}

\description{Check if input object is darray.}

\usage{is.darray(x)}

\arguments{
  \item{x}{input object.}
}

\value{ Returns true if object is distributed array.}

\references{
  \itemize{
   \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
    Learning and Graph Processing with Sparse
    Matrices. \emph{EuroSys'13},
    197--210.
    \item Homepage: http://www.hpl.hp.com/research/distributedr.htm
%    \item Mailing list: presto-dev@external.groups.hp.com
  }
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

\seealso{
\code{\link{darray}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
m<-matrix(sample(0:1, 16, replace=T), nrow=4)
is.darray(m)
dm<-darray(dim=c(5,5),blocks=c(1,5))
is.darray(dm)
distributedR_shutdown()
}
}
