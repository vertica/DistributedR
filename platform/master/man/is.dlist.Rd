\name{is.dlist}
\alias{is.dlist}
\title{is.dlist}

\description{Check if input object is dlist.}

\usage{is.dlist(x)}

\arguments{
  \item{x}{input object.}
}

\value{ Returns true if object is distributed list.}

\references{
  \itemize{
   \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
    Learning and Graph Processing with Sparse
    Matrices. \emph{EuroSys'13},
    197--210.
    \item Homepage: http://www.vertica.com/distributedr
%    \item Mailing list: presto-dev@external.groups.hp.com
  }
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

\seealso{
\code{\link{dlist}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
l <- list(x=sample(1:10, 5), y=sample(1:50, 5))
is.dlist(l)
dl<-dlist(naprtitions=4)
is.dlist(dl)
distributedR_shutdown()
}
}
