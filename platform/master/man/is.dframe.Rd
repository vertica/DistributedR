\name{is.dframe}
\alias{is.dframe}
\title{is.dframe}

\description{Check if input object is dframe.}

\usage{is.dframe(x)}

\arguments{
  \item{x}{input object.}
}

\value{ Returns true if object is distributed data.frame.}

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
\code{\link{dframe}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
df<-data.frame(x=rep(sample(0:1),4), y=rep(sample(5:6), 4), z=rep(sample(2:3), 4))
is.dframe(df)
ddf<-dframe(dim=c(5,5),blocks=c(1,5))
is.dframe(ddf)
distributedR_shutdown()
}
}
