\name{distributedR_master_info}
\alias{distributedR_master_info}
\title{distributedR_master_info}

\description{Shows the master information of the current distributedR session.
The master information includes the address, port number and session ID that idenfies the session from other distributedR sessions.
}

\usage{distributedR_master_info()}

\arguments{
}

\value{}

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

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{distributedR_start}}, \code{\link{distributedR_status}}
}

\examples{
\dontrun{
library(distributedR)
##Start worker process 
distributedR_start()
distributedR_master_info()
distributedR_shutdown()
}
}
