\name{distributedR_shutdown}
\alias{distributedR_shutdown}
\title{distributedR_shutdown}

\description{Shutdown session. Stops all workers, closes connections to
  them, and cleans resources.  \code{\link{distributedR_shutdown}} is called
  automatically in the following cases:

\itemize{
\item a worker or an executor is killed 
\item user interrupts execution using CTRL-C and decides to shutdown the whole session
}
}

\usage{distributedR_shutdown()}

\arguments{
%  \item{pm}{pointer to PrestoMaster object. By default, finds the
%  object from session.}
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
distributedR_status()
distributedR_shutdown()
}
}
