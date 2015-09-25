\name{distributedR_status}
\alias{distributedR_status}
\title{distributedR_status}

\description{ Show status of \pkg{distributedR} workers.}

\usage{
distributedR_status (help=FALSE)
}

\arguments{
  \item{help}{If true, describes each column}
}

\value{
Worker information is returned as a data.frame with the following
columns:
\item{Workers}{IP and port of each worker.}
\item{Inst}{number of executors at each worker.}
\item{SysMem}{total system memory at each worker.}
\item{MemUsed}{used system memory at each worker.}
\item{DarrayQuota}{total memory assgined for arrays. Not enforced by runtime.}
\item{DarrayUsed}{memory used to store arrays.}
}

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
\code{\link{distributedR_start}}, \code{\link{distributedR_shutdown}}
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
