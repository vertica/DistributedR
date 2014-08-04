\name{update}
\alias{update}
\title{update}

\description{ Globally publish modifications done to a
  \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} 
  inside a \code{\link{foreach}} loop.
}

\usage{update(x)}

\arguments{
  \item{x}{input array, data.frame or list.}
}

\details{
  \code{\link{update}} can be used only inside the \code{\link{foreach}}
  loop function.

  The \code{\link{foreach}} loop or the function
  executed by it does not return any value. Instead, users can call
  \code{\link{update}} to modify distributed arrays, data frames 
  or lists and publish changes. Note that \code{\link{update}} is
  the only way to make side-effects globally visible.
}

\value{
}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
    AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
      Learning and Graph Processing with Sparse
      Matrices. \emph{EuroSys'13},
      197--210.
      \item Homepage: http://www.hpl.hp.com/research/distributedr.htm
%      \item Mailing list: presto-dev@external.groups.hp.com
    }
  }

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{darray}}, \code{\link{dframe}}, \code{\link{dlist}}, \code{\link{update}}, \code{\link{foreach}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
da <- darray(dim=c(9,9), blocks=c(3,3), sparse=FALSE, data=10)
cat("Number of partitions of da are ", npartitions(da),"\n")
db <- darray(dim=c(9,9), blocks=c(3,3), sparse=FALSE, data=5)
result <- darray(dim=c(9,9), blocks=c(3,3))
##Add two matrices in parallel
foreach(i, 1:npartitions(da),
  add<-function(a = splits(da,i),
                b = splits(db,i),
                c = splits(result,i)){
    c <- a + b
    update(c)
  })
getpartition(result)
distributedR_shutdown()
}
}
