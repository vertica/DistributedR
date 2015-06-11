\name{update}
\alias{update}
\title{update}

\description{ Globally publish modifications done to a
  \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} 
  inside a \code{\link{foreach}} loop.
}

\usage{update(x)}

\arguments{
  \item{x}{input array, data.frame, list, or list of multiple splits.}
}

\details{
  \code{\link{update}} can be used only inside the \code{\link{foreach}}
  loop function.

  The \code{\link{foreach}} loop or the function
  executed by it does not return any value. Instead, users can call
  \code{\link{update}} to modify distributed arrays, data frames 
  or lists and publish changes. Note that \code{\link{update}} is
  the only way to make side-effects globally visible. 

  Also note that if x is a list of multiple splits, then all of the splits associated with the list will be updated. Updating a specific partition of a list of multiple splits is not currently supported.
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
      \item Homepage: http://www.vertica.com/distributedr
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
\dontrun{
library(distributedR)
distributedR_start()

da <- darray(dim=c(6,9),blocks=c(3,3),sparse=FALSE,data=0)

##First index gets c(1,2,3), second index gets c(4,5,6), etc.
get.three.indices <- function(index){
    start = 3*(index-1) + 1
    indices = c(start:(start+2))
    indices
}

##Update darray 3 splits at a time, with parallelism of 2
foreach(i,1:2,function(a=splits(da,get.three.indices(i)),data=get.three.indices(i)){
       a[[1]] = matrix(data[[1]],nrow(a[[1]]),ncol(a[[1]]))
       a[[2]] = matrix(data[[2]],nrow(a[[2]]),ncol(a[[2]]))
       a[[3]] = matrix(data[[3]],nrow(a[[3]]),ncol(a[[3]]))
       update(a)
    })

getpartition(da) ##counts from 1 to 6 in 3x3 blocks!
distributedR_shutdown()
}
}
