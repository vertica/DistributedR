\name{splits}
\alias{splits}
\title{splits}

\description{Pass partition(s) of \code{\link{darray}}, 
  \code{\link{dframe}} or \code{\link{dlist}} to function in \code{\link{foreach}}.
}

\usage{ splits(x, y, z)}

\arguments{

  \item{x}{input distributed array, distributed data frame or distributed list.}

  \item{y}{index of partition to fetch. In a 2-D partition this is the
  row-index of partition (number of partitions above).}
  
  \item{z}{column-index of the partition in a 2-D partitioning scheme
    (number of partitions to the left). } 
}

\details{ \code{\link{splits}} can be used only as an argument to the
    function in a \code{\link{foreach}} loop. 

  If both y and z are missing then the full input
  \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} is returned.

  2-D partitioning is valid only for \code{\link{darray}} and \code{\link{dframe}}.
  Since \code{\link{dlist}} is paritioned length wise, only argument \code{y} is used to
  fetch a \code{\link{dlist}} partition. Argument \code{z} is undefined for \code{\link{dlist}}.
  
  Partitions are numbered from left to right and then top to bottom,
  i.e., row-major order. Partition numbers start from 1. For row
  partitioning (each partition has all the columns) or column
  partitioning (each partition has all the rows) index argument
  \code{z} should not be used. For 2-D partitioning, both index argument
  \code{y} and \code{z} may be used. 

  For example, the array \code{darray(dim=c(5,5),blocks=c(3,3))}
  has four partitions. To fetch the bottom left partition we can
  either only use argument \code{y = 3} or 2-D indexing where \code{y=2,
  z=1}.
}

\value{A reference to the \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} partition(s).}

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
