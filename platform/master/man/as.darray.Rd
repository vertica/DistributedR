\name{as.darray}
\alias{as.darray}
\title{as.darray}

\description{Convert input matrix into a distributed array.}

\usage{as.darray(input, blocks)}

\arguments{
  \item{input}{input matrix that will be converted to darray.}
  \item{blocks}{size of each partition as a vector specifying number of
  rows and columns.}
}

\details{ If partition size (blocks) is not present then a distributed
array with only a single partition is created.
Last set of partitions may have fewer
rows or columns if input matrix size is not an integer multiple of partition
size. For example, the distributed array \code{as.darray(matrix(1,nrow=5,ncol=5),
blocks=c(2,5))} has three partitions. The first two partitions have two
rows each but the last partition has only one row. All three partitions
have five columns.
}

\value{ Returns a distributed array with dimensions equal to that of the
 input matrix and partitioned according to argument blocks.  Data may
 reside as partitions in remote nodes.}  

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
##Create 4x4 matrix
mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
##Create distributed array with single partition
da<-as.darray(mtx)
da@dim
da@blocks
getpartition(da)
##Create distributed array with two partitions
db<- as.darray(mtx, blocks=c(2,4))
db@blocks
##Fetch first partition
getpartition(db,1)
distributedR_shutdown()
}
}
