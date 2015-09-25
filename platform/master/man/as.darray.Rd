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

\details{ If partition size (blocks) is missing then the input matrix
is row partitioned and striped across the cluster, i.e., the returned
distributed array has approximately as many partitions as the number
of Executors in the Distributed R session. 
%If partition size (blocks) is not present then a distributed
%array with only a single partition is created.  
The last set of partitions may have fewer rows or columns if input matrix size is not
an integer multiple of partition size. If \code{A} is a 5x5 matrix, then 
\code{as.darray(A, blocks=c(2,5))} is a distributed array with 
three partitions. The first two partitions have two rows each but the
last partition has only one row. All three partitions have five
columns. 

To create a distributed array with just one partition, pass the dimension of the input array, 
i.e. \code{as.darray(A, blocks=dim(A))}
}

\value{ Returns a distributed array with dimensions equal to that of the
 input matrix and partitioned according to argument 'blocks'.  Data may
 reside as partitions on remote nodes.}  

\references{
  \itemize{
   \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
    Learning and Graph Processing with Sparse
    Matrices. \emph{EuroSys'13},
    197--210.
    \item Homepage: http://www.vertica.com/distributedr
  }
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

\seealso{
\code{\link{darray}},\code{\link{partitionsize}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
##Create 4x4 matrix
mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
##Create distributed array spread across the cluster
da<-as.darray(mtx)
partitionsize(da)
##Create distributed array with single partition
db<-as.darray(mtx, blocks=dim(mtx))
partitionsize(db)
##Create distributed array with two partitions
dc<- as.darray(mtx, blocks=c(2,4))
partitionsize(dc)
##Fetch first partition
getpartition(dc,1)
distributedR_shutdown()
}
}
