\name{as.dframe}
\alias{as.dframe}
\title{as.dframe}

\description{Convert input matrix/data.frame into a distributed frame.}

\usage{as.dframe(input, blocks)}

\arguments{
  \item{input}{input matrix/data.frame that will be converted to dframe.}
  \item{blocks}{size of each partition as a vector specifying number of
  rows and columns.}
}

\details{ If partition size (blocks) is missing then the input matrix/data.frame
is row partitioned and striped across the cluster, i.e., the returned
distributed frame has approximately as many partitions as the number
of Executors in the Distributed R session. 
  
The last set of partitions may have fewer rows or columns if input matrix size is not
an integer multiple of partition size. If \code{A} is a 5x5 matrix, then 
\code{as.dframe(A, blocks=c(2,5))} is a distributed frame with 
three partitions. The first two partitions have two rows each but the
last partition has only one row. All three partitions have five
columns. 

To create a distributed frame with just one partition, pass the dimension of the input frame, 
i.e. \code{as.dframe(A, blocks=dim(A))}
}

\value{ Returns a distributed frame with dimensions equal to that of the
 input matrix/data.frame and partitioned according to argument 'blocks'.  Data may
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
\code{\link{dframe}},\code{\link{partitionsize}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
##Create 4x4 matrix
mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
##Create distributed frame spread across the cluster
df<-as.dframe(mtx)
partitionsize(df)
##Create distributed frame with single partition
db<-as.dframe(mtx, blocks=dim(mtx))
partitionsize(db)
##Create distributed frame with two partitions
dc<- as.dframe(mtx, blocks=c(2,4))
partitionsize(dc)
##Fetch first partition
getpartition(dc,1)
#creating of dframe with data.frame
dfa <- c(2,3,4)
dfb <- c("aa","bb","cc")
dfc <- c(TRUE,FALSE,TRUE)
df <- data.frame(dfa,dfb,dfc)
#creating dframe from data.frame with default block size
ddf <- as.dframe(df)
getpartition(ddf)
#creating dframe from data.frame with 1x1 block size
ddf <- as.dframe(df,blocks=c(1,1))
getpartition(ddf)
distributedR_shutdown()
}
}
