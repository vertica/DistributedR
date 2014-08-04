\name{darray}
\alias{darray}
\title{darray}

\description{Store in-memory, multi-dimensional data across several
  machines. Data can be partitioned into chunks of rows, columns, or
  blocks. Distributed arrays can store only numeric data.}

\usage{
darray (dim, blocks, sparse = FALSE, data = 0, empty=FALSE)
}

\arguments{
  \item{dim}{the dim attribute for the array to be created. A vector specifying number of rows and columns.}
  \item{blocks}{size of each partition as a vector specifying number of rows and 
  columns.}
  \item{sparse}{logical. Indicates if input array is a sparse.}
  \item{data}{initial value of all elements in array. Default is 0.}
  \item{empty}{if TRUE array is left unitialized, each partition is a zero matrix. Default is FALSE.}
}

\details{ By default, array partitions are internally stored as dense
matrices. If an array is specified sparse, partitions are stored in the
compressed sparse column format.  Last set of partitions may have fewer
rows or columns if array size is not an integer multiple of partition
size. For example, the distributed array \code{darray(dim=c(5,5),
blocks=c(2,5))} has three partitions. The first two partitions have two
rows each but the last partition has only one row. All three partitions
have five columns. 

  Distributed arrays can be read-shared by multiple concurrent tasks,
but modified by only a single writer per partition.  Programmers express
parallelism by applying functions on array partitions in
\code{\link{foreach}} loops. Loop body is executed at
workers. Partitions can be passed as arguments using
\code{\link{splits}}. Array modifications can be published globally
using \code{\link{update}}.

 Distributed arrays can be fetched at the master using
 \code{\link{getpartition}}. Number of partitions can be obtained by
 \code{\link{npartitions}}. Partitions are numbered from left to right,
 and then top to bottom, i.e., row major order. Dimension of each partition can 
 be obtained using \code{\link{dimpartition}}.
}

\value{ Returns a distributed array with the specified dimensions.  Data
  may reside as partitions in remote nodes.}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
  Learning and Graph Processing with Sparse Matrices. \emph{EuroSys'13},
  197--210.
  \item Homepage: http://www.hpl.hp.com/research/distributedr.htm
%  \item Mailing list: presto-dev@external.groups.hp.com
}
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
    \code{\link{getpartition}}, \code{\link{npartitions}}, \code{\link{partitionsize}},
    \code{\link{foreach}}, \code{\link{splits}}, \code{\link{update}}, \code{\link{dframe}}, \code{\link{dlist}}
    \code{\link{dimnames}}
}
  
\examples{
\dontrun{
library(distributedR)
distributedR_start()
##Sparse array of size 10X10 with 10 partitions and each partition is of size 1X10
da<-darray(dim=c(10,10), blocks=c(1,10), sparse=TRUE) 
getpartition(da)
cat("Input matrix dimension: ", da@dim, " block dimension: ", da@blocks,
" total number of partitions: ", npartitions(da),"\n")
##Dense array of size 9X9 with 3 partitions and each partition is of size 3X3
db<-darray(dim=c(9,9), blocks=c(3,3), sparse=FALSE, data=11)
cat("value of 3rd partition is: \n", getpartition(db,3),"\n")
distributedR_shutdown()
}
}
