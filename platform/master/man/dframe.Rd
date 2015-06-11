\name{dframe}
\alias{dframe}
\title{ dframe}

\description{Store in-memory, multi-dimensional data across several
  machines. Data can be partitioned into chunks of rows, columns, or
  blocks. Unlike distributed arrays, \code{\link{dframe}} can store both
  numeric and string data. However, \code{\link{dframe}} can be
  space-inefficient, and should be replaced by \code{\link{darray}}
  whereever possible.}

\usage{
dframe (dim, blocks, distribution_policy='roundrobin')
dframe (npartitions, distribution_policy='roundrobin')
}

\arguments{
  \item{dim}{the dim attribute for the data frame to be created. A vector
   specifying number of rows and columns.}
 \item{blocks}{size of each partition as a vector specifying number of
   rows and columns.}
 \item{npartitions}{vector specifying number of partitions.}
 \item{distribution_policy}{defines policy to distribute data.frame partitions across the workers.
  The distribution policy used is 'roundrobin'. Currently, this argument is only for internal use.}
}     

\details{ Distributed data frame partitions are internally stored as
  data.frame objects.  Last set of partitions may have fewer rows or columns if
  data frame size is not an integer multiple of partition size. For
  example, the distributed data frame \code{dframe(dim=c(5,5),
  blocks=c(2,5))} has three partitions. The first two partitions have two
  rows each but the last partition has only one row. All three
  partitions have five columns.

 Distributed data frames can also be defined by specifying just the number 
 of partitions, but not their sizes. This flexibility is useful when
 the size of an data frame is not known apriori. For example,
 \code{dframe(npartitions=c(5,1))} is a data frame with five
 partitions.  Each partition can contain any number of rows, though the
 number of columns should be same to conform to a well formed data frame.

 Too many partitions increase the overheads of managing distributed
objects.  We recommend users to create objects with as many partitions
as the total number of CPU cores in the system.  We restrict users
from creating objects when the number of partitions is more than
100,000 or more than no-of-cores*max(50, no-of-cores).

  Distributed data frames can be read-shared by multiple concurrent
  tasks, but modified by only a single writer per partition.
  Programmers express parallelism by applying functions on 
  partitions in \code{\link{foreach}} loops. Loop body is executed at
  workers. Partitions can be passed as arguments using
  \code{\link{splits}}. Data frame modifications can be published globally
  using \code{\link{update}}.

  Distributed data frames can be fetched at the master using
  \code{\link{getpartition}}. Number of partitions can be obtained by
  \code{\link{npartitions}}.  Partitions are numbered from left to right,
  and then top to bottom.  }

\value{ Returns a distributed data frame with the specified dimensions.  Data
  may reside as partitions in remote nodes.}

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
  \code{\link{getpartition}}, \code{\link{npartitions}},
  \code{\link{foreach}}, \code{\link{splits}}, \code{\link{update}}, \code{\link{darray}}, \code{\link{dimnames}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
df <- dframe(c(20,4),c(10,2))
data_path<-system.file("extdata",package="distributedR")
file_path <- paste(data_path,"/df_data",sep="")
##Populate distributed data frame
foreach(i, 1:npartitions(df), function(sf=splits(df,i),ii=i,path=file_path){
  sf<-read.table(paste(path,ii,sep=""))
  update(sf)
})
getpartition(df)
##Rename columns
name_sample <- as.character(sample(1:4))
dimnames(df)[[2]] <- name_sample
getpartition(df)

##Flexible sized data frame. Five partitions, each with variable number of rows.
dc<-dframe(npartitions=c(5,1))
foreach(i, 1:npartitions(dc), initArrays<-function(y=splits(dc,i), index=i) {
     y<-data.frame(matrix(index, nrow=index,ncol=5))
     update(y)
})
cat("value of 2nd partition is: \n")
getpartition(dc,2)
getpartition(dc)

distributedR_shutdown()
}
}
