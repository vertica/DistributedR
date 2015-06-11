\name{dlist}
\alias{dlist}
\title{ dlist}

\description{Stores in-memory lists across several machines.
  %A dlist can be 
  %partitioned into multiple blocks/partitions such that the elements of the 
  %list are partitioned and stored across machines in the distributedR 
  %cluster.
  Just like R's list, \code{\link{dlist}} can store other R objects such as
  character, numeric and logical vectors, lists, matrices, and models. However, 
  \code{\link{dlist}} can be space-inefficient, and should be replaced by 
  \code{\link{darray}} whereever possible.}

\usage{dlist (npartitions)}

\arguments{
 \item{npartitions}{an integer specifying number of partitions of the list.}
}     

\details{ Distributed lists are internally stored as list objects. Each partition
  of the list can have variable number of elements in it. For example, 
  the distributed list \code{dlist(npartitions=5)} has five partitions. Each
  partition is an empty list \code{list()}.

Too many partitions increase the overheads of managing distributed
objects.  We recommend users to create objects with as many partitions
as the total number of CPU cores in the system.  We restrict users
from creating objects when the number of partitions is more than
100,000 or more than no-of-cores*max(50, no-of-cores).

  Distributed lists can be read-shared by multiple concurrent tasks, but 
  modified by only a single writer per partition. Programmers express parallelism 
  by applying functions on dlist partitions in \code{\link{foreach}} loops. 
  Loop body is executed at workers. Partitions can be passed as arguments using
  \code{\link{splits}}. List modifications can be published globally
  using \code{\link{update}}.

  Distributed lists can be fetched at the master using
  \code{\link{getpartition}}. Number of partitions can be obtained by
  \code{\link{npartitions}}.  Partitions are numbered from left to right}

\value{ Returns a distributed list with the specified number of partitions.  
        Data may reside as partitions in remote nodes.}

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
  \code{\link{foreach}}, \code{\link{splits}}, \code{\link{update}}, \code{\link{darray}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
dl <- dlist(5)
##Populate distributed list
foreach(i, 1:npartitions(dl), function(sf=splits(dl,i), idx=i){
  sf<-list(c("HP", idx))
  update(sf)
})
getpartition(dl)
distributedR_shutdown()
}
}
