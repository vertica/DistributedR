\name{partitionsize}
\alias{partitionsize}
\title{partitionsize}
\description{Return dimension of partitions in \code{\link{darray}}, 
  \code{\link{dframe}} or \code{\link{dlist}}.}

\usage{
    partitionsize (x, index)
    partitionsize (x)
}

\arguments{
  \item{x}{input distributed array, distributed data frame or distributed list.}
  \item{index}{index of partition. If missing sizes of all partitions are returned.}
}

\value{ A matrix that denotes the number of rows and columns in the partition. Row i of the matrix corresponds or size of i'th partition.}
  
\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
        AuYoung, A., and Schreiber, R. (2013) Presto: Distributed
	Machine
	Learning and Graph Processing with Sparse
	Matrices. \emph{EuroSys'13},
	197--210.
	\item Homepage:	http://www.vertica.com/distributedr
%	\item Mailing list: presto-dev@external.groups.hp.com
      }
    }

    \author{HP Vertica Development Team}
    
    \note{
      %%  ~~further notes~~
    }

    %% ~Make other sections like Warning with
    %% \section{Warning }{....} ~

    \seealso{
      \code{\link{darray}}, \code{\link{dframe}}, \code{\link{getpartition}}, \code{\link{dlist}}
    }

\examples{
\dontrun{
library(distributedR)
distributedR_start()
##Input array of size 5X5 with 4 partitions
da<-darray(dim=c(5,5), blocks=c(3,3), data=7)
partitionsize(da,1)
partitionsize(da,2)
partitionsize(da)
distributedR_shutdown()
}
}
