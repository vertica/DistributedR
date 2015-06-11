\name{npartitions}
\alias{npartitions}
\title{npartitions}
\description{Return number of partitions in \code{\link{darray}}, 
  \code{\link{dframe}} or \code{\link{dlist}}.}

\usage{
    npartitions (x)
    npartitions2D (x)
}

\arguments{
  \item{x}{input distributed array, distributed data frame or distributed list.}
}

\details{ \code{npartitions} returns the total number of partitions in
  the distributed object. Use \code{npartitions2D} to obtain the
  number of partitions along each direction.  

  In \code{darray(dim=c(9,10), blocks=c(3,5))}, the distributed array is
  partitioned blockwise.  \code{npartittions} will return 6 (total
  number of partitions) while \code{npartitions2D} will return (3,2),
  i.e., 3 partitions along the row and 2 along the column
  axis. 

  Mathematically,
  \code{npartitions(x)=npartitions2D(x)[1]*npartitions2D(x)[2]}}

\value{ \code{npartitions} return an integer that denotes the number of partitions.
\code{npartitions2D} return a vector that denotes the number of partitions in each direction.
}
 
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
npartitions(da)
npartitions2D(da)
distributedR_shutdown()
}
}
