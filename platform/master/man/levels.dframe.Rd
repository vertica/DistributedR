\name{levels.dframe}
\alias{levels.dframe}
\title{levels.dframe}

\description{Finds the list of the labels on the categorical columns of a dframe.}

\usage{levels.dframe(DF, colName, colID, trace=FALSE)}

\arguments{
  \item{DF}{the input dframe. It must be partitioned row-wise.}
  \item{colName}{a vector of the name of the interested categorical columns.}
  \item{colID}{when colName is not available, column positions can be specified using a numerical vector.}
  \item{trace}{when it is FALSE (default) the progress of the foreach will be hidden.}
}

\details{
    The specified columns should be of type character, logical, or factor. When neither colName nor colID is specified, any column of these types will be selected. When both are specified, colID will be ignored.
}

\value{ 
  \item{Levels}{the array of the labels on the categorical columns of a dframe.}
  \item{columns}{the position of columns that their levels are respectively returned in \code{Levels}.}
}

\references{
  \itemize{
   \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
    Learning and Graph Processing with Sparse
    Matrices. \emph{EuroSys'13},
    197--210.
    \item Homepage: http://www.vertica.com/distributedr
%    \item Mailing list: presto-dev@external.groups.hp.com
  }
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

\seealso{
\code{\link{dframe}}
\code{\link{factor.dframe}}
\code{\link{as.factor.dframe}}
\code{\link{unfactor.dframe}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
originalDF <- dframe(c(9,3),c(3,3))
foreach(i,1:npartitions(originalDF),function(dfi=splits(originalDF,i),idx=i){
    if(idx==1) {
        dfi[,1] <- 1:3
        dfi[,2] <- c('c1','c2','c3')
        dfi[,3] <- c('t1','t2','t3')
    } else if(idx==2) {
        dfi[,1] <- 2:4
        dfi[,2] <- c('c2','c3','c4')
        dfi[,3] <- c('t1','t2','t3')
    } else {
        dfi[,1] <- 11:13
        dfi[,2] <- c('c3','c4','c5')
        dfi[,3] <- c('t4','t5','t6')
    }
    update(dfi)
})
levels.dframe(originalDF, colID=c(2,3))
distributedR_shutdown()
}
}
