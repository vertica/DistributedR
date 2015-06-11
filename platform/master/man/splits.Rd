\name{splits}
\alias{splits}
\title{splits}

\description{Pass partition(s) of \code{\link{darray}}, 
  \code{\link{dframe}} or \code{\link{dlist}} to function in \code{\link{foreach}}.
}

\usage{ splits(x, y)}

\arguments{

  \item{x}{input distributed array, distributed data frame or distributed list.}

  \item{y}{index or indices of partition to fetch; this can be an expression that evaluates to a scalar, vector, or list}
  
%  \item{z}{column-index of the partition in a 2-D partitioning scheme
%    (number of partitions to the left). } 
}

\details{ \code{\link{splits}} can be used only as an argument to the
    function in a \code{\link{foreach}} loop. 

  If y is missing then the full input
  \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} is returned.

  y can be either evaluate to a scalar value, a vector, or an R list object.

  If y is a scalar, it represents one split.
  If y is a vector or a list, then it represents anything from one to multiple splits.

  If y is defined to be a vector or a list (of length > 1), then \code{splits(x,y)} will represent multiple splits. For example, in the statement, \code{foreach(i,1:2,function(B=splits(A,1:3)){})}, B will contain splits 1-3 of dobject A. Inside the body of the foreach function statement, B will be a list -- i.e., \code{B[[1]]} will contain the first split of A. 

  Similarly, one can may also write \code{B = splits(A,list(2,4,5))}, in which case \code{B[[1]]} would be equivalent to \code{splits(A,2)}.

  In place of y may also be a function. If this function evaluates to a scalar,list,or vector, then it will be a valid splits statement.

%  2-D partitioning is valid only for \code{\link{darray}} and \code{\link{dframe}}.
%  Since \code{\link{dlist}} is paritioned length wise, only argument \code{y} is used to
%  fetch a \code{\link{dlist}} partition. Argument \code{z} is undefined for \code{\link{dlist}}.
  
  Partitions are numbered from left to right and then top to bottom,
  i.e., row-major order. Partition numbers start from 1. 
  %For row
  %partitioning (each partition has all the columns) or column
  %partitioning (each partition has all the rows) index argument
  %\code{z} should not be used. For 2-D partitioning, both index argument
  %\code{y} and \code{z} may be used. 

  For example, the array \code{A=darray(dim=c(5,5),blocks=c(3,3))}
  has four partitions. To fetch the bottom left partition we can
  use \code{splits(A, 3)}.
}

\value{A reference to the \code{\link{darray}}, \code{\link{dframe}} or \code{\link{dlist}} partition(s).}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
    AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
    Learning and Graph Processing with Sparse
      Matrices. \emph{EuroSys'13},
      197--210.
      \item Homepage: http://www.vertica.com/distributedr
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
\dontrun{
library(distributedR)
distributedR_start()

da <- darray(dim=c(6,9),blocks=c(3,3),sparse=FALSE,data=0)

##First index gets c(1,2,3), second index gets c(4,5,6), etc.
get.three.indices <- function(index){
    start = 3*(index-1) + 1
    indices = c(start:(start+2))
    indices
}

##Update darray 3 splits at a time, with parallelism of 2
foreach(i,1:2,function(a=splits(da,get.three.indices(i)),data=get.three.indices(i)){
       a[[1]] = matrix(data[[1]],nrow(a[[1]]),ncol(a[[1]]))
       a[[2]] = matrix(data[[2]],nrow(a[[2]]),ncol(a[[2]]))
       a[[3]] = matrix(data[[3]],nrow(a[[3]]),ncol(a[[3]]))
       update(a)
    })

getpartition(da) ##counts from 1 to 6 in 3x3 blocks!
distributedR_shutdown()
}
}
