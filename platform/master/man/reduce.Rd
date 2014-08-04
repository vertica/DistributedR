\name{getpartition}
\alias{getpartition}
\title{
reduce
}
\description{
\code{\link{reduce}} does a reduce operation on a darray, treating the
splits as elements of a reduce.
}
\usage{
reduce(f,d,idx=1:length(splits(d)))
}
\arguments{
  \item{f}{
The reduce operator. It most produce a valid split (of correct size)
from two other splits, i.e. f(split,split) -> split.
  }
  \item{d}{
The input \code{\link{darray}}.
}
  \item{idx}{
The splits we want to include in the reduce (by default all splits are included).
}
}
\value{
The handle of a \code{\link{darray}} containing a single split that is
the result of the reduction.
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{darray}}
}
\examples{
\dontrun{
library(distributedR)
distributedR_start()
da1<-darray(dim=c(10,10), blocks=c(5,5), sparse=FALSE, data=1) %%input matrix size is 10X10 and a block size is 5X5
blocksum<-reduce(`+`,da1) %%a 5x5 darray that is the sum of the blocks
			  %%of da1
distributedR_shutdown()
}
}
