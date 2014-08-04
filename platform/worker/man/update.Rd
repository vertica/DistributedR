\name{update}
\alias{update}
\title{
update
}
\description{
\code{\link{update}} allows propagating changes of a given \code{\link{darray}}. The \code{\link{update}} constructor creates a version vector that describes the state of the input \code{\link{darray}}, including the version of partitions that may be distributed across machines. The version vector is sent to all waiting tasks registered by \code{\link{onchange}}.
}
\usage{
update(inDarray)
}
\arguments{
  \item{inDarray}{
An input \code{\link{darray}} whose changes are going to be propagated
}
}
\value{
No return value. The changes of the input \code{\link{darray}} will be propagated to workers that are registered using \code{\link{onchange}}.
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
\code{\link{darray}}, \code{\link{splits}}, \code{\link{update}}, \code{\link{onchange}}
}
\examples{
\dontrun{
library(PrestoMaster)
presto_start()
da1 <- darray(dim=c(9,9), blocks=c(3,3), sparse=F, data=10)
cat("Number of partitions of da1 is ", length(splits(da1)), " and ", numsplits(da1))
da2 <- darray(dim=c(9,9), blocks=c(3,3), sparse=F, data=5)
result <- darray(dim=c(9,9), blocks=c(3,3))
foreach(i, 1:numsplits(da1),
  add<-function(a = splits(da1,i),
                b = splits(da2,i),
                c = splits(result,i)){
    c <- a + 2*b
    update(c)
  })
getpartition(result)
presto_shutdown()
}
}

