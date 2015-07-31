\name{onchange}
\alias{onchange}
\title{
onchange
}
\description{
\code{\link{onchange}} allows expressing data dependences by registering a callback function upon change events of input \code{\link{darrays}}. The notification includes the version vector of the array the registered callback function should process. By leveraging \code{\link{onchange}} with \code{\link{update}}, programmers can implement incremental algorithms that run on a consistent view of data. \code{\link{onchange}} supports fine-grained darray event notifications by allowing superset relations. For example, tasks that are waiting for changes to the whole matrix, onchange(M), would also be notified even if only a sub-part of the matrix has changed - when update(M[v]) occurs.
}
\usage{
onchange(..., func())
}
\arguments{
  \item{darrayPointers}{
Input \code{\link{darray}} objects that we want to observe the value change. This can be the entire \code{\link{darray}} or partial \code{\link{darray}} blocks where a user wants to observe changes.
}
  \item{func()}{
A callback function that will be called upon detection of observed darray changes.
}
}
\value{
No return value.
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
\code{\link{darray}},\code{\link{update}}, \code{\link{foreach}}
}
\examples{
\dontrun{
library(PrestoMaster)
presto_start()
da <- darray(c(4,4), c(2,4), sparse=F, data=2)

fAny <- function(d) {
  cat("Onchange invoked in any portion of input darray with version ", d@version, "\n")
  cat("Current value is:\n", getpartition(d),"\n")
}

fSub <- function(d){
  cat("Onchange invoked in the sub-portion of input darray with version ", d@version, "\n")
  cat("Current value is:\n", getpartition(d),"\n")
}

onchange(da, func=f)
foreach(i, 1:length(splits(da)), function(ds = splits(d, i)) {
  ds <- 2*ds
  update(ds)
})

ea <- darray(c(4,4), c(2,4), sparse=F, data=1)
foreach(i, 1:length(splits(da)), function(ds = splits(da, i),
                                          es = splits(ea, i)) {
  f <- function(ds, es) {
    cat("worker-side onchange invoked", ds, es, "\n")
  }
  # Whenever ds changes print it
  onchange(ds, es, func=f)
})


foreach(i, 1:length(splits(ea)), function(es = splits(ea, i)) {
  es <- 4*es
  update(es)
})

presto_shutdown()
}
}
