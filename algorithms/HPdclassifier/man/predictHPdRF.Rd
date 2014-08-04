\name{predictHPdRF}
\alias{predictHPdRF}
\title{distributed predict method for applying a random forest objects on a darray or a dframe}
\description{
  Prediction of distributed test data using random forest.
}
\usage{
predictHPdRF(object, newdata, trace=FALSE)
}
\arguments{
  \item{object}{an object of class \code{randomForest}, as that
    created by the function \code{randomForest} or 
    \code{hpdrandomForest}.}
  \item{newdata}{a darray or a dframe containing new data. 
    darray is highly recommended when there is no categorial data}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
}

\value{
  It returns a darray or a dframe of predicted classes. The type
  of returned value will be darray unless there is any categorical data.
}
\references{
  Breiman, L. (2001), \emph{Random Forests}, Machine Learning 45(1),
  5-32.
}
\author{ Arash Fard <afard@vertica.com>
}

\seealso{\code{\link{hpdrandomForest}}}

\examples{
 \dontrun{
# example for darray
library(HPdclassifier)
distributedR_start()
drs <- distributedR_status()
nparts <- sum(drs$Ins)

nSamples <- 100
nAttributes <- 5
nSpllits <- 1

dax <- darray(c(nSamples,nAttributes), c(round(nSamples/nSpllits),nAttributes))
day <- darray(c(nSamples,1), c(round(nSamples/nSpllits),1))

foreach(i, 1:npartitions(dax), function(x=splits(dax,i),y=splits(day,i),id=i){
    x <- matrix(runif(nrow(x)*ncol(x)), nrow(x),ncol(x))
    y <- matrix(runif(nrow(y)), nrow(y), 1)
    update(x)
    update(y)
})

(myrf1 <- hpdrandomForest(dax, day, nExecutor=nparts))
dp <- predictHPdRF(myrf1, dax)

# example for dframe
nSamples <- 100
nAttributes <- 5
nSpllits <- 4

dfx <- dframe(c(nSamples,nAttributes), c(round(nSamples/nSpllits),nAttributes))
dfy <- dframe(c(nSamples,1), c(round(nSamples/nSpllits),1))
blockSize <- dfx@blocks[1]

foreach(i, 1:npartitions(dfx), function(xi=splits(dfx,i),yi=splits(dfy,i),
  blockSize=blockSize,nAttributes=nAttributes){
     xi <- data.frame( matrix(runif(blockSize*nAttributes,1,10), 
           nrow=blockSize,ncol=nAttributes) )
     yi <- data.frame( gl(2, blockSize/2) )
     update(xi)
     update(yi)
})

(myrf2 <- hpdrandomForest(dfx, dfy, nExecutor=nparts))
fp2 <- predictHPdRF(myrf2, dfx)
    
 }
}
\keyword{classif}% at least one, from doc/KEYWORDS
\keyword{regression}
