\name{predict.hpdRF_parallelForest}
\alias{predict.hpdRF_parallelForest}
\title{distributed predict method for applying a random forest objects on a darray or a dframe}
\description{
  This function can be used to apply a model of type \code{hpdRF_parallelForest} or \code{randomForest} to a new data for prediction.
}
\usage{
predict.hpdRF_parallelForest(object, newdata, trace=FALSE)
}
\arguments{
  \item{object}{an object of class \code{randomForest}, as that
    created by the function \code{randomForest} or 
    \code{hpdRF_parallelForest}.}
  \item{newdata}{a darray, a dframe, a data.frame, or a matrix
    that contains new data. darray is highly recommended to dframe 
    when there is no categorial data}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
}

\value{
  It returns predicted classes in a distributed or non-distributed objects 
    depending on the type of the input. When the newdata is of type darray,
  the type of returned value will be also darray unless the output is categorical data.
  When the output is a dframe when the newdata is of type dframe.
}
\references{
  Breiman, L. (2001), \emph{Random Forests}, Machine Learning 45(1),
  5-32.
}
\author{
    HP Vertica Analytics Team
}

\seealso{\code{\link{hpdRF_parallelForest}}}

\examples{
 \dontrun{
# example for darray
library(HPdclassifier)
distributedR_start()
drs <- distributedR_status()
nparts <- sum(drs$Ins)

nSamples <- 100
nAttributes <- 5
nPartitions <- 2

dax <- darray(c(nSamples,nAttributes), 
              c(ceiling(nSamples/nPartitions),nAttributes))
day <- darray(c(nSamples,1), c(ceiling(nSamples/nPartitions),1))

foreach(i,1:npartitions(dax),function(x=splits(dax,i),
                                      y=splits(day,i),id=i){
    x <- matrix(runif(nrow(x)*ncol(x)), nrow(x),ncol(x))
    y <- matrix(runif(nrow(y)), nrow(y), 1)
    update(x)
    update(y)
})

(myrf1 <- hpdRF_parallelForest(dax, day, nExecutor=nparts))
dp <- predict.hpdRF_parallelForest(myrf1, dax)

# example for dframe
nSamples <- nrow(iris)
nAttributes <- ncol(iris)
nPartitions <- 4

df <- dframe(c(nSamples,nAttributes), 
             c(ceiling(nSamples/nPartitions),nAttributes))
end = cumsum(partitionsize(df)[,1])
start = c(0,end[-length(end)])

foreach(i, 1:npartitions(df), function(xi=splits(df,i),
  start = start[i]+1, end = end[i], iris=iris){
     xi <- iris[start:end,]
     update(xi)
})
# the following line will be redundant in the next release
colnames(df) <- colnames(iris)

(myrf2 <- hpdRF_parallelForest(Species ~ ., df, nExecutor=nPartitions))
fp2 <- predict.hpdRF_parallelForest(myrf2, df)
    
 }
}
\keyword{classification}
\keyword{regression}
