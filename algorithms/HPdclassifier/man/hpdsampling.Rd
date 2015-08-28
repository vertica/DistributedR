\name{predict.hpdegbm}
\alias{predict.hpdegbm}
\title{Data-Distributed and Model-Centralized Prediction Method for Distributed Ensemble GBM}
\description{
  This function can be used to, in a distributed manner, sample input data to formulate a random output data.
}
\usage{
hpdsampling <- function(dfX, daY, Ns, npartition)
}
\arguments{
  \item{dfX}{input predictor variables in a dframe/darray.}
  \item{daY}{input vector of outcomes in a darray.}
  \item{Ns}{size of the sub-chunks. #Ns <- 1*ceiling((nTrain/npartition)/npartition) # size of the sampled sub-chunk}
  \item{npartition}{in Distributed R 1.2, the sampled data has the same npartition as input.}  
}

\value{
  Returns a list of distributed sampled data fron dfX and DaY. The first element of this list is the sampled data from dfX, while the second element of the list is the sampled data from daY.
}

\references{
  Package 'gbm' version 2.1.1 \url{http://cran.r-project.org/web/packages/gbm/gbm.pdf}.
}

\author{
    HP Vertica Analytics Team
}


\examples{
 \dontrun{
# example for distributed sampling
library(gbm)
library(distributedR)

### Generate distributed large simulated training data
npartition <- 6 

### generate training data with Distributed R
nTrain <- 20000 
p <- 10

dfX <- dframe(c(nTrain,p), blocks=c(ceiling(nTrain/npartition),p))  # horizontal partition
daY <- darray(c(nTrain,1), blocks=c(ceiling(nTrain/npartition),1))
dl_GBM_model <- dlist(npartition)
nExecutor <- npartition

dbest.iter <- darray(c(npartition,1), c(1,1))  

foreach(i, 1:nExecutor, function(X_train=splits(dfX,i),Y_train=splits(daY,i)) {
     n <- nrow(X_train)
     p <- ncol(X_train)
     X_train <- as.data.frame(matrix(rnorm(n*p), nrow=n))
     y <- rep(0, n)
     y[ apply(X_train*X_train, 1, sum) > qchisq(0.5, p) ] <- 1
     #colnames(X_train) <- paste("X", 1:p, sep="")

     Y_train <- y  ### is.vector: truth, numeric

     update(X_train)
     update(Y_train)
})


##################################################################################################
### test distributed sampling: hpdsampling
Ns <- 1*ceiling((nTrain/npartition)/npartition)
npartition <- npartitions(dfX)

sampledXY <- hpdsampling(dfX,daY, Ns, npartition)

dfRX <- sampledXY[[1]]
daRY <- sampledXY[[2]]
 
 }
}

\keyword{distributed sampling}
\keyword{distributed R}

