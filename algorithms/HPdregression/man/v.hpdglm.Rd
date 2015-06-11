\name{v.hpdglm}
\alias{v.hpdglm}
\title{
    Split-Sample-Validation Method for hpdglm Models
}
\description{
    This function is implemented for evaluating a model built by hpdglm using Split-Sample-Validation method. In simple words, a percent of the data is randomly selected as test data, and a new model is built using the rest. Finally, the prediction cost of the new model on the test data is measured.
}
\usage{
    v.hpdglm(responses, predictors, hpdglmfit, 
             percent = 30, sampling_threshold = 1e+06)
}

\arguments{
  \item{responses}{
    the darray that contains the vector of responses.
  }
  \item{predictors}{
    the darray that contains the vector of predictors.
  }
  \item{hpdglmfit}{
    a built model of type hpdglm.
  }
  \item{percent}{
    the percent of data which should be set aside for validation.
  }
  \item{sampling_threshold}{
    threshold for the method of sampling (centralized or distributed). It should be always smaller than 1e9. When (blockSize > sampling_threshold || nSample > 1e9), the distributed sampling is selected in which a percent of each block is selected for test data. Here, blockSize is the number of samples in each partition of predictors, and nSample is the total number of samples in predictors.
  }
}
\details{
    In order to randomly select the validation set, sample.int function of R is used. This function does not support sample space bigger than 1e9; moreover, it is slow for big numbers. Therefore, when the number of samples in each block of darray becomes bigger than sampling_threshold, instead of purely random selection on the master side, each block will randomly contribute its portion to validation set in a distributed fashion. When the ratio of number of samples in each block to the total number of blocks is huge, the skew of randomness is negligible, but performance will be improved.
}
\value{
    \item{call }{the original call to v.hpdglm}
    \item{percent }{the percent value used at the input}
    \item{delta }{a vector of length two. The first component is the raw validation estimate of prediction error. The second component is the adjusted validation estimate. Lower cost indicates better fitting. In more detail,
        the 1st-cost is prediction cost of new model on test data,
        and the 2nd-cost is [1st-cost + (cost of the old model on all data - the cost of the new model on all data)]
    }
    \item{seed }{the value of .Random.seed when v.hpdglm was called.}
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    library(HPdregression)
    distributedR_start()
    Y <- as.darray(data.matrix(faithful["eruptions"]))
    X <- as.darray(data.matrix(faithful["waiting"]))

    myModel <- hpdglm(Y, X, completeModel=TRUE)
    testV <- v.hpdglm(Y, X, myModel)
 }
}

\keyword{ Split-Sample-Validation }
\keyword{ hpdglm model }
\keyword{ Distributed R }
