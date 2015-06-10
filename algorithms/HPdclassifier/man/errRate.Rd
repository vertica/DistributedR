\name{errorRate}
\alias{errorRate}
\title{Error Rates}
\description{
  This function calculates total error rate and error rates of each class for observed and predicted values of a classifier.
}
\usage{
errorRate(observed, predicted)
}
\arguments{
  \item{observed}{the response observed in the test data.}
  \item{predicted}{the predicted value for response.}
}

\value{
  the returned value is an array. The first element of the array is the error rate, 
  which equals to the total number of correct predictions divided by the total number of predictions.
  The remaind elements of the array, represent error rates per class. An error rate per class
  is the error rate for the samples with a particular category in their response.
}
\note{
  it is assumed that an appropriate predict function has generated 'provided' input.
}
\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
library(HPdclassifier)
distributedR_start()

rRF <- randomForest(Species ~ ., data=iris, keep.forest=TRUE, 
                              xtest=iris[,-5],ytest=iris[,5])

predicted <- predict(rRF,iris[, -5])
errorRate(iris[,5], predicted)
    
 }
}
\keyword{classification}

