\name{confusionMatrix}
\alias{confusionMatrix}
\title{Confusion Matrix}
\description{
  This function generates confusion matrix for observed and predicted values of a classifier.
}
\usage{
confusionMatrix(observed, predicted)
}
\arguments{
  \item{observed}{the response observed in the test data.}
  \item{predicted}{the predicted value for response.}
}

\value{
  the returned value is the generated confusion matrix.
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
confusionMatrix(iris[,5], predicted)
    
 }
}
\keyword{classification}

