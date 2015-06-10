\name{rSquared}
\alias{rSquared}
\title{R-squared}
\description{
  This function calculates R-squared ( 1 - mse / Var(y)) for observed and predicted values.
}
\usage{
rSquared(observed, predicted, na.rm=FALSE)
}
\arguments{
  \item{observed}{the response observed in the test data.}
  \item{predicted}{the predicted value for response.}
  \item{na.rm}{logical.  Should missing values (including ‘NaN’) be removed?}
}

\value{
  the value of R-squared is returned.
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

testData <- na.omit(airquality)
rRF.ozone <- randomForest(Ozone ~ ., data=airquality, 
            mtry=3, na.action=na.omit, 
            xtest=testData[,-1],ytest=testData[,1],keep.forest=TRUE)

predicted <- predict(rRF.ozone,testData[,-1])
rSquared(testData[,1], predicted)
 }
}
\keyword{regression}

