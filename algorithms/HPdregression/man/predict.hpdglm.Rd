\name{predict.hpdglm}
\alias{predict.hpdglm}
\title{
Predict Method for hpdglm fits
}
\description{
It produces predicted values, obtained by evaluating the regression function on provided new data.
}
\usage{
predict.hpdglm(object, newdata, type = c("link", "response"), 
               na.action = na.pass, mask = NULL, trace = TRUE, ...)
}
\arguments{
  \item{object}{
   a built model of type hpdglm.
  }
  \item{newdata}{
   a matrix or a darray containing predictors of new samples.
  }
  \item{type}{
   the type of prediction required which can be "link" or "response".
  }
  \item{na.action}{
   a function to determine what should be done with missing values. At this version it is always na.pass (reserved for future improvement).
  }
  \item{mask}{
   a darray with a single column, and 0 or 1 as the value of its elements. It indicates which samples (rows) should be considered in the calculation.
  }
  \item{trace}{
   when this argument is true, intermediate steps of the progress are displayed.
  }
  \item{\dots}{
further arguments passed to or from other methods.
  }
}
\details{
This function produces predicted values, obtained by evaluating the regression function on provided new data. New data can be either a darray or a normal matrix.
}
\value{
The output is a matrix or a darray, depending to the type of newdata, which contains predicted values for response.
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

    myModel <- hpdglm(Y, X)
    newSamples <- matrix(c(1:3),,1)
    predict(myModel, newSamples, "link")
 }
}

\keyword{models}
\keyword{regression}
\keyword{distributed R}
