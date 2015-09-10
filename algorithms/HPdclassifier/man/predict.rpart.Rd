\name{predict.hpdrpart}
\alias{predict.hpdrpart}
\title{
	Predict function for distributed rpart model
}
\description{
	given a distributed rpart model and new observations of the feature variables, predict the responses of the new observations
}
\usage{
predict.hpdrpart(model, newdata, do.trace = FALSE)
}
\arguments{
  \item{object}{
an object of class hpdrpart, created by the function hpdrpart

}
  \item{newdata}{
a dframe or data.frame containing new data
}
}

\value{
A list that has the following  components:
  \item{response}{
predictions of the newdata
}
}

\author{
    HP Vertica Analytics Team
}
