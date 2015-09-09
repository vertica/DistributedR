\name{predict.hpdRF_parallelTree}
\alias{predict.hpdRF_parallelTree}
\title{
	Predict function for distributed random forest model
}
\description{
	given a distributed random forest model and new observations of the feature variables, predict the responses of the new observations
}
\usage{
predict.hpdRF(object, newdata, cutoff, do.trace = FALSE, na.action = na.fail)
}
\arguments{
  \item{object}{
an object of class hpdRF_parallelTree, as that created by the function hpdRF_parallelTree

}
  \item{newdata}{
a dframe or data.frame containing new data
}
  \item{cutoff}{
(Classification only) A vector of length equal to number of classes. The ‘winning’ class for an observation is the one with the maximum ratio of proportion of votes to cutoff. Default is taken from the forest$cutoff component of object
}
  \item{do.trace}{
If set to TRUE, give a more verbose output as randomForest is run.
}

  \item{na.action}{
A function to specify the action to be taken if NAs are found
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
