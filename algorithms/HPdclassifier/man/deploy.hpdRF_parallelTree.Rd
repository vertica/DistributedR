\name{deploy.hpdRF_parallelTree}
\alias{deploy.hpdRF_parallelTree}
\title{
Convert hpdRF_parallelTree model to that of randomForest model
}
\description{
This function converts the formatting of the trees to match that of randomForest model so that predict.randomForest can be used
}
\usage{
deploy.hpdRF_parallelTree <- function(model)

}
\arguments{
  \item{model}{
an object of class hpdRF_parallelTree, as that created by the function hpdRF_parallelTree
}
}
\details{
	The randomForest model can only handle categorical variables with less than 32 categories
}
\value{
An object of class randomForest
}
\author{
    HP Vertica Analytics Team
}
