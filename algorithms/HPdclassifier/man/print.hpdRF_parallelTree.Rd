\name{print.hpdRF_parallelTree}
\alias{print.hpdRF_parallelTree}
\title{
Print hpdRF_parallelTree models
}
\description{
A function that can print summary information for models of class hpdRF_parallelTree
}
\usage{
print.hpdRF_parallelTree <- function(model, max_depth = 2)
}
\arguments{
  \item{model}{
an object of class hpdRF_parallelTree, as that created by the function hpdRF_parallelTree
}
  \item{max_depth}{
The maximum depth the trees will be printed (trees can be very deep)
}


}
\details{
Does not display the trees in the model.
}
\value{
A summary of the model is printed
}
\author{
    HP Vertica Analytics Team
}
