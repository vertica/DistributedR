\name{print.hpdRFtree}
\alias{print.hpdRFtree}
\title{
Print Trees returned by hpdRF_parallelTree
}
\description{
A function that can print the trees returned by hpdRF_parallelTree
}
\usage{
print.hpdRFtree <- function(tree, max_depth = 2,classes)
}
\arguments{
  \item{model}{
an object of class hpdRFtree, as created by the function hpdRF_parallelTree
}
  \item{max_depth}{
The maximum depth the trees will be printed (trees can be very deep)
}
  \item{classes}{
(Classification only) The list of classes. Default value is to check if there are any classes associated with the tree
}

}
\details{
Classes should be be passed in otherwise numerical values will be displayed. The class of subtrees is not set to hpdRFtree so to print them, explicity use print.hpdRFtree(subtree) instead of just print(subtree)
}
\value{
Tree is printed in an XML format
}
\author{
    HP Vertica Analytics Team
}
