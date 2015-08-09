\name{spvm}
\alias{spvm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
spvm
}
\description{
This function performs a multiplication of vector/matrix and sparse matrix
}
\usage{
spvm(x, y)
}
\arguments{
  \item{x}{
input vector or matrix. The number of column should be the same as the number of row of y.
}
  \item{y}{
input sparse matrix.
}
}
\details{
}
\value{
Multiplication of x and y
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
HP Vertica Development Team
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
library(Matrix)
sm=sparseMatrix(i=sample(1:10, size=5),j=sample(1:10, size=5),x=sample(1:100, size=5, replace=TRUE), dims=c(10,10))
in_vector = as.numeric(sample(-500:500, size=10, replace=TRUE))
in_mat = (matrix(as.numeric(sample(-100:100,size=100,replace=TRUE)),nrow=10,ncol=10))
spvm(in_vector, sm)
spvm(in_mat, sm)
}
