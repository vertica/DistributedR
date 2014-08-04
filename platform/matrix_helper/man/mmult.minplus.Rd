\name{mmult.minplus}
\alias{mmult.minplus}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
mmult.minplus
}
\description{
This function finds the minimum path of the source vector given the input weight matrix using Bellman-Ford algorithm
}
\usage{
mmult.minplus(weight_matrix, src_vector, out_vector=src_vector)
}
\arguments{
  \item{weight_matrix}{
Input weight matrix. Each entry value represents the cost of traversing from i to j, where i is the row index and j is the column index. The value of -1 or empty (in case of sparse matrix) means no path. This matrix can be either dense or sparse. In case of dense matrix, the number of rows and the columns should be same.
}
  \item{src_vector}{
a vector of current distance (weight) of a given entry. If the input matrix is dense, the number of entry should be same as the number of rows of input matrix. If the input matrix is sparse, the entry number should be larger or equal to the max(number of rows, number of columns) of input matrix.
}
  \item{output_vector}{
The shortest path of the input src_vector after applying Bellman-Ford algorithm on src_vector. If this argument is empty, it is set as the src_vector.
}
}
\details{
Please reference Bellman-Ford algorithm
}
\value{
The distance vector
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
weightMatrix1 = array(c(0,-1,2,-1,7,0,2,3,4,3,0,3,-1,-1,5,0),dim=c(4,4))
mmult.minplus(weight_matrix=weightMatrix1, src_vector=c(0,-1,-1,-1), out_vector=c(0,-1,-1,-1))

library(Matrix)
sm1 = sparseMatrix(i=c(1,3,5,6,7,8,8),j=c(2,1,3,1,5,6,4),x=c(10,3,4,1,4,1,4))
mmult.minplus(weight_matrix=sm1,src_vector=c(10,24,46,11,5,17,41,13,30,12))
}
