\name{page.rank}
\alias{page.rank}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
page.rank
}
\description{
Calculates PageRank value of a given matrix in parallel
}
\usage{
page.rank (inGraph, numRow=0, rowPerBlock=0, sparse=TRUE, iter=-1, dmp=0.85, qerr=0.001, normalized=F)
}
\arguments{
  \item{inGraph}{
The input adjacency matrix. It can be a path of a graph or a darray object that contains the adjacency matrix. If this argument is a path, \code{numRow} and \code{rowPerBlock} have to explicitly specified to create a darray graph object. If this argument is a darray object, it should be column-partitioned.
}
  \item{numRow}{
The number of row of an input graph (adjacency matrix). This value has to be specified explicitly in case \code{inGraph} is a path of input matrix.
}
  \item{rowPerBlock}{
The number of rows in each blocks. This value has to be specified explicitly in case \code{inGraph} is a path of input matrix.
}
  \item{sparse}{
A flag that specifies if a transition darray will be in sparse mode
}
  \item{iter}{
The maximum number of iteration to perform. If this value is -1, it will run until convergence - the second norm of new page rank value is smaller than the pre-defined quadratic error.
}
  \item{dmp}{
Damping factor to calculate page-rank value
}
  \item{qerr}{
Quadratic error value to determine if a page-rank value is converging. If L2 norm of difference of page-rank of two adjacent iteration is smaller than this value, the calculation terminates
}
  \item{normalized}{
A flag which specifies if the input darray is normalized column-wise. If this flag is FALSE, the module normalizes the input graph and perform page-rank calculation.
}
}
\details{
For efficient operation, we restrict the transition matrix darray has to row-partitioned. Regarding transition matrix, we assume a matrix entry M_i,j - an entry with row index i and column index j, this entry means a connection from node j to node i. Thus, for the normalization, the sum of each column has to be 1.
}
\value{
An darray that has final page rank value. \code{\link{getpartition}} can be used to fetch the vector
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
\dontrun{
library(PrestoMaster)
presto_start()
pr<-presto.page.rank(paste(system.file("extdata",package="PrestoMaster"),"/pr_ex1_",sep=""), 5, 1, iter=10)
getpartition(pr)
trans_mtx<-matrix(c(0,1,0,0,1,0,1,1,1,1,0,1,0,1,1,0), nrow=4)
tmda<-as.darray(trans_mtx,(1,4))  # convert the transistion matrix into darray object
pr<-presto.page.rank(tmda)   # perform page-rank using darray. The input darray is normalized column-wise
getpartition(pr)
pr<-presto.page.rank(tmda, normalized=T) # page rank with normalized input matrix
presto_shutdown()
}
}
