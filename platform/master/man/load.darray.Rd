\name{load.darray}
\alias{load.darray}
\title{
load.darray
}
\description{
\code{\link{load.darray}} allows loading data saved in a file or database into the input \code{\link{darray}}
}
\usage{
load.darray (da, filename, triplet = TRUE, transpose = FALSE)
}
\arguments{
  \item{da}{
An input \code{\link{darray}} where we want to load the data into.
}
  \item{filename}{
An input data file path. The path should be accessible from all worker nodes. The input file should be split into blocks prior to loading according to the dimension and block size of the input \code{\link{darray}}. Files with splitted blocks should numbered from 0 and incremented by 1, and the id is concatenated with the input \emph{filename}. Each file block is kept at a worker node that is responsible for \emph{id+1}th \code{\link{darray}}. For example, if an input file 'sameple_data' is divided into 3 blocks, files with name 'sample_data0', 'sample_data1', and 'sample_data2' should exist, and the data of 'sample_data0' is assgined to a node that is responsible for the 1st split of input \code{\link{darray}}. For automatic data splitting, refer to the $PRESTO_HOME/tools/splitter.cpp.
}
  \item{triplet}{
A boolean flag which determines the format of the input data set. If this is set to TRUE, the input data should be expressed using triplet matrix expression, i.e., index_of_row index_of_column value. If this value is FALSE, the input file is expressed in the matrix format. The default value is TRUE.
}
  \item{transpose}{
This boolean flag indicates if the input dataset needs to be transposed. The default value is FALSE.
}
}
\value{
No data is returned. Each worker loads the responsible \code{\link{darray}} blocks and share the changes using \code{\link{update}} command.
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{darray}},\code{\link{getpartition}}
}
\examples{
\dontrun{
library(distributedR)
extdata.dir = system.file(package="distributedR", "extdata")
distributedR_start()
da1<-darray(dim=c(4,4), blocks=c(2,4)) 
getpartition(da1)
load.darray(da1, paste(extdata.dir, "/A", sep=""))
getpartition(da1)
distributedR_shutdown()
}
}
