\name{HPdata-package}
\alias{HPdata-package}
\alias{HPdata}
\docType{package}
\title{Distributed Data Package}
\description{
\pkg{HPdata} encapsulate all data related functions - data loading, data preparation etc for distributed R environment. It is written based on the infrastructure created in HP-Labs for distributed computing in R.
}

\details{
    \tabular{ll}{
        Package: \tab HPdata\cr
        Type: \tab Package\cr
        Version: \tab 1.1.0\cr
        Date: \tab 2015-01-16\cr
    }
 Main Functions:
 \itemize{
  \item {db2darray:} {It is an example for loading a set of unlabeled samples stored in a table to a darray.}
  \item {db2darrays:} {It is an example for loading a set of labeled samples stored in a table to two darrays.}
  \item {db2matrix:} {It is an example for loading a set of unlabeled samples stored in a table to a matrix.}
  \item {db2dframe:} {It is an example for loading a set of samples stored in a table to a dframe.}
  \item {db2dgraph:} {It loads an adjaceny matrix to a darray from an edgelist strored in a database.}
  \item {file2dgraph:} {It loads an adjaceny matrix to a darray from an edgelist strored in a set of files.}
  \item {splitGraphFile:} {It is an example for splitting an edge-list file of a graph, and distributing the results among the active nodes of a cluster system.}
 }
}

\author{
HP Vertica Analytics Team <distributedRTeam@external.groups.hp.com>
}
\references{
 \enumerate{
  \item{Using R for Iterative and Incremental Processing. Shivaram Venkataraman, Indrajit Roy, Alvin AuYoung, Rob Schreiber. HotCloud 2012, Boston, USA.}
 }   
}
\keyword{distributed R}
\keyword{Big Data Analytics}

