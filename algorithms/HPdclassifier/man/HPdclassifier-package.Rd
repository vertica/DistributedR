\name{HPdclassifier-package}
\alias{HPdclassifier-package}
\alias{HPdclassifier}
\docType{package}
\title{Distributed algorithms for classifiers}
\description{
\pkg{HPdclassifier} provides several distributed algorithms for classifiers. It is written based on the infrastructure created in HP-Labs for distributed computing in R.
}

\details{
    \tabular{ll}{
        Package: \tab HPdclassifier\cr
        Type: \tab Package\cr
        Version: \tab 1.2.0\cr
        Date: \tab 2015-01-16\cr
    }
 Main Functions:
 \itemize{
  \item {hpdRF_parallelTree:} {It is a distributed function for randomForest that utilizes parallelism in creating each tree of the forest}
  \item {hpdRF_parallelForest:} {It is a distributed function for randomForest that utilizes parallelism in creating sub-forests of the forest}
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
\keyword{Distributed R}
\keyword{Scalable Machine Learning algorithms}
\keyword{Big Data Analytics}

