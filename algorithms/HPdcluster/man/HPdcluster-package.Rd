\name{HPdcluster-package}
\alias{HPdcluster-package}
\alias{HPdcluster}
\docType{package}
\title{Distributed clustering for Big Data}
\description{
\pkg{HPdcluster} provides a few distributed clustering functions. It is written based on the infrastructure created in HP-Labs for distributed computing in R.
}

\details{
    \tabular{ll}{
        Package: \tab HPdcluster\cr
        Type: \tab Package\cr
        Version: \tab 1.1.0\cr
        Date: \tab 2015-01-16\cr
    }
 Main Functions:
 \itemize{
  \item {hpdkmeans:} {It is a distributed version of kmeans.}
  \item {hpdapply:} {It finds cluster label of a set of samples according to a given set of centers.}
 }
}

\author{
HP Vertica Analytics Team <distributedRTeam@external.groups.hp.com>
}
\references{
 \enumerate{
  \item{Using R for Iterative and Incremental Processing. Shivaram Venkataraman, Indrajit Roy, Alvin AuYoung, Rob Schreiber. HotCloud 2012, Boston, USA.}
  \item{http://stat.ethz.ch/R-manual/R-devel/library/stats/html/kmeans.html}
 }   
}
\keyword{distributed R}
\keyword{distributed clustering}
\keyword{Big Data Analytics}

