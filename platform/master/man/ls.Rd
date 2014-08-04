\name{distributedR_ls}
\alias{distributedR_ls}
\title{
distributedR_ls
}
\description{
This function shows a list of darray objects in the current Presto session. The size of a partition of a darray object is shown in KBytes with its name and the location where the splits is located.
}
\usage{
presto_ls()
}
\arguments{
}
\value{
This function shows the status of workers in the current Presto session.
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
\code{\link{distributedR_shutdown}}
}
\examples{
\dontrun{
library(distributedR)
##Start worker process here!!
distributedR_start()
da<-darray(c(4,4),c(2,4))
distributedR_ls()
distributedR_shutdown()
}
}
