\name{odbcClose}
\alias{odbcClose}
\alias{odbcCloseAll}
\alias{close.vRODBC}
\title{ODBC Close Connections}
\description{
  Close connections to ODBC databases.
}
\usage{
odbcClose(channel)

\method{close}{vRODBC}(con, \dots)

odbcCloseAll()
}
\arguments{
  \item{channel, con}{vRODBC connection object as returned by
    \code{odbcConnect}.}
  \item{\dots}{additional arguments passed from the generic.}
}
\details{
  \code{odbcClose} cleans up and frees resources.  It is also the method for the
  generic function \code{close}.

  \code{odbcCloseAll} closes all open channels (amongst the first 1000
  used in the session).

  Channels are closed at the end of an \R session, and may also be
  closed by garbage collection if no object refers to them.  In both
  cases a warning is given (but may not be seen at the end of a console
  session).
}
\value{
  Function \code{odbcClose} returns invisibly a logical indicating if it
  succeeded.
}
\author{
  Michael Lapsley and Brian Ripley
}
\seealso{
  \code{\link{odbcConnect}}
}
\keyword{IO}
\keyword{database}
