\name{distributedR}
\alias{distributedR}
\docType{package}
\title{Distributed R for Big Data}

\description{\pkg{distributedR} simplifies large-scale data analysis. It
  includes new language constructs to express distributed programs in R
  and an infrastructure to execute them. \pkg{distributedR} provides
  data-structures such as distributed array \code{\link{darray}} to
  partition and share data across multiple executors. Users can
  express parallel execution using \code{\link{foreach}} loops.}

\section{Commands}{ \pkg{distributedR} contains the following commands. For
more details use help function on each command.

\subsection{Session manangement}{
\itemize{
\item \code{\link{distributedR_start}} - start session 
\item \code{\link{distributedR_shutdown}} - end session
\item \code{\link{distributedR_status}} - obtain worker node information
\item \code{\link{distributedR_master_info}} - obtain master host details
%\item \code{\link{presto_ls}} – get information of existing darray instances
}
}

\subsection{Distributed array, data frame and list}{
\itemize{
  \item \code{\link{darray}} - create distributed array
  \item \code{\link{dframe}} - create distributed data frame
  \item \code{\link{dlist}} - create distributed list
  \item \code{\link{as.darray}} - create darray object from matrix object
  \item \code{\link{is.darray}} - check if object is distributed array
  \item \code{\link{npartitions}} - obtain total number of partitions
  \item \code{\link{partitionsize}} - obtain dimensions of partitions
  \item \code{\link{getpartition}} - fetch darray, dframe or dlist object
  \item \code{\link{clone}} - clone or deep copy of a darray 
}
}

\subsection{Distributed execution}{
\itemize{
  \item \code{\link{foreach}} - execute function on cluster
  \item \code{\link{splits}} - pass partition to foreach loop
  \item \code{\link{update}} - make partition changes inside foreach
  loop globally visible
}
}

}
%\section{Running tests}{
%Presto provides the following tests.

%1. Unit test : Basic tests to verify Presto installation.
%Use script in $PRESTO_HOME/tests/RunPrestoUnitTest.R or type "make
%unittest" on shell terminal

%2. Algorithm test :  Tests to verify Presto installation on existing algorithm implementations.
%Use script in $PRESTO_HOME/tests/RunPrestoAlgoTest.R or type "make
%algotest" on shell terminal

%3. Stress test : Stress test Presto installation. Recommended only for
%Presto infrastructure developers.
%Use script in $PRESTO_HOME/tests/RunPrestoStressTest.R or
%type "make stresstest" on shell terminal
%}

\author{HP Vertica Development Team}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and Schreiber,
    R. (2013) Presto: Distributed Machine Learning and Graph Processing
    with Sparse Matrices. \emph{EuroSys'13}, 197--210.
    \item Homepage: http://www.vertica.com/distributedr
%    \item Mailing list: presto-dev@external.groups.hp.com
}
}

\examples{
\dontrun{                  
  library(distributedR)
  distributedR_start()
  distributedR_status()
  distributedR_shutdown()
}
}

\keyword{distributed R}
\keyword{parallel R}
\keyword{Big Data}
