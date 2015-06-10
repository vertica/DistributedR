\name{clone}
\alias{clone}
\title{clone}

\description{
    Create a copy of input object. Can be used to clone the structure of the object, e.g., same number of partitions and each partition with the same dimension.
}

\usage{
     clone(input)
     clone(input, nrow=NA, ncol=NA, data=0, sparse=NA)
}

\arguments{
  \item{input}{ object to be cloned.}
  \item{nrow}{ number of rows in the output. By default each partition in the output will have same number of rows as the input object's partitions.}
\item{ncol}{ number of columns in the output. By default each partition in the output will have same number of columns as the input object's partitions.}
  \item{data}{ value of each element in the output object. Default is 0.}
  \item{sparse}{ whether the output object should be a sparse array. By default the output object is dense (sparse) if the input objet is dense (sparse). Used only when input object is an array.}
}

\details{ Setting distributed data-structures such as a \code{\link{darray}} equal to another does not
  result in a copy. For example, after assignment \code{da = db}, the
  two distributed arrays \code{da} and \code{db} will refer to the same
  data. Operations on any of these arrays will manipulate the same
  single copy of data. To make a copy, a \code{\link{darray}} needs to be
  explicitly cloned using \code{\link{clone}}.

  \code{\link{clone}} can also be used to copy just the structure of a
distributed object, such as the number of partitions and the partition
sizes. For example, if \code{da} is a Nx10 distributed dense array,
\code{db<-} \code{clone(da, ncol=1, data=2)} will create a dense array with
same number of rows as \code{da} but with only 1 column. All elements
in the resulting darray will be 2. When copying the structure of a
distributed object, only one of \code{nrow} or \code{ncol} can be
used, ensuring that the system keeps one of the dimension same as the
original data-structure. Note that if any argument, such as
\code{nrow} or \code{ncol}, is used with \code{\link{clone}} then only
the structure, and not the contents, of the input object is
copied. The content of the output object is determined by the argument
\code{data}.}

\value{
  A \code{\link{darray}} with the dimension, block size, and values as
    the input distributed array unless \code{\link{clone}} is called with options.
}

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

\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{darray}}
}

\examples{
\dontrun{
library(distributedR)
distributedR_start()
mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
da<-as.darray(mtx)
db<-clone(da)
all(da==db)
dc<-clone(da, ncol=2, data=2)
getpartition(dc)
distributedR_shutdown()
}
}
