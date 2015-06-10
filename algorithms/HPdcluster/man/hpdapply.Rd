\name{hpdapply}
\alias{hpdapply}
\title{Cluster labeling}
\description{
hpdapply function finds cluster label of a set of samples according to a given set of centers.
}
\usage{
hpdapply(newdata, centers, trace=FALSE)
}

\arguments{
  \item{newdata}{
    a darray (dense or sparse) or a matrix which contains the samples.
  }
  \item{centers}{
    a matrix of cluster centres. Each row represents a center. Each sample in newdata will be assigned a label which indicates the row number of its corresponding center.
  }
  \item{trace}{
    when this argument is true, intermediate steps of the progress are displayed.
  }
}
\details{
This function applies the centers found by hpdkmeans on a new set of samples in order to label them.
}
\value{
hpdapply returns a darray or a matrix based on the type of newdata which contains the corresponding label of each sample.
}
\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    iris2 <- iris
    iris2$Species <- NULL

    library(HPdcluster)
    distributedR_start()

    X <- as.darray(data.matrix(iris2))

    mykm <- hpdkmeans(X,centers=3)

    newdata <- matrix(c(5,4,3,5,7,1,0,8),2,4)
    labels <- hpdapply(newdata,mykm$centers)
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed clustering}
\keyword{distributed kmeans}
