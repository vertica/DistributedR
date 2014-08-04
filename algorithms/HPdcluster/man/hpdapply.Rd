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
Arash Fard <afard@vertica.com>
}

\examples{
 \dontrun{
    iris2 <- iris
    iris2$Species <- NULL
    centers <- matrix(c(5.901613,5.006000,6.850000,2.748387,3.428000,
3.073684,4.393548,1.462000,5.742105,1.433871,0.246000,2.071053),3,4)
    dimnames(centers) <- list(1L:3, colnames(iris2))

    library(distributedR)
    distributedR_start()
    (drs <- distributedR_status())
    nblocks = sum(drs$Inst)
    X <- as.darray(as.matrix(cbind(iris2$Sepal.Length,iris2$Sepal.Width,
iris2$Petal.Length,iris2$Petal.Width)),c(ceiling(length(iris2$Sepal.Length)
/nblocks),4))
    colnames(X) <- colnames(iris2)

    library(HPdcluster)
    mykm <- hpdkmeans(X,centers=3)

    newdata <- matrix(c(5,4,3,5,7,1,0,8),2,4)
    labels <- hpdapply(newdata,mykm$centers)
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed clustering}
\keyword{distributed kmeans}
