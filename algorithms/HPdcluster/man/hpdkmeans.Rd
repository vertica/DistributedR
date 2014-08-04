\name{hpdkmeans}
\alias{hpdkmeans}
\alias{fitted.hpdkmeans}
\alias{print.hpdkmeans}
\title{Distributed kmeans}
\description{
hpdkmeans function is intended to be a distributed alternative for kmeans function.
}
\usage{
hpdkmeans(X, centers, iter.max = 10, nstart = 1, 
            sampling_threshold = 1e+06, trace = FALSE, 
            na_action = c("exclude","fail"), 
            returnCluster = FALSE)
}

\arguments{
  \item{X}{
    a darray (dense or sparse) which contains the samples.
  }
  \item{centers}{
    either the number of clusters, say k, or a set of initial (distinct) cluster centres. If a number, a random set of (distinct) samples in X is chosen as the initial centres.
  }
  \item{iter.max}{
    the maximum number of iterations allowed.
  }
  \item{nstart}{
    when the value specified for 'centers' argument is a number, clustering will be performed several times and the best result is reported. The best result would be the one with highest value of 'withinss' regardless of its number of iterations. 'nstart' gives the number of times that a random set of centers is chosen and clustering is performed. When 'centers' argument is a matrix of centers, 'nstart' will be discarded.
  }
  \item{sampling_threshold}{
    threshold for the method which Randomly finds centers (centralized or distributed). It should be always smaller than 1e9. When (blockSize > sampling_threshold || nSample > 1e9), the distributed sampling is selected, in which first a set of blocks are randomly chosen, and then the centers are randomly selected from the samples of those bloks. Here, blockSize is the number of samples in each partition of X, and nSample is the total number of samples in X.
  }
  \item{trace}{
    when this argument is true, intermediate steps of the progress are displayed.
  }
  \item{na_action}{
    it indicates what should happen when the data contain missed values. Values of NA, NaN, and Inf in samples are treated as missed values. There are two options for this argument exclude and fail. When exclude is selected (the default choice), any sample with missed values will be ignored in the clustering process. In the darray which will be created for cluster, the value corresponding to these samples will be NA. When fail is selected, the function will stop in the case of any missed value in the dataset.
  }
  \item{returnCluster}{
    when it is FALSE (default), the function does not return cluster label of the samples and quality measures of the clusters
  }
}
\details{
The data given by X is clustered by the k-means method, which aims to partition the points into k groups such that the sum of squares from points to the assigned cluster centres is minimized. At the minimum, all cluster centres are at the mean of their Voronoi sets (the set of data points which are nearest to the cluster centre).

The algorithm of Lloyd–Forgy (Lloyd 1957 and Forgy 1965) is used at the current version. If an initial matrix of centres is supplied, it is possible that no point will be closest to one or more centres, which currently generates a warning message.
}
\value{
hpdkmeans returns an object of class "hpdkmeans" which has a print and a fitted method. It is a list with components:
  \item{cluster}{(only when returnCluster=TRUE) A darray of integers (from 1:k) indicating the cluster to which each point is allocated.}
  \item{centers}{A matrix of cluster centres.}
  \item{totss}{(only when returnCluster=TRUE) The total sum of squares.}
  \item{withinss}{(only when returnCluster=TRUE) Vector of within-cluster sum of squares, one component per cluster.}
  \item{tot.withinss}{(only when returnCluster=TRUE) Total within-cluster sum of squares, i.e., sum(withinss).}
  \item{betweenss}{(only when returnCluster=TRUE) The between-cluster sum of squares, i.e. totss-tot.withinss.}
  \item{size}{The number of points in each cluster.}
  \item{iter}{The number of iterations used for clustering. Its value will be iter.max+1 when the algorithm is not converged.}
}
\references{
Forgy, E. W. (1965) Cluster analysis of multivariate data: efficiency vs interpretability of classifications. Biometrics 21, 768–769.

Lloyd, S. P. (1957, 1982) Least squares quantization in PCM. Technical Note, Bell Laboratories. Published in 1982 in IEEE Transactions on Information Theory 28, 128–137.
}
\author{
Arash Fard <afard@vertica.com>
}

\seealso{
 \code{\link[stats]{kmeans}}
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
    mykm1 <- hpdkmeans(X,centers=centers)
    
    mykm2 <- hpdkmeans(X,centers=3)
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed clustering}
\keyword{distributed kmeans}
