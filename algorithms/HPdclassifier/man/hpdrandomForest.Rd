\name{hpdrandomForest}
\alias{hpdrandomForest}
\title{Distributed randomForest}
\description{
hpdrandomForest function runs \code{randomForest} function of randomForest package in a distributed fashion.
}
\description{
  \code{hpdrandomForest} calls several instances of \code{randomForest} distributed across a cluster system. Therefore, the master distributes the input data among all R-executors of the distributedR environment, and trees on different sub-sections of the forest are created simultaneously. At the end, all these trees are combined to result a single forest.

  The interface of \code{hpdrandomForest} is similar to \code{randomForest}. Indeed it adds two arguments \code{nExecutor} and \code{trace}, and removes several other arguments \code{do.trace}, \code{keep.inbag} \code{proximity}, and \code{oob.prox}. Its returned result is also completely compatible to the result of \code{randomForest}.
}

\usage{ 
\method{hpdrandomForest}{formula}(formula, data=NULL, ..., subset, na.action=na.fail,
                                     nExecutor, trace=FALSE)
\method{hpdrandomForest}{default}(x, y=NULL,  xtest=NULL, ytest=NULL, ntree=500,
         mtry=if (!is.null(y) && !is.factor(y) && !is.dframe(y))
         max(floor(ncol(x)/3), 1) else floor(sqrt(ncol(x))),
         replace=TRUE, classwt=NULL, cutoff, strata,
         sampsize = if (replace) nrow(x) else ceiling(.632*nrow(x)),
         nodesize = if (!is.null(y) && !is.factor(y) && 
         !is.dframe(y)) 5 else 1,
         maxnodes = NULL,
         importance=FALSE, localImp=FALSE, nPerm=1,
         norm.votes=TRUE,
         keep.forest=!is.null(y) && is.null(xtest), 
         corr.bias=FALSE, nExecutor, trace=FALSE, ...)
}

\arguments{
  \item{data}{an optional data frame containing the variables in the model.
    By default the variables are taken from the environment which
    \code{hpdrandomForest} is called from.}
  \item{subset}{an index vector indicating which rows should be used.
    (NOTE: If given, this argument must be named.)}
  \item{na.action}{A function to specify the action to be taken if NAs
    are found.  (NOTE: If given, this argument must be named.)}
  \item{formula}{a formula describing the model to be fitted.}
  \item{x}{when a data frame or a matrix of predictors assigned to x,
    its size should not be bigger than 64MB. For bigger datasets, darray
    or dframe should be used. darray is more memory efficient than dframe,
    but it does not support categorical data. Therefore, using darray is
    highly recommended when there is no categorical data.}
  \item{y}{a response vector. If a factor, classification is assumed,
    otherwise regression is assumed.  If omitted, \code{hpdrandomForest}
    will run in unsupervised mode. When \code{x} is a distributed structure
    (darray or a dframe), \code{y} should be also a distributed structure
    with a single column.}
  \item{xtest}{a data frame or matrix (like \code{x}) containing
    predictors for the test set. When \code{x} is a darray or a dframe, 
    it should be of the same type.}
  \item{ytest}{response for the test set. Its type should be consistent
    with \code{y}. Moreover, it should have a single column.}
  \item{ntree}{Number of trees to grow.  This should not be set to too
    small a number, to ensure that every input row gets predicted at
    least a few times. }
  \item{mtry}{Number of variables randomly sampled as candidates at each
    split.  Note that the default values are different for
    classification (sqrt(p) where p is number of variables in \code{x})
    and regression (p/3)}
  \item{replace}{Should sampling of cases be done with or without
    replacement?}
  \item{classwt}{Priors of the classes.  Need not add up to one.
    Ignored for regression.}
  \item{cutoff}{(Classification only)  A vector of length equal to
    number of classes.  The `winning' class for an observation is the
    one with the maximum ratio of proportion of votes to cutoff.
    Default is 1/k where k is the number of classes (i.e., majority vote
    wins).}
  \item{strata}{A (factor) variable that is used for stratified sampling.}
  \item{sampsize}{Size(s) of sample to draw.  For classification, if
    sampsize is a vector of the length the number of strata, then
    sampling is stratified by strata, and the elements of sampsize
    indicate the numbers to be drawn from the strata.}
  \item{nodesize}{Minimum size of terminal nodes.  Setting this number
    larger causes smaller trees to be grown (and thus take less time).
    Note that the default values are different for classification (1)
    and regression (5).}
  \item{maxnodes}{Maximum number of terminal nodes trees in the forest
	can have.  If not given, trees are grown to the maximum possible
	(subject to limits by \code{nodesize}).  If set larger than maximum
	possible, a warning is issued.}
  \item{importance}{Should importance of predictors be assessed? }
  \item{localImp}{Should casewise importance measure be computed?
    (Setting this to \code{TRUE} will override \code{importance}.) }
  \item{nPerm}{Number of times the OOB data are permuted per tree for
    assessing variable importance.  Number larger than 1 gives slightly
    more stable estimate, but not very effective.  Currently only
    implemented for regression.}
  \item{norm.votes}{If \code{TRUE} (default), the final result of votes
    are expressed as fractions.  If \code{FALSE}, raw vote counts are
    returned (useful for combining results from different runs).
    Ignored for regression.}
  \item{keep.forest}{If set to \code{FALSE}, the forest will not be
    retained in the output object.  If \code{xtest} is given, defaults
    to \code{FALSE}.}
  \item{corr.bias}{perform bias correction for regression?  Note:
    Experimental.  Use at your own risk.}
  \item{...}{optional parameters to be passed to the low level function.}
  \item{nExecutor}{a positive integer number indicating the number of tasks for running the function. To have optimal performance, it is recommended to have this number smaller than the number of R-executors in the distributedR environment. It cannot be bigger than ntree.}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
}

\value{
An object of class \code{randomForest}. The result is consistent with the result of the combine function in randomForest package.
}

\note{
When ntree is not big enough in comparison to nExecutor, some of the returned predicted values may become NULL.

Returned values for err.rate, votes, and oob.times are valid only for classification type.
}

\references{
  Breiman, L. (2001), \emph{Random Forests}, Machine Learning 45(1),
  5-32.

  Breiman, L (2002), ``Manual On Setting Up, Using, And Understanding
  Random Forests V3.1'', \url{http://oz.berkeley.edu/users/breiman/Using_random_forests_V3.1.pdf}.

  Random Forests V4.6-7, \url{http://cran.r-project.org/web/packages/randomForest/randomForest.pdf}.
}

\author{
Arash Fard <afard@vertica.com>
}

\examples{
 \dontrun{
    
library(HPdclassifier)
distributedR_start()
drs <- distributedR_status()
nparts <- sum(drs$Ins)

## Classification:
##data(iris)
iris.rf <- hpdrandomForest(Species ~ ., data=iris, importance=TRUE,
                        nExecutor=nparts)
print(iris.rf)

## The `unsupervised' case:
iris.urf <- hpdrandomForest(iris[, -5], nExecutor=nparts)
MDSplot(iris.urf, iris$Species)

## stratified sampling: draw 20, 30, and 20 of the species to grow each tree.
(iris.rf2 <- hpdrandomForest(iris[1:4], iris$Species, 
                          sampsize=c(20, 30, 20), nExecutor=nparts))

## Regression:
## data(airquality)
ozone.rf <- hpdrandomForest(Ozone ~ ., data=airquality, mtry=3,
                         importance=TRUE, na.action=na.omit, nExecutor=nparts)
print(ozone.rf)
## Show "importance" of variables: higher value mean more important:
round(importance(ozone.rf), 2)

## "x" can be a matrix instead of a data frame:
x <- matrix(runif(5e2), 100)
y <- gl(2, 50)
(myrf <- hpdrandomForest(x, y, nExecutor=nparts))
(predict(myrf, x))

## "complicated" formula:
(swiss.rf <- hpdrandomForest(sqrt(Fertility) ~ . - Catholic + I(Catholic < 50),
                          data=swiss, nExecutor=nparts))
(predict(swiss.rf, swiss))
## Test use of 32-level factor as a predictor:
x <- data.frame(x1=gl(32, 5), x2=runif(160), y=rnorm(160))
(rf1 <- hpdrandomForest(x[-3], x[[3]], ntree=10, nExecutor=nparts))

## Grow no more than 4 nodes per tree:
(treesize(hpdrandomForest(Species ~ ., data=iris, maxnodes=4, ntree=30,
                                 nExecutor=nparts)))

distributedR_shutdown()
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed random forest}
