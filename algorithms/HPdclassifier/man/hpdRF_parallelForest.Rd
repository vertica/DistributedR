\name{hpdRF_parallelForest}
\alias{hpdRF_parallelForest}
\title{Distributed randomForest with parallelism in sub-forest level}
\description{
hpdRF_parallelForest function runs \code{randomForest} function of randomForest package in a distributed fashion with parallelism in sub-forest level.
}
\description{
  \code{hpdRF_parallelForest} calls several instances of \code{randomForest} distributed across a cluster system in order to create sub-forests concurrently. Therefore, the master distributes the input data among all R-executors of the distributedR environment, and trees on different sub-sections of the forest are created simultaneously. At the end, all these trees are combined to result a single forest.

  The interface of \code{hpdRF_parallelForest} is similar to \code{randomForest}. Indeed it adds two arguments \code{nExecutor} and \code{trace}, and removes several other arguments: \code{subset}, \code{do.trace}, \code{corr.bias}, \code{keep.inbag}, and \code{oob.prox}. Nevertheless, it must be noticed that default value of some arguments are changed as well to make the algorithm more scalable for big data problems; e.g, \code{proximity} is FALSE by default. Its returned result is also completely compatible to the result of \code{randomForest}.
}

\usage{ 
\method{hpdRF_parallelForest}{formula}(formula, data=NULL, ..., ntree=500, 
         na.action=na.fail, nExecutor, trace=FALSE, 
         completeModel=FALSE)
\method{hpdRF_parallelForest}{default}(x, y=NULL,  xtest=NULL, ytest=NULL, ntree=500,
         mtry=if (!is.null(y) && !is.factor(y) && !is.dframe(y))
         max(floor(ncol(x)/3), 1) else floor(sqrt(ncol(x))),
         replace=TRUE, classwt=NULL, cutoff, strata,
         sampsize = if (replace) nrow(x) else ceiling(.632*nrow(x)),
         nodesize = if (!is.null(y) && !is.factor(y) && 
         !is.dframe(y)) 5 else 1,
         maxnodes=NULL, importance=FALSE, localImp=FALSE, nPerm=1,
         proximity=FALSE,norm.votes=TRUE, keep.forest=TRUE,
         nExecutor, trace=FALSE, completeModel=FALSE, ...)
}

\arguments{
  \item{data}{a data frame or dframe which contains samples.}
  \item{na.action}{A function to specify the action to be taken if NAs
    are found.  (NOTE: If given, this argument must be named.)}
  \item{formula}{a formula describing the model to be fitted. 
        It must be a simple formula without any arithmetic operation 
        among columns.}
  \item{x}{when a data frame or a matrix of predictors assigned to x,
    its size should not be bigger than 2GB. For bigger datasets, darray
    should be used. darray does not support categorical data. Therefore, 
    dframe and the first interface should be used for classification
    problems of large datasets with categorical data.}
  \item{y}{a response vector. If a factor, classification is assumed,
    otherwise regression is assumed.  If omitted, \code{hpdRF_parallelForest}
    will run in unsupervised mode. When \code{x} is a darray), \code{y} 
    should be also a darray with a single column.}
  \item{xtest}{a data frame or matrix (like \code{x}) containing
    predictors for the test set. When \code{x} is a darray, 
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
    number of classes.  The 'winning' class for an observation is the
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
  \item{proximity}{a logical value which indicates if the proximity measure
    among the rows should be calculated. It is FALSE by default because 
    it is very memory inefficient. Moreover, it is calculated only on 
    'out-of-bag' data}
  \item{norm.votes}{If \code{TRUE} (default), the final result of votes
    are expressed as fractions.  If \code{FALSE}, raw vote counts are
    returned (useful for combining results from different runs).
    Ignored for regression.}
  \item{keep.forest}{If set to \code{FALSE}, the forest will not be
    retained in the output object.}
  \item{nExecutor}{a positive integer number indicating the number of tasks for running the function. To have optimal performance, it is recommended to have this number smaller than or equal to the number of R-executors in the distributedR environment. It cannot be bigger than ntree. When it is not specified, it will be equal to the number of available executors by default.}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
  \item{completeModel}{
    when it is FALSE (default), the output values that preserve information per sample are discarded. They are 'oob.times', 'votes', 'predicted', 'confusion', 'err.rate', 'mse', 'rsq', 'proximity', and 'test'. This feature is intended to keep the size of the output model small.}
  \item{...}{optional parameters to be passed to the low level function.}
}

\value{
An object of class \code{randomForest}. The result is similar to the result of the combine function in randomForest package
 and will contain the following components.

  \item{call}{the original call to \code{hpdRF_parallelForest}}
  \item{type}{one of \code{regression}, \code{classification}, or
    \code{unsupervised}.}
  \item{predicted}{(only when completeModel=TRUE) the predicted values of 
    the input data based on out-of-bag samples.}
  \item{importance}{a matrix with \code{nclass} + 2 (for classification)
    or two (for regression) columns.  For classification, the first
    \code{nclass} columns are the class-specific measures computed as
    mean descrease in accuracy.  The \code{nclass} + 1st column is the
    mean descrease in accuracy over all classes.  The last column is the
    mean decrease in Gini index.  For Regression, the first column is
    the mean decrease in accuracy and the second the mean decrease in MSE.
    If \code{importance=FALSE}, the last measure is still returned as a
    vector.}
  \item{importanceSD}{The ''standard errors'' of the permutation-based
    importance measure.  For classification, a \code{p} by \code{nclass + 1} 
    matrix corresponding to the first \code{nclass + 1} columns
    of the importance matrix.  For regression, a length \code{p} vector.}
  \item{localImp}{a p by n matrix containing the casewise importance
    measures, the [i,j] element of which is the importance of i-th
    variable on the j-th case. \code{NULL} if \code{localImp=FALSE}.}
  \item{ntree}{number of trees grown.}
  \item{mtry}{number of predictors sampled for spliting at each node.}
  \item{forest}{(a list that contains the entire forest; \code{NULL} if
    \code{hpdRF_parallelForest} is run in unsupervised mode or if
    \code{keep.forest=FALSE}.}
  \item{err.rate}{(classification only) 
    vector error rates of the prediction on the input data, the i-th element
    being the (OOB) error rate for all trees up to the i-th.}
  \item{confusion}{(classification only) 
    the confusion matrix of the prediction (based on OOB data).}
  \item{votes}{(classification only, and only when completeModel=TRUE) 
    a matrix with one row for each input data point and one column for each class,
    giving the fraction or number of (OOB) 'votes' from the random forest.}
  \item{oob.times}{(only when completeModel=TRUE) number of times cases are 'out-of-bag'
    (and thus used in computing OOB error estimate)}
  \item{y}{(only when completeModel=TRUE) the response vector if it is made available
    in the input.}
  \item{proximity}{(only when completeModel=TRUE) a matrix of proximity measures among
    the input (based on the frequency that pairs of data points are in
    the same terminal nodes) when \code{proximity=TRUE}.}
  \item{mse}{(regression only) 
    vector of mean square errors: sum of squared residuals divided by \code{n}.}
  \item{rsq}{(regression only) 
    ''pseudo R-squared'': 1 - \code{mse} / Var(y).}
  \item{test}{(only when completeModel=TRUE) if test set is given 
    (through the \code{xtest} or additionally \code{ytest} arguments), 
    this component is a list which contains the corresponding \code{predicted}, 
    \code{err.rate}, \code{confusion}, \code{votes} (for classification) 
    or \code{predicted}, \code{mse} and \code{rsq} (for regression) for the test set.}
  \item{terms}{it contains a formula identifying response and predictors 
                (for classification and regression types).}
}

\note{
When ntree is not big enough in comparison to nExecutor, some of the returned predicted values may become NULL. It is the same for values of 'votes' matrix when they are normalized (norm.votes=TRUE).
Returned values for err.rate, votes, and oob.times are valid only for classification type.

Three scenarios can be imagined for the type of input data. When ordinary R types are used (matrix or data.frame) the behavior is similar to the \code{randomForest} function; however, the total size of the input data cannot be bigger than 2GB. In fact, for bigger data size distributed data types; i.e., darray or dframe, should be used. When \code{x} is of type darray, in the case of existence \code{y} must be of type darray as well. Regarding the fact that darray does not support categorical data, this data type cannot be used for classification mode. When \code{x} is of type dframe, no value can be assigned to \code{y}; indeed for this data type, the formula interface should be used for classification and the default interface for unsupervised mode.
}

\references{
  Breiman, L. (2001), \emph{Random Forests}, Machine Learning 45(1),5-32.

  Breiman, L (2002), ''Manual On Setting Up, Using, And Understanding
  Random Forests V3.1'', \url{http://oz.berkeley.edu/users/breiman/Using_random_forests_V3.1.pdf}.

  Random Forests V4.6-10, \url{http://cran.r-project.org/web/packages/randomForest/randomForest.pdf}.
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    
library(HPdclassifier)
distributedR_start()

## Classification:
##data(iris)
iris.rf <- hpdRF_parallelForest(Species ~ ., data=iris, importance=TRUE)
print(iris.rf)

## The 'unsupervised' case:
iris.urf <- hpdRF_parallelForest(iris[, -5], 
                            proximity=TRUE, completeModel=TRUE)
MDSplot(iris.urf, iris$Species)

## stratified sampling: draw 20, 30, and 20 of the species to grow each tree.
(iris.rf2 <- hpdRF_parallelForest(iris[1:4], iris$Species, 
                          sampsize=c(20, 30, 20)))

## Regression:
## data(airquality)
ozone.rf <- hpdRF_parallelForest(Ozone ~ ., data=airquality, mtry=3,
                    importance=TRUE, na.action=na.omit, 
                    completeModel=TRUE)
print(ozone.rf)
## Show "importance" of variables: higher value mean more important:
round(importance(ozone.rf), 2)

## "x" can be a matrix instead of a data frame:
x <- matrix(runif(5e2), 100)
y <- gl(2, 50)
(myrf <- hpdRF_parallelForest(x, y))
(predict(myrf, x))

## "complicated" formula:
(swiss.rf <- hpdRF_parallelForest(sqrt(Fertility)~. - Catholic + I(Catholic<50),
                          data=swiss))
(predict(swiss.rf, swiss))
## Test use of 32-level factor as a predictor:
x <- data.frame(x1=gl(32, 5), x2=runif(160), y=rnorm(160))
(rf1 <- hpdRF_parallelForest(x[-3], x[[3]], ntree=10))

## Grow no more than 4 nodes per tree:
(treesize(hpdRF_parallelForest(Species ~ ., data=iris, maxnodes=4, ntree=30)))

distributedR_shutdown()
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed random forest}
