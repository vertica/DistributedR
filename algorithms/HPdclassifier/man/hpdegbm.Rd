\name{hpdegbm}
\alias{hpdegbm}
\title{Distributed GBM Algorithm Based On R gbm Package}
\description{
The hpdegbm function runs the \code{gbm.fit} function of the R gbm package in a distributed fashion with parallelism at the sub-ensemble GBM level.
}
\description{
  \code{hpdegbm} calls several instances of \code{gbm.fit} distributed across a cluster system in order to create multiple GBM models concurrently. The master distributes the input data among all R executors in the Distributed R environment, and GBM models are created simultaneously. Ultimately, all of these GBM models are combined to formulate an ensemble GBM model.

  The interface of \code{hpdegbm} is similar to \code{gbm.fit}. It adds four arguments: \code{dl_GBM_model}, \code{dbest.iter}, \code{nExecutor}, and \code{trace}. The returned result is a list of GBM models which are compatible with the result of \code{gbm.fit}.
}

\usage{
hpdegbm(X_train, Y_train, 
       nExecutor,                                     
       distribution,
       n.trees = 1000, 
       interaction.depth = 1, 
       n.minobsinnode = 10,
       shrinkage = 0.050,    
       bag.fraction = 0.50,
       samplingFlag = TRUE,  
       sampleThresh=100,
       trace = FALSE) 
}

\arguments{
  \item{X_train}{a data frame, dframe, darray, or matrix which contains samples of predictor variables}
  \item{Y_train}{a data frame, dframe, darray, or matrix which contains a vector of output} 
  \item{nExecutor}{the number of gbm models in the ensemble. These will be trained in parallel. In the case where X_train/Y_train are dobjects, this defaults to the number of partitions in X_train. When X_train/Y_train are not dobjects, nExecutor is equal to the total number of cores in the distributed R cluster by default.}
  \item{distribution}{a string parameter that currently (as of Distributed R version 1.2.0) supports four values: (1) "gaussian": Gaussian distribution for regression; (2) "adaoost": AdaBoost distribution for binary classification; (3) "bernoulli": Bernoulli distribution for binary classification; (4) "multinomial" multinomial distribution for multi-class classification}  
  \item{n.trees}{the total number of trees to fit.}
  \item{interaction.depth}{the maximum depth of variable interactions.}
  \item{n.minobsinnode}{minimum number of observations in the trees' terminal nodes.}
  \item{shrinkage}{a shrinkage parameter applied to each tree in the expansion (learning rate).}
  \item{bag.fraction}{the fraction of the training set observations randomly selected for the next tree in the expansion.}
  \item{samplingFlag}{a boolean that indicates whether or not to perform distributed sampling before training the models of the ensemble. When X_train/Y_train are not dobjects, this is ignored. Otherwise, if this is true, the input dobjects are sampled into a number of output partitions equal to nExecutor, and then gbm.fit is called on each output partition. This is important for ensuring that each model in the ensemble is trained on a representative portion of the overall dataset.
  However, sampling does incur some performance overhead, so if it is known ahead of time that the input data is already well-shuffled, sampling may be disabled using samplingFlag = FALSE. }
  \item{sampleThresh}{a positive number used (when samplingFlag = T and input X_train and Y_train are dobjects) as part of a heuristic to determine the number of samples used to build each model. The number of samples to use for each model is calculated as sampleThresh * nClass * <number of predictor variables>. 'nClass' is computed automatically for multinomial distributions, and is set to nClass = 1 when distribution = 'gaussian'}
  \item{trace}{if TRUE, print out the progress of the algorithm as it executes, including information on the execution times of various parts of the algorithm}
}

\value{
An object of class hpdegbm, which is a list with the following components:
  \item{call}{the original call to hpdegbm}
  \item{model}{a list that contains multiple GBM models. The number of models is determined by the parameter nExecutor.}
  \item{distribution}{the distribution of the response variable as specified by the input parameter}
  \item{n.trees}{the number of trees grown}
  \item{numGBMModel}{the number of models in the ensemble}
  \item{bestIterations}{a vector that contains the best iterations of each of the trained GBM models. This is used to determine the number of trees used during prediction.}
  \item{featureNames}{a vector containing the names of the predictor variables used during training}
}

\note{
Two algorithms are implemented in hpdegbm. They are: 
(1) When the training data set is small and stored as a matrix or data.frame, the whole data set is copied to the number of executors specified by nExecutor. Each executor trains one GBM model in parallel and in a distributed manner. Because bagging is applied in gbm.fit, multiple different GBM models are obtained, although the same training data is used in every executor; (2) When the training data set is large and is stored as a distributed data types, such as a dframe or darray,
X_train and Y_train must have the same number of partitions and the same number of rows in each corresponding partition.
Distributed sampling is performed (when samplingFlag = TRUE) to obtain a number of partitions equal to nExecutor. Each data partition is loaded into an executor and is used to train a GBM model. When samplingFLag = F, the nExecutor parameter is set to be equal to the number of partitions of your input data. 

The trained GBM models from executors are combined into an ensemble on the master side to formulate the distributed gbm object. 
}

\references{
  Package 'gbm' version 2.1.1 \url{http://cran.r-project.org/web/packages/gbm/gbm.pdf}.
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    
library(distributedR)
library(HPdclassifier)
distributedR_start()

data(iris)
irisX       <- iris[which(names(iris) != "Species")]
irisY       <- as.character(iris$Species)
trainPerc   <- 0.8
trainIdx    <- sample(1:nrow(irisX), ceiling(trainPerc * nrow(irisX)))
irisX_train <- irisX[trainIdx,]
irisY_train <- irisY[trainIdx]
irisX_test  <- irisX[-trainIdx,]
irisY_test  <- irisY[-trainIdx]

# Create distributed versions of the training data
dirisX_train <- as.dframe(irisX_train)
dirisY_train <- as.dframe(as.data.frame(irisY_train))
dirisX_test  <- as.dframe(irisX_test)
dirisY_test  <- as.dframe(as.data.frame(irisY_test))

# Using centralized data structures. Will build 6 different models in parallel
# on the whole dataset
mod <- hpdegbm(irisX_train, irisY_train, distribution = 'multinomial',
               nExecutor = 6)
# Make predictions using the ensemble
pred <- predict(mod, irisX_test)

# Using dobjects. Will sample the training data into 6 partitions and then build 
# a different model on each partition in parallel
dmod <- hpdegbm(dirisX_train, dirisY_train, distribution = 'multinomial',
                nExecutor = 6)
# Make predictions using the ensemble
dpred <- predict(dmod, irisX_test)

distributedR_shutdown()
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed GBM}
