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
hpdegbm <- function(
       X_train,Y_train, 
       dl_GBM_model,
       dbest.iter, 
       nExecutor,                                         
       distribution = "bernoulli",
       n.trees = 1000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.50,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       offset = NULL, 
       misc = NULL, 
       w = NULL,
       var.monotone = NULL,
       nTrain = NULL,
       train.fraction = NULL,
       keep.data = FALSE,
       verbose = FALSE, # If TRUE, gbm will prints out progress and performance indicators
       var.names = NULL,
       #response.name = "y",
       group = NULL,
       trace = FALSE,  # If TRUE, hpdegbm prints out progress outside the gbm.fit R function
       completeModel = FALSE) # default system parameters are defined here
}

\arguments{
  \item{X_train}{a data frame, dframe, darray, or matrix which contains samples of predictor variables.}
  \item{Y_train}{a data frame, dframe, darray, or matrix which contains a vector of output.} 
  \item{dl_GBM_mode}{a dlist storing GBM models trained in a distributed manner and in parallel.}
  \item{dbest.iter}{a darray storing the best iterations of the trained GBM models.} 
  \item{trace}{if TRUE, print out the training time.}
  \item{completeModel}{if TRUE, add training time in the model.}
  \item{nExecutor}{the number of executors.}
  \item{distribution}{supports: (1) Gaussian distribution for regression; (2) AdaBoost distribution for binary classification; (3) Bernoulli distribution for binary classification; (4) multinomial distribution for multi-class classification in Distributed R 1.2.}  
  \item{n.trees}{the total number of trees to fit.}
  \item{interaction.depth}{the maximum depth of variable interactions.}
  \item{n.minobsinnode}{minimum number of observations in the trees' terminal nodes.}
  \item{shrinkage}{a shrinkage parameter applied to each tree in the expansion (learning rate).}
  \item{bag.fraction}{the fraction of the training set observations randomly selected for the next tree in the expansion.}
  \item{offset}{a vector of values for the offset. Set to NULL in Distributed R 1.2}
  \item{misc}{not used in Distributed R 1.2.}
  \item{w}{a vector of weights. Not used in Distributed R 1.2.}
  \item{var.monotone}{set to NULL in Distributed R 1.2.}
  \item{nTrain}{an integer representing the number of cases on which to train. If neither nTrain nor train.fraction are specified, all of the data is used for training.}
  \item{train.fraction}{the first train.fraction * nrows(X_train) observations are used to fit the gbm and the remainder are used for computing out-of-sample estimates of the loss function. In Distributed R 1.2, all of the data is used for training.}
  \item{keep.data}{a logical variable indicating whether to keep the data and an index of the data stored in the object.}
  \item{verbose}{if TRUE, gbm prints out progress and performance indicators.}
  \item{var.names}{for gbm.fit: a vector of strings of length equal to the number of columns in X_train constraining the names of the predictor variables.}
  \item{response.name}{for gbm.fit: a character string label for the response variable.}
  \item{group}{group used when distribution = 'pairwise'.}
}

\value{
An object of class hpdegbm, which is a list with the following components:
  \item{call}{The original call to hpdegbm.}
  \item{GBM_model}{A list that contains multiple GBM models.}
  \item{best.iter}{A vector that contains the best iterations of GBM models.}
  \item{ntree}{The number of trees grown.}
}

\note{
Two algorithms are implemented in hpdegbm. They are: 
(1) When the training data set is small and stored as a matrix or data.frame, the whole data set is loaded into every core. Each executor trains one GBM model in parallel and in a distributed manner. Because bagging is applied in gbm.fit, multiple different GBM models are obtained, although the same training data is applied to every  executor; (2) When the training data set is large and is stored as a distributed data types, such as a  dframe or darray, each data partition is loaded into every executor and is used to train a GBM model. 

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
    
library(gbm)
library(distributedR)
library(caTools)
library(randomForest)
library(HPdclassifier)


confusion <- function(a, b){
  tbl <- table(a, b)  
  mis <- 1 - sum(diag(tbl))/sum(tbl)
  list(table = tbl, misclass.prob = mis)
}


##############################################################################################
### Generate large, distributed, simulated training data
npartition <- 6 

### generate training data by Distributed R
nTrain <- 20000 
p <- 10

dfX <- dframe(c(nTrain,p), blocks=c(ceiling(nTrain/npartition),p))  # horizontal partition
daY <- darray(c(nTrain,1), blocks=c(ceiling(nTrain/npartition),1))
dl_GBM_model <- dlist(npartition)
nExecutor <- npartition

dbest.iter <- darray(c(npartition,1), c(1,1)) 

foreach(i, 1:nExecutor, function(X_train=splits(dfX,i),Y_train=splits(daY,i)) {
     n <- nrow(X_train)
     p <- ncol(X_train)
     X_train <- as.data.frame(matrix(rnorm(n*p), nrow=n))
     y <- rep(0, n)
     y[ apply(X_train*X_train, 1, sum) > qchisq(0.5, p) ] <- 1
     #colnames(X_train) <- paste("X", 1:p, sep="")

     Y_train <- y  ### is.vector: truth, numeric

     update(X_train)
     update(Y_train)
})


#############################################################################################
################ generate distributed testing data
### generate training data with Distributed R
nTest <- 20000 
p <- 10

npartition_test <- 6
nExecutor_test <- npartition_test

dfX_test <- dframe(c(nTest,p), blocks=c(ceiling(nTest/npartition_test),p))  # horizontal partition
daY_test <- darray(c(nTest,1), blocks=c(ceiling(nTest/npartition_test),1))

foreach(i, 1:nExecutor_test, function(X_test=splits(dfX_test,i),Y_test=splits(daY_test,i)) {
     n <- nrow(X_test)
     p <- ncol(X_test)
     X_test <- as.data.frame(matrix(rnorm(n*p), nrow=n))
     y <- rep(0, n)
     y[ apply(X_test*X_test, 1, sum) > qchisq(0.5, p) ] <- 1
     #colnames(X_train) <- paste("X", 1:p, sep="")

     Y_test <- y  ### is.vector: truth, numeric

     update(X_test)
     update(Y_test)
})

########################################################################################################################
#### test hpdegbm functions 
### test distributed sampling: hpdsampling
Ns <- 1*ceiling((nTrain/npartition)/npartition)
npartition <- npartitions(dfX)

sampledXY <- hpdsampling(dfX,daY, Ns, npartition)

dfRX <- sampledXY[[1]]
daRY <- sampledXY[[2]]

#########################################################################################################################
# test hpdegbm: model training
dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
dbest.iter <- darray(c(npartition,1), c(1,1))  # ## dNumericVector, dFactorVector

finalModel <- hpdegbm(
       dfRX,daRY,  # dfRX,daRY: distributed large training data
       dl_GBM_model,
       dbest.iter, 
       nExecutor,                                         
       #distribution = "bernoulli",
       distribution = "adaboost",
       #distribution = "gaussian",
       #distribution = "multinomial",
       n.trees = 1000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       offset = NULL, 
       misc = NULL, 
       w = NULL,
       var.monotone = NULL,
       nTrain = NULL,
       train.fraction = NULL,
       keep.data = TRUE,
       verbose = FALSE, # if TRUE, gbm prints out progress and performance indicators
       var.names = NULL,
       #response.name = "y",
       group = NULL,
       trace = FALSE,  # if TRUE, hpdegbm prints out progress outside the gbm.fit R function
       completeModel = FALSE) # default system parameters are defined here

newdata <- dfX_test     # test distributed large data set
Predictions <- predict.hpdegbm(finalModel[[1]], finalModel[[2]], newdata, appType="binary-classification", type="link", trace = FALSE)
print(confusion(Predictions > 0, getpartition(daY_test) > 0)) 

distributedR_shutdown()
 }
}

\keyword{distributed R}
\keyword{Big Data Analytics}
\keyword{distributed GBM}
