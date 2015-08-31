\name{hpdRF_parallelTree}
\alias{hpdRF_parallelTree}
\alias{hpdrandomForest}
\title{
Random Forest Models over Distributed Data
}
\description{
Distributed alternative for randomForest package that uses distributedR framework
}
\usage{
hpdRF_parallelTree (formula, data, ntree = 50, xtest, ytest, mtry, replace=TRUE, cutoff, nodesize, maxnodes = .Machine$integer.max, do.trace = FALSE, keep.forest = TRUE, na.action = na.fail, nBins=256, completeModel=FALSE)

}
\arguments{
  \item{formula}{
a formula describing the model to be fitted
}
  \item{data}{
a data.frame or dframe containing the variables in the model
}
  \item{xtest}{
a dframe or data.frame containing predictors for the test set
}
  \item{ytest}{
response for the test set
}
  \item{ntree}{
Number of trees to grow. This should not be set to too small a number, to ensure that every input row gets predicted at least a few times. However it should also not be too large a number as it will take a long time and a lot of memory to train  
}
  \item{mtry}{
Number of variables randomly sampled as candidates at each split. Note that the default values are different for classification (sqrt(p) where p is number of variables in x) and regression (p/3)
}
  \item{replace}{
Should sampling of cases be done with or without replacement?
}
  \item{cutoff}{
(Classification only) A vector of length equal to number of classes. The ‘winning’ class for an observation is the one with the maximum ratio of proportion of votes to cutoff. Default is 1/k where k is the number of classes (i.e., majority vote wins).
}
  \item{nodesize}{
Minimum size of terminal nodes. Setting this number larger causes smaller trees to be grown (and thus take less time). Note that the default values are different for classification (1) and regression (5).
}
  \item{maxnodes}{
Maximum number of terminal nodes trees in the forest can have. If not given, trees are grown to the maximum possible (subject to limits by nodesize). 
}
  \item{do.trace}{
If set to TRUE, give a more verbose output
}
  \item{keep.forest}{
If set to FALSE, the forest will not be retained in the output object
}
  \item{na.action}{
A function to specify the action to be taken if NAs are found. This does not apply to xtest,ytest. All NA observations for xtest will be predicted (with perhaps lower accuracy). Any NA observations in ytest are ignored (corresponding observations of xtest are ignored as well)
}
  \item{nBins}{
Number of bins to use for numerical variables. Number of bins for categorical variables is set to the number of categories for the variable
}
  \item{completeModel}{
If set to FALSE, xtest,ytest will be ignored and out of bag samples will not be predicted
}

}
\details{
	predictors and responses must align with each other for variables xtest and ytest (have the same number of rows and the same number of blocks in the case of a dframe) because row names are not used for alignment.
    The alias name for this function is \code{hpdrandomForest}.
}
\value{
An object of class hpdRF_parallelTree, which is a list with the following components:
  \item{call}{
the original call to hpdRF_parallelTree
}
  \item{type}{
either regression or classification
}
  \item{predicted}{
the predicted values of the input data based on out-of-bag samples.
}
  \item{ntree}{
number of trees grown.
}
  \item{mtry}{
number of predictors sampled for spliting at each node.
}
  \item{forest}{
A list that contains entire forest. This is not available if keep.forest is set to FALSE
}
  \item{err.rate}{
(classification only, and only when completeModel=TRUE) vector error rates of the prediction on the input data, the i-th element being the (OOB) error rate for all trees up to the i-th
}
  \item{confusion}{
(classification only, and only when completeModel=TRUE) the confusion matrix of the prediction (based on OOB data).
}
  \item{mse}{
(regression only, and only when completeModel=TRUE) vector of mean square errors: sum of squared residuals divided by n.

}
  \item{rsq}{
(regression only, and only when completeModel=TRUE) “pseudo R-squared”: 1 - mse / Var(y)
}

  \item{test}{
if test set is given (through the xtest or additionally ytest arguments), this component is a list which contains the corresponding predicted, err.rate, confusion, votes (for classification) or predicted, mse and rsq (for regression)
for the test set.
}
}
\author{
    HP Vertica Analytics Team
}
