\name{hpdrpart}
\alias{hpdrpart}
\title{
Rpart Models over Distributed Data
}
\description{
Distributed alternative for rpart package that uses distributedR framework
}
\usage{
hpdRF_parallelTree (formula, data, weights, subset , na.action = na.omit, model = TRUE, x = FALSE, y = FALSE, params = NULL, control = NULL, cost = NULL, completeModel = FALSE, nBins = 256L, nExecutor = 1, do.trace = FALSE)



}
\arguments{
  \item{formula}{
a formula describing the model to be fitted
}
  \item{data}{
a data.frame or dframe containing the variables in the model
}
  \item{weights}{
a dframe or data.frame containing weights to be used
}
  \item{subset}{
this parameter is not implemented. Use the weights parameter instead
}
 \item{na.action}{
A function to specify the action to be taken if NAs are found
}
  \item{model}{
if logical: keep a copy of the model frame in the result? If the input value for
model is a model frame (likely from an earlier call to the rpart function), then
this frame is used rather than constructing new data. This parameter is not implemented
}
  \item{x}{
keep a copy of the x matrix in the result. This parameter is not implemented
}
  \item{y}{
keep a copy of the dependent variable in the result. If missing and model is
supplied this defaults to FALSE. This parameter is not implemented
}

  \item{params}{
Not implemented
}
  \item{control}{
Only control$minsplit, control$minbucket, control$maxdepth, control$cp are handled
}
  \item{cost}{
Not yet implemented
}
  \item{completeModel}{
Minimum size of terminal nodes. Setting this number larger causes smaller trees to be grown (and thus take less time). Note that the default values are different for classification (1) and regression (5).
}
   \item{nBins}{
Number of bins to use for numerical variables. Number of bins for categorical variables is set to the number of categories for the variable
}
   \item{nExecutor}{
Amount of parallelism to use for building the tree. Using higher amounts of parallelism for smaller datasets will degrade performance. Increase if the input data size is large or the output model is expected to be large or training is expected to take a long time. 
}
   \item{do.trace}{
If set to TRUE, give a more verbose output
}


}
\value{
An object of class hpdrpart, which is a list with the following components:
  \item{call}{
the original call to hpdrpart
}
  \item{terms}{
an object of class c("terms", "formula") (see terms.object) summarizing
the formula. Used by various methods, but typically not of direct relevance to
users.
}
  \item{frame}{
data frame with one row for each node in the tree. The row.names of frame
contain the (unique) node numbers that follow a binary ordering indexed by node
depth. Columns of frame include var, a factor giving the names of the variables
used in the split at each node (leaf nodes are denoted by the level "<leaf>"), n,
the number of observations reaching the node, wt, the sum of case weights for
observations reaching the node, dev, the deviance of the node, yval, the fitted
value of the response at the node, and splits, a two column matrix of left and
right split labels for each node. Also included in the frame are complexity, the
complexity parameter at which this split will collapse, ncompete, the number
of competitor splits recorded, and nsurrogate, the number of surrogate splits
recorded.
}
  \item{splits}{
a numeric matrix describing the splits: only present if there are any. The row
label is the name of the split variable, and columns are count, the number of
observations (which are not missing and are of positive weight) sent left or right
by the split (for competitor splits this is the number that would have been sent
left or right had this split been used, for surrogate splits it is the number missing
the primary split variable which were decided using this surrogate), ncat,
the number of categories or levels for the variable (+/-1 for a continuous variable),
improve, which is the improvement in deviance given by this split, or,
for surrogates, the concordance of the surrogate with the primary, and index,
the numeric split point. The last column adj gives the adjusted concordance for
surrogate splits. For a factor, the index column contains the row number of the
csplit matrix. For a continuous variable, the sign of ncat determines whether
the subset x < cutpoint or x > cutpoint is sent to the left.
}
  \item{csplit}{
an integer matrix. (Only present only if at least one of the split variables is a
factor or ordered factor.) There is a row for each such split, and the number of
columns is the largest number of levels in the factors. Which row is given by the
index column of the splits matrix. The columns record 1 if that level of the
factor goes to the left, 3 if it goes to the right, and 2 if that level is not present at
this node of the tree (or not defined for the factor).
}
  \item{na.action}{
(where relevant) information returned by model.frame on the special handling
of NAs derived from the na.action argument.
}
  \item{numresp}{
integer number of responses; the number of levels for a factor response.
}
  \item{classes}{
classes of the dependent variable.
}
  \item{params,control}{
a record of the arguments supplied, with defaults filled in.
}
  \item{variable.importance}{
a data.frame with a row for each variable giving its importance
}

}
\author{
    HP Vertica Analytics Team
}
