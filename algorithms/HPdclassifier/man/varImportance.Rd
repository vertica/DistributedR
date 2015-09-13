\name{varImportance}
\alias{varImportance}
\title{Distributed Variable Importance function used in randomForest}
\description{
varImportance function calculates the importance of each feature given a trained model and a set of testing data. To calculate the importance of each variable, the variable is permuted while the other variables are unchanged and then the permutated data is predicted. These predictions as well as unaltered predictions are compared to actual responses. The metric used to compare is dependant upon the data. 
}

\usage{ 
varImportance(model, xtest, ytest, ..., distance_metric, trace = FALSE)
}

\arguments{
  \item{model}{a model that can be used to predict inputs xtest}
  \item{xtest}{a dframe or data.frame with observations of features required by model}
  \item{ytest}{a dframe or data.frame with observations of responses corresponding to xtest}
  \item{...}{optional parameters to be passed to the predict function.}
  \item{distance_metric}{a model that can be used to predict inputs xtest}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
}

\value{
A data.frame with a row for each variable that has the importance of each variable
}

\note{
Although only tested for models of type randomForest, the varImportance function can be used on any generic model type assuming the corresponding predict function returns a data.frame or dframe with 1 column as the output. 
}



\author{
    HP Vertica Analytics Team
}

