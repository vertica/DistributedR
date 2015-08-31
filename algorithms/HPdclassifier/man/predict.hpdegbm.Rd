\name{predict.hpdegbm}
\alias{predict.hpdegbm}
\title{Data-Distributed and Model-Centralized Predict Method for Distributed, Ensemble GBM}
\description{
  This function can be used to apply a model of type \code{hpdegbm} to new data for prediction.
}
\usage{
predict.hpdegbm <- function(model, best.iter, newdata, appType="binary-classification", type="link", trace = FALSE)
}
\arguments{
  \item{model}{a list of GBM models, such as those created by the function \code{hpdegbm}.}
  \item{best.iter}{a vector storing the best iterations of GBM models, such as those created by the function \code{hpdegbm}.}
  \item{newdata}{a darray, dframe, data.frame, or a matrix that contains new data. Hewlett-Packard recommends darray over dframe when there is no categorical data.}
  \item{appType}{application types: regression, binary classification, or multi-class classification.}  
  \item{type}{the scale on which gbm makes the predictions.}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
}

\value{
  Returns predicted classes or regression values in distributed or non-distributed objects, 
    depending on the type of the input. When the new data is of type darray,
  the type of returned value is also darray, unless the output is categorical data.
  The output is a dframe when the new data is of type dframe.
}

\references{
  Package 'gbm' version 2.1.1 \url{http://cran.r-project.org/web/packages/gbm/gbm.pdf}.
}

\author{
    HP Vertica Analytics Team
}


\examples{
 \dontrun{
# example for multiple classification of iris by predict.hpdegbm
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


#### real data: iris
library(caret)
data(iris)

train <- createDataPartition(iris$Species, p=0.5, list=F)
train.iris <- iris[train,]
valid.iris <- iris[-train,]

X_train <- train.iris
Y_train <- train.iris$Species ### Y_train is a "factor" vector
Y_train1 <- as.data.frame(Y_train) ### Y_train1 is a list (data.frame)3
Y_train2 <- unlist(Y_train1)       ### unlist: Y_train2 is a "factor" vector
  
X_train1 <- as.data.frame(as.matrix(cbind(train.iris$Sepal.Length, train.iris$Sepal.Width,train.iris$Petal.Length,train.iris$Petal.Width))) 
colnames(X_train1) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width") 

#testing data set
#gbm.pred <- predict(fit.gbm4, valid.iris, n.trees=1000, type="response")
  valid1.iris <- as.data.frame(as.matrix(cbind(valid.iris$Sepal.Length, valid.iris$Sepal.Width,valid.iris$Petal.Length,valid.iris$Petal.Width))) 
  colnames(valid1.iris) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width") 


#########################################################################################################################
# test hpdegbm: model training
dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
dbest.iter <- darray(c(npartition,1), c(1,1))  
finalModel <- hpdegbm(
       X_train1,Y_train1, 
       dl_GBM_model,
       dbest.iter, 
       nExecutor,                                         
       distribution = "multinomial",
       n.trees = 100, 
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
       trace = FALSE,  # if TRUE, hpdegbm prints out progress outside gbm.fit R function
       completeModel = FALSE) # default system parameters are defined here


Predictions <- predict.hpdegbm(finalModel[[1]], finalModel[[2]], newdata, appType="multi-classification", type="response", trace = FALSE)
print(Predictions)
print(table (Predictions, as.numeric(valid.iris$Species)))
    
 }
}

\keyword{distributed GBM}
\keyword{classification}
\keyword{regression}
