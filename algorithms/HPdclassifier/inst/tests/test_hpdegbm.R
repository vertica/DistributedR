################################################################################################
# Unit test of hpdegbm: (1) AdaBoost distribution for binary classification
#                       (2) bernoulli distribution for binary classification
#                       (3) multinomial distribution for multi-class classification
#                       (4) gaussian distribution for regression


#################################################################################################
######################### generate testing scenarios for classification ######################### 
library(gbm)
#library(testthat)
library(HPdclassifier)


confusion <- function(a, b){
  tbl <- table(a, b)	
  mis <- 1 - sum(diag(tbl))/sum(tbl)
  list(table = tbl, misclass.prob = mis)
}


#### real data: iris
#library(caret)
data(iris)

# split iris by caret function
#train <- createDataPartition(iris$Species, p=0.7, list=F)
#train.iris <- iris[train,]
#valid.iris <- iris[-train,]

# another method to split iris
ind <- sample(2,nrow(iris),replace=TRUE,prob=c(0.7,0.3))
train.iris <- iris[ind==1,]
valid.iris <- iris[ind==2,]

X_train0 <- train.iris
Y_train0 <- train.iris$Species ### Y_train is a "factor" vector
Y_train1 <- as.data.frame(Y_train0) ### Y_train1 is a data.frame
Y_train2 <- unlist(Y_train1)       ### unlist or as.factor: Y_train2 is a "factor" vector

rm(X_train0)
rm(Y_train0)
  
X_train1 <- as.data.frame(as.matrix(cbind(train.iris$Sepal.Length, train.iris$Sepal.Width,train.iris$Petal.Length,train.iris$Petal.Width))) 
colnames(X_train1) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width") 

#testing data set
valid1.iris <- as.data.frame(as.matrix(cbind(valid.iris$Sepal.Length, valid.iris$Sepal.Width,valid.iris$Petal.Length,valid.iris$Petal.Width))) 
colnames(valid1.iris) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width") 



#################################################################################################
#################################################################################################
######### generate small and medium-size simulated training and testing data by R matrix ########
n <- 4000
p <- 10
X_train <- matrix(rnorm(n*p), nrow=n)
y <- rep(0, n)
y[ apply(X_train*X_train, 1, sum) > qchisq(0.5, p) ] <- 1
colnames(X_train) <- paste("X", 1:p, sep="")
X_test <- matrix(rnorm(n*p), nrow=n)
y_test <- rep(0, n)
y_test[ apply(X_test*X_test, 1, sum) > qchisq(0.5, p) ] <- 1  ### y_test: numeric
colnames(X_test) <- paste("X", 1:p, sep="")
N <- nrow(X_train)
nrowX <- nrow(X_train)
ncolX <- ncol(X_train)
Y_train <- y  ### is.vector: truth, numeric



##############################################################################################
### Generate distributed big simulated training data
npartition <- 4 

### generate training data by distributedR
nTrain <- 10000 
p <- 10

dfX <- dframe(c(nTrain,p), blocks=c(ceiling(nTrain/npartition),p))  # horizontal partition
daY <- darray(c(nTrain,1), blocks=c(ceiling(nTrain/npartition),1))
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition

#dbest.iter <- darray(c(npartition,1), c(1,1))  # ## dNumericVector, dFactorVector

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
### generate training data by distributedR
nTest <- 20000 
p <- 10

npartition_test <- npartition
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




#################################################################################################

dfRX <- dfX
daRY <- daY 


#################################################################################################
######################### test AdaBoost distribution for binary classification ################## 
# testAdaBoost1: AdaBoost distribution, small training data, small testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  # ## dNumericVector, dFactorVector

context("Checking the classification accuracy: AdaBoost")
test_that("Test classification accuracy: AdaBoost", {
    finalModel1 <- hpdegbm(
       X_train,Y_train,  # dfRX,daRY: distributed big data, ### X_train, Y_train: centralized small data, ### X_train1,Y_train1: iris data
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
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) 


    newdata1 <- X_test        # test centralized small simulated data
    Predictions1 <- predict.hpdegbm(finalModel1, newdata1, trace = FALSE)
    result1 <- confusion(Predictions1 > 0, y_test > 0)

    PredictionsGBM1_1 <- predict.gbm(finalModel1$model[[1]], newdata1, n.trees=finalModel1$bestIterations[1], trace = FALSE)
    result1_1 <- confusion(PredictionsGBM1_1 > 0, y_test > 0)
    PredictionsGBM1_2 <- predict.gbm(finalModel1$model[[2]], newdata1, n.trees=finalModel1$bestIterations[2], trace = FALSE)
    result1_2 <- confusion(PredictionsGBM1_2 > 0, y_test > 0)
    PredictionsGBM1_3 <- predict.gbm(finalModel1$model[[3]], newdata1, n.trees=finalModel1$bestIterations[3], trace = FALSE)
    result1_3 <- confusion(PredictionsGBM1_3 > 0, y_test > 0)
    PredictionsGBM1_4 <- predict.gbm(finalModel1$model[[4]], newdata1, n.trees=finalModel1$bestIterations[4], trace = FALSE)
    result1_4 <- confusion(PredictionsGBM1_4 > 0, y_test > 0)

    # availability of some outputs
    expect_false(is.null(Predictions1))
    expect_false(is.null(PredictionsGBM1_1))
    expect_false(is.null(PredictionsGBM1_2))
    expect_false(is.null(PredictionsGBM1_3))
    expect_false(is.null(PredictionsGBM1_4))

    # prediction accuracy
    expect_true(result1$misclass.prob < 0.10)
    expect_true(result1_1$misclass.prob < 0.10)
    expect_true(result1_2$misclass.prob < 0.10)
    expect_true(result1_3$misclass.prob < 0.10)
    expect_true(result1_4$misclass.prob < 0.10)

    expect_true(abs(result1$misclass.prob - result1_1$misclass.prob) < 0.10)
})



#################################################################################################
# testAdaBoost2: AdaBoost distribution, small training data, distributed testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  

context("Checking the classification accuracy: AdaBoost")
test_that("Test classification accuracy: AdaBoost", {
    finalModel2 <- hpdegbm(
       X_train,Y_train,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
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
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    newdata2 <- dfX_test     # test distributed big data
    Predictions2 <- getpartition(predict.hpdegbm(finalModel2, newdata2,  trace = FALSE))
    print(confusion(Predictions2 > 0, getpartition(daY_test) > 0)) 
    result2 <- confusion(Predictions2 > 0, getpartition(daY_test) > 0)

    newdata21 <- getpartition(newdata2)
    PredictionsGBM2_1 <- predict.gbm(finalModel2$model[[1]], newdata21, n.trees=finalModel2$bestIterations[1], trace = FALSE)
    result2_1 <- confusion(PredictionsGBM2_1 > 0, getpartition(daY_test) > 0)
    PredictionsGBM2_2 <- predict.gbm(finalModel2$model[[2]], newdata21, n.trees=finalModel2$bestIterations[2], trace = FALSE)
    result2_2 <- confusion(PredictionsGBM2_2 > 0, getpartition(daY_test) > 0)
    PredictionsGBM2_3 <- predict.gbm(finalModel2$model[[3]], newdata21, n.trees=finalModel2$bestIterations[3], trace = FALSE)
    result2_3 <- confusion(PredictionsGBM2_3 > 0, getpartition(daY_test) > 0)
    PredictionsGBM2_4 <- predict.gbm(finalModel2$model[[4]], newdata21, n.trees=finalModel2$bestIterations[4], trace = FALSE)
    result2_4 <- confusion(PredictionsGBM2_4 > 0, getpartition(daY_test) > 0)

    # availability of some outputs
    expect_false(is.null(Predictions2))
    expect_false(is.null(PredictionsGBM2_1))
    expect_false(is.null(PredictionsGBM2_2))
    expect_false(is.null(PredictionsGBM2_3))
    expect_false(is.null(PredictionsGBM2_4))

    # prediction accuracy
    expect_true(result2$misclass.prob < 0.10)
    expect_true(result2_1$misclass.prob < 0.10)
    expect_true(result2_2$misclass.prob < 0.10)
    expect_true(result2_3$misclass.prob < 0.10)
    expect_true(result2_4$misclass.prob < 0.10)

    expect_true(abs(result2$misclass.prob - result2_1$misclass.prob) < 0.10)
})


# testAdaBoost3: AdaBoost distribution, distributed training data, distributed testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
 

context("Checking the classification accuracy: AdaBoost")
test_that("Test classification accuracy: AdaBoost", {
  finalModel3 <- hpdegbm(
       dfRX,daRY, #dfX, daY, # # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
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
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    newdata3 <- dfX_test     # test distributed big data
    Predictions3 <- getpartition(predict.hpdegbm(finalModel3, newdata3, trace = FALSE))
    print(confusion(Predictions3 > 0, getpartition(daY_test) > 0)) #daY: distributed big data
    result3 <- confusion(Predictions3 > 0, getpartition(daY_test) > 0)

    newdata31 <- getpartition(newdata3)
    PredictionsGBM3_1 <- predict.gbm(finalModel3$model[[1]], newdata31, n.trees=finalModel3$bestIterations[1], trace = FALSE)
    result3_1 <- confusion(PredictionsGBM3_1 > 0, getpartition(daY_test)> 0)
    PredictionsGBM3_2 <- predict.gbm(finalModel3$model[[2]], newdata31, n.trees=finalModel3$bestIterations[2], trace = FALSE)
    result3_2 <- confusion(PredictionsGBM3_2 > 0, getpartition(daY_test) > 0)
    PredictionsGBM3_3 <- predict.gbm(finalModel3$model[[3]], newdata31, n.trees=finalModel3$bestIterations[3], trace = FALSE)
    result3_3 <- confusion(PredictionsGBM3_3 > 0, getpartition(daY_test) > 0)
    PredictionsGBM3_4 <- predict.gbm(finalModel3$model[[4]], newdata31, n.trees=finalModel3$bestIterations[4], trace = FALSE)
    result3_4 <- confusion(PredictionsGBM3_4 > 0, getpartition(daY_test) > 0)

    # availability of some outputs
    expect_false(is.null(Predictions3))
    expect_false(is.null(PredictionsGBM3_1))
    expect_false(is.null(PredictionsGBM3_2))
    expect_false(is.null(PredictionsGBM3_3))
    expect_false(is.null(PredictionsGBM3_4))

    # prediction accuracy
    expect_true(result3$misclass.prob < 0.10)
    expect_true(result3_1$misclass.prob < 0.10)
    expect_true(result3_2$misclass.prob < 0.10)
    expect_true(result3_3$misclass.prob < 0.10)
    expect_true(result3_4$misclass.prob < 0.10)

    expect_true(abs(result3$misclass.prob - result3_1$misclass.prob) < 0.10)
})


#################################################################################################
######################### test bernoulli distribution for binary classification ################# 
# testBernoulli1: bernoulli distribution, small training data, small testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  

context("Checking the classification accuracy: bernoulli")
test_that("Test classification accuracy: bernoulli", {
    finalModel4 <- hpdegbm(
       X_train,Y_train,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
       nExecutor,                                         
       distribution = "bernoulli",
       #distribution = "adaboost",
       #distribution = "gaussian",
       #distribution = "multinomial",
       n.trees = 1000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       #response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    newdata4 <- X_test        # test centralized small simulated data
    Predictions4 <- predict.hpdegbm(finalModel4, newdata4,  trace = FALSE)
    print(confusion(Predictions4 > 0, y_test > 0))   # Y_test: centralized small data
    result4 <- confusion(Predictions4 > 0, y_test > 0)

    PredictionsGBM4_1 <- predict.gbm(finalModel4$model[[1]], newdata4, n.trees=finalModel4$bestIterations[1], trace = FALSE)
    result4_1 <- confusion(PredictionsGBM4_1 > 0, y_test > 0)
    PredictionsGBM4_2 <- predict.gbm(finalModel4$model[[2]], newdata4, n.trees=finalModel4$bestIterations[2], trace = FALSE)
    result4_2 <- confusion(PredictionsGBM4_2 > 0, y_test > 0)
    PredictionsGBM4_3 <- predict.gbm(finalModel4$model[[3]], newdata4, n.trees=finalModel4$bestIterations[3], trace = FALSE)
    result4_3 <- confusion(PredictionsGBM4_3 > 0, y_test > 0)
    PredictionsGBM4_4 <- predict.gbm(finalModel4$model[[4]], newdata4, n.trees=finalModel4$bestIterations[4], trace = FALSE)
    result4_4 <- confusion(PredictionsGBM4_4 > 0, y_test > 0)

    # availability of some outputs
    expect_false(is.null(Predictions4))
    expect_false(is.null(PredictionsGBM4_1))
    expect_false(is.null(PredictionsGBM4_2))
    expect_false(is.null(PredictionsGBM4_3))
    expect_false(is.null(PredictionsGBM4_4))

    # prediction accuracy
    expect_true(result4$misclass.prob < 0.10)
    expect_true(result4_1$misclass.prob < 0.10)
    expect_true(result4_2$misclass.prob < 0.10)
    expect_true(result4_3$misclass.prob < 0.10)
    expect_true(result4_4$misclass.prob < 0.10)

    expect_true(abs(result4$misclass.prob - result4_1$misclass.prob) < 0.10)
})



#################################################################################################
# testBernoulli2: bernoulli distribution, small training data, distributed testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  

test_that("Test classification accuracy: bernoulli", {
    finalModel5 <- hpdegbm(
       X_train,Y_train,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
       nExecutor,                                         
       distribution = "bernoulli",
       #distribution = "adaboost",
       #distribution = "gaussian",
       #distribution = "multinomial",
       n.trees = 1000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    newdata5 <- dfX_test     # test distributed big data
    Predictions5 <- getpartition(predict.hpdegbm(finalModel5, newdata5, trace = FALSE))
    print(confusion(Predictions5 > 0, getpartition(daY_test) > 0)) #daY: distributed big data
    result5 <- confusion(Predictions5 > 0, getpartition(daY_test) > 0)

    newdata51 = getpartition(newdata5)
    PredictionsGBM5_1 <- predict.gbm(finalModel5$model[[1]], newdata51, n.trees=finalModel5$bestIterations[1], trace = FALSE)
    result5_1 <- confusion(PredictionsGBM5_1 > 0, getpartition(daY_test) > 0)
    PredictionsGBM5_2 <- predict.gbm(finalModel5$model[[2]], newdata51, n.trees=finalModel5$bestIterations[2], trace = FALSE)
    result5_2 <- confusion(PredictionsGBM5_2 > 0, getpartition(daY_test) > 0)
    PredictionsGBM5_3 <- predict.gbm(finalModel5$model[[3]], newdata51, n.trees=finalModel5$bestIterations[3], trace = FALSE)
    result5_3 <- confusion(PredictionsGBM5_3 > 0, getpartition(daY_test) > 0)
    PredictionsGBM5_4 <- predict.gbm(finalModel5$model[[4]], newdata51, n.trees=finalModel5$bestIterations[4], trace = FALSE)
    result5_4 <- confusion(PredictionsGBM5_4 > 0, getpartition(daY_test) > 0)

    # availability of some outputs
    expect_false(is.null(Predictions5))
    expect_false(is.null(PredictionsGBM5_1))
    expect_false(is.null(PredictionsGBM5_2))
    expect_false(is.null(PredictionsGBM5_3))
    expect_false(is.null(PredictionsGBM5_4))

    # prediction accuracy
    expect_true(result5$misclass.prob < 0.10)
    expect_true(result5_1$misclass.prob < 0.10)
    expect_true(result5_2$misclass.prob < 0.10)
    expect_true(result5_3$misclass.prob < 0.10)
    expect_true(result5_4$misclass.prob < 0.10)

    expect_true(abs(result5$misclass.prob - result5_1$misclass.prob) < 0.10)
})


# testBernoulli3: bernoulli distribution, distributed training data, distributed testing data
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  # ## dNumericVector, dFactorVector

context("Checking the classification accuracy: bernoulli")
test_that("Test classification accuracy: bernoulli", {
    finalModel6 <- hpdegbm(
       dfRX,daRY,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
       nExecutor,                                         
       distribution = "bernoulli",
       #distribution = "adaboost",
       #distribution = "gaussian",
       #distribution = "multinomial",
       n.trees = 1000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    newdata6 <- dfX_test     # test distributed big data
    Predictions6 <- getpartition(predict.hpdegbm(finalModel6, newdata6, trace = FALSE))
    print(confusion(Predictions6 > 0, getpartition(daY_test) > 0)) #daY: distributed big data
    result6 <- confusion(Predictions6 > 0, getpartition(daY_test) > 0)

    newdata61 <- getpartition(newdata6)
    PredictionsGBM6_1 <- predict.gbm(finalModel6$model[[1]], newdata61, n.trees=finalModel6$bestIterations[1], trace = FALSE)
    result6_1 <- confusion(PredictionsGBM6_1 > 0, getpartition(daY_test) > 0)
    PredictionsGBM6_2 <- predict.gbm(finalModel6$model[[2]], newdata61, n.trees=finalModel6$bestIterations[2], trace = FALSE)
    result6_2 <- confusion(PredictionsGBM6_2 > 0, getpartition(daY_test) > 0)
    PredictionsGBM6_3 <- predict.gbm(finalModel6$model[[3]], newdata61, n.trees=finalModel6$bestIterations[3], trace = FALSE)
    result6_3 <- confusion(PredictionsGBM6_3 > 0, getpartition(daY_test)> 0)
    PredictionsGBM6_4 <- predict.gbm(finalModel6$model[[4]], newdata61, n.trees=finalModel6$bestIterations[4], trace = FALSE)
    result6_4 <- confusion(PredictionsGBM6_4 > 0, getpartition(daY_test) > 0)

    # availability of some outputs
    expect_false(is.null(Predictions6))
    expect_false(is.null(PredictionsGBM6_1))
    expect_false(is.null(PredictionsGBM6_2))
    expect_false(is.null(PredictionsGBM6_3))
    expect_false(is.null(PredictionsGBM6_4))

    # prediction accuracy
    expect_true(result6$misclass.prob < 0.10)
    expect_true(result6_1$misclass.prob < 0.10)
    expect_true(result6_2$misclass.prob < 0.10)
    expect_true(result6_3$misclass.prob < 0.10)
    expect_true(result6_4$misclass.prob < 0.10)

    expect_true(abs(result6$misclass.prob - result6_1$misclass.prob) < 0.10)
})


#################################################################################################
######################### test multinomial distribution for multiclass classification ########### 
# testMultinomial1: multinomial distribution, small real training data, small realtesting data
context("Checking the classification accuracy: multinomial")
test_that("Test classification accuracy: multinomial", {
    finalModel7 <- hpdegbm(
       X_train1,Y_train1,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
       nExecutor,                                         
       #distribution = "bernoulli",
       #distribution = "adaboost",
       #distribution = "gaussian",
       distribution = "multinomial",
       n.trees = 100, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.050,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 3,
       sampleThresh = 200) # default system parameters are defined here


    newdata7 <- valid1.iris  # test small real data 
    Predictions7 <- predict.hpdegbm(finalModel7, newdata7, trace = FALSE)
    print(Predictions7)
    as.factor(valid.iris$Species)
    print(table ((Predictions7), (valid.iris$Species)))
    result7 <- table ((Predictions7), (valid.iris$Species))
    errorRate7 <- 1 - sum(diag(result7))/sum(result7)
    print(errorRate7)


    # compute classification error rate
    PredictionsGBM7_1 <- predict.gbm(finalModel7$model[[1]], newdata7, n.trees=finalModel7$bestIterations[1], trace = FALSE)
    aa7_1 <- apply(PredictionsGBM7_1, 1, which.max) - as.numeric(valid.iris$Species)
    correctCount7_1 <- sum(aa7_1 == 0)
    errorRate7_1 <- 1 - correctCount7_1/(nrow(newdata7))
    print(errorRate7_1)

    PredictionsGBM7_2 <- predict.gbm(finalModel7$model[[2]], newdata7, n.trees=finalModel7$bestIterations[2], trace = FALSE)
    aa7_2 <- apply(PredictionsGBM7_2, 1, which.max) - as.numeric(valid.iris$Species)
    correctCount7_2 <- sum(aa7_2 == 0)
    errorRate7_2 <- 1 - correctCount7_2/(nrow(newdata7))
    print(errorRate7_2)

   # PredictionsGBM7_3 <- predict.gbm(finalModel7$model[[3]], newdata7, n.trees=finalModel7$bestIterations[3], trace = FALSE)
   # aa7_3 <- apply(PredictionsGBM7_3, 1, which.max) - as.numeric(valid.iris$Species)
   # correctCount7_3 <- sum(aa7_3 == 0)
   # errorRate7_3 <- 1 - correctCount7_3/(nrow(newdata7))
   # print(errorRate7_3)

   # PredictionsGBM7_4 <- predict.gbm(finalModel7$model[[4]], newdata7, n.trees=finalModel7$bestIterations[4], trace = FALSE)
   # aa7_4 <- apply(PredictionsGBM7_4, 1, which.max) - as.numeric(valid.iris$Species)
   # correctCount7_4 <- sum(aa7_4 == 0)
   # errorRate7_4 <- 1 - correctCount7_4/(nrow(newdata7))
   # print(errorRate7_4)

    # availability of some outputs
    expect_false(is.null(Predictions7))
    expect_false(is.null(PredictionsGBM7_1))
    expect_false(is.null(PredictionsGBM7_2))
   # expect_false(is.null(PredictionsGBM7_3))
   # expect_false(is.null(PredictionsGBM7_4))

    # prediction accuracy
    expect_true(errorRate7 < 0.150)
    expect_true(errorRate7_1 < 0.150)
    expect_true(errorRate7_2 < 0.150)
   # expect_true(errorRate7_3 < 0.150)
   # expect_true(errorRate7_4 < 0.150)

    expect_true(abs(errorRate7 - errorRate7_1) < 0.15)
})


#################################################################################################
######################### test gaussian distribution for regression ############################# 
# testGaussian1: gaussian, small training data, small testing data
##################################################################################################
#### generate simulated training data for Gaussian distribution and regression
N <- 4000
X1 <- runif(N)
X2 <- 2*runif(N)
X3 <- ordered(sample(letters[1:4],N,replace=TRUE),levels=letters[4:1])
X4 <- factor(sample(letters[1:6],N,replace=TRUE))
X5 <- factor(sample(letters[1:3],N,replace=TRUE))
X6 <- 3*runif(N)
mu <- c(-1,0,1,2)[as.numeric(X3)]


SNR <- 10 # signal-to-noise ratio
Y <- X1**1.5 + 2 * (X2**.5) + mu
sigma <- sqrt(var(Y)/SNR) 
Y <- Y + rnorm(N,0,sigma)

data <- data.frame(Y=Y,X1=X1,X2=X2,X3=X3,X4=X4,X5=X5,X6=X6)
X <- data.frame(X1=X1,X2=X2,X3=X3,X4=X4,X5=X5,X6=X6)


nrowX <- nrow(X)
ncolX <- ncol(X)

###dfX <- dframe(X, c(nrowX/2,ncolX))
dfX8 <- as.dframe(X, blocks=c(500,6))  ### how to convert data.frame to dframe?
daY8 <- as.darray(as.matrix(Y), blocks=c(500,1))


#daPredict <- darray(dim=c(N,1), blocks=c(500,1), sparse=FALSE)
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition




# testGaussian1: gaussian distribution for regression, distributed training data, distributed testing data
########################################################################################################################

dfRX8 <- dfX8 
daRY8 <- daY8 



#########################################################################################################################
# test hpdegbm: model training
#dl_GBM_model <- dlist(npartition)
nExecutor <- npartition
#dbest.iter <- darray(c(npartition,1), c(1,1))  # ## dNumericVector, dFactorVector

context("Checking the regression accuracy: gaussian")
test_that("Test regression accuracy: gaussian", {
    finalModel8 <- hpdegbm(
       dfRX8,daRY8,  # dfRX,daRY: distributed big data, ### X_train, Y_train:centralized small data ### ### X_train1,Y_train1: iris data
       nExecutor,                                         
       #distribution = "bernoulli",
       #distribution = "adaboost",
       distribution = "gaussian",
       #distribution = "multinomial",
       n.trees = 3000, 
       interaction.depth = 3, 
       n.minobsinnode = 10,
       shrinkage = 0.10,     #[0.001, 1]
       bag.fraction = 0.632, #0.5-0.8,
       #offset = NULL, 
       #misc = NULL, 
       #w = NULL,
       #var.monotone = NULL,
       #nTrain = NULL,
       #train.fraction = NULL,
       #keep.data = FALSE,
       #verbose = FALSE, # If TRUE, gbm will print out progress and performanc eindicators
       #var.names = NULL,
       ##response.name = "y",
       #group = NULL,
       trace = TRUE, #FALSE,  # If TRUE, hpdegbm will print out progress outside gbm.fit R function
       samplingFlag = TRUE,
       nClass = 2,
       sampleThresh = 200) # default system parameters are defined here


    ############################################################################################################
    # make some new data
    N <- 1000
    X1 <- runif(N)
    X2 <- 2*runif(N)
    X3 <- ordered(sample(letters[1:4],N,replace=TRUE))
    X4 <- factor(sample(letters[1:6],N,replace=TRUE))
    X5 <- factor(sample(letters[1:3],N,replace=TRUE))
    X6 <- 3*runif(N)
    mu <- c(-1,0,1,2)[as.numeric(X3)]


    Y <- X1**1.5 + 2 * (X2**.5) + mu + rnorm(N,0,sigma)
    data2 <- data.frame(Y,X1=X1,X2=X2,X3=X3,X4=X4,X5=X5,X6=X6)
 
    newdata8 <- data2
    Predictions8 <- predict.hpdegbm(finalModel8, newdata8, trace = FALSE)
    print (sum((data2$Y - Predictions8)^2))
    result8 <- sum((data2$Y - Predictions8)^2)

    PredictionsGBM8_1 <- predict.gbm(finalModel8$model[[1]], newdata8, n.trees=finalModel8$bestIterations[1], trace = FALSE)
    result8_1 <-  sum((data2$Y - PredictionsGBM8_1)^2)
    PredictionsGBM8_2 <- predict.gbm(finalModel8$model[[2]], newdata8, n.trees=finalModel8$bestIterations[2], trace = FALSE)
    result8_2 <- sum((data2$Y - PredictionsGBM8_2)^2)
    PredictionsGBM8_3 <- predict.gbm(finalModel8$model[[3]], newdata8, n.trees=finalModel8$bestIterations[3], trace = FALSE)
    result8_3 <- sum((data2$Y - PredictionsGBM8_3)^2)
    PredictionsGBM8_4 <- predict.gbm(finalModel8$model[[4]], newdata8, n.trees=finalModel8$bestIterations[4], trace = FALSE)
    result8_4 <- sum((data2$Y - PredictionsGBM8_4)^2)


    # availability of some outputs
    expect_false(is.null(Predictions8))
    expect_false(is.null(PredictionsGBM8_1))
    expect_false(is.null(PredictionsGBM8_2))
    expect_false(is.null(PredictionsGBM8_3))
    expect_false(is.null(PredictionsGBM8_4))

    # prediction accuracy
    expect_true(result8 < 5000)
    expect_true(result8_1 < 5000)
    expect_true(result8_2 < 5000)
    expect_true(result8_3 < 5000)
    expect_true(result8_4 < 5000)

    expect_true(abs(result8 - result8_1) < 5000)
})




###############################################################################################
context("centralized and distributed prediction of iris real data")
data(iris)
irisX       <- iris[which(names(iris) != "Species")]
irisY       <- as.character(iris$Species)
trainPerc   <- 0.8
trainIdx    <- sample(1:nrow(irisX), ceiling(trainPerc * nrow(irisX)))
irisX_train <- irisX[trainIdx,]
irisY_train <- irisY[trainIdx]
irisX_test  <- irisX[-trainIdx,]
irisY_test  <- irisY[-trainIdx]
 
dirisX_train <- as.dframe(irisX_train)
dirisY_train <- as.dframe(as.data.frame(irisY_train))
dirisX_test  <- as.dframe(irisX_test)
dirisY_test  <- as.dframe(as.data.frame(irisY_test))

# Big data case
dmod <- hpdegbm(dirisX_train, dirisY_train, distribution = 'multinomial', nClass = 3)

# centralized prediction of multi-class classification
a <- predict.hpdegbm(dmod, irisX_test)
a


# distributed prediction of multi-class classification
b <- getpartition(predict.hpdegbm(dmod, dirisX_test))
b



context("Checking the interface of hpdegbm")
test_that("The tree hyper-parameters are validated for AdaBoost", {
    expect_error(hpdegbm(X_train, Y_train, nExecutor=4, n.trees=-1000, distribution = "adaboost", nClass=2), "'n.trees' must be a positive integer") 

    expect_error(hpdegbm(X_train, Y_train,  nExecutor=4, n.trees= 1000, interaction.depth = -3, distribution = "adaboost", nClass=2), "'interaction.depth' must be a positive integer") 

    expect_error(hpdegbm(X_train, Y_train,  nExecutor=4, n.trees= 1000, interaction.depth = 3,  n.minobsinnode = -10, distribution = "adaboost", nClass=2), "'n.minobsinnode' must be a positive integer")
}) 


test_that("The inputs are validated for AdaBoost", {
    expect_error(hpdegbm(Y_train=Y_train, nExecutor=4, n.trees=1000, distribution = "adaboost", nClass=2), "'X_train' is a required argument")

    expect_error(hpdegbm(X_train=X_train,  nExecutor=4, n.trees=1000, distribution = "adaboost", nClass=2), "'Y_train' is a required argument")  
    
    expect_error(hpdegbm(X_train, Y_train, nExecutor=-4, Y_train, n.trees=1000,  distribution = "adaboost", nClass=2), " 'nExecutor' must be a positive integer") 
})


test_that("The bag.fraction is validated for AdaBoost", {
   expect_error(hpdegbm(X_train, Y_train,  nExecutor=4,  bag.fraction = -0.632, distribution = "adaboost", nClass=2), "'bag.fraction' must be a number in the interval \\(0,1\\]")                                        
})

test_that("The learning rate is validated for AdaBoost", {
    expect_error(hpdegbm(X_train, Y_train, nExecutor=4,  shrinkage=-0.50, distribution="adaboost", nClass=2), "'shrinkage' must be a number in the interval \\[0.001,1\\]")                                      
})



