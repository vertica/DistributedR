library(HPdclassifier)

nInst <- sum(distributedR_status()$Inst)

########## Tests for formula interface ##########
# The input is combination of a formula and a data.frame or dframe

### Tests for data.frame ###
context("Checking the formula interface of hpdRF_parallelForest with data.frame")

test_that("the inputes are validated", {
    expect_error(hpdRF_parallelForest(Species ~ ., data=iris, nExecutor = nInst, ntree=-1))
    expect_error(hpdRF_parallelForest(Species ~ ., airquality, nExecutor = nInst))
    expect_error(hpdRF_parallelForest(Ozone ~ ., data=airquality, nExecutor=nInst)) # airquality contains NA
    expect_error(hpdRF_parallelForest(Ozone ~ ., data=airquality, nExecutor=nInst, na.action=na.pass))
})

test_that("the results are returned correctly", {
    hRF <- hpdRF_parallelForest(Species ~ ., data=iris, nExecutor = nInst)
    rRF <- randomForest(Species ~ ., data=iris)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$terms))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_identical(hRF$classes, rRF$classes)
    expect_is(hRF, "hpdrandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, iris[-5])
    rpredicted <- predict(rRF, iris[-5])
    herrRate <- errorRate(iris[5], hpredicted)
    rerrRate <- errorRate(iris[5], rpredicted)

    expect_equivalent(herrRate, rerrRate)

    # complete mode
    expect_warning(hRFc <- hpdRF_parallelForest(Species ~ ., data=iris, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=iris[-5], ytest=iris[5]))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$votes))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))

    # handling missed data
    hRFm <- hpdRF_parallelForest(Ozone ~ ., data=airquality, nExecutor = nInst, na.action=na.omit, completeModel=TRUE)
    rRFm <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit)
    
    expect_identical(length(hRFm$y), length(rRFm$y))
})

### Tests for dframe ###
context("Checking the formula interface of hpdRF_parallelForest with dframe")
    Diris <- as.dframe(iris)
    Xiris <- as.dframe(iris[-5])
    Yiris <- as.dframe(iris[5])
    Dairquality <- as.dframe(airquality)

test_that("the inputes are validated", {
    expect_error(hpdRF_parallelForest(Species ~ ., data=Diris, nExecutor = nInst, ntree=-1))
    expect_error(hpdRF_parallelForest(Species ~ ., Dairquality, nExecutor = nInst))
    expect_error(hpdRF_parallelForest(Ozone ~ ., data=Dairquality, nExecutor=nInst))
    expect_error(hpdRF_parallelForest(Ozone ~ ., data=Dairquality, nExecutor=nInst, na.action=na.pass))
})

test_that("the results are returned correctly", {
    hRF <- hpdRF_parallelForest(Species ~ ., data=Diris, nExecutor = nInst)
    rRF <- randomForest(Species ~ ., data=iris)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$terms))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_identical(hRF$classes, rRF$classes)
    expect_is(hRF, "hpdrandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, Xiris)
    rpredicted <- predict(rRF, iris[-5])
    herrRate <- errorRate(Yiris, hpredicted)
    rerrRate <- errorRate(iris[5], rpredicted)

    expect_equivalent(herrRate, rerrRate)

    # complete mode
    expect_warning(hRFc <- hpdRF_parallelForest(Species ~ ., data=Diris, nExecutor = nInst, completeModel=TRUE, proximity=TRUE))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$votes))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))

    # handling missed data
    hRFm <- hpdRF_parallelForest(Ozone ~ ., data=Dairquality, nExecutor = nInst, na.action=na.omit, completeModel=TRUE)
    rRFm <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit)
    
    expect_identical(length(hRFm$y), length(rRFm$y))
})

########## Tests for default interface ##########
# There are 2 main different scenarios for the inputs that are not available through formula: the input data is an ordinary R object, or it is darray

context("Checking the default interface of hpdRF_parallelForest with ordibary R objects")

cleandata <- na.omit(airquality)
x <- cleandata[-1]
y <- cleandata[1]

test_that("the inputes are validated", {
    expect_error(hpdRF_parallelForest(x=x, y=y, nExecutor = nInst, ntree=-1))
    expect_error(hpdRF_parallelForest(x=airquality[-1], y=airquality[1], nExecutor = nInst)) # airquality contains NA
    expect_error(hpdRF_parallelForest(x=x, y=y, nExecutor=100, ntree=50))
})

test_that("the results are returned correctly for regression", {
    hRF <- hpdRF_parallelForest(x=x, y=y, nExecutor = nInst, ntree=5000)
    rRF <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit, ntree=5000)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$mse))
    expect_false(is.null(hRF$rsq))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_is(hRF, "hpdrandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, x)
    rpredicted <- predict(rRF, x)
    hrsq <- rSquared(y, hpredicted)
    rrsq <- rSquared(y, rpredicted)

    expect_true( abs(hrsq - rrsq)/rrsq < 0.01)

    # complete mode
    expect_warning(hRFc <- hpdRF_parallelForest(x=x, y=y, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=x, ytest=y))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))

})

test_that("the results are returned correctly for unsupervised", {
    expect_warning(hRFc <- hpdRF_parallelForest(x=x, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, ntree=501, mtry=3))

    # availability of some outputs
    expect_false(is.null(hRFc$call))
    expect_false(is.null(hRFc$importance))
    expect_false(is.null(hRFc$proximity))

    # the value of some outputs
    expect_identical(hRFc$ntree, 501)
    expect_identical(hRFc$type, "unsupervised")
    expect_identical(hRFc$mtry, 3)
    expect_is(hRFc, "hpdrandomForest")
    expect_is(hRFc, "randomForest")
})

context("Checking the default interface of hpdRF_parallelForest with darray")

X <- as.darray(data.matrix(x))
Y <- as.darray(data.matrix(y))

test_that("the inputes are validated", {
    expect_error(hpdRF_parallelForest(x=X, y=Y, nExecutor = nInst, ntree=-1))
    expect_error(hpdRF_parallelForest(x=X, y=Y, nExecutor=100, ntree=50))
})

test_that("the results are returned correctly", {
    hRF <- hpdRF_parallelForest(x=X, y=Y, nExecutor = nInst, ntree=5000)
    rRF <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit, ntree=5000)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$mse))
    expect_false(is.null(hRF$rsq))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_is(hRF, "hpdrandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, x)
    rpredicted <- predict(rRF, x)
    hrsq <- rSquared(y, hpredicted)
    rrsq <- rSquared(y, rpredicted)

    expect_true( abs(hrsq - rrsq)/rrsq < 0.01)

    # complete mode
    expect_warning(hRFc <- hpdRF_parallelForest(x=x, y=y, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=x, ytest=y))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))
})

test_that("the results are returned correctly for unsupervised", {
    expect_warning(hRFc <- hpdRF_parallelForest(x=X, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, ntree=501, mtry=3))

    # availability of some outputs
    expect_false(is.null(hRFc$call))
    expect_false(is.null(hRFc$importance))
    expect_false(is.null(hRFc$proximity))

    # the value of some outputs
    expect_identical(hRFc$ntree, 501)
    expect_identical(hRFc$type, "unsupervised")
    expect_identical(hRFc$mtry, 3)
    expect_is(hRFc, "hpdrandomForest")
    expect_is(hRFc, "randomForest")
})

context("Checking effectiveness of set.seed")

test_that("setting seed must make the output of the function deterministic", {
    DFdata <- dframe(npartitions=nInst)
    foreach(i, 1:npartitions(DFdata), function(di=splits(DFdata, i)) {
        di <- data.frame(matrix(sample.int(1e6, 50), nrow=10, ncol=5))
        update(di)
    })
    colnames(DFdata) <- c("X1", "X2", "X3", "X4", "X5")

    set.seed(100)
    model1 <- hpdRF_parallelForest(X1~., data=DFdata)

    set.seed(100)
    model2 <- hpdRF_parallelForest(X1~., data=DFdata)

    expect_true(all(model1$mse == model2$mse))
})

