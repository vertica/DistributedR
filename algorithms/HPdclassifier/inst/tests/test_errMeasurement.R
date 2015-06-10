library(HPdclassifier)

nInst <- sum(distributedR_status()$Inst)

# classification samples
obse_c_array1 <- c('t1','t2','t3','t4', 't2', 't4')
pred_c_array1 <- c('t1','t3','t3','t2', 't2', 't1')
pred_c_array2 <- c('t1','t5','t3','t2', 't2', 't4')
pred_c_array3 <- c('t1','t2','t3','t4')

obse_c_factor1 <- factor(obse_c_array1)
pred_c_factor1 <- factor(pred_c_array1)

obse_c_matrix1 <- matrix(obse_c_array1, ncol=1)
obse_c_matrix2 <- matrix(obse_c_array1, nrow=3)
pred_c_matrix1 <- matrix(pred_c_array1, ncol=1)
pred_c_matrix2 <- matrix(pred_c_array1, nrow=3)
pred_c_matrix3 <- matrix(pred_c_array2, ncol=1)
pred_c_matrix4 <- matrix(pred_c_array3, ncol=1)

obse_c_data.frame1 <- as.data.frame(obse_c_matrix1)
obse_c_data.frame2 <- as.data.frame(obse_c_matrix2)
pred_c_data.frame1 <- as.data.frame(pred_c_matrix1)
pred_c_data.frame2 <- as.data.frame(pred_c_matrix2)
pred_c_data.frame3 <- as.data.frame(pred_c_matrix3)
pred_c_data.frame4 <- as.data.frame(pred_c_matrix4)

obse_c_dframe1 <- as.dframe(obse_c_matrix1)
obse_c_dframe2 <- as.dframe(obse_c_matrix2)
pred_c_dframe1 <- as.dframe(pred_c_matrix1)
pred_c_dframe2 <- as.dframe(pred_c_matrix2)
pred_c_dframe3 <- as.dframe(pred_c_matrix3)
pred_c_dframe4 <- as.dframe(pred_c_matrix4)

# regression samples
obse_r_array1 <- c(10.1, 2.2, 30.5, 4.6, 50.7, 6.8)
pred_r_array1 <- c(11.1, 2.3, 32.5, 4.8, 40.7, 6.7)
pred_r_array2 <- c(11.1, 2.3, 32.5, 4.8)
obse_r_arrayNA <- c(10.1, NA, 30.5, 4.6, 50.7, 6.8)
pred_r_arrayNA <- c(11.1, NA, 32.5, 4.8, 40.7, 6.7)

obse_r_matrix1 <- matrix(obse_r_array1, ncol=1)
obse_r_matrix2 <- matrix(obse_r_array1, nrow=3)
pred_r_matrix1 <- matrix(pred_r_array1, ncol=1)
pred_r_matrix2 <- matrix(pred_r_array1, nrow=3)
pred_r_matrix3 <- matrix(pred_r_array2, ncol=1)
obse_r_matrixNA <- matrix(obse_r_arrayNA, ncol=1)
pred_r_matrixNA <- matrix(pred_r_arrayNA, ncol=1)

obse_r_data.frame1 <- data.frame(obse_r_matrix1)
obse_r_data.frame2 <- data.frame(obse_r_matrix2)
pred_r_data.frame1 <- data.frame(pred_r_matrix1)
pred_r_data.frame2 <- data.frame(pred_r_matrix2)
pred_r_data.frame3 <- data.frame(pred_r_matrix3)
obse_r_data.frameNA <- data.frame(obse_r_matrixNA)
pred_r_data.frameNA <- data.frame(pred_r_matrixNA)

obse_r_dframe1 <- as.dframe(obse_r_matrix1)
obse_r_dframe2 <- as.dframe(obse_r_matrix2)
pred_r_dframe1 <- as.dframe(pred_r_matrix1)
pred_r_dframe2 <- as.dframe(pred_r_matrix2)
pred_r_dframe3 <- as.dframe(pred_r_matrix3)
obse_r_dframeNA <- as.dframe(obse_r_matrixNA)
pred_r_dframeNA <- as.dframe(pred_r_matrixNA)

obse_r_darray1 <- as.darray(obse_r_matrix1)
obse_r_darray2 <- as.darray(obse_r_matrix2)
pred_r_darray1 <- as.darray(pred_r_matrix1)
pred_r_darray2 <- as.darray(pred_r_matrix2)
pred_r_darray3 <- as.darray(pred_r_matrix3)
obse_r_darrayNA <- as.darray(obse_r_matrixNA)
pred_r_darrayNA <- as.darray(pred_r_matrixNA)

# invalid types
samplelist <- list(obse_c_array1)
sampledlist <- dlist(nInst)
# different partitioning
obse_3p <- dframe(c(6,1),c(2,1))
pred_2p <- dframe(c(6,1),c(3,1))

########## Tests for confusionMatrix ##########
context("Checking invalid arguments for confusionMatrix")

test_that("confusionMatrix validates the type of the inputes", {
    # wrong type of predicted
    expect_error(confusionMatrix(obse_c_array1, samplelist))
    expect_error(confusionMatrix(obse_c_array1, sampledlist))
    expect_error(confusionMatrix(obse_c_array1, pred_r_array1))
    expect_error(confusionMatrix(obse_c_matrix1, pred_r_matrix1))
    expect_error(confusionMatrix(obse_c_data.frame1, pred_r_data.frame1))
    expect_error(confusionMatrix(obse_c_dframe1, pred_r_dframe1))
    expect_error(confusionMatrix(obse_c_array1, pred_r_darray1))
    expect_error(confusionMatrix(obse_c_matrix1, pred_c_dframe1))
    expect_error(confusionMatrix(obse_c_data.frame1, pred_c_dframe1))

    # wrong type of observed
    expect_error(confusionMatrix(samplelist, pred_c_array1))
    expect_error(confusionMatrix(sampledlist, pred_c_array1))   
    expect_error(confusionMatrix(obse_r_array1, pred_c_array1))
    expect_error(confusionMatrix(obse_r_matrix1, pred_c_array1))
    expect_error(confusionMatrix(obse_r_data.frame1, pred_c_array1))
    expect_error(confusionMatrix(obse_r_dframe1, pred_c_array1))
    expect_error(confusionMatrix(obse_r_darray1, pred_c_array1))
    expect_error(confusionMatrix(obse_c_dframe1, pred_c_array1))
})

test_that("confusionMatrix validates the size of the inputes", {
    # more than one column
    expect_error(confusionMatrix(obse_c_matrix2, pred_c_matrix1))
    expect_error(confusionMatrix(obse_c_matrix1, pred_c_matrix2))
    expect_error(confusionMatrix(obse_c_data.frame2, pred_c_data.frame1))
    expect_error(confusionMatrix(obse_c_data.frame1, pred_c_data.frame2))
    expect_error(confusionMatrix(obse_c_dframe2, pred_c_dframe1))
    expect_error(confusionMatrix(obse_c_dframe1, pred_c_dframe2))

    # the number of rows are not the same in observed and predicted
    expect_error(confusionMatrix(obse_c_array1, pred_c_array3))
    expect_error(confusionMatrix(obse_c_matrix1, pred_c_matrix4))
    expect_error(confusionMatrix(obse_c_data.frame1, pred_c_data.frame4))
    expect_error(confusionMatrix(obse_c_dframe1, pred_c_dframe4))

    # different partitioning
    expect_error(confusionMatrix(obse_3p, pred_2p))
})

test_that("confusionMatrix validates the case that there is a new category in predict", {
    expect_error(confusionMatrix(obse_c_array1, pred_c_array2))
    expect_error(confusionMatrix(obse_c_matrix1, pred_c_matrix3))
    expect_error(confusionMatrix(obse_c_data.frame1, pred_c_data.frame3))
    expect_error(confusionMatrix(obse_c_dframe1, pred_c_dframe3))
})

context("Checking the correct result of confusionMatrix")

test_that("confusionMatrix returns the correct result", {
    lables <- levels(obse_c_factor1)
    confM <- table(obse_c_factor1, factor(pred_c_array1, levels=lables))[lables,lables]

    expect_equivalent(confusionMatrix(obse_c_array1, pred_c_array1), confM)
    expect_equivalent(confusionMatrix(obse_c_array1, pred_c_matrix1), confM)
    expect_equivalent(confusionMatrix(obse_c_array1, pred_c_data.frame1), confM)

    expect_equivalent(confusionMatrix(obse_c_matrix1, pred_c_matrix1), confM)
    expect_equivalent(confusionMatrix(obse_c_data.frame1, pred_c_data.frame1), confM)
    expect_equivalent(confusionMatrix(obse_c_dframe1, pred_c_dframe1), confM)
})

########## Tests for errorRate ########## 
context("Checking invalid arguments for errorRate")
# the errorRate function relys on the confusionMatrix function for validating its inputs; nevertheless, it is still good to have independent tests
test_that("errorRate validates the type of the inputes", {
    # wrong type of predicted
    expect_error(errorRate(obse_c_array1, samplelist))
    expect_error(errorRate(obse_c_array1, sampledlist))
    expect_error(errorRate(obse_c_array1, pred_r_array1))
    expect_error(errorRate(obse_c_matrix1, pred_r_matrix1))
    expect_error(errorRate(obse_c_data.frame1, pred_r_data.frame1))
    expect_error(errorRate(obse_c_dframe1, pred_r_dframe1))
    expect_error(errorRate(obse_c_array1, pred_r_darray1))
    expect_error(errorRate(obse_c_matrix1, pred_c_dframe1))
    expect_error(errorRate(obse_c_data.frame1, pred_c_dframe1))

    # wrong type of observed
    expect_error(errorRate(samplelist, pred_c_array1))
    expect_error(errorRate(sampledlist, pred_c_array1))   
    expect_error(errorRate(obse_r_array1, pred_c_array1))
    expect_error(errorRate(obse_r_matrix1, pred_c_array1))
    expect_error(errorRate(obse_r_data.frame1, pred_c_array1))
    expect_error(errorRate(obse_r_dframe1, pred_c_array1))
    expect_error(errorRate(obse_r_darray1, pred_c_array1))
    expect_error(errorRate(obse_c_dframe1, pred_c_array1))
})

test_that("errorRate validates the size of the inputes", {
    # more than one column
    expect_error(errorRate(obse_c_matrix2, pred_c_matrix1))
    expect_error(errorRate(obse_c_matrix1, pred_c_matrix2))
    expect_error(errorRate(obse_c_data.frame2, pred_c_data.frame1))
    expect_error(errorRate(obse_c_data.frame1, pred_c_data.frame2))
    expect_error(errorRate(obse_c_dframe2, pred_c_dframe1))
    expect_error(errorRate(obse_c_dframe1, pred_c_dframe2))

    # the number of rows are not the same in observed and predicted
    expect_error(errorRate(obse_c_array1, pred_c_array3))
    expect_error(errorRate(obse_c_matrix1, pred_c_matrix4))
    expect_error(errorRate(obse_c_data.frame1, pred_c_data.frame4))
    expect_error(errorRate(obse_c_dframe1, pred_c_dframe4))

    # different partitioning
    expect_error(errorRate(obse_3p, pred_2p))
})

test_that("errorRate validates the case that there is a new category in predict", {
    expect_error(errorRate(obse_c_array1, pred_c_array2))
    expect_error(errorRate(obse_c_matrix1, pred_c_matrix3))
    expect_error(errorRate(obse_c_data.frame1, pred_c_data.frame3))
    expect_error(errorRate(obse_c_dframe1, pred_c_dframe3))
})

context("Checking the correct result of errorRate")

test_that("errorRate returns the correct result", {
    lables <- levels(obse_c_factor1)
    confM <- table(obse_c_factor1, factor(pred_c_array1, levels=lables))[lables,lables]
    class.error <- 1 - diag(confM)/rowSums(confM)
    nCorrectPrediction <- sum(diag(confM))
    nPredictions <- sum(confM)
    err.rate <- (nPredictions - nCorrectPrediction) / nPredictions
    names(err.rate) <- "err.rate"
    expectedArray <- c(err.rate, class.error)    

    expect_equivalent(errorRate(obse_c_array1, pred_c_array1), expectedArray)
    expect_equivalent(errorRate(obse_c_array1, pred_c_matrix1), expectedArray)
    expect_equivalent(errorRate(obse_c_array1, pred_c_data.frame1), expectedArray)

    expect_equivalent(errorRate(obse_c_matrix1, pred_c_matrix1), expectedArray)
    expect_equivalent(errorRate(obse_c_data.frame1, pred_c_data.frame1), expectedArray)
    expect_equivalent(errorRate(obse_c_dframe1, pred_c_dframe1), expectedArray)
})

########## Tests for meanSquared ##########
context("Checking invalid arguments for meanSquared")

test_that("meanSquared validates the type of the inputes", {
    # wrong type of observed
    expect_error(meanSquared(samplelist, pred_r_array1))
    expect_error(meanSquared(sampledlist, pred_r_array1))   
    expect_error(meanSquared(obse_c_array1, pred_r_array1))
    expect_error(meanSquared(obse_c_matrix1, pred_r_matrix1))
    expect_error(meanSquared(obse_c_array1, pred_r_darray1))
    expect_error(meanSquared(obse_r_matrix1, pred_r_dframe1))
    expect_error(meanSquared(obse_r_data.frame1, pred_r_dframe1))

    # wrong type of predicted
    expect_error(meanSquared(obse_r_array1, samplelist))
    expect_error(meanSquared(obse_r_array1, sampledlist))
    expect_error(meanSquared(obse_r_array1, pred_c_array1))
    expect_error(meanSquared(obse_r_matrix1, pred_c_matrix1))
    expect_error(meanSquared(obse_r_dframe1, pred_r_array1))
})

test_that("meanSquared validates the size of the inputes", {
    # more than one column
    expect_error(meanSquared(obse_r_matrix2, pred_r_matrix1))
    expect_error(meanSquared(obse_r_matrix1, pred_r_matrix2))
    expect_error(meanSquared(obse_r_data.frame2, pred_r_data.frame1))
    expect_error(meanSquared(obse_r_data.frame1, pred_r_data.frame2))
    expect_error(meanSquared(obse_r_dframe2, pred_r_dframe1))
    expect_error(meanSquared(obse_r_dframe1, pred_r_dframe2))
    expect_error(meanSquared(obse_r_darray2, pred_r_darray1))
    expect_error(meanSquared(obse_r_darray1, pred_r_darray2))

    # the number of rows are not the same in observed and predicted
    expect_error(meanSquared(obse_r_array1, pred_r_array2))
    expect_error(meanSquared(obse_r_matrix1, pred_r_matrix3))
    expect_error(meanSquared(obse_r_data.frame1, pred_r_data.frame3))
    expect_error(meanSquared(obse_r_dframe1, pred_r_dframe3))
    expect_error(meanSquared(obse_r_darray1, pred_r_darray3))

    # different partitioning
    expect_error(meanSquared(obse_3p, pred_2p))
})

context("Checking the correct result of meanSquared")

test_that("meanSquared returns the correct result", {
    # cases that it returns NA
    expect_true(is.na(meanSquared(obse_c_data.frame1, pred_r_data.frame1)))
    expect_true(is.na(meanSquared(obse_c_dframe1, pred_r_dframe1)))
    expect_true(is.na(meanSquared(obse_r_data.frame1, pred_c_data.frame1)))
    expect_true(is.na(meanSquared(obse_r_dframe1, pred_c_dframe1)))
    expect_true(is.na(meanSquared(obse_r_darray1, pred_c_dframe1)))

    expect_true(is.na(meanSquared(obse_r_arrayNA, pred_r_arrayNA)))
    expect_true(is.na(meanSquared(obse_r_arrayNA, pred_r_array1)))
    expect_true(is.na(meanSquared(obse_r_matrixNA, pred_r_matrixNA)))
    expect_true(is.na(meanSquared(obse_r_data.frameNA, pred_r_data.frameNA)))
    expect_true(is.na(meanSquared(obse_r_dframeNA, pred_r_dframeNA)))
    expect_true(is.na(meanSquared(obse_r_darrayNA, pred_r_darrayNA)))
    expect_true(is.na(meanSquared(obse_r_darrayNA, pred_r_darray1)))
    
    # the first, without NA
    expectedResult1 <- mean((obse_r_array1 - pred_r_array1) ^2)
    expect_equivalent(meanSquared(obse_r_array1, pred_r_array1), expectedResult1)
    expect_equivalent(meanSquared(obse_r_array1, pred_r_matrix1), expectedResult1)
    expect_equivalent(meanSquared(obse_r_array1, pred_r_data.frame1), expectedResult1)

    expect_equivalent(meanSquared(obse_r_matrix1, pred_r_matrix1), expectedResult1)
    expect_equivalent(meanSquared(obse_r_data.frame1, pred_r_data.frame1), expectedResult1)
    expect_equivalent(meanSquared(obse_r_dframe1, pred_r_dframe1), expectedResult1)
    expect_equivalent(meanSquared(obse_r_darray1, pred_r_darray1), expectedResult1)

    # the second with NA
    expectedResult2 <- mean((obse_r_arrayNA - pred_r_arrayNA) ^2, na.rm=TRUE)
    expect_equivalent(meanSquared(obse_r_arrayNA, pred_r_arrayNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_arrayNA, pred_r_matrixNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_arrayNA, pred_r_data.frameNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_arrayNA, pred_r_array1, na.rm=TRUE), expectedResult2)

    expect_equivalent(meanSquared(obse_r_matrixNA, pred_r_matrixNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_data.frameNA, pred_r_data.frameNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_dframeNA, pred_r_dframeNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_darrayNA, pred_r_darrayNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(meanSquared(obse_r_darrayNA, pred_r_darray1, na.rm=TRUE), expectedResult2)
})

########## Tests for rSquared ##########
context("Checking invalid arguments for rSquared")
# the rSquared function relys on the meanSquared function for validating its inputs; nevertheless, it is still good to have independent tests
test_that("rSquared validates the type of the inputes", {
    # wrong type of observed
    expect_error(rSquared(samplelist, pred_r_array1))
    expect_error(rSquared(sampledlist, pred_r_array1))   
    expect_error(rSquared(obse_c_array1, pred_r_array1))
    expect_error(rSquared(obse_c_matrix1, pred_r_matrix1))
    expect_error(rSquared(obse_c_array1, pred_r_darray1))
    expect_error(rSquared(obse_r_matrix1, pred_r_dframe1))
    expect_error(rSquared(obse_r_data.frame1, pred_r_dframe1))

    # wrong type of predicted
    expect_error(rSquared(obse_r_array1, samplelist))
    expect_error(rSquared(obse_r_array1, sampledlist))
    expect_error(rSquared(obse_r_array1, pred_c_array1))
    expect_error(rSquared(obse_r_matrix1, pred_c_matrix1))
    expect_error(rSquared(obse_r_dframe1, pred_r_array1))
})

test_that("rSquared validates the size of the inputes", {
    # more than one column
    expect_error(rSquared(obse_r_matrix2, pred_r_matrix1))
    expect_error(rSquared(obse_r_matrix1, pred_r_matrix2))
    expect_error(rSquared(obse_r_data.frame2, pred_r_data.frame1))
    expect_error(rSquared(obse_r_data.frame1, pred_r_data.frame2))
    expect_error(rSquared(obse_r_dframe2, pred_r_dframe1))
    expect_error(rSquared(obse_r_dframe1, pred_r_dframe2))
    expect_error(rSquared(obse_r_darray2, pred_r_darray1))
    expect_error(rSquared(obse_r_darray1, pred_r_darray2))

    # the number of rows are not the same in observed and predicted
    expect_error(rSquared(obse_r_array1, pred_r_array2))
    expect_error(rSquared(obse_r_matrix1, pred_r_matrix3))
    expect_error(rSquared(obse_r_data.frame1, pred_r_data.frame3))
    expect_error(rSquared(obse_r_dframe1, pred_r_dframe3))
    expect_error(rSquared(obse_r_darray1, pred_r_darray3))

    # different partitioning
    expect_error(rSquared(obse_3p, pred_2p))
})

context("Checking the correct result of rSquared")

test_that("rSquared returns the correct result", {
    # cases that it returns NA
    expect_true(is.na(rSquared(obse_c_data.frame1, pred_r_data.frame1)))
    expect_true(is.na(rSquared(obse_r_data.frame1, pred_c_data.frame1)))
    expect_true(is.na(rSquared(obse_r_dframe1, pred_c_dframe1)))
    expect_true(is.na(rSquared(obse_r_darray1, pred_c_dframe1)))
    expect_true(is.na(rSquared(obse_c_dframe1, pred_r_dframe1)))

    expect_true(is.na(rSquared(obse_r_arrayNA, pred_r_arrayNA)))
    expect_true(is.na(rSquared(obse_r_arrayNA, pred_r_array1)))
    expect_true(is.na(rSquared(obse_r_matrixNA, pred_r_matrixNA)))
    expect_true(is.na(rSquared(obse_r_data.frameNA, pred_r_data.frameNA)))
    expect_true(is.na(rSquared(obse_r_dframeNA, pred_r_dframeNA)))
    expect_true(is.na(rSquared(obse_r_darrayNA, pred_r_darrayNA)))
    expect_true(is.na(rSquared(obse_r_darrayNA, pred_r_darray1)))
    
    # the first, without NA
    expectedResult1 <- 1 - mean((obse_r_array1 - pred_r_array1) ^2) / var(obse_r_array1)
    expect_equivalent(rSquared(obse_r_array1, pred_r_array1), expectedResult1)
    expect_equivalent(rSquared(obse_r_array1, pred_r_matrix1), expectedResult1)
    expect_equivalent(rSquared(obse_r_array1, pred_r_data.frame1), expectedResult1)

    expect_equivalent(rSquared(obse_r_matrix1, pred_r_matrix1), expectedResult1)
    expect_equivalent(rSquared(obse_r_data.frame1, pred_r_data.frame1), expectedResult1)
    expect_equivalent(rSquared(obse_r_dframe1, pred_r_dframe1), expectedResult1)
    expect_equivalent(rSquared(obse_r_darray1, pred_r_darray1), expectedResult1)

    # the second with NA
    expectedResult2 <- 1 - mean((obse_r_arrayNA - pred_r_arrayNA) ^2, na.rm=TRUE) / var(obse_r_arrayNA, na.rm=TRUE)
    expect_equivalent(rSquared(obse_r_arrayNA, pred_r_arrayNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_arrayNA, pred_r_matrixNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_arrayNA, pred_r_data.frameNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_arrayNA, pred_r_array1, na.rm=TRUE), expectedResult2)

    expect_equivalent(rSquared(obse_r_matrixNA, pred_r_matrixNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_data.frameNA, pred_r_data.frameNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_dframeNA, pred_r_dframeNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_darrayNA, pred_r_darrayNA, na.rm=TRUE), expectedResult2)
    expect_equivalent(rSquared(obse_r_darrayNA, pred_r_darray1, na.rm=TRUE), expectedResult2)
})

