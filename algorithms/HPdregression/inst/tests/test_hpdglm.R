library(HPdregression)

nInst <- sum(distributedR_status()$Inst)

########## General Tests for input validation ##########
context("Checking the input validation in hpdglm")

 Y_correct <- as.darray (data.matrix(airquality[1]))
 X_correct <- as.darray (data.matrix(airquality[-1]))

test_that("the inputes are validated", {
    Y_wrong <- as.darray (data.matrix(airquality[1]), blocks=c(ceiling(nrow(airquality)/2), 1) )
    X_wrong1 <- as.darray (data.matrix(airquality[-1]),blocks=c(ceiling(nrow(airquality)/2)+1, ncol(airquality[-1])) )
    X_wrong2 <- as.darray (data.matrix(airquality[-1]),blocks=c(nrow(airquality), ceiling(ncol(airquality[-1])/2)) )

    expect_error(hpdglm(Y_wrong, X_wrong1))
    expect_error(hpdglm(Y_wrong, X_wrong2))
    expect_error(hpdglm(X_correct, Y_correct))
    expect_error(hpdglm(Y_correct, X_correct, family = "myFamily"))
    expect_error(hpdglm(Y_correct, X_correct, weights = matrix(1,nrow=nrow(airquality)) ))
    expect_error(hpdglm(Y_correct, X_correct, weights = as.darray(matrix(1,nrow=nrow(airquality)+1)) ))
    expect_error(hpdglm(Y_correct, X_correct, na_action = "something"))
    expect_error(hpdglm(Y_correct, X_correct, start = rep(1,ncol(airquality[-1])) ))
    expect_error(hpdglm(Y_correct, X_correct, etastart = matrix(1,nrow=nrow(airquality)) ))
    expect_error(hpdglm(Y_correct, X_correct, etastart = as.darray(matrix(1,nrow=nrow(airquality)+1)) ))
    expect_error(hpdglm(Y_correct, X_correct, mustart = matrix(1,nrow=nrow(airquality)) ))
    expect_error(hpdglm(Y_correct, X_correct, mustart = as.darray(matrix(1,nrow=nrow(airquality)+1)) ))
    expect_error(hpdglm(Y_correct, X_correct, offset = matrix(1,nrow=nrow(airquality)) ))
    expect_error(hpdglm(Y_correct, X_correct, offset = as.darray(matrix(1,nrow=nrow(airquality)+1)) ))
    expect_error(hpdglm(Y_correct, X_correct, control = list(epsilon = 1e-9, maxit = 0) ))
    expect_error(hpdglm(Y_correct, X_correct, method = "hpdglm.Arash" ))
    expect_error(hpdglm(Y_correct, X_correct, completeModel = "Yes" ))
})


########## Tests for Linear Regression ##########
context("Checking the accuracy of hpdglm for linear regression")

  rglm <- glm(Ozone ~ ., data=airquality)
  hglm <- hpdglm(Y_correct, X_correct, method = "hpdglm.fit.Newton")

test_that("it generates the correct results in incomplete mode", {

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_true(is.na(hglm$aic))
    expect_true(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(hglm$coefficients[,1], rglm$coefficients)
})

  hglm <- hpdglm(Y_correct, X_correct, completeModel=TRUE, method = "hpdglm.fit.Newton")
test_that("it generates the correct results in complete mode", {

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_false(is.na(hglm$aic))
    expect_false(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$predictors))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_false(is.null(hglm$d.residuals))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(hglm$coefficients[,1], rglm$coefficients)
})

test_that("the predict function works properly", {
    expect_equivalent(predict(hglm, newdata=as.matrix(airquality[1,-1],nrow=1), type="response", trace=FALSE)[1], predict(rglm, newdata=airquality[1,], type="response"))
})

########## Tests for Logestic Regression ##########
context("Checking the accuracy of hpdglm for logistic regression")

  Y.am <- as.darray(data.matrix(mtcars["am"]))
  X.hp_wt <- as.darray(data.matrix(mtcars[c("hp","wt")]))

  rglm <- glm(am~hp+wt, data=mtcars, family=binomial)

test_that("it generates the correct results in incomplete mode", {
    expect_error(hpdglm(Y_correct, X_correct, family=binomial)) # responses must be binary for logistic regression

    hglm <- hpdglm(Y.am, X.hp_wt, family=binomial, method = "hpdglm.fit.Newton")

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_true(is.na(hglm$aic))
    expect_true(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(hglm$coefficients[,1], rglm$coefficients)
})


  hglm <- hpdglm(Y.am, X.hp_wt, family=binomial, completeModel=TRUE, method = "hpdglm.fit.Newton")

test_that("it generates the correct results in complete mode", {

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_false(is.na(hglm$aic))
    expect_false(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$predictors))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_false(is.null(hglm$d.residuals))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(coef(hglm)[,1], coef(rglm))
    expect_equivalent(getpartition(residuals(hglm))[,1], residuals(rglm))
})

test_that("the predict and verification functions work properly", {
    expect_equivalent(predict(hglm, newdata=as.matrix(mtcars[1,c(4,6)],nrow=1), type="response", trace=FALSE)[1], predict(rglm, newdata=mtcars[1,], type="response"))

    set.seed(10)
    vhglm <- v.hpdglm(Y.am, X.hp_wt, hglm, percent = 10)
    expect_true( all(vhglm$delta < 0.3) ) # based on one experiment
    expect_false(is.null(vhglm$call))
    expect_false(is.null(vhglm$percent))
    expect_false(is.null(vhglm$seed))
    set.seed(10)
    cvhglm <- cv.hpdglm(Y.am, X.hp_wt, hglm)
    expect_true( all(cvhglm$delta < 0.1)) # based on one experiment
    expect_false(is.null(cvhglm$call))
    expect_false(is.null(cvhglm$K))
    expect_false(is.null(cvhglm$seed))
})

########## Tests for Poisson Regression ##########
context("Checking the accuracy of hpdglm for poisson regression")

  Y.carb <- as.darray(data.matrix(mtcars[11]))
  X.rest <- as.darray(data.matrix(mtcars[-11]))
  rglm <- glm(carb ~ ., data=mtcars, family=poisson)

test_that("it generates the correct results in incomplete mode", {

    hglm <- hpdglm(Y.carb, X.rest, family=poisson, method = "hpdglm.fit.Newton")

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_true(is.na(hglm$aic))
    expect_true(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(coef(hglm)[,1], coef(rglm))
})


  hglm <- hpdglm(Y.carb, X.rest, family=poisson, completeModel=TRUE, method = "hpdglm.fit.Newton")

test_that("it generates the correct results in complete mode", {

    expect_false(is.null(hglm$coefficients))
    expect_false(is.null(hglm$d.fitted.values))
    expect_false(is.null(hglm$family))
    expect_false(is.null(hglm$d.linear.predictors))
    expect_false(is.null(hglm$deviance))
    expect_false(is.na(hglm$aic))
    expect_false(is.na(hglm$null.deviance))
    expect_false(is.null(hglm$iter))
    expect_false(is.null(hglm$weights))
    expect_false(is.null(hglm$prior.weights))
    expect_false(is.null(hglm$df.residual))
    expect_false(is.null(hglm$df.null))
    expect_false(is.null(hglm$converged))
    expect_false(is.null(hglm$boundary))
    expect_false(is.null(hglm$responses))
    expect_false(is.null(hglm$predictors))
    expect_false(is.null(hglm$call))
    expect_false(is.null(hglm$control))
    expect_false(is.null(hglm$method))

    expect_false(is.null(hglm$d.residuals))

    expect_is(hglm, "hpdglm")
    expect_is(hglm, "glm")

    expect_equal(coef(hglm)[,1], coef(rglm))
    expect_equivalent(getpartition(residuals(hglm))[,1], residuals(rglm))
})

test_that("the predict and verification functions work properly", {
    expect_equivalent(predict(hglm, newdata=as.matrix(mtcars[1,-11],nrow=1), type="response", trace=FALSE)[1], predict(rglm, newdata=mtcars[1,], type="response"))

    set.seed(10)
    vhglm <- v.hpdglm(Y.carb, X.rest, hglm, percent = 10)
    expect_true( all(vhglm$delta < 0.2) )
    expect_false(is.null(vhglm$call))
    expect_false(is.null(vhglm$percent))
    expect_false(is.null(vhglm$seed))
#    set.seed(10)
#    cvhglm <- cv.hpdglm(Y.carb, X.rest, hglm)
#    expect_true( all(cvhglm$delta < 2))
#    expect_false(is.null(cvhglm$call))
#    expect_false(is.null(cvhglm$K))
#    expect_false(is.null(cvhglm$seed))
})

