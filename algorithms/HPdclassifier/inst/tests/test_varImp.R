library(HPdclassifier)
library(rpart)
set.seed(2)

context("Invalid Inputs to Variable Importance")
model <- hpdrpart(Species~.,data = iris)
expect_error(varImportance(model, iris,iris$Species),
				   "'ytest' must be a dframe or data.frame")
expect_error(varImportance(model, as.dframe(iris),data.frame(iris$Species)),
				   "'xtest' and 'ytest' must both be either a dframe or data.frame")
expect_error(varImportance(model, iris[-1,],data.frame(iris$Species)),
				   "'xtest' and 'ytest' must have same number of rows")
expect_error(varImportance(model, iris,iris),
				   "'ytest' must have exactly one column")




context("Valid Inputs to Variable Importance")
xtest = iris
xtest$Species = NULL
ytest = data.frame(iris$Species)
varImp.data.frame <- varImportance(model, xtest, ytest)
varImp.dframe <- varImportance(model, as.dframe(xtest), as.dframe(ytest))


expect_equal(nrow(model$variable.importance),ncol(xtest)+1)
expect_equal(nrow(varImp.data.frame),ncol(xtest))
expect_true(any(varImp.data.frame > 0))
expect_equal(nrow(varImp.dframe),ncol(xtest))
expect_true(any(varImp.dframe > 0))
