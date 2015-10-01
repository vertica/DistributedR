library(HPdclassifier)
library(HPdutility)
set.seed(2)

context("Invalid Inputs to Variable Importance")
model <- hpdrpart(Species~.,data = iris, completeModel = TRUE)
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
varImp.data.frame <- varImportance(model, xtest, ytest,type = "class")
varImp.dframe <- varImportance(model, as.dframe(xtest), as.dframe(ytest), type = "class")


expect_equal(nrow(model$variable.importance),ncol(xtest))
expect_equal(nrow(varImp.data.frame),ncol(xtest))
expect_true(any(varImp.data.frame > 0))
expect_equal(nrow(varImp.dframe),ncol(xtest))
expect_true(any(varImp.dframe > 0))



#generateData is a function that will output a dframe with nrow, ncol, npartitions
#The columns of the dframe will be either categorical or numerical 
#depending upon the value of features_categorical data
#The response variable will be either categorical or numerical 
#depending upon the value of the response_categorical

#The features are numbers from 1:3 sampled randomly 
#The output is the max of the features
#The features are then cast as categories if necessary
#The response is then cast as a category if necessary

#This means that using generateData we can have regression, binary, multiclass testing
#For regression testing, response_categorical = FALSE
#For binary testing, response_categorical = FALSE, ncol = 2
#For multiclass testing, response_categorical = FALSE, ncol > 2

#Output dframe has columns X1:X<ncol> with X1 being response
nInst <- sum(distributedR_status()$Inst)
generateData <- function(nrow, ncol, 
	     features_categorical, response_categorical, 
	     npartitions = nInst)
{

	features_cardinality = 3
	response_cardinality = features_cardinality

	observations = lapply(1:ncol, 
	     function(i) sample.int(features_cardinality,nrow,replace=TRUE))
	observations = do.call(cbind,observations)
	observations = matrix(as.integer(observations),ncol=ncol)
	observations = data.frame(observations)

	responses = apply(observations,1,max)
	responses = as.numeric(responses)
	responses = data.frame(responses)

	indices = as.darray(matrix(1:nrow,ncol = 1))

	data = dframe(npartitions = npartitions)
	foreach(i,1:npartitions,function(
	data = splits(data,i),
	observations = observations[getpartition(indices,i),],
	responses = responses[getpartition(indices,i),],
	ncol = ncol(observations),
	response_categorical = response_categorical,
	features_categorical = features_categorical)
	{
		observations_dataframe = observations
		responses_dataframe = data.frame(matrix(responses,ncol=1))

		if(response_categorical)
		responses_dataframe[,1] = as.factor(responses_dataframe[,1])

		if(features_categorical)
		for(i in 1:ncol(observations_dataframe))
		      observations_dataframe[,i] = 
		      as.factor(observations_dataframe[,i])

		data = cbind(responses_dataframe, observations_dataframe)
		update(data)
	},progress = FALSE)
	rm(observations)
	rm(responses)
	colnames(data) <- paste("X",1:ncol(data),sep="")

	return(data)
}

library(rpart)
context("Testing variable importance with rpart model")
test_that("testing with classification trees", {

set.seed(1)
data = generateData(100,2,TRUE,TRUE)
data = getpartition(data)

model <- rpart(X1 ~ ., data = data)
variable.importance = varImportance(model, data, data.frame(data$X1),type = "class")
expect_true(variable.importance[1,] > 0)
expect_true(variable.importance[2,] > 0)
})

test_that("testing with regression trees", {

set.seed(1)
data = generateData(100,2,TRUE,FALSE)
data = getpartition(data)

model <- rpart(X1 ~ ., data = data)
set.seed(1)
variable.importance1 = varImportance(model, data, data.frame(data$X1))
set.seed(1)
variable.importance2 = varImportance(model, data, data.frame(data$X1))

expect_equal(variable.importance1,variable.importance2)

expect_true(variable.importance1[1,] > 0)
expect_true(variable.importance1[2,] > 0)

})


library(randomForest)
context("Testing variable importance with randomForest model")
test_that("testing with classification trees", {

set.seed(1)
data = generateData(100,2,TRUE,TRUE)
data = getpartition(data)

model <- randomForest(formula = X1 ~ ., data = data, ntree = 10)
variable.importance = varImportance(model, data, data.frame(data$X1))
expect_true(variable.importance[1,] > 0)
expect_true(variable.importance[2,] > 0)
})

test_that("testing with regression trees", {

set.seed(1)
data = generateData(100,2,TRUE,FALSE)
data = getpartition(data)

model <- randomForest(formula = X1 ~ ., data = data, ntree = 10)
variable.importance = varImportance(model, data, data.frame(data$X1))
expect_true(variable.importance[1,] > 0)
expect_true(variable.importance[2,] > 0)

})

