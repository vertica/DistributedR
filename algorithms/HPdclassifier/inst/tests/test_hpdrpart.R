library(HPdclassifier)
library(rpart)
nInst <- sum(distributedR_status()$Inst)


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

data = generateData(100,2,TRUE,TRUE)
context("Invalid inputs to hpdrpart")

test_that("testing formula parameter", {
expect_error(hpdrpart(data = data),
				   "'formula' is a required argument")
expect_error(hpdrpart(1,data = data),
				   "'formula' is not of class formula")
expect_error(hpdrpart(X4 ~ .,data = data,do.trace = TRUE),
				   "unable to apply formula to 'data'")
expect_error(hpdrpart(X1 ~ X2 + X3 + X4,data = data),
				   "unable to apply formula to 'data'")

})

test_that("testing data parameter", {
expect_error(hpdrpart(X1 ~ .),
				   "'data' is a required argument")
expect_error(hpdrpart(X1 ~ .,data = matrix(1:9,3,3)),
				   "'data' must be a dframe or data.frame")
expect_error(hpdrpart(X1 ~ .,data = dframe(npartitions = c(1,2))),
				   "'data' must have a positive number of rows")

})

test_that("testing nBins parameter", {
expect_error(hpdrpart(X1 ~ .,data = data, nBins = -1),
				   "'nBins' must be more than 0")
expect_error(hpdrpart(X1 ~ .,data = data, nBins = 0),
				   "'nBins' must be more than 0")

})

test_that("testing na.action parameter", {
expect_error(hpdrpart(X1 ~ .,data = data, na.action = 0),
				   "'na.action' must be either na.exclude, na.omit, na.fail")

})


test_that("testing na.action parameter", {
expect_warning(hpdrpart(X1 ~ .,data = data, subset = 0),
				   "'subset' not implemented. Adjust using weights parameter")

})


context("Invalid Inputs to Predict Function of hpdrpart")
model <- hpdrpart(X1 ~ ., data = data)
test_that("invalid newdata input to predict function",{
expect_error(predict(model), "'newdata' is a required argument")
expect_error(predict(model,newdata = matrix(1:9,3,3)),
				   "'newdata' must be a dframe or data.frame")
expect_error(predict(model,newdata = dframe(npartitions = c(1,2))),
				   "'newdata' must have a positive number of rows")
})


context("Validating Outputs of Training Function for hpdrpart")
test_that("testing output for categorical trees", {
data = generateData(100,2,TRUE,TRUE)
model <- hpdrpart(X1 ~ ., data = data, completeModel = TRUE)

expect_true("frame" %in% names(model))
expect_true("splits" %in% names(model))
expect_true("csplit" %in% names(model))
expect_true("call" %in% names(model))
expect_true("terms" %in% names(model))
expect_true("variable.importance" %in% names(model))
expect_true("na.action" %in% names(model))
expect_true("control" %in% names(model))

})

test_that("testing output for numerical trees", {
data = generateData(100,2,FALSE,FALSE)
model <- hpdrpart(X1 ~ ., data = data, completeModel = TRUE)

expect_true("frame" %in% names(model))
expect_true("splits" %in% names(model))
expect_true("call" %in% names(model))
expect_true("terms" %in% names(model))
expect_true("variable.importance" %in% names(model))
expect_true("na.action" %in% names(model))
expect_true("control" %in% names(model))

})



context("Accuracy Results for Training Simple Models for hpdrpart") 
test_that("basic training with categorical features and categorical output", {
#Building a classification tree with categorical variables
set.seed(1)
data = generateData(1000,2,TRUE,TRUE)
model <- hpdrpart(X1 ~ ., data = data)
data <- generateData(1000,2,TRUE,TRUE)
predictions = predict(model, data, type = "class")
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

test_that("basic training with numeric features and categorical output", {
#Building a classification tree with numeric features
set.seed(1)
data = generateData(1000,2,FALSE,TRUE)
model <- hpdrpart(X1 ~ ., data = data)
data <- generateData(1000,2,FALSE,TRUE)
predictions = predict(model, data,type = "class")
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

test_that("basic training with categorical features and numeric output", {
#Building a regression tree from categorical variables
set.seed(1)
data = generateData(1000,2,TRUE,FALSE)
model <- hpdrpart(X1 ~ ., data = data)
data <-generateData(1000,2,TRUE,FALSE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

test_that("basic training with numeric features and numeric output", {
#Building a regression tree from numeric features
set.seed(1)
data = generateData(1000,2,FALSE,FALSE)
model <- hpdrpart(X1 ~ ., data = data)
data <- generateData(1000,2,FALSE,FALSE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

context("Testing deploy.hpdrpart function")
expect_no_error <- function(x) {
expect_that(x,not(throws_error()))
expect_that(x,not(gives_warning()))
}

test_that("testing valid rpart model", {
model <- rpart(Species ~ ., iris)
expect_no_error(deploy.hpdrpart(model))
})

test_that("testing invalid rpart models", {
model <- rpart(Species ~ ., iris)
model$frame <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model' does not have an element called 'frame'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$var <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'var'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$n <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'n'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$wt <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'wt'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$dev <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'dev'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$yval <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'yval'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$frame$complexity <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model$frame' does not have an element called 'complexity'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- NULL
expect_error(deploy.hpdrpart(model), 
	"'model' does not have an element called 'splits'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- model$splits[,colnames(model$splits) != "count"] 
expect_error(deploy.hpdrpart(model), 
	"'model$splits' does not have column 'count'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- model$splits[,colnames(model$splits) != "ncat"] 
expect_error(deploy.hpdrpart(model), 
	"'model$splits' does not have column 'ncat'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- model$splits[,colnames(model$splits) != "improve"] 
expect_error(deploy.hpdrpart(model), 
	"'model$splits' does not have column 'improve'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- model$splits[,colnames(model$splits) != "index"] 
expect_error(deploy.hpdrpart(model), 
	"'model$splits' does not have column 'index'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$splits <- model$splits[,colnames(model$splits) != "adj"] 
expect_error(deploy.hpdrpart(model), 
	"'model$splits' does not have column 'adj'",fixed = TRUE)

model <- rpart(Species ~ ., iris)
model$csplit <- NULL
expect_warning(deploy.hpdrpart(model), 
	"'model' does not have an element called 'csplit'")

})