library(HPdclassifier)

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

data = generateData(100,2,TRUE,TRUE)


context("Invalid Inputs to Training Function")
test_that("testing formula parameter", {
expect_error(hpdRF_parallelTree(data = data),
				   "'formula' is a required argument")
expect_error(hpdRF_parallelTree(1,data = data),
				   "'formula' is not of class formula")
expect_error(hpdRF_parallelTree(X4 ~ .,data = data),
				   "unable to apply formula to 'data'")
expect_error(hpdRF_parallelTree(X1 ~ X2 + X3 + X4,data = data),
				   "unable to apply formula to 'data'")

})

test_that("testing data parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .),
				   "'data' is a required argument")
expect_error(hpdRF_parallelTree(X1 ~ .,data = matrix(1:9,3,3)),
				   "'data' must be a dframe or data.frame")
expect_error(hpdRF_parallelTree(X1 ~ .,data = dframe(npartitions = c(1,2))),
				   "'data' must be partitioned rowise")

})

test_that("testing ntree parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, ntree = -1),
				   "'ntree' must be more than 0")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, ntree = 0),
				   "'ntree' must be more than 0")

})

test_that("testing nBins parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, nBins = -1),
				   "'nBins' must be more than 0")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, nBins = 0),
				   "'nBins' must be more than 0")

})

test_that("testing mtry parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, mtry = 3),
				   "'mtry' must be less than number of columns")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, mtry = 0),
				   "'mtry' must be more than 0")

})

test_that("testing maxnodes parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, maxnodes = 0),
				   "'maxnodes' must be more than 0")

})

test_that("testing nodesize parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, nodesize = 0),
				   "'nodesize' must be more than 0")

})

test_that("testing xtest/ytest parameters", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, xtest = 0), "'xtest' must be a dframe or data.frame")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, xtest = dframe(npartitions = c(1,2))), "'xtest' must be partitioned rowise")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, ytest = dframe(npartitions = c(1,2))), "'xtest' required if 'ytest' is supplied")

})

test_that("testing cutoff parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, cutoff = 0),  "'cutoff' must have exactly as many elements as number of classes")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, cutoff = Inf), "'cutoff' must have exactly as many elements as number of classes")
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, cutoff = c(0,0,0)), "'cutoff' must be a positive vector and sum to 1")

})

test_that("testing na.action parameter", {
expect_error(hpdRF_parallelTree(X1 ~ .,data = data, na.action = 0),
				   "'na.action' must be either na.exclude, na.omit, na.fail")

})

model <- hpdRF_parallelTree(X1 ~ .,data = data)

context("Invalid Inputs to Predict Function")
test_that("invalid newdata input to predict function",{
expect_error(predict(model), "'newdata' is a required argument")
expect_error(predict(model,newdata = matrix(1:9,3,3)),
				   "'newdata' must be a dframe or data.frame")
expect_error(predict(model,newdata = dframe(npartitions = c(1,2))),
				   "'newdata' must be partitioned rowise")
})

test_that("invalid cutoff input to predict function",{
expect_error(predict(model,newdata = data, cutoff = 0),  "'cutoff' must have exactly as many elements as number of classes")
expect_error(predict(model,newdata = data, cutoff = Inf), "'cutoff' must have exactly as many elements as number of classes")
expect_error(predict(model,newdata = data, cutoff = c(0,0,0)), "'cutoff' must be positive and finite")

})

test_that("invalid na.action input to predict function",{
expect_error(predict(model,newdata = data, na.action = 0), "'na.action' must be either na.pass, na.exclude, na.omit, na.fail")
})




context("Validating Outputs of Training Function")
test_that("testing output for categorical trees", {

data = generateData(100,2,TRUE,TRUE)
model = hpdRF_parallelTree(X1 ~ .,data = data, completeModel = TRUE)
expect_equal(model$mtry, 1)
expect_false(is.null(model$classes))
expect_equal(model$type, "classification")
expect_false(is.null(model$confusion))
expect_false(is.null(model$err.rate))
expect_true(is.null(model$mse))
expect_true(is.null(model$rsq))
})

test_that("testing output for regression trees", {
data = generateData(100,2,TRUE,FALSE)
model = hpdRF_parallelTree(X1 ~ .,data = data, completeModel =TRUE)
expect_equal(model$mtry, 1)
expect_true(is.null(model$classes))
expect_equal(model$type, "regression")
expect_true(is.null(model$confusion))
expect_true(is.null(model$err.rate))
expect_false(is.null(model$mse))
expect_false(is.null(model$rsq))
})

context("Validating Outputs of Predict Function")
data = generateData(100,2,TRUE,TRUE)
model = hpdRF_parallelTree(X1 ~ .,data = data, completeModel =TRUE)
test_that("testing output of predict function",{
expect_false(is.null(predict(model,data)))
predictions = predict(model,getpartition(data))
expect_false(is.null(predictions))
data = getpartition(data)
data[1,1] = NA
expect_false(any(is.na(predictions)))
})


context("Accuracy Results for Training Simple Models")
test_that("basic training with categorical/categorical", {
set.seed(1)
data = generateData(1000,2,TRUE,TRUE)
model <- hpdRF_parallelTree(X1 ~ .,data = data, mtry = 2, completeModel = TRUE)
expect_equal(sum(diag(model$confusion)), nrow(data))
expect_equal(as.numeric(model$err.rate[model$ntree,1]), 0)

data = generateData(100,2,TRUE,TRUE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

test_that("basic training with numerical/categorical", {
set.seed(1)
data = generateData(1000,2,FALSE,TRUE)
model <- hpdRF_parallelTree(X1 ~ .,data = data, mtry = 2, completeModel = TRUE)
expect_equal(sum(diag(model$confusion)), nrow(data))
expect_equal(as.numeric(model$err.rate[model$ntree,1]), 0)

data = generateData(100,2,FALSE,TRUE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})


test_that("basic training with categorical/numerical", {
set.seed(1)
data = generateData(1000,2,TRUE,FALSE)
model <- hpdRF_parallelTree(X1 ~ .,data = data, mtry = 2, completeModel = TRUE)
expect_equal(as.numeric(model$mse),rep(0,model$ntree))
expect_equal(as.numeric(model$rsq), rep(1,model$ntree))

data = generateData(100,2,TRUE,FALSE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})


test_that("basic training with numerical/numerical", {
set.seed(1)
data = generateData(1000,2,FALSE,FALSE)
model <- hpdRF_parallelTree(X1 ~ .,data = data, mtry = 2, completeModel = TRUE)
expect_equal(as.numeric(model$mse),rep(0,model$ntree))
expect_equal(as.numeric(model$rsq), rep(1,model$ntree))

data = generateData(100,2,FALSE,FALSE)
predictions = predict(model, data)
predictions = getpartition(predictions)$predictions
responses = getpartition(data)$X1
expect_equal(predictions,responses)
})

