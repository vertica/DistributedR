##This is a generic function for calculating permutation based variable importance
##This function requires a model with which to test as well as xtest,ytest variables 
##Also required is the a distance_metric function that can compare the loss in accuracy
##between predictions. 

varImportance <- function(model, xtest, ytest,  ..., distance_metric, trace = FALSE, nrow = 1000)
{

	if(!is.dframe(xtest) & !is.data.frame(xtest))
		stop("'xtest' must be a dframe or data.frame")

	if(missing(ytest) & is.element("terms",names(model)))
	{
		model_terms = model$terms
		if(attr(model_terms,"response") == 1)
 		{
			response_name = all.vars(model_terms)[1]
			if(response_name %in% colnames(xtest))
			{
				if(is.dframe(xtest))
				{
				ytest = dframe(npartitions = npartitions(xtest))
				foreach(i,1:npartitions(ytest), function(ytest = splits(ytest,i),
								xtest = splits(xtest,i),
								response_name = response_name)
				{
					ytest <- data.frame(xtest[,response_name])
					update(ytest)
				},progress = FALSE)
				}
				if(is.data.frame(xtest))
				{
					ytest <- data.frame(xtest[,response_name])
				}
			}
			else
				stop("dependant variable must be a unmodified column in the data")
		}
		else
			stop("could not detect response variable from model$terms")
	}
	else if(missing(ytest))
		stop("model$terms and 'ytest' both are missing") 

	if(!is.dframe(ytest) & !is.data.frame(ytest))
		stop("'ytest' must be a dframe or data.frame")
	if((is.dframe(xtest) & !is.dframe(ytest)) | 
		(!is.dframe(xtest) & is.dframe(ytest)))
		stop("'xtest' and 'ytest' must both be either a dframe or data.frame")
	if(ncol(ytest) != 1)
		stop("'ytest' must have exactly one column")
	if(nrow(ytest) != nrow(xtest))
		stop("'xtest' and 'ytest' must have same number of rows")

	#setting the shuffle function
	shuffle_column <- .shuffle_column_data_frame
	if(is.dframe(xtest))
	{
		shuffle_column <- .shuffle_column_dframe
		#if the input was a dframe first randomize data then set shuffle function
		sampled_data<-hpdsample(xtest,ytest,
			sum(distributedR_status()$Inst), nrow/nrow(xtest))
		xtest <- sampled_data$sdata1
		ytest <- sampled_data$sdata2
	}
	else if(is.data.frame(xtest))
	{
		permutation <- sample.int(nrow(xtest),nrow,replace = TRUE)
		xtest = data.frame(xtest[permutation,])
		ytest = data.frame(ytest[permutation,])
	}

	#determine if the output is categorical or not
	#this is required to determine the default value of distance_metric
	categorical = FALSE
	if(is.dframe(ytest))
	{
		temp_categorical = dlist(npartitions = 1)
		foreach(i, 1, function(ytest = splits(ytest,i),
		       categorical = splits(temp_categorical,i))
		       {
				categorical = is.factor(ytest[,1]) | 
			    		    is.logical(ytest[,1]) |
			    		    is.character(ytest[,1])
				categorical = list(categorical)
				update(categorical)
		       },progress = FALSE)
		categorical = getpartition(temp_categorical)[[1]]
	}
	if(is.data.frame(ytest))
	{
		categorical = is.factor(ytest[,1]) | 
			    is.logical(ytest[,1]) |
			    is.character(ytest[,1])
	}
	if(missing(distance_metric))
	{
		if(categorical)
			distance_metric <- errorRate
		if(!categorical)
			distance_metric <- meanSquared
	}

	features = 1:ncol(xtest)

	if(is.element("terms",names(model)))
	{
		features <-names(which(apply(attr(model$terms,"factors"),1,any)))
		features <-match(features,colnames(xtest)) 	
	}

	#this loop will shuffle the column locally and predict and 
	#compute the difference in errors
	importance = sapply(features, function(var)
	{
		if(trace)
		print(paste("Calculating Importance for feature: ",
			colnames(xtest)[var],sep=""))
		timing_info <- Sys.time()
		if(trace)
		print("shuffling data")
		shuffled_data <- shuffle_column(xtest, var)
		if(trace)
		print(Sys.time() - timing_info)


		if(trace)
		print("predicting shuffled data")
		timing_info <- Sys.time()
		tryCatch({
			shuffled_predictions <- predict(model, shuffled_data, ...)
		},error = function(e)
		{
			stop(paste("Could not run predictions using model. Possibly invalid model. Error from predict function: ",e,sep = "\n"))
		})
		if(trace)
		print(Sys.time() - timing_info)

		tryCatch({
		if(is.data.frame(ytest) & !is.data.frame(shuffled_predictions))
			shuffled_predictions <- data.frame(shuffled_predictions)
		},error = function(e) 
			stop("could not coerce output of predict function to data.frame"))

		if(ncol(shuffled_predictions) != ncol(ytest))
			stop("predict function must output only 1 column of predictions")
		if(nrow(shuffled_predictions) != nrow(ytest))
			stop("predict function must output as many predictions as 'ytest'")

		if(trace)
		print("calculating error metric")
		timing_info <- Sys.time()
		var_imp = distance_metric(ytest, shuffled_predictions)[1]
		if(trace)
		print(Sys.time() - timing_info)
		return(var_imp)
	})
	names(importance) <- colnames(xtest)[features]


	#compute the errors without any shuffling
	tryCatch({
		normal_predictions = predict(model, xtest,...)
	},error = function(e)
	{
		stop(paste("Could not run predictions using model. Possibly invalid model. Error from predict function: ",e,sep = "\n"))
	})


	normal_predictions = predict(model, xtest,...)
	tryCatch({
	if(is.data.frame(ytest) & !is.data.frame(normal_predictions))
		shuffled_predictions <- data.frame(normal_predictions)
	},error = function(e) 
		stop("could not coerce output of predict function to data.frame"))

	base_accuracy = distance_metric(ytest, normal_predictions)[1]

	importance <- (importance - base_accuracy) / base_accuracy
	importance <- as.data.frame(importance)
	colnames(importance) <- "Importance"
	return(importance)
}
	
##This function shuffles an individual column of a data.frame

.shuffle_column_data_frame <- function(data, column)
{
	shuffled_data <- data
	shuffled_data[,column]<-data[sample.int(nrow(data)),column]
	return(shuffled_data)
}

##This function shuffles an individual column of a dframe
##The shuffling only occurs locally. This is why there is a randomization
##if xtest is a dframe

.shuffle_column_dframe <- function(data, column)
{
	shuffled_data <- dframe(npartitions = npartitions(data))
	foreach(i,1:npartitions(data), 
		function(data = splits(data,i), 
		shuffled_data = splits(shuffled_data,i), 
		column = column, nrow = nrow,
		random_seed = sample.int(1000,1))
		{
			set.seed(random_seed)
			shuffled_data = data
			shuffled_data[,column] = 
				data[sample.int(nrow(data)),column]
			update(shuffled_data)
		},progress = FALSE)
	colnames(shuffled_data) <- colnames(data)
	return(shuffled_data)
}