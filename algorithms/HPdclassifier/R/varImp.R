varImportance <- function(model, xtest, ytest, distance_metric)
{
	if(!is.dframe(xtest) & !is.data.frame(xtest))
		stop("'xtest' must be a dframe or data.frame")
	if(!is.dframe(ytest) & !is.data.frame(ytest))
		stop("'ytest' must be a dframe or data.frame")
	if((is.dframe(xtest) & !is.dframe(ytest)) | 
		(!is.dframe(xtest) & is.dframe(ytest)))
		stop("'xtest' and 'ytest' must both be either a dframe or data.frame")
	if(ncol(ytest) != 1)
		stop("'ytest' must have exactly one column")
	if(nrow(ytest) != nrow(xtest))
		stop("'xtest' and 'ytest' must have same number of rows")
	permutation = sample.int(nrow(xtest))
	shuffle_column <- .shuffle_column_data_frame
	if(is.dframe(xtest))
		shuffle_column <- .shuffle_column_dframe

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

	importance = sapply(1:ncol(xtest), function(var)
	{
		shuffled_data = shuffle_column(xtest, var, permutation)
		shuffled_predictions <- predict(model, shuffled_data)
		distance_metric(ytest, shuffled_predictions)[1]
	})

	names(importance) <- colnames(xtest)

	normal_predictions = predict(model, xtest)
	base_accuracy = distance_metric(ytest, normal_predictions)

	importance <- importance - base_accuracy
	importance <- as.data.frame(importance)
	colnames(importance) <- "Mean Decrease in Accuracy"
	return(importance)
}
	
.shuffle_column_data_frame <- function(data, column, permutation)
{
	shuffled_data <- data
	shuffled_data[,column]<-data[permutation,column]
	return(shuffled_data)
}

.shuffle_column_dframe <- function(data, column, permutation)
{
	rows_partition = partitionsize(data)[,1]
	rows_partition = cumsum(rows_partition)
	start_rows_partition = c(1,rows_partition[-length(rows_partition)]+1)
	end_rows_partition = rows_partition
	shuffle_column = permutation
	dest_partition = sapply(shuffle_column, function(new) 
			      min(which((start_rows_partition <= new) &
			      	(new <= end_rows_partition))))
	

	temp_data = dframe(npartitions = npartitions(data)*npartitions(data))
	foreach(i,1:(npartitions(data)*npartitions(data)),
	function(column = column,
		source = ceiling(i/npartitions(data)),
		dest = (i %% npartitions(data))+1,
		temp_data = splits(temp_data,i), 
		data = splits(data,ceiling(i/npartitions(data))),
		dest_partition = dest_partition[
			start_rows_partition[ceiling(i/npartitions(data))]:
			end_rows_partition[ceiling(i/npartitions(data))]])
	{
		relevent_rows = dest_partition == dest
		temp_data = data.frame(data[relevent_rows,column])
		update(temp_data)
	},progress = FALSE)

	shuffled_data = dframe(npartitions = npartitions(data))
	foreach(i,1:npartitions(data),
	function(column = column,
		temp_data = splits(temp_data,
			as.list(npartitions(data)*(i-1)+1:npartitions(data))),
		shuffled_data = splits(shuffled_data,i),
		data = splits(data,i))
	{
		shuffled_data = data
		shuffled_data[,column] = do.call(rbind,temp_data)
		update(shuffled_data)
	},progress = FALSE)
	colnames(shuffled_data) <- colnames(data)
	return(shuffled_data)
}

