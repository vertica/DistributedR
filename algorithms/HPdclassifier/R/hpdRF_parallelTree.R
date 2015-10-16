#####################################################################################
# Copyright [2013] Hewlett-Packard Development Company, L.P.                        # 
#                                                                                   #
# This program is free software; you can redistribute it and/or                     #
# modify it under the terms of the GNU General Public License                       #
# as published by the Free Software Foundation; either version 2                    #
# of the License, or (at your option) any later version.                            #
#                                                                                   #
# This program is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                    #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                      #
# GNU General Public License for more details.                                      #
#                                                                                   #
# You should have received a copy of the GNU General Public License                 #
# along with this program; if not, write to the Free Software                       #
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.    #
#####################################################################################

hpdrandomForest <- hpdRF_parallelTree <- function(formula, data, 
		   ntree = 500, xtest, ytest, 
		   mtry, replace=TRUE, cutoff, nodesize, 
		   maxnodes = min(.Machine$integer.max,nrow(data)), do.trace = FALSE, 
		   keep.forest = TRUE, na.action = na.omit, 
		   nBins=256, completeModel=FALSE, 
		   reduceModel = FALSE, varImp = FALSE)
{
	start_timing <- Sys.time()
	ddyn.load("HPdclassifier")

	if(do.trace)
	.master_output("Input validation and preprocessing")
	timing_info <- Sys.time()
	if(do.trace)
	.master_output("\tinput validation: ", appendLF = FALSE)
		
	if(!identical(na.action, na.exclude) &
		!identical(na.action, na.omit) &
		!identical(na.action, na.fail))
		stop("'na.action' must be either na.exclude, na.omit, na.fail")

	if(missing(formula))
		stop("'formula' is a required argument")
	if(!inherits(formula, "formula"))
		stop("'formula' is not of class formula")

	if(missing(data))
		stop("'data' is a required argument")
	if(!is.dframe(data) & !is.data.frame(data))
		stop("'data' must be a dframe or data.frame")
	if(is.data.frame(data))
		data = as.dframe(data)
	if(attr(data,"npartitions")[2] > 1)
		stop("'data' must be partitioned rowise")

	tryCatch({
	test_formula <- data.frame(matrix(0,0,ncol(data)))
	colnames(test_formula) <- colnames(data)
	model.frame(terms(formula, data = test_formula),
		data = test_formula)
	}, error = function(e){
	   stop(paste("unable to apply formula to 'data'.",e))
	})


	if(!missing(xtest))
	{ 
	if(!is.dframe(xtest) & !is.data.frame(xtest))
		stop("'xtest' must be a dframe or data.frame")
	if(is.data.frame(xtest))
		xtest = as.dframe(xtest)
	if(attr(xtest,"npartitions")[2] > 1)
		stop("'xtest' must be partitioned rowise")
	}

	if(!missing(xtest)){
	tryCatch({
	test_formula <- data.frame(matrix(0,0,ncol(xtest)))
	colnames(test_formula) <- colnames(xtest)
	model.frame(delete.response(terms(formula, data = test_formula)),
		data = test_formula)
	}, error = function(e){
	   stop(paste("unable to apply formula to 'xtest'.",e))
	})}

	if(!missing(ytest))
	{
	if(missing(xtest))
		stop("'xtest' required if 'ytest' is supplied")
	if( !is.dframe(ytest) & !is.data.frame(ytest))
		stop("'ytest' must be a dframe or data.frame")
	if(is.data.frame(ytest))
		ytest = as.dframe(ytest)
	if( attr(ytest,"npartitions")[2] > 1)
		stop("'ytest' must be partitioned rowise")
	if(ncol(ytest) !=  1)
		stop("'ytest' must have exactly one column")
	if(nrow(ytest) != nrow(xtest))
		stop("'ytest' must have same number of columns as 'xtest'")
	if((is.dframe(xtest) & !is.dframe(ytest)) | 
		(!is.dframe(xtest) & is.dframe(ytest)))
		stop("'xtest' and 'ytest' must be a pair of dframes or a pair of data.frames")
	if(!is.dframe(xtest) & !is.dframe(ytest))
	{
		xpartitionsize = partitionsize(xtest)[,1]
		ypartitionsize = partitionsize(ytest)[,1]
		if(!identical(xpartitionsize,ypartitionsize))
			stop("'xtest' and 'ytest' must be partitioned identically")

	}
	}

	ntree = as.integer(ntree)
	if(ntree <= 0)
		stop("'ntree' must be more than 0")

	nBins = as.integer(nBins)
	if(nBins <= 0)
		stop("'nBins' must be more than 0")


	maxnodes = as.integer(maxnodes)
	if(maxnodes <= 0)
		stop("'maxnodes' must be more than 0")
	if(maxnodes > nrow(data))
		warning("'maxnodes' is greater than number of observations")
		maxnodes = as.integer((maxnodes-1)/2)

	if(do.trace)
	.master_output(format(round(Sys.time() - timing_info, 2), 
		nsmall = 2))


	tryCatch({
	variables <- .parse_formula(formula, data, 
		  na.action = na.action, trace = do.trace)
	},
	error = function(cond){
	      stop(paste("could not apply formula to 'data'.", cond))
	})

	observations = variables$x
	responses = variables$y
	features_cardinality = as.integer(variables$x_cardinality)
	response_cardinality = as.integer(variables$y_cardinality)
	classes = variables$y_classes
	true_responses = variables$true_responses
	xlevels = variables$x_classes
	model_terms=variables$terms
	model_data_type=variables$data_type
	rm(variables)

	if(missing(mtry) & !is.na(response_cardinality))
		mtry = max(1,floor(sqrt(ncol(observations))))
	if(missing(mtry) & is.na(response_cardinality))
		mtry = max(1,floor(ncol(observations)/3))

	mtry = as.integer(mtry)
	if(mtry <= 0)
		stop("'mtry' must be more than 0")	
	else if(mtry > ncol(observations))
	     	stop(paste("'mtry' must be less than number of columns:",
		ncol(observations)))

	if(length(classes) > 0)
		classes = classes[[1]]
	else
		classes = NULL

	if(!missing(cutoff) & !is.na(response_cardinality))
	{
		if(length(cutoff) != length(classes))
			  stop("'cutoff' must have exactly as many elements as number of classes")
		if(any(cutoff <= 0) | sum(cutoff) != 1)
		      stop("'cutoff' must be a positive vector and sum to 1")
		cutoff = as.numeric(cutoff)
	}
	else if(!is.na(response_cardinality))
		cutoff = as.numeric(rep(1/response_cardinality, 
		       response_cardinality))
	else if(is.na(response_cardinality))
		cutoff = 0

	if(missing(nodesize) & !is.na(response_cardinality))
		nodesize = 1
	else if(missing(nodesize) & is.na(response_cardinality))
	     	nodesize = 5
	nodesize = as.integer(nodesize)
	if(nodesize <= 0)
		stop("'nodesize' must be more than 0")

	DR_status = distributedR_status()

	free_sh_mem = (DR_status$DarrayQuota - 
		 DR_status$DarrayUsed)/
		 DR_status$Inst
	free_mem = (DR_status$SysMem - 
		 DR_status$MemUsed)/
		 DR_status$Inst

	sizeof_double = 8 #8 bytes

	free_sh_mem = min(free_sh_mem)*1024*1024/sizeof_double/2
	free_mem = min(free_mem)*1024*1024/sizeof_double/2

	########## Memory Restrictions:
	#(ncol(observations)+3)*threshold*nodes_per_executor + 
	#		(2)*max_trees_per_iteration*nrow(observations) +
	#		sizeof_node_histogram*max_nodes_per_iteration +
	#		sizeof_tree_node*threshold*nodes_per_executor < free_mem

	#sizeof_node_histogram*max_nodes_per_iteration +
	#		(2)*max_trees_per_iteration*nrow(observations)
	#		(2)*nodes_per_executor*threshold)< free_mem
	
	#empirically it is about 20x slower to grow trees in distributed setting than local
	#load balancing trees is important since foreach statement is blocking and wastes 40% of time
	

	
	sizeof_node_histogram = if(is.na(response_cardinality)) 2 
			      else response_cardinality
	sizeof_node_histogram = sizeof_node_histogram*nBins * mtry 
	sizeof_tree_node = 15
	
	#limit 20% of free_mem and free_sh_mem to book keeping
	max_rows_per_partition = max(partitionsize(responses)[,1])
	max_trees_per_iteration = as.integer(floor(min(10000,ntree, 
				0.05*free_mem/max_rows_per_partition,
				0.05*free_sh_mem/max_rows_per_partition)))

	#limit 25% of free_mem and free_sh_mem to building histograms
	max_nodes_per_iteration = as.integer(floor(min(10000,
				0.25*free_mem/sizeof_node_histogram,
				0.25*free_sh_mem/sizeof_node_histogram)))

	free_mem = free_mem - max_nodes_per_iteration*sizeof_node_histogram - 
		 max_trees_per_iteration*max_rows_per_partition
	free_sh_mem = free_sh_mem - max_nodes_per_iteration*sizeof_node_histogram - 
		 max_trees_per_iteration*max_rows_per_partition

	#use 80% of remaining memory to transfer data during local tree building step
	nodes_per_executor= 1
	threshold = as.integer(min(
		  .8*free_mem/nodes_per_executor/ncol(observations),
		  .8*free_sh_mem/nodes_per_executor/ncol(observations)))

	if(threshold > nrow(observations))
	{
		threshold = nrow(observations)
		nodes_per_executor = as.integer(min(
				   free_mem/threshold/ncol(observations),
				   free_sh_mem/threshold/ncol(observations)))
	}

	free_mem = free_mem - ncol(observations)*threshold*nodes_per_executor
	free_sh_mem = free_sh_mem - ncol(observations)*threshold*nodes_per_executor


	#####see if its possible to increase the value of max_nodes_per_iteration########
	free_mem = free_mem + max_nodes_per_iteration*sizeof_node_histogram
	free_sh_mem = free_sh_mem + max_nodes_per_iteration*sizeof_node_histogram
	
	max_nodes_per_iteration_temp = as.integer(floor(min(
				0.50*free_mem/sizeof_node_histogram,
				0.50*free_sh_mem/sizeof_node_histogram)))
	max_nodes_per_iteration = max(max_nodes_per_iteration, max_nodes_per_iteration_temp)

	free_mem = free_mem - max_nodes_per_iteration*sizeof_node_histogram 
	free_sh_mem = free_sh_mem - max_nodes_per_iteration*sizeof_node_histogram 


	threshold = as.integer(floor(threshold))
	max_nodes_per_iteration = as.integer(floor(max_nodes_per_iteration))
	nodes_per_executor = as.integer(floor(nodes_per_executor))
	max_trees_per_iteration=as.integer(floor(min(max_trees_per_iteration,ntree)))

	if(do.trace)
		.master_output("Starting to build forest")

	if(do.trace)
		.master_output(paste("\ttrees left: ", ntree, " out of a total of ", ntree))

	suppressWarnings(model <-
		.hpdRF_distributed(observations, responses, 
		ntree = as.integer(max_trees_per_iteration), nBins,
		features_cardinality, response_cardinality, 
		features_num = mtry,
		threshold, NULL, nodes_per_executor,
		maxnodes, nodesize, 
		replace, cutoff, classes = classes, 
		completeModel = completeModel,
		max_nodes_per_iteration = max_nodes_per_iteration,
		trace = do.trace, 
		features_min = NULL, features_max = NULL,
		scale = as.integer(1)))

	if(do.trace)
	.master_output("\tdistributing forest: ", appendLF = FALSE)
	timing_info <- Sys.time()
	forest = .distributeForest(model$forest)
	if(do.trace)
	.master_output(format(round(Sys.time() - timing_info, 2), 
		nsmall = 2))
	oob_indices = model$oob_indices
	curr_ntree = as.integer(ntree - max_trees_per_iteration)
	features_min = model$features_min
	features_max = model$features_max
	rm(model)
	gc()

	if(do.trace)
	.master_output("\tcurrent distributed forest size: ",
		format(round(d.object.size(forest)/1024/1024,2),nsmall = 2), 
		" MB")

	while(curr_ntree > 0)
	{
		if(do.trace)
		.master_output("\ttrees left: ",curr_ntree, " out of a total of ", ntree)

		suppressWarnings(model <-
			.hpdRF_distributed(observations, responses, 
			ntree = as.integer(min(curr_ntree,max_trees_per_iteration)), 
			nBins, features_cardinality, response_cardinality, 
			features_num = mtry,
			threshold, NULL, nodes_per_executor,
			maxnodes, nodesize, 
			replace, cutoff, classes = classes, 
			completeModel = completeModel,
			max_nodes_per_iteration = max_nodes_per_iteration,
			trace = do.trace, 
			features_min = features_min,
			features_max = features_max, 
			scale = as.integer(0)))

		new_oob_indices = dlist(npartitions = npartitions(oob_indices))
		foreach(i,1:npartitions(oob_indices), function(old = splits(oob_indices,i),
						      current = splits(model$oob_indices,i),
						      new = splits(new_oob_indices,i))
		{
			new = c(old,current)
			update(new)
		},progress = FALSE)
		oob_indices = new_oob_indices
		rm(new_oob_indices)
		model$oob_indices <- NULL

		if(do.trace)
		.master_output("\tdistributing forest: ",appendLF = FALSE)
		timing_info <- Sys.time()
		forest = .distributeForest(model$forest,forest )
		if(do.trace)
		.master_output(format(round(Sys.time() - timing_info, 2), 
			nsmall = 2))

		model$forest <- NULL
		curr_ntree=as.integer(curr_ntree-min(ntree,max_trees_per_iteration))
		rm(model)
		gc()
		if(do.trace)
		.master_output("\tcurrent distributed forest size: ",
			format(round(d.object.size(forest)/1024/1024,2),nsmall = 2), 
			" MB")
	}
	oob_predictions = NULL
	if(completeModel)
	{
		tryCatch({
		if(do.trace)
			.master_output("\tComputing oob statistics")
			timing_info <- Sys.time()
		oob_predictions = .predictOOB(forest, observations, 
			responses, oob_indices, cutoff, classes, 
			ntree = ntree, reduceModel = reduceModel, do.trace)
			forest = oob_predictions$dforest
			},error = function(e)
			{
				warning(paste("aborting oob computations. received error:", e))
			})

		if(do.trace)
		.master_output("\tcurrent distributed forest size: ",
			format(round(d.object.size(forest)/1024/1024,2),nsmall = 2), 
			" MB")

			

	}

	if(do.trace & completeModel)
	.master_output("\tcomputing additional statistics: ", appendLF = FALSE)
	timing_info <- Sys.time()

	rm(observations)
	gc()

	model = list()
	model$predicted = oob_predictions$oob_predictions
	model$classes = classes
	class(model) = c("hpdRF_parallelTree", "hpdrandomForest")
	if(keep.forest)
	{
		model$forest$trees = forest
		model$forest$cutoff = cutoff
		model$forest$xlevels = xlevels
	}

	model$call = match.call()
	model$ntree = ntree
	if(!is.null(attr(model$forest$trees,"ntree")))
		model$ntree = attr(model$forest$trees,"ntree")
	attr(model$forest$trees,"ntree") <- NULL
	model$mtry = mtry
	model$test = list()
	model$terms = model_terms

	if(is.na(response_cardinality))
	{
		model$type = "regression"
		if(completeModel)
		{
			tryCatch(
			{
			model$mse = oob_predictions$mse
			model$rsq = oob_predictions$rsq

			if(!missing(xtest))
			{
				predictions = predict(model,xtest, 
					    na.action = na.pass,
					    do.trace = do.trace)
				model$test$predicted = predictions
			}
			if(!missing(xtest) && !missing(ytest))
			{
				model$test$mse = meanSquared(ytest,
					       model$test$predicted,
					       na.rm = TRUE)
				model$test$rsq = rSquared(ytest,
					       model$test$predicted,
					       na.rm = TRUE)
			}
			},
			error = function(e){
			      warning(paste("could not compute additional statistics due to error:", e))
			})			

		}
	}
	else
	{
		model$type = "classification"

		if(completeModel)
		{
			tryCatch(
			{
			confusion = HPdutility::confusionMatrix(true_responses, 
				    	model$predicted)
			model$err.rate = oob_predictions$err.rate
			classErr <- model$err.rate[nrow(model$err.rate),-1]
			model$confusion <- cbind(confusion, classErr)
			model$cutoff <- cutoff
			if(!missing(xtest))
			{
				predictions = predict(model,xtest, 
					    na.action = na.pass, 
					    do.trace = do.trace)
				model$test$predicted = predictions
			}
			if(!missing(xtest) && !missing(ytest))
			{
				if(is.dframe(ytest))
				{
					ytest_levels = 
					     suppressWarnings(levels.dframe(ytest)$Levels)
					if(length(ytest_levels) == 0)
						stop("No categories in 'ytest'")
					ytest_levels = ytest_levels[[1]]
				}
				if(is.data.frame(ytest))
				{
					y_test_levels = levels(ytest[[1]])
					if(is.na(y_test_levels))
						stop("No categories in 'ytest'")
				}
				if(!all(ytest_levels %in% model$classes))
				{
					stop("Categories of 'ytest' are not the same as categories of response variable trained in model")
				}
				confusionTest = HPdutility::confusionMatrix(ytest,
			       		model$test$predicted)
				model$test$err.rate = errorRate(ytest,
					model$test$predicted)
            			classErrTest <- 
					matrix(model$test$err.rate[-1], 
					     ncol=1)
            			colnames(classErrTest) <- "class.error"
            			model$test$confusion  <-cbind(confusionTest, 
						      classErrTest)
			}
			},
			error = function(e){
			      warning(paste("could not compute additional statistics due to error:", e))
			})			

		}
	}

	if(do.trace & completeModel)
	.master_output(format(round(Sys.time() - timing_info, 2), 
		nsmall = 2))


	if(do.trace & completeModel & varImp)
	.master_output("\tcomputing variable importance: ",appendLF = FALSE)
	timing_info <- Sys.time()
	if(varImp & completeModel)
		model$importance <- varImportance(model,data)
	timing_info <- Sys.time() - timing_info
	if(do.trace & completeModel & varImp)
	.master_output(format(round(timing_info, 2), 
		nsmall = 2))


	rm(responses)
	gc()
	if(do.trace)
	.master_output("hpdRF_parallelTree time: ", 
		format(round(Sys.time() - start_timing, 2),nsmall = 2))
	return(model)
}

predict.hpdRF_parallelTree <- function(model, newdata, cutoff,
			   do.trace = FALSE, na.action = na.pass)
{
	start_timing <- Sys.time()
	if(do.trace)
	.master_output("predicting data using hpdRF_parallelTree model")
	if(do.trace)
	.master_output("\tinput validation: ", appendLF = FALSE)
	timing_info <- Sys.time()

	if(!identical(na.action, na.pass) & 
		!identical(na.action, na.exclude) &
		!identical(na.action, na.omit) &
		!identical(na.action, na.fail))
		stop("'na.action' must be either na.pass, na.exclude, na.omit, na.fail")

	if(missing(newdata))
		stop("'newdata' is a required argument")
	if(!is.dframe(newdata) & !is.data.frame(newdata))
		stop("'newdata' must be a dframe or data.frame")
	was.data.frame = is.data.frame(newdata)
	if(is.data.frame(newdata))
		newdata = as.dframe(newdata, blocks = c(nrow(newdata), ncol(newdata)))
	if(attr(newdata,"npartitions")[2] > 1)
		stop("'newdata' must be partitioned rowise")


	if(!missing(cutoff))
	{
		if(length(cutoff) != length(model$classes))
			stop("'cutoff' must have exactly as many elements as number of classes")
		if(any(cutoff <= 0) | any(is.infinite(cutoff)))
			      stop("'cutoff' must be positive and finite")
		cutoff = as.numeric(cutoff)
	}
	else if(missing(cutoff) & !is.null(model$forest$cutoff))
		cutoff = model$forest$cutoff
	else 
		cutoff = rep(1/length(model$classes),length(model$classes))
	timing_info <- Sys.time() - timing_info
	if(do.trace)
	.master_output(format(round(timing_info, 2), nsmall = 2))


	
	tryCatch({
	variables <- .parse_formula(model$terms, 
		  data = newdata, trace = do.trace, na.action = na.action)
	},
	error = function(cond){
	      stop(paste("'newdata' incompatible with model. possibly missing columns.",cond))
	})


	newdata = variables$x
	
	prediction_outputs = .predict.hpdRF_distributedForest(model$forest,
		    newdata, cutoff = cutoff, classes= model$classes, 
		    trace = do.trace, dforest = model$dforest)

	predictions = prediction_outputs$predictions
	dforest = prediction_outputs$dforest
	model$dforest = dforest

	colnames(predictions) <- "predictions"	
	if(was.data.frame)
		predictions <- getpartition(predictions)

	timing_info <- Sys.time() - start_timing
	if(do.trace)
	.master_output("predictions took: ",format(round(timing_info, 2), nsmall = 2))
	return(predictions)
}


.parse_formula <- function(formula, data, na.action=na.fail, weights = NULL, trace = FALSE) 
{
	timing_info <- Sys.time()

	y <- dframe(npartitions = npartitions(data))
	x <- dframe(npartitions = npartitions(data))
	w <- dframe(npartitions = npartitions(data))
		
	if(trace)
	.master_output("\tprocessing formula: ", appendLF = FALSE)
	

	if(is.null(weights))
		weights = clone(data,ncol = 1, data = 1)



	terms = dlist(npartitions = npartitions(x))
	x_colnames = dlist(npartitions = npartitions(x))

	foreach(i,1:npartitions(data), function(
				       data = splits(data,i),
				       column_names = colnames(data),
				       x = splits(x,i),
				       y = splits(y,i),
				       model_formula = formula,
				       model_terms = splits(terms,i),
				       x_colnames = splits(x_colnames,i),
				       weights = splits(weights,i),
				       w = splits(w,i),
				       na.action = na.action)
	{
  		assign("data", na.action(cbind(weights,data)), globalenv())
		w = data.frame(as.double(data[,1]))
		data[,1] = NULL
		update(w)
		colnames(data) <- column_names
		if(inherits(model_formula, "formula"))
			model_terms = terms(model_formula, data = data)
		else
			model_terms = model_formula
		data_type = NULL
		if(attr(model_terms,"response") == 1)
 		{
			response_name = all.vars(model_terms)[1]
			if(response_name %in% colnames(data))
			{
			y <- data.frame(data[,response_name])
			update(y)
			data_type = sapply(y,class)
			names(data_type) = response_name
			}
		}
		x <- model.frame(delete.response(model_terms), 
		     	data = data, na.action = na.action)
		attr(x,"terms") <- NULL
		data_type = c(data_type,sapply(x,class))
		data_type[which(data_type=="integer")] = "numeric"
		data_type[which(data_type=="double")] = "numeric"

		rm(data)
		gc()
		update(x)
		x_colnames <- list(colnames(x))
		update(x_colnames)
		attr(model_terms,"dataClasses") <- data_type
		model_terms = list(model_terms)
		update(model_terms)
	},progress = FALSE)
	x_colnames = getpartition(x_colnames,1)[[1]]


	suppressWarnings({
	x_levels = levels.dframe(x)
	x_cardinality = rep(NA,ncol(x))
	if(nrow(y)>0 & !is.null(y))
		y_levels = levels.dframe(y)
	else
		y_levels = list()
	y_cardinality = rep(NA,ncol(y))

	if(length(x_levels$columns)>0)
	{
		factor.dframe(x, colID = x_levels$columns)
		x_cardinality[x_levels$columns] = sapply(x_levels$Levels,length)
	}
	if(length(y_levels$columns)>0)
	{
		factor.dframe(y, colID = y_levels$columns)
		y_cardinality[y_levels$columns] = sapply(y_levels$Levels,length)
	}
	})

	true_responses = dframe(npartitions = npartitions(y))
	foreach(i,1:npartitions(x), function(x = splits(x,i),
				    y = splits(y,i), 
				    true_responses = splits(true_responses,i),
				    x_columns = x_levels$columns,
				    y_columns = y_levels$columns)
		{
			true_responses = y
			for(column in x_columns)
				x[,column]=as.numeric(x[,column])
			y[,y_columns] = sapply(y[,y_columns], as.numeric)
			update(x)
			update(y)
			update(true_responses)
		},progress = FALSE)
	terms = getpartition(terms,1)[[1]]

	y_classes = lapply(1:ncol(y), function(a) NULL)
	y_classes[y_levels$columns] = y_levels$Levels
	x_classes = lapply(1:ncol(x), function(a) NULL)
	names(x_classes) <- x_colnames
	x_classes[x_colnames[x_levels$columns]] = x_levels$Levels

	if(trace)
	.master_output(format(round(Sys.time() - timing_info, 2), 
		nsmall = 2))


    return(list(x=x,y=y, weights = w, terms = terms,
    		  x_cardinality = x_cardinality, 
		  y_cardinality = y_cardinality,
		  y_classes = y_classes,
		  x_classes = x_classes,
		  true_responses = true_responses,
		  x_colnames = x_colnames))
}

deploy.hpdRF_parallelTree <- function(model)
{
	if(!("hpdRF_parallelTree" %in% class(model)))
		stop("model must be of class hpdRF_parallelTree")

	model$test <- NULL
	model$predicted <- NULL
	xlevels = model$forest$xlevels
	cutoff <- model$forest$cutoff
	if(is.null(cutoff))
		cutoff = rep(1/length(model$classes),length(model$classes))

	new_trees <- .gatherDistributedForest(model$forest$trees)
	new_trees = .Call("unserializeForest", new_trees)
	new_trees = .Call("reformatForest",new_trees)
	max_nodes = new_trees[[6]]
	ntree = new_trees[[7]]
	dim(new_trees[[1]]) <- c(max_nodes, ntree)
	dim(new_trees[[2]]) <- c(max_nodes, ntree)
	dim(new_trees[[3]]) <- c(max_nodes, 2,ntree)
	dim(new_trees[[4]]) <- c(max_nodes, ntree)
	dim(new_trees[[5]]) <- c(max_nodes, ntree)
	features_cardinality = new_trees[[8]]

	model$forest = list()
	model$forest$ndbigtree = new_trees[[9]]
	model$forest$maxcat = new_trees[[10]]
	model$forest$ncat = sapply(features_cardinality, function(i)
			    	if(is.na(i)) 1 else i)
	model$forest$nodestatus = new_trees[[1]]
	model$forest$bestvar = new_trees[[2]]
	model$forest$nodepred = new_trees[[4]]
	model$forest$xbestsplit = new_trees[[5]]

	if(model$type == "classification")
	{
		model$forest$treemap = new_trees[[3]]
        	model$forest$nclass <- length(model$classes)
        	model$forest$cutoff <- cutoff
        	model$forest$pid <- rep(1, model$forest$nclass)
	}
	if(model$type == "regression")
	{
		model$forest$leftDaughter = new_trees[[3]][,1,]
		model$forest$rightDaughter = new_trees[[3]][,2,]
	}
	model$forest$nrnodes = max_nodes
	model$forest$ntree = model$ntree
	model$forest$xlevels =xlevels
	variables = attr(model$terms,"term.labels")
	variables = variables[attr(model$terms,"order")==1]
	model$importance = data.frame(rep(0,length(variables)))
	rownames(model$importance) = variables
	class(model) <- c("hpdrandomForest", 
		     "randomForest", "randomForest.formula")
	model$forest$trees <- NULL
    	# clearing environment
	environment(model$terms) <- globalenv()

	return(model)
}


print.hpdRF_parallelTree <- function(model, max_depth = 2)
{

	cat("\nCall:\n", deparse(model$call), "\n")
	cat(paste("               Type of random forest: ",model$type ," \n"))
	cat(paste("                     Number of Trees: ",model$ntree, "\n"))
	cat(paste("No. of variables tried at each split: ", model$mtry, "\n"))

	if(model$type == "classification")
	{
	        if(!is.null(model$err.rate))
		{
			errRate <- model$err.rate[nrow(model$err.rate),1]
            		names(errRate) <- NULL
	        	cat("         OOB estimate of  error rate: ",
                  		     round(errRate*100,digits=max_depth),"%\n", sep="")
	        	cat("OOB Confusion matrix:\n")
	        	print(model$confusion)
        	}
        	if(!is.null(model$test$err.rate))
        	{
			errRate <- model$test$err.rate["err.rate"]
            		names(errRate) <- NULL
	        	cat("                Test set error rate: ",
                  		round(errRate*100, digits=max_depth), "%\n", sep="")
	        	cat("Test Confusion matrix:\n")
	        	print(model$test$confusion)
        	}
	}
	if(model$type == "regression")
	{
		if(!is.null(model$mse))
        	{
			cat("          OOB Mean of squared residuals: ", 
				model$mse[length(model$mse)], "\n", sep="")
            		cat("                    % OOB Var explained: ", 
				round(100*model$rsq[length(model$rsq)], digits=max_depth), "\n", sep="")
        	}
        	if(!is.null(model$test$mse))
        	{
			cat("          Test Mean of squared residuals: ", 
				model$test$mse, "\n", sep="")
            		cat("                    % Test Var explained: ", 
				round(100*model$test$rsq, digits=max_depth), "\n", sep="")
        	}
	}

}

