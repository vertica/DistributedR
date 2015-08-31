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


hpdrpart <- function(formula, data, weights, subset , na.action = na.omit, 
	 model = FALSE, x = FALSE, y = FALSE, params = NULL, 
	 control = NULL, cost = NULL, 
	 completeModel = FALSE, nBins = 256L, do.trace = FALSE)
{

	ddyn.load("HPdclassifier")
	if(missing(weights))
		weights = NULL

	if(!identical(na.action, na.exclude) &
		!identical(na.action, na.omit) &
		!identical(na.action, na.fail))
		stop("'na.action' must be either na.exclude, na.omit, na.fail")

	if(!missing(subset))
		warning("'subset' not implemented. Adjust using weights parameter")
	if(missing(subset))
		subset = NULL
	if(is.null(control))
		control = rpart.control()

	if(missing(formula))
		stop("'formula' is a required argument")
	if(!inherits(formula, "formula"))
		stop("'formula' is not of class formula")

	if(missing(data))
		stop("'data' is a required argument")
	if(!is.dframe(data) & !is.data.frame(data))
		stop("'data' must be a dframe or data.frame")
	if(is.dframe(data) & !is.dframe(weights) & !is.null(weights))
		stop("'weights' must be same type as data")
	if(is.data.frame(data) & !is.data.frame(weights) & !is.null(weights))
		stop("'weights' must be same type as data")
	if(is.data.frame(data))
		data = as.dframe(data)
	if(is.data.frame(weights))
		weights = as.dframe(weights)
	if(attr(data,"npartitions")[2] > 1)
		stop("'data' must be partitioned rowise")

	if(!is.null(weights))
	{
		if(nrow(weights) != nrow(data))
			stop("'weights' must have same number of rows as data") 
		if(partitionsize(weights)[,1] != parititionsize(data)[,1])
			stop("'weights' must be partitioned similarly to data")
	}

	nBins = as.integer(nBins)
	if(nBins <= 0)
		stop("'nBins' must be more than 0")

	tryCatch({
	test_formula <- data.frame(matrix(0,0,ncol(data)))
	colnames(test_formula) <- colnames(data)
	model.frame(terms(formula, data = test_formula),
		data = test_formula)
	}, error = function(e){
	   stop(paste("unable to apply formula to 'data'.",e))
	})

	
	tryCatch({
	variables <- .parse_formula(formula, data, weights = weights, 
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
	x_colnames = variables$x_colnames
	weights = variables$weights
	if(length(classes) > 0)
		classes = classes[[1]]
	else
		classes = NULL




	if(!is.na(response_cardinality))
		cutoff = as.numeric(rep(1/response_cardinality, 
		       response_cardinality))
	else if(is.na(response_cardinality))
		cutoff = 0




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


	sizeof_node_histogram = if(is.na(response_cardinality)) 2 
			      else response_cardinality
	sizeof_node_histogram = sizeof_node_histogram*nBins * ncol(observations) 
	sizeof_tree_node = 15

	#limit 50% of free_mem and free_sh_mem to building histograms
	max_nodes_per_iteration = as.integer(floor(min(10000,
				0.5*free_mem/sizeof_node_histogram,
				0.5*free_sh_mem/sizeof_node_histogram)))

	free_mem = free_mem - max_nodes_per_iteration*sizeof_node_histogram 
	free_sh_mem = free_sh_mem - max_nodes_per_iteration*sizeof_node_histogram

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


	threshold = as.integer(floor(threshold))
	max_nodes_per_iteration = as.integer(floor(max_nodes_per_iteration))
	nodes_per_executor = as.integer(floor(nodes_per_executor))

	threshold = min(threshold,nrow(observations)/sum(DR_status$Inst))

	if(do.trace)
		print(paste("threshold",
			toString(threshold),sep=" = "))

	if(do.trace)
		print(paste("nodes_per_executor",
			toString(nodes_per_executor),sep=" = "))

	if(do.trace)
		print(paste("max_nodes_per_iteration",
			toString(max_nodes_per_iteration),sep=" = "))


	suppressWarnings({
	tree <- .hpdRF_distributed(observations, responses, ntree = 1L, 
	     bin_max = as.integer(nBins), 
	     features_cardinality = features_cardinality,
	     response_cardinality = response_cardinality, 
	     features_num = as.integer(ncol(observations)),
	     threshold = threshold, weights = weights, 
	     nodes_per_worker=nodes_per_executor,
	     max_nodes = .Machine$integer.max, node_size = control$minsplit, 
	     replacement = TRUE, cutoff = cutoff, classes = classes, 
	     completeModel = FALSE, 
	     max_nodes_per_iteration = max_nodes_per_iteration,
	     trace = do.trace, features_min = NULL, features_max = NULL,
	     min_split = control$minbucket, max_depth = control$maxdepth, 
	     cp = control$cp)
	})
	     
	if(do.trace)
	print("converting to rpart model")
	timing_info <- Sys.time()
	model = .convertToRpartModel(tree$forest, x_colnames)
	timing_info <- Sys.time() - timing_info
	if(do.trace )
	print(timing_info)


	model$call = match.call()
	model$terms = variables$terms
	if(is.na(response_cardinality))
		model$method = "regression"
	if(!is.na(response_cardinality))
		model$method = "classification"
	model$control = control
	model$params = params
	model$na.action = na.action
	model$numresp = 0
	model$numresp = length(classes)
	model$classes = classes
	class(model) <- c("hpdrpart","rpart")

	if(is.data.frame(data))
		responses = getpartition(responses)

	if(completeModel)
	{
		if(do.trace)
		print("calculating variable importance")
		timing_info <- Sys.time()
		model$variable.importance <- 
			varImportance(model,data,responses)
		timing_info <- Sys.time() - timing_info
		if(do.trace )
		print(timing_info)
	}
	return(model)
}

predict.hpdrpart <- function(model, newdata,do.trace = FALSE)
{
	if(missing(newdata))
		stop("'newdata' is a required argument")
	if(!is.dframe(newdata) & !is.data.frame(newdata))
		stop("'newdata' must be a dframe or data.frame")
	was.data.frame = is.data.frame(newdata)
	if(is.data.frame(newdata))
		newdata = as.dframe(newdata)
	if(attr(newdata,"npartitions")[2] > 1)
		stop("'newdata' must be partitioned rowise")

	predictions = dframe(npartitions = npartitions(newdata))
	foreach(i,1:npartitions(newdata),
		function(model=model, 
			newdata = splits(newdata,i), 
			predictions = splits(predictions,i))
		{
			library(rpart)
			class(model) <- class(model)[-1]
			predictions = predict(model,newdata,type = "vector")
			if(model$method=="classification")
			predictions = factor(model$classes[predictions], 
				    levels = model$classes)
			predictions = data.frame(predictions)
			update(predictions)
		},progress = do.trace)
	if(was.data.frame)
		predictions = getpartition(predictions)
	return(predictions)
}

.convertToRpartModel <- function(tree, varnames)
{
	model <- .Call("rpartModel",tree)
	csplit <- model[[8]]
	splits <- cbind(count = 0, model[[7]], improve = 0, model[[6]],adj = 0)
	model[[8]] <- NULL
	varnames = c("<leaf>", varnames)
	model[[2]] = varnames[model[[2]]+1]
	leaf_ids = model[[1]]
	model = data.frame(var = model[[2]], n = 0, wt = 0, 
	      dev = model[[3]], yval = model[[4]], complexity = model[[5]])
	colnames(model) <- c("var", "n","wt","dev", "yval", "complexity")
	rownames(model) <- leaf_ids
	model <- cbind(model, ncompete = 0, nsurrogate = 0)
	colnames(splits) <- c("count","ncat", "improve","index","adj")
	rownames(splits) <- model$var 
	splits <- splits[complete.cases(splits),]
	csplit <- matrix(3-csplit,nrow = attr(csplit,"nrow"))
	model <- list(frame = model, splits = splits, csplit = csplit)
	return(model)
}

deploy.hpdrpart <- function(model)
{
	if(is.null(model$frame))
		stop("'model' does not have an element called 'frame'")
	if(is.null(model$frame$var))
		stop("'model$frame' does not have an element called 'var'")
	if(is.null(model$frame$n))
		stop("'model$frame' does not have an element called 'n'")
	if(is.null(model$frame$wt))
		stop("'model$frame' does not have an element called 'wt'")
	if(is.null(model$frame$dev))
		stop("'model$frame' does not have an element called 'dev'")
	if(is.null(model$frame$yval))
		stop("'model$frame' does not have an element called 'yval'")
	if(is.null(model$frame$complexity))
		stop("'model$frame' does not have an element called 'complexity'")

	if(is.null(model$splits))
		stop("'model' does not have an element called 'splits'")
	if(is.null(model$splits$count))
		stop("'model$splits' does not have an element called 'count'")
	if(is.null(model$splits$ncat))
		stop("'model$splits' does not have an element called 'ncat'")
	if(is.null(model$splits$improve))
		stop("'model$splits' does not have an element called 'improve'")
	if(is.null(model$splits$index))
		stop("'model$splits' does not have an element called 'index'")
	if(is.null(model$splits$adj))
		stop("'model$splits' does not have an element called 'adj'")

	if(is.null(model$csplit))
		warning("'model' does not have an element called 'csplit'")
	return(model)
}