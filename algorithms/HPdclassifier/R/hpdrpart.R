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
	 completeModel = TRUE, nBins = 256L, do.trace = FALSE)
{

	if(!identical(na.action, na.exclude) &
		!identical(na.action, na.omit) &
		!identical(na.action, na.fail))
		stop("'na.action' must be either na.exclude, na.omit, na.fail")

	if(missing(weights))
		weights = NULL
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
	if(is.data.frame(data))
		data = as.dframe(data)
	if(attr(data,"npartitions")[2] > 1)
		stop("'data' must be partitioned rowise")

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
	x_colnames = variables$x_colnames



	tree <- .hpdRF_distributed(observations, responses, ntree = 1L, 
	     bin_max = as.integer(nBins), 
	     features_cardinality = features_cardinality,
	     response_cardinality = response_cardinality, 
	     features_num = as.integer(ncol(observations)),
	     threshold = 1L, weights = weights, nodes_per_worker=1L,
	     max_nodes = .Machine$integer.max, node_size = control$minsplit, 
	     replacement = TRUE, cutoff = 1L, classes = classes, 
	     completeModel = FALSE, 
	     max_nodes_per_iteration = .Machine$integer.max,
	     trace = do.trace, features_min = NULL, features_max = NULL,
	     min_split = control$minbucket, max_depth = control$maxdepth, 
	     cp = control$cp)
	     
	model = convertToRpartModel(tree$forest, x_colnames)
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
	if(length(classes) > 0)
	{
		model$numresp = length(classes[[1]])
		model$classes = classes[[1]]
	}
	class(model) <- c("hpdrpart","rpart")

	if(is.data.frame(data))
		responses = getpartition(responses)
	model$variable.importance <- 
		varImportance(model,data,responses)
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

convertToRpartModel <- function(tree, varnames)
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
