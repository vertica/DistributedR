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


.computeFeatureAttributes <- function(observations, trace = FALSE)
{
	timing_info <- Sys.time()
	features_min = darray(dim = c(ncol(observations),
		       		  npartitions(observations)),
		     blocks = c(ncol(observations),1),empty=TRUE)
	features_max = darray(dim = c(ncol(observations),
		       		  npartitions(observations)),
		     blocks = c(ncol(observations),1),empty=TRUE)

	foreach(i,1:npartitions(observations), 
		function(observations = splits(observations,i),
			features_min = splits(features_min,i),
			features_max = splits(features_max,i))
		{
			features_min = apply(observations,2,min)
			features_max = apply(observations,2,max)
			features_min = matrix(features_min,ncol = 1)
			features_max = matrix(features_max,ncol = 1)
			update(features_min)
			update(features_max)
		},progress = trace,scheduler = 1)

	features_min = getpartition(features_min)
	features_max = getpartition(features_max)
	features_min = as.numeric(apply(features_min,1,min))
	features_max = as.numeric(apply(features_max,1,max))

	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)
	return(list(features_min,features_max))
}


.initializeDForest <- function(observations, responses,	
	      		ntree, bin_max,
			features_min, 
			features_max,
			features_cardinality,
			response_cardinality,
			features_num,
			weights = NULL,
			replacement = TRUE,
			max_nodes = Inf,
			scale = 1L,
			trace = FALSE)
{
	timing_info <- Sys.time()
	workers = sum(distributedR_status()$Inst)
	hist <- dlist(npartitions = npartitions(observations)*workers)

	null_observations = lapply(1:ncol(observations), function(i)
			  return(integer(0)))
	null_responses = list(integer(0))
	null_indices = lapply(1:ntree, function(i)
			  return(integer(0)))
	null_weights = lapply(1:ntree, function(i)
			  return(numeric(0)))

	forest = .Call("initializeForest",null_observations, null_responses, 
	       ntree, bin_max, as.numeric(features_min), 
	       as.numeric(features_max), 
	       features_cardinality, response_cardinality, 
	       features_num,null_weights,null_indices, as.integer(scale), max_nodes,
	       as.integer(1:ntree),
	       PACKAGE = "HPdclassifier")


	dforest = dlist(npartitions = npartitions(observations))
	oob_indices = dlist(npartitions = npartitions(observations))


	foreach(i,1:npartitions(observations),
		function(observations = splits(observations,i),
			responses = splits(responses,i),
			ntree = ntree, bin_max = bin_max,
			features_min = features_min, 
			features_max = features_max,
			features_cardinality = features_cardinality,
			response_cardinality = response_cardinality,
			features_num = features_num,
			forest = splits(dforest,i),
			oob_indices = splits(oob_indices,i),
			max_nodes = max_nodes,
			replacement = replacement,
			scale = scale,
			init_seed = sample.int(1000,i))
		{
			library(HPdclassifier)
			set.seed(init_seed)
			if(replacement)
				weights = lapply(1:ntree, function(treeID) 
					return(matrix(rpois(nrow(observations),
						1),ncol = 1)))
			else
				weights = lapply(1:ntree,function(i) 
					as.double(rnorm(nrow(observations))<.632))
						
			observation_indices = lapply(weights,
				function(tree_weights)
				which(tree_weights > 0))

			oob_indices = lapply(weights,function(tree_weights)
				which(tree_weights == 0))

			weights = lapply(weights,function(tree_weights)
				tree_weights[which(tree_weights > 0)])

			weights = lapply(weights,as.numeric)
			observation_indices = lapply(observation_indices, 
					    as.integer)
			

			dforest = .Call("initializeForest",
			       observations, responses, 
	       		       as.integer(ntree), as.integer(bin_max),
	       		       as.numeric(features_min), 
			       as.numeric(features_max), 
	       		       as.integer(features_cardinality),
	       		       as.integer(response_cardinality), 
	       		       as.integer(features_num),
			       weights,observation_indices, 
			       as.integer(scale), max_nodes,
			       as.integer(1:ntree),
	       		       PACKAGE = "HPdclassifier")
			forest = list(.Call("serializeForest", dforest))
			.Call("garbageCollectForest",dforest)

			update(oob_indices)
			update(forest)
			update(observations)
		}, progress = trace,scheduler = 1)
	attr(forest,"dforest") <- dforest
	

	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)

	return(list(forest=forest, oob_indices = oob_indices))
}

.computeHistogramsAndSplits <- function(observations, responses, forest, active_nodes, workers, max_nodes, trace = FALSE, hist = NULL)
{

	dforest = attr(forest,"dforest")
	active_nodes = as.darray(matrix(active_nodes,ncol = 1),
		     blocks = c(ceiling(length(active_nodes)/workers),1))
	workers = npartitions(active_nodes)

	if(trace)
	print("computing hists")

	timing_info <- Sys.time()
	foreach(i,1:npartitions(observations),
		function(observations = splits(observations,i),
			responses = splits(responses,i),
			forest = splits(dforest,i),
			active_nodes = splits(active_nodes,as.list(1:workers)),
			hist = splits(hist,as.list(1:workers+(i-1)*workers)),
			random_seed = sample.int(1000,1))
		{
			library(HPdclassifier)
			forest = .Call("unserializeForest", forest[[1]])
			forestparam = .Call("getForestParameters", forest)
			features_num = forestparam[[3]]
			set.seed(random_seed)
			packageData <- function(active_nodes, observations, responses, forest)
			{
				random_features = lapply(active_nodes, function(i)
					sample.int(ncol(observations),features_num))
				hist = .Call("buildHistograms",observations, responses,
			       	        forest, active_nodes, random_features, NULL,
	       		       	        PACKAGE = "HPdclassifier")
				return(hist)
			}

			hist = lapply(active_nodes, packageData, observations = observations, 
			       			    responses = responses, 
						    forest = forest)

			.Call("garbageCollectForest", forest)		       
			update(hist)
		}, progress = trace, scheduler = 1)

	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)

	total_completed = darray(npartitions = workers)
	splits_info = dlist(npartitions = workers)


	forestparam = .Call("getForestParameters",forest)


	if(trace)
	print("computing splits")
	timing_info <- Sys.time()	

	foreach(i, 1:workers,
		   function(splits_info = splits(splits_info,i),
			data_partitions = npartitions(observations),
			total_completed = splits(total_completed,i),
			active_nodes = splits(active_nodes,i),
			hist = splits(hist,
			     as.list((0:(npartitions(observations)-1))*workers + i)),
			features_cardinality = forestparam[[1]],
			response_cardinality = forestparam[[2]],
			bin_num = forestparam[[6]])
		{
			library(HPdclassifier)

			active_nodes = as.integer(active_nodes)
			hist = lapply(1:length(hist[[1]]), function(i) 
			     lapply(1:length(hist[[1]][[1]]), function(j)
			     {
				indiv_hist = apply(sapply(1:length(hist), function(k)
					hist[[k]][[i]][[j]]),1,sum)
				attr(indiv_hist,"feature") <- attr(hist[[1]][[i]][[j]],"feature")
				return(indiv_hist)
			     }))


			splits_info = .Call("computeSplits",hist, active_nodes, 
				features_cardinality,response_cardinality,bin_num,
				NULL,
	       			PACKAGE = "HPdclassifier")
			total_completed = matrix(attr(splits_info,"total_completed"),
					nrow = 1)

			update(splits_info)
			update(total_completed)
		}, progress = trace,scheduler = 1)


	splits_info = getpartition(splits_info)
	total_completed = sum(getpartition(total_completed))
	attr(splits_info,"total_completed") <- total_completed
	active_nodes = as.vector(getpartition(active_nodes))
	active_nodes = .Call("applySplits",forest,splits_info, active_nodes,
	       PACKAGE = "HPdclassifier")

	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)


	return(list(splits_info,active_nodes))
}


.updateDistributedForest <- function(observations, responses, forest, active_nodes, splits_info, trace = FALSE)
{
	timing_info <- Sys.time()
	null_observations = lapply(1:ncol(observations), function(i)
			  return(integer(0)))
	null_responses = list(integer(0))
	.Call("updateNodes",null_observations, null_responses, 
		forest, active_nodes, splits_info,
	        PACKAGE = "HPdclassifier")

	leaf_counts = darray(npartitions = npartitions(observations))
	foreach(i,1:npartitions(observations),
		function(forest = splits(attr(forest,"dforest"),i),
			observations = splits(observations,i),
			responses = splits(responses,i),
			active_nodes = active_nodes,
			splits_info = splits_info,
			leaf_counts = splits(leaf_counts,i))
		{
			library(HPdclassifier)
			dforest = .Call("unserializeForest",forest[[1]])
			
			leaf_counts = .Call("updateNodes",
				observations, responses,
				dforest, active_nodes, splits_info,
	       			PACKAGE = "HPdclassifier")
			leaf_counts = matrix(leaf_counts,nrow = 1)
			forest = list(.Call("serializeForest",dforest))
			.Call("garbageCollectForest",dforest)
			update(forest)
			update(leaf_counts)
		}, progress = trace,scheduler = 1)
	leaf_counts = getpartition(leaf_counts)
	leaf_counts = apply(leaf_counts,2,sum)
	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)
	return(leaf_counts)
	
}


.localizeData <-function(observations, responses, forest, bin_max, 
	      nodes, max_nodes, node_size, tree_ids, 
	      max_nodes_per_iteration, trace = FALSE,
	      data_local=NULL)
{
	workers = length(nodes)

	if(trace)
	print("allocating temporary dobjects")
	timing_info <- Sys.time()
	data_local <- dlist(npartitions=workers*npartitions(observations))
	timing_info <- timing_info - Sys.time()
	if(trace)
	print(timing_info)


	if(trace)
	print("shuffling data")
	timing_info <- Sys.time()
	if(trace)
	print(distributedR_status())
	foreach(i, 1:npartitions(observations), 
		   function(		   
		   forest = splits(attr(forest,"dforest"),i),
		   observations = splits(observations,i),
		   responses = splits(responses,i),
		   nodes = nodes,
		   data_local = splits(data_local,
		   	         as.list(1:workers +(i-1)*workers)))

		   {
			library(HPdclassifier)
			forest = .Call("unserializeForest",forest[[1]])
			weights_local = .Call("getLeafWeights",forest);
			observations_indices_local = .Call("getLeafIndices",forest);

			loadData <- function(nodes, observations, responses, 
				 weights_local, observations_indices_local)
		   	{

			weights_local = weights_local[nodes]
			observations_indices_local = 
				observations_indices_local[nodes]

			total_indices = unlist(observations_indices_local)
			total_indices = unique(total_indices)
			observations_local = observations[total_indices,]
			observations_local = data.frame(observations_local)
			names(observations_local) <- rep(" ",ncol(observations_local))
			responses_local = responses[total_indices,]
			responses_local = data.frame(responses_local)
			names(responses_local) <- rep(" ", ncol(responses_local))
			
			observations_indices_local = 
				lapply(observations_indices_local,
				function(indices) 
				match(indices,total_indices))
			local_data = list(observations_local=observations_local, 
				   responses_local = responses_local, 
				   weights_local = weights_local, 
				   observations_indices_local = observations_indices_local)
			return(local_data)
			}

			data_local = lapply(nodes, loadData, 
				      observations = observations, 
				      responses = responses, 
				      weights_local = weights_local,
				      observations_indices_local)
			update(data_local)
			.Call("garbageCollectForest",forest)
			gc()			
		   }, progress = trace, scheduler = 1)

		 
	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)
	if(trace)
	print(distributedR_status())

	if(trace)
	print("building subtrees")
	timing_info <- Sys.time()

	forestparam = .Call("getForestParameters",forest)

	dforest_temp = dlist(npartitions = workers)
	foreach(i,1:workers, function(
			dforest = splits(dforest_temp,i),
     		       	features_cardinality = forestparam[[1]],
		       	response_cardinality = forestparam[[2]],
		       	bin_max = bin_max,
			features_num = forestparam[[3]],
		       	features_min = forestparam[[4]],
		       	features_max = forestparam[[5]],
			data_local = splits(data_local,
				as.list(i + workers*
				(0:(npartitions(observations)-1)))),
			tree_ids = tree_ids[[i]],
			max_nodes = max_nodes[tree_ids[[i]]],
			node_size = node_size,
			nparts = npartitions(observations),
			max_nodes_per_iteration = max_nodes_per_iteration,
			hpdRF_local = .hpdRF_local,
			random_seed = sample.int(1000,i))
      {
		library(HPdclassifier)
		set.seed(random_seed)
		
		observations_local = lapply(data_local, 
				   function(x) x$observations_local)
		responses_local = lapply(data_local, 
				   function(x) x$responses_local)
		weights_local = lapply(data_local, 
			      	   function(x) x$weights_local)
		observations_indices_local = lapply(data_local, 
				   function(x) x$observations_indices_local)
		rm(data_local)



		observations = do.call(rbind, observations_local)
		responses = do.call(rbind, responses_local)

		offset = sapply(observations_local, function(obs_local) 
		       if(is.null(obs_local)) 0 else nrow(obs_local))
		offset = c(0,cumsum(offset[-length(observations_local)]))

		weights = lapply(1:length(weights_local[[1]]), function(i)
			do.call(c,lapply(1:length(weights_local), function(j)
				weights_local[[j]][[i]])))
		observations_indices_local = 
		lapply(1:length(observations_indices_local[[1]]), function(i)
			lapply(1:length(observations_indices_local), function(j)
				observations_indices_local[[j]][[i]]))

		observations_indices = lapply(observations_indices_local, 
			function(obs_local) do.call(c,
			lapply(1:length(obs_local), function(partition_id) 
			{
				if(length(obs_local[[partition_id]]) > 0)
					return(obs_local[[partition_id]]+
					offset[partition_id])
				return(integer(0))
			})))
		tree_ids = as.integer(1:length(tree_ids))

		forest = hpdRF_local(observations, responses, 
			length(weights), as.integer(bin_max), features_cardinality, 
			response_cardinality, features_num, 
			node_size = node_size,
			weights, observations_indices, 
			features_min, features_max, 
			max_nodes = as.integer(max_nodes),
			tree_ids = as.integer(tree_ids), 
			max_nodes_per_iteration = max_nodes_per_iteration)

		
		dforest = list(.Call("serializeForest",forest))
		.Call("garbageCollectForest",forest)
		update(dforest)
		gc()
     	},progress = trace, scheduler = 1)


	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)


	if(trace)
	print("gathering forest on master")
	timing_info <- Sys.time()
	

	dforest_temp = lapply(1:npartitions(dforest_temp), 
		     function(id) 
		     .Call("unserializeForest",getpartition(dforest_temp,id)[[1]]))
	invisible(.Call("stitchForest", forest, dforest_temp, nodes))
	

	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)

}


.predictOOB <- function(forest, observations, responses, oob_indices, 
	    cutoff, classes, trace)
{
	timing_info <- Sys.time()
	oob_predictions = dframe(npartitions = npartitions(observations))
	sse = darray(npartitions = npartitions(observations))
	err.count = darray(npartitions = npartitions(observations))
	L0 = darray(npartitions = npartitions(observations))
	L1 = darray(npartitions = npartitions(observations))
	L2 = darray(npartitions = npartitions(observations))
	temp_forest = .Call("serializeForest",forest,PACKAGE="HPdclassifier")
	class_count = darray(npartitions = npartitions(observations))

	dforest = dlist(npartitions = 1)
	oob_dobject<- function(forest = splits(dforest,1))
	{
		forest = list()
		for(i in 1:get("forest_chunks"))
	      	      forest[[i]] <- get(paste("forest",i,"_temp",sep="")) 
		update(forest)
	}
	formals(oob_dobject)[["forest_chunks"]] = length(temp_forest)
	for(i in 1:length(temp_forest))
	      formals(oob_dobject)[[paste("forest",i,"_temp",sep="")]] = temp_forest[[i]]
	foreach(i,1,oob_dobject,progress = trace)


	foreach(i,1:npartitions(observations), function(
		observations = splits(observations,i),
		responses = splits(responses,i),
		oob_indices = splits(oob_indices,i),
		oob_predictions = splits(oob_predictions,i),
		cutoff = cutoff, classes = classes,
		err.count = splits(err.count,i),
		sse = splits(sse,i),
		L0 = splits(L0,i),
		L1 = splits(L1,i),
		L2 = splits(L2,i),
		class_count = splits(class_count,i),
		forest = splits(dforest,1),
		i = i)
		{

			library(HPdclassifier)
			forest = .Call("unserializeForest",forest, 
			       	    PACKAGE = "HPdclassifier")  

			forestparam = .Call("getForestParameters", forest,  
				    PACKAGE = "HPdclassifier")

			features_min = forestparam[[4]]
			features_max = forestparam[[5]]
			bin_num = forestparam[[6]]


			features_cardinality = forestparam[[1]]
			numeric_variables = which(is.na(features_cardinality))
			categorical_variables = 
					which(!is.na(features_cardinality))

			observations[,numeric_variables] = 
					lapply(numeric_variables, function(var)
				 	(observations[,var]+0.5)/
					bin_num[var]*
					(features_max[var]-features_min[var])+
					features_min[var])
			observations[,categorical_variables] = 
					lapply(categorical_variables,
					function(var)
					observations[,var]+1)
			
			predictions = matrix(as.numeric(NA), 
				    ncol = nrow(observations),
				    length(oob_indices))

			temp_predictions = lapply(1:length(oob_indices),
			function(tree_id)
			{
				tree_oob_indices = oob_indices[[tree_id]]
				tree_predictions = sapply(tree_oob_indices,
					function(obs)
					.Call("specificTreePredictObservation", 
					forest, tree_id,
					observations, 
					as.integer(obs),
					PACKAGE = "HPdclassifier"))
				return(cbind(rep(tree_id,
						length(tree_oob_indices)),
					tree_oob_indices,as.numeric(tree_predictions)))
			})

			for(i in 1:length(temp_predictions))
			{
			      predictions[temp_predictions[[i]][,c(1,2)]] = 
			      		temp_predictions[[i]][,3]
			}

			ntree = length(oob_indices)
			err.count = matrix(as.integer(0),nrow=1,
				  ncol = nrow(predictions)*length(classes))
			class_count = matrix(as.integer(0),ncol = ntree*length(classes),nrow = 1)

			sse = matrix(as.numeric(0),
				  ncol = nrow(predictions),nrow = 1)
			L0 = matrix(as.integer(0),ncol = ntree,nrow = 1)
			L1 = matrix(as.double(0),ncol = ntree,nrow = 1)
			L2 = matrix(as.double(0),ncol = ntree,nrow = 1)

			oob_predictions = .Call("cumulativePredictions",
					predictions, responses,  
					as.numeric(cutoff), classes, 
					err.count, class_count, sse, L0, L1, L2,
					PACKAGE = "HPdclassifier")
			if(length(classes) > 0)
			{
				oob_predictions = factor(oob_predictions, 
						levels = classes)
				update(class_count)
			}
			oob_predictions = data.frame(oob_predictions)
			.Call("garbageCollectForest",forest)

			update(oob_predictions)
			update(err.count)
			update(sse)
			update(L0)
			update(L1)
			update(L2)
		},progress = trace)



	colnames(oob_predictions) <- "oob_predictions"
	sse = apply(getpartition(sse),2,sum, na.rm = TRUE)
	err.count = apply(getpartition(err.count),2,sum)
	L0 = apply(getpartition(L0),2,sum)
	L1 = apply(getpartition(L1),2,sum)
	L2 = apply(getpartition(L2),2,sum)


	err.rate = 0
	if(length(classes)>0)
	{
		class_count = apply(getpartition(class_count),2,sum)
		class_count = matrix(class_count,nrow = length(classes))
		sum_class_count = apply(class_count,2,sum)
		err.count = matrix(err.count, ncol = length(classes))
		total_err.count = matrix(apply(err.count,1,sum),ncol = 1)
		weighted_err.rate = matrix(sapply(1:ncol(err.count),
			function(x) err.count[,x]/class_count[x,]),
			ncol = ncol(err.count))
		weighted_total_err.rate = matrix(apply(total_err.count,2,
				function(x) x/sum_class_count),ncol=1)
		err.rate = cbind(weighted_total_err.rate,weighted_err.rate)
		colnames(err.rate) <- c("OOB", classes)
	}
	mean_response = L1/L0
	var_response = L2/L0 - mean_response*mean_response;
	mse = sse/L0
	rsq = 1 - mse/var_response


	timing_info <- Sys.time() - timing_info
	if(trace)
	print(timing_info)

	return(list(oob_predictions = oob_predictions, err.rate = err.rate, rsq = rsq, mse = mse))
}

.hpdRF_distributed <- function(observations, responses, ntree, bin_max, 
      features_cardinality, response_cardinality, features_num,
      threshold=60000, weights=NULL, nodes_per_worker = 10, 
      max_nodes = .Machine$integer.max, node_size = 1, 
      replacement = TRUE, cutoff , classes, completeModel = FALSE, 
      max_nodes_per_iteration =  .Machine$integer.max, 
      trace = FALSE, features_min = NULL, features_max = NULL, scale = 1L)
{
	gc()
	if(trace)
	print(distributedR_status())
	workers = sum(distributedR_status()$Inst)
	threshold = max(threshold, node_size)

	if(trace)
	print("computing feature min/max")
	if(is.null(features_min) | is.null(features_min))
	{
		features_attributes = .computeFeatureAttributes(observations,trace)
		if(is.null(features_min))
			features_min = features_attributes[[1]]
		if(is.null(features_max))
			features_max = features_attributes[[2]]
		if(any(is.infinite(features_min)) | any(is.infinite(features_max)))
			stop("Infinite Values and/or NaN are not supported")
	}
	gc()
	if(nrow(observations) > threshold)
		active_nodes = as.integer(1:ntree)
	else
		active_nodes = integer(0)
	if(trace)
	print(distributedR_status())

	if(trace)
	print("initializing")
	initparam = .initializeDForest(observations, responses, ntree, bin_max, 
		features_min, features_max, features_cardinality,
		response_cardinality, features_num, weights, 
		replacement, max_nodes, scale, 
		trace)
	if(trace)
	print(distributedR_status())

	forest = initparam$forest
	oob_indices = initparam$oob_indices
	forestparam = .Call("getForestParameters", forest)

	leaf_nodes = rep(threshold+1,ntree)	
	max_nodes = .Call("getMaxNodes", forest)
	hist = dlist(npartitions = npartitions(observations)*workers)
	gc()
	while(length(active_nodes) > 0 & any(max_nodes > 0))
	{	
		if(length(active_nodes) > max_nodes_per_iteration)
		{
			active_nodes = sample(active_nodes,
				     as.integer(max_nodes_per_iteration))
			active_nodes = active_nodes[order(active_nodes)]
		}
		if(trace)
		print("computing splits from hists")
		result = .computeHistogramsAndSplits(observations, 
			   responses, forest, active_nodes,workers, max_nodes,
			   trace, hist)
		active_nodes = result[[2]]
		splits_info = result[[1]]
		
		if(trace)
		print("updating nodes with splits") 
		leaf_nodes = .updateDistributedForest(observations, responses,
			   forest, active_nodes, splits_info, trace)
		leaf_attempted = .Call("getAttemptedNodes",forest)
		active_nodes = which(leaf_nodes > threshold & leaf_attempted == 0)
		max_nodes = .Call("getMaxNodes", forest)
		gc()
	}

	if(trace)
	print("local tree building")

	num_leaf = .Call("numLeafNodes",forest,package="HPdclassifier")
	if(num_leaf > 0)
	{
		nodes = 1:num_leaf
		leaf_attempted = .Call("getAttemptedNodes",forest)
		nodes = which(leaf_attempted == 0)
		nodes = nodes[order(leaf_nodes[nodes],decreasing = TRUE)]

		tree_ids = .Call("getTreeIDs", forest)
		tree_counts = table(tree_ids)
		tree_names = as.numeric(names(tree_counts))
		temp_count = rep(0,ntree)
		temp_count[tree_names] = tree_counts
		max_nodes = .Call("getMaxNodes", forest)/temp_count

		while(length(nodes) > 0)
		{
			if(trace)
			print(paste("nodes remaining:", length(nodes)))

			active_nodes = as.list(nodes[1:min(workers,length(nodes))])
			nodes = nodes[-1:-min(workers,length(nodes))]
			sum_obs = sapply(active_nodes,function(nodes) sum(leaf_nodes[nodes]))
			index = 1
			while(index <= length(nodes))
			{
				next_worker = which.min(sum_obs)
				while(index <= length(nodes)) 
				{
					if((sum_obs[next_worker] + leaf_nodes[nodes[index]]) < 
						(threshold * nodes_per_worker))
						break
					index = index + 1
				}
				if(index <= length(nodes))
				{
					active_nodes[[next_worker]] = 
					matrix(c(active_nodes[[next_worker]], nodes[[index]]))
					nodes = nodes[-index]
					sum_obs = sapply(active_nodes,
						function(nodes) sum(leaf_nodes[nodes]))
				}
			}
			
			if(trace)
			print(paste("Building nodes: ",toString(active_nodes)))

			active_tree_ids = lapply(active_nodes, function(nodes) tree_ids[nodes])

			.localizeData(observations, 
				responses, forest, bin_max, 
				active_nodes,
				max_nodes, node_size, 
				active_tree_ids,
				max_nodes_per_iteration,
				trace,
				data_local)
			gc()
		}
	}


	attr(forest,"dforest") <- NULL
	return(list(forest=forest, features_min = features_min, 
				   features_max = features_max, 
				   oob_indices = oob_indices))
}


.hpdRF_local <- function(observations, responses, ntree, bin_max, 
      features_cardinality, response_cardinality, features_num,
      node_size=1, weights=NULL, observation_indices=NULL, 
      features_min = NULL, features_max = NULL, max_nodes = Inf,
      tree_ids = NULL, max_nodes_per_iteration = .Machine$integer.max, 
      trace = TRUE)
{
	nrow = nrow(observations)
	ncol = ncol(observations)
	if(is.null(weights))
		weights = data.frame(matrix(1, 
			  	nrow = nrow, ncol = ntree))
	if(is.null(observation_indices))
		observation_indices = data.frame(matrix(1:nrow, 
				    nrow = nrow, ncol = ntree))

	scale = as.integer(0)
	if(is.null(features_min) || is.null(features_max))
		scale = as.integer(1)	
	if(is.null(features_min))
		features_min = apply(observations,2,min)
	if(is.null(features_max))
		features_max = apply(observations,2,max)
	if(any(is.infinite(features_min)) | any(is.infinite(features_max)))
		stop("Infinite Values and/or NaN are not supported")

	ntree = as.integer(ntree)
	bin_max = as.integer(bin_max)
	features_min = as.numeric(features_min)
	features_max = as.numeric(features_max)
	features_cardinality = as.integer(features_cardinality)
	response_cardinality = as.integer(response_cardinality)
	features_num = as.integer(features_num)
	weights = lapply(weights,as.numeric)
	observation_indices = lapply(observation_indices,as.integer)
	tree_ids = as.integer(tree_ids)
	node_size = as.integer(node_size)
	forest = .Call("hpdRF_local",observations, responses, ntree, bin_max,
			features_cardinality, response_cardinality,
			features_num, node_size, weights, observation_indices,
			features_min, features_max, max_nodes, tree_ids, 
			max_nodes_per_iteration, trace, scale)
	return(forest)
}



.predict.hpdRF_distributedForest <- function(forest, new_observations, 
				cutoff = NA, classes, trace = FALSE)
{

	predictions = dframe(npartitions = npartitions(new_observations))
	if(trace)
	print("predicting observations")

	dforest = dlist(npartitions = 1)
	oob_dobject<- function(forest = splits(dforest,1))
	{
		forest = list()
		for(i in 1:get("forest_chunks"))
	      	      forest[[i]] <- get(paste("forest",i,"_temp",sep="")) 
		update(forest)
	}
	formals(oob_dobject)[["forest_chunks"]] = length(forest$trees)
	for(i in 1:length(forest$trees))
	      formals(oob_dobject)[[paste("forest",i,"_temp",sep="")]] = forest$trees[[i]]
	foreach(i,1,oob_dobject,progress = trace)


	foreach(i,1:npartitions(new_observations), function( 
		forest = splits(dforest),
		new_observations = splits(new_observations,i),
		predictions = splits(predictions,i),
		cutoff = cutoff, classes = classes)
	{
		library(HPdclassifier)
		forest = .Call("unserializeForest",forest)
		forestparam=.Call("getForestParameters", forest)
		response_cardinality = forestparam[[2]]
		ntree = forestparam[[7]]

		predictions = sapply(1:nrow(new_observations), function(index)
			sapply(1:ntree, function(tree_id)
			       as.numeric(.Call("specificTreePredictObservation",
					forest, as.integer(tree_id), 
			      	      	new_observations, 
			      		as.integer(index),
		     			PACKAGE = "HPdclassifier"))))

		if(is.na(response_cardinality))
		{
			predictions = data.frame(apply(predictions,2,mean))
		}
		if(!is.na(response_cardinality))
		{
			k = as.integer(response_cardinality)
			if(is.na(cutoff))
				cutoff = rep(1/k,k)
			cutoff = as.numeric(cutoff)
			predictions = .Call("combineVotesClassification",
				 predictions, cutoff, k,
				 PACKAGE = "HPdclassifier")
			predictions = data.frame(factor(classes[predictions], 
				    levels = classes))
		}
		update(predictions)
		.Call("garbageCollectForest",forest)
	}, progress = trace)

	return(predictions)
}

hpdRF_local_test <- .hpdRF_local