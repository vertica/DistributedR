

.buildSubTreesLocally <- function(observations, responses, 
	forest, nodes, max_time = 300)
{
	workers = length(nodes)
	data_local = dlist(npartitions = npartitions(observations)*workers)

	foreach(i, 1:npartitions(observations), function(
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
	}, progress = TRUE, scheduler = 1)
	print("here2")
	local_tree <- .train_tree_locally(forest, data_local, nodes,max_time)
	return(local_tree)
}


#x,y,weights,indices are dobjects
.train_tree_locally <- function(tree, data_local, nodes, max_time = 300)
{
	workers = length(nodes)
	cores <- sum(distributedR_status()$Inst)
	dnodes <- .buildLocalNodes(tree, data_local, max_time, 
	       workers, combine = TRUE)

	local_tree = .initializeLocalNodes(dnodes)
	leaf_table <- .attachLocalNodes(local_tree, NULL, NULL)
	redistribution_info <- .assignNodes(leaf_table, cores, workers)
	nodes <- redistribution_info$nodes 
	leaf_table <- redistribution_info$leaf_table 
	workers <- redistribution_info$workers
	.Call("printForest",local_tree,NULL,NULL)

	while(!is.null(nodes))
	{
		data_local <- .loadNodeData(dnodes, data_local, nodes)
		dnodes <- .buildLocalNodes(tree, data_local, max_time, workers,
		       combine = FALSE)
		leaf_table <- .attachLocalNodes(local_tree, dnodes, leaf_table)
		redistribution_info <- .assignNodes(leaf_table, cores, workers)
		nodes <- redistribution_info$nodes 
		leaf_table <- redistribution_info$leaf_table 
		workers <- redistribution_info$workers
		gc()
	}
	#.Call("printForest",local_tree,NULL,NULL)
	return(local_tree)
}
.initializeLocalNodes <- function(dnodes)
{
	local_tree = lapply(1:npartitions(dnodes), function(i) 
		   .Call("unserializeForest",getpartition(dnodes,i)[[1]]))

	
	if(length(local_tree) == 1)
	{
		local_tree = local_tree[[1]]
	}
	if(length(local_tree)>1)
	{
		for(i in 2:length(local_tree))
		{
		      .Call("mergeCompletedForest",
		      local_tree[[1]],local_tree[[i]])
		}
		local_tree = local_tree[[1]]
	}
	return(local_tree)
}

.loadNodeData <- function(tree, data_local_old, nodes, trace = TRUE)
{

	if(trace)
	print("loading data")

	workers = length(nodes)
	new_workers = length(nodes[[1]])
	input_partitions = npartitions(data_local_old)/workers
	data_local_new <- dlist(npartitions=workers*new_workers)

	foreach(i, 1:workers, 
		   function(		   
		   tree = splits(tree,i),
		   data_local_old = splits(data_local_old,
		   	         as.list(i + workers*(1:input_partitions-1))),
		   data_local_new = splits(data_local_new,
		   	         as.list(1:new_workers +(i-1)*new_workers)),
		   nodes = nodes[[i]],
		   i = i)
		   {
			library(HPdclassifier)
			tree = .Call("unserializeForest",tree[[1]])
			observations = do.call(rbind,lapply(data_local_old, 
				   function(x) x$observations_local))
			responses = do.call(rbind,lapply(data_local_old, 
				   function(x) x$responses_local))


			observations_indices_local = 
				.Call("getLeafIndices",tree);
			weights_local = .Call("getLeafWeights",tree);

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
			names(observations_local) <- 
				rep(" ",ncol(observations_local))
			responses_local = responses[total_indices,]
			responses_local = data.frame(responses_local)
			names(responses_local) <- 
				rep(" ", ncol(responses_local))
			
			observations_indices_local = 
				lapply(observations_indices_local,
				function(indices) 
				match(indices,total_indices))
			local_data = list(observations_local=
						observations_local, 
				   responses_local = 
				   		responses_local, 
				   weights_local = 
				   		weights_local, 
				   observations_indices_local = 
				   		observations_indices_local)
			return(local_data)
			}

			data_local_new = lapply(nodes, loadData, 
				      observations = observations, 
				      responses = responses, 
				      weights_local = weights_local,
				      observations_indices_local)
			update(data_local_new)
			.Call("garbageCollectForest",tree)
			gc()	
		   }, progress = FALSE, scheduler = 1)

	return(data_local_new)
}

.buildLocalNodes <- function(tree, data_local, max_time, workers,
		 node_size = 1, max_nodes_per_iteration = 100, 
		 trace = TRUE, combine = TRUE)
{
	if(trace)
	print("building nodes")

	forestparam = .Call("getForestParameters",tree)
	dnodes = dlist(npartitions = workers)
	input_partitions = npartitions(data_local)/workers

	temp = sapply(1:npartitions(data_local),
		function(x) length(getpartition(data_local,x)$weights_local))

	foreach(i,1:workers, function(
			dnodes = splits(dnodes,i),
     		       	features_cardinality = forestparam[[1]],
		       	response_cardinality = forestparam[[2]],
			features_num = forestparam[[3]],
		       	features_min = forestparam[[4]],
		       	features_max = forestparam[[5]],
		       	bin_max = forestparam[[6]],
			data_local = splits(data_local,
				as.list(i + workers*
				(1:input_partitions-1))),
			node_size = node_size,
			nparts = input_partitions,
			max_nodes_per_iteration = max_nodes_per_iteration,
			hpdRF_local = .hpdRF_local,
			max_time = max_time,
			combine = combine,
			tree_ids = i,
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

		if(combine)
		{
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
		}
		if(!combine)
		{
		weights = unlist(weights_local,recursive = FALSE)
		observations_indices = lapply(
			1:length(observations_indices_local), 
			function(x) lapply(observations_indices_local[[x]], 
			function(y) y + offset[x]))
		observations_indices = unlist(observations_indices_local,
				     recursive = FALSE)
		}

		max_nodes = as.integer(rep(100,tree_ids))
		tree_ids = as.integer(rep(tree_ids,length(weights)))
		nodes = hpdRF_local(observations, responses, 
			length(weights), as.integer(bin_max), 
			as.integer(features_cardinality), 
			as.integer(response_cardinality), 
			features_num, 
			node_size = node_size,
			weights, observations_indices, 
			features_min, features_max, 
			max_nodes = as.integer(max_nodes),
			tree_ids = as.integer(tree_ids), 
			max_nodes_per_iteration = 
				as.integer(max_nodes_per_iteration),
			max_time = max_time)

		dnodes = list(.Call("serializeForest",nodes))
		.Call("garbageCollectForest",nodes)
		update(dnodes)
		gc()
     	},progress = FALSE, scheduler = 1)

	return(dnodes)
}

.attachLocalNodes <- function(tree, dnodes, leaf_table, trace = TRUE)
{
	if(trace)
	print("attaching nodes")
	if(!is.null(dnodes))	
	{
		temp_nodes = lapply(1:npartitions(dnodes), function(x)
		   	   .Call("unserializeForest",getpartition(dnodes,x)[[1]]))
		temp_nodes_indices = lapply(1:length(temp_nodes),function(x) 
			   as.integer(leaf_table[which(leaf_table[,1]==x),2]))
		tree_indices = lapply(1:length(temp_nodes),function(x) 
			   as.integer(leaf_table[which(leaf_table[,1]==x),3]))

		.Call("stitchForest",tree, temp_nodes, 
			tree_indices, temp_nodes_indices)
	
	}		
	leaf_table <- .Call("getLeafTreeIDs",tree)
	leaf_table <- cbind(leaf_table,.Call("getLeafIDs",tree))
	leaf_table <- cbind(leaf_table,1:nrow(leaf_table))
	leaf_table <- cbind(leaf_table, .Call("getLeafCounts",tree))
	return(leaf_table)
}

.assignNodes <- function(leaf_table, cores, prev_workers, trace = TRUE)
{
	if(trace)
	print("assigning nodes")
	if(nrow(leaf_table) == 0)
		return(NULL)

	temp_leaf_table = leaf_table[order(leaf_table[,4],decreasing = TRUE),]
	input_partitions = prev_workers
	workers = min(cores,nrow(leaf_table))
	nodes_temp = lapply(1:input_partitions, 
		   function(x) vector('list',workers))
	counts = rep(0,workers)
	indices = rep(1,workers)

	for(i in 1:nrow(temp_leaf_table))
	{
		worker = which.min(counts)
		counts[[worker]] = counts[[worker]]+temp_leaf_table[i,4]
		nodes_temp[[temp_leaf_table[i,1]]][[worker]] <- 
			c(nodes_temp[[temp_leaf_table[i,1]]][[worker]],
			temp_leaf_table[i,2])
	}

	new_leaf_table = NULL
	if(length(nodes_temp) > 0)
	{
	for(i in 1:length(nodes_temp))
	{
		if(length(nodes_temp[[i]]) > 0)
		{
		for(j in 1:length(nodes_temp[[i]]))
		{
			if(length(nodes_temp[[i]][[j]]) > 0)
			{
			for(k in 1:length(nodes_temp[[i]][[j]]))
			{
				leaf_index = which(leaf_table[,1] == i & 
					   leaf_table[,2] == 
					   nodes_temp[[i]][[j]][[k]])

				new_leaf_table = rbind(new_leaf_table,
				c(j,indices[j], leaf_table[leaf_index,3]))
				indices[j] <- indices[j] + 1
			}
			}
		}
		}
	}
	}
	return(list(nodes = nodes_temp, 
			  leaf_table = new_leaf_table, 
			  workers = workers))
}

