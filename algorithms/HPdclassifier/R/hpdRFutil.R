.distributeForest <- function(new_forest, prev_dforest)
{
	if(missing(prev_dforest))
	prev_dforest = dlist(npartitions = sum(distributedR_status()$Inst))
	trees_per_partition = darray(npartitions = npartitions(prev_dforest))
	foreach(i,1:npartitions(trees_per_partition), 
	function(ntree = splits(trees_per_partition,i),
		 dforest = splits(prev_dforest,i))
	{
		ntree = matrix(max(0,length(which(sapply(dforest,length)>0))-1))
		update(ntree)
	},progress = FALSE)
	trees_per_partition = getpartition(trees_per_partition)
	prev_ntree = sum(trees_per_partition)
	temp_forest = .Call("serializeForest",new_forest,PACKAGE="HPdclassifier")
	.Call("garbageCollectForest",new_forest)
	ntree = length(temp_forest)
	assignment = lapply(1:npartitions(prev_dforest),function(x) integer(0))
	while(ntree > 1)
	{
		smallest_partition = which.min(trees_per_partition)
		assignment[[smallest_partition]] = 
			c(assignment[[smallest_partition]],ntree)
		trees_per_partition[[smallest_partition]]=
			trees_per_partition[[smallest_partition]]+1
		ntree <- ntree - 1
	}
	trees = lapply(assignment, function(ids) temp_forest[ids])
	ntree = length(temp_forest)+prev_ntree
	assignment = lapply(assignment, function(x) x+prev_ntree)

	foreach(i,1:npartitions(prev_dforest), 
		function(dforest = splits(prev_dforest,i),
			  forest_header = temp_forest[[1]],
			  tree_ids = assignment[[i]],
			  trees = trees[[i]],
			  ntree = ntree)
	{
		new_dforest =  vector(mode = "list", length = ntree)
		new_dforest[[1]] <- forest_header
		new_dforest[[1]][[1]] <- as.integer(ntree-1)
		if(length(dforest) > 1)
			new_dforest[2:length(dforest)] = dforest[2:length(dforest)]
		dforest <- new_dforest		
		if(length(tree_ids)>0)
			dforest[tree_ids] <- trees
		update(dforest)

	},progress = FALSE)
	rm(temp_forest)
	rm(trees)
	return(prev_dforest)

}

.combineDistributedForests <- function(forest1,forest2)
{
	if(npartitions(forest1) < npartitions(forest2))
	{
		temp = forest1
		forest1 = forest2
		forest2 = temp
	}
	nparts = npartitions(forest1)
	forest = dlist(npartitions = nparts)
	valid_index <- function(i,dobj)
	{
		if(i <= npartitions(dobj))
		     	return(list(i))
		else
			return(list())
	}
	foreach(i,1:npartitions(forest),
		function(forest1 = splits(forest1,i),
			forest2 = splits(forest2,valid_index(i,forest2)))
		{
			if(length(forest2) > 0)
			{
				ntree = as.integer(length(forest1[[1]])+length(forest2[[1]])-2)
				forest1 = forest1[[1]]
				forest2 = forest2[[1]]
				forest1 = c(forest1,forest2[-1])
				forest1[[1]][[1]] <- ntree
				update(forest1)
			}
		},progress = FALSE)
	return(forest1)
}

.gatherDistributedForest <- function(dforest)
{
	forest = lapply(1:npartitions(dforest), function(i)
	       getpartition(dforest,i))
	forest <- .Call("gatherForest",forest)			
}

.redistributeForest<- function(dforest, tree_ids)
{
	tree_ids = tree_ids[sapply(tree_ids,length) > 0]
	old = npartitions(dforest)
	new = length(tree_ids)
	new_dforest = dlist(npartitions = new)
	temp_dforest = dlist(npartitions = old*new)
	foreach(i, 1:old, 
		function(dforest = splits(dforest,i),
		temp_dforest = splits(temp_dforest,
		   	as.list(1:new + new*(i-1))),
		tree_ids = tree_ids)
		{
			temp_dforest = lapply(tree_ids, function(ids)
				dforest[c(1,ids+1)])
			update(temp_dforest)
		},progress = FALSE)
	rm(dforest)
	gc()

	start_ids = sapply(tree_ids, function(x) length(x))
	start_ids = c(0,cumsum(start_ids))
	end_ids = start_ids[-1]
	start_ids = start_ids[-length(start_ids)]+1
	
	foreach(i,1:new, 
		function(temp_dforest = splits(temp_dforest,
			as.list((1:old-1)*new + i)),
			new_dforest = splits(new_dforest,i),
			ntree = length(unlist(tree_ids)),
			start = start_ids[i]+1,
			end = end_ids[i]+1)
		{
			header = temp_dforest[[1]][[1]]
			for(i in 1:length(temp_dforest))
			      temp_dforest[[i]][[1]] = NULL
			temp_dforest = do.call(c,temp_dforest)
			temp_dforest=temp_dforest[!sapply(temp_dforest,is.null)]
			new_dforest = vector(mode = "list", length = ntree+1)
			new_dforest[[1]] = header
			new_dforest[start:end] = temp_dforest
			new_dforest[[1]][[1]] = ntree
			update(new_dforest)
		},progress = FALSE)
	return(new_dforest)
} 

d.object.size <- function(object)
{
	if(!inherits(object,"dlist") && 
	   !inherits(object,"darray") && 
	   !inherits(object, "dframe") && 
	   !class(object) == "list")
	   return(0)

	if(class(object) == "list")
	return(sum(rapply(object,d.object.size,
		classes =  c("dlist","dframe","darray"),how = "unlist")))

	nparts = npartitions(object)
	size_darray <- darray(npartitions = nparts)
	foreach(i,1:nparts, function(object = splits(object,i),
			    size_array = splits(size_darray,i))
			    {
				size_array <- matrix(as.numeric(object.size(object)))
				update(size_array)
			    },progress = FALSE)
	size_darray <- sum(getpartition(size_darray))
	return(size_darray)
}

d.environment.size <- function(e = environment())
{
	objects <- ls(e)
	objects_size <- sapply(objects,function(object) 
		     d.object.size(get(object)))
	return(data.frame(objects = objects, size = objects_size))
}

.master_output = function(..., appendLF = TRUE)
{
	cat(...)
	if(appendLF)
		cat("\n")
}