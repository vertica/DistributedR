.distributeForest <- function(forest)
{
	temp_forest = .Call("serializeForest",forest,PACKAGE="HPdclassifier")
	.Call("garbageCollectForest",forest)

	ntree = length(temp_forest) - 1
	dforest = dlist(npartition = min(ntree,sum(distributedR_status()$Inst)))
	trees_per_partition <- floor(ntree/npartitions(dforest))
	leftover = ntree - trees_per_partition*npartitions(dforest)
	trees = sapply(1:npartitions(dforest), function(i) 
	{
	      if(i <= leftover) return(trees_per_partition+1)
	      return(trees_per_partition)
	})
	tree_ids = c(0,cumsum(trees))+1
	tree_ids = lapply(1:(length(tree_ids)-1), function(i) 
	{
		ids = (tree_ids[i]+1):tree_ids[i+1]
		if(tree_ids[i] >= tree_ids[i+1])
			ids = integer(0)
		return(ids)

	})
	trees = lapply(tree_ids, function(ids) temp_forest[ids])

	foreach(i,1:npartitions(dforest), function(dforest = splits(dforest,i),
					  forest_header = temp_forest[[1]],
					  tree_ids = tree_ids[[i]],
					  trees = trees[[i]],
					  ntree = ntree)
	{
		dforest =  vector(mode = "list", length = ntree+1)
		dforest[[1]] <- forest_header
		if(length(tree_ids)>0)
			dforest[tree_ids] <- trees
		update(dforest)
	},progress = FALSE)

	rm(temp_forest)
	rm(trees)
	return(dforest)

}

.combineDistributedForests <- function(forest1,forest2)
{
	nparts = max(npartitions(forest1),npartitions(forest2))
	forest = dlist(npartitions = nparts)
	valid_index <- function(i,dobj)
	{
		if(i <= npartitions(dobj))
		     	return(list(i))
		else
			return(list())
	}
	foreach(i,1:npartitions(forest),
		function(forest1 = splits(forest1,valid_index(i,forest1)),
			forest2 = splits(forest2,valid_index(i,forest2)),
			forest = splits(forest,i))
		{
			if(length(forest1) == 0 | length(forest2) == 0)
			{
				forest = forest1[[1]]
				update(forest)
				return()
			}
			forest1 = forest1[[1]]
			forest2 = forest2[[1]]
			forest = c(forest1,forest2[-1])
			forest[[1]][[1]] <- 
				as.integer(length(forest1)+length(forest2)-2)
			update(forest)
		},progress = FALSE)
	return(forest)
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