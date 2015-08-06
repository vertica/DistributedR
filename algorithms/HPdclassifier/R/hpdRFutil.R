.distributeForest <- function(forest)
{
	temp_forest = .Call("serializeForest",forest,PACKAGE="HPdclassifier")
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

	return(dforest)

}

.combineDistributedForests <- function(forest1,forest2)
{
	forest = dlist(npartitions = npartitions(forest1))
	foreach(i,1:npartitions(forest),
		function(forest1 = splits(forest1,i),
			forest2 = splits(forest2,i),
			forest = splits(forest,i))
		{
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


	foreach(i,1:new, 
		function(temp_dforest = splits(temp_dforest,
			as.list((1:old-1)*new + i)),
			new_dforest = splits(new_dforest,i),
			ntree = length(unlist(tree_ids)))
		{
			header = temp_dforest[[1]][[1]]
			for(i in 1:length(temp_dforest))
			      temp_dforest[[i]][[1]] = NULL
			new_dforest = do.call(c,temp_dforest)
			new_dforest = new_dforest[!sapply(new_dforest,is.null)]
			new_dforest = c(list(header),new_dforest)
			new_dforest[[1]][[1]] = ntree
			update(new_dforest)
		},progress = FALSE)
	return(new_dforest)
} 