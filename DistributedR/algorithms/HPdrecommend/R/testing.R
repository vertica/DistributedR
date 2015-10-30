trainALS <- function(R, Rrow, numIterations=5, numFactors=10, lambda=0.13, seed) {
	matrixInfo <- .get_matrixInfo(R)
	numPartitions <- matrixInfo$numPartitions
	numUsers <- matrixInfo$numUsers
	numItems <- matrixInfo$numItems
	userSplit <- matrixInfo$userSplit
	itemSplit <- matrixInfo$itemSplit

	U <- darray(dim=c(numFactors, numUsers), blocks=c(numFactors, userSplit), sparse=FALSE)
	M <- darray(dim=c(numFactors, numItems* numPartitions), blocks=c(numFactors, numItems), sparse=FALSE)

	# initialize M with random values
	foreach(i, 1:npartitions(M),
		function(ms = splits(M, i), seed=seed) {
			cols <- seq(1, ncol(ms))
			if(!is.null(seed)) {
				set.seed(seed)
			}
			ms[,cols] <- runif(nrow(ms) * ncol(ms))
			update(ms)
		}
	)

	# iterations
	for(iter in 1:numIterations) {
		# train U
		foreach(k, 1:npartitions(U),
			function(us = splits(U, k),
					ms = splits(M, k),
					rs = splits(Rrow, k),
					numFactors = numFactors,
					lambda = lambda) {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)

				for(i in 1:ncol(us)) {
					ratings <- rs[ ,i]
					idx_rated <- which(ratings != 0)

					if(length(idx_rated) != 0) {
						Y_i <- matrix(nrow=numFactors, data=ms[ ,idx_rated])
						r_i <- ratings[idx_rated]
						x_i <- qr.solve(Y_i %*% t(Y_i) + length(idx_rated) * lambda_I, Y_i %*% r_i)
					} else {
						x_i <- rep(0, numFactors)
					}

					us[ ,i] <- x_i
				}

				update(us)
			}
		)

		# train M
		foreach(k, 1:npartitions(M),
			function(ms = splits(M, k),
					us = splits(U, k),
					rs = splits(Rrow, k),
					numFactors = numFactors,
					lambda = lambda,
					k=k) {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)

				rs <- t(rs)

				for(i in 1:ncol(ms)) {
					ratings <- rs[ ,i]
					idx_rated <- which(ratings != 0)

					if(length(idx_rated) != 0) {
						Y_i <- matrix(nrow=numFactors, data=us[ ,idx_rated])
						r_i <- ratings[idx_rated]
						x_i <- qr.solve(Y_i %*% t(Y_i) + length(idx_rated) * lambda_I, Y_i %*% r_i)
					} else {
						x_i <- rep(0, numFactors)
					}

					ms[ ,i] <- x_i
				}

				update(ms)
			}
		)
	}

	# get the number of rating scores for each item
	weights <- darray(dim=c(numPartitions, numItems), blocks=c(1, numItems))
	foreach(k, 1:numPartitions,
		function(rs = splits(Rrow, k),
				ws = splits(weights, k)) {
			rs <- t(rs)

			for(i in 1:ncol(rs)) {
				ratings <- rs[ ,i]
				idx_rated <- which(ratings != 0)
				num_ratings <- length(idx_rated)
				ws[ ,i] <- num_ratings
			}

			update(ws)
		}
	)

	# compute the sum of the number of rating scores for each item from partitions
	temp <- getpartition(weights)
	weights_sum <- apply(temp, 2, sum)

	# compute weights
	foreach(k, 1:numPartitions,
		function(ws = splits(weights, k),
				total = weights_sum) {
			for(i in 1:ncol(ws)) {
				ws[, i] <- ws[, i] / total[i]
			}
			update(ws)
		}
	)

	# get weigthed sum of item factors
	foreach(k, 1:numPartitions,
		function(ms = splits(M, k),
				ws = splits(weights, k)) {
			for(i in 1:ncol(ms)) {
				ms[, i] <- ms[, i] * ws[, i]
			}
		}
	)
	
	temp <- getpartition(M)
	weighted_M <- matrix(0, nrow=numFactors, ncol=numItems)
	for(i in 1:numPartitions) {
		for(j in 1:ncol(weighted_M)) {
			cnum <- (i-1) * ncol(weighted_M) + j
			weighted_M[, j] <- weighted_M[, j] + temp[, cnum]
		}
	}

	M <- as.darray(weighted_M, blocks=c(numFactors, itemSplit))

	return(list(U=U, M=M))
}

computeRMSE <- function(x, P) {
	rmse_d <- darray(dim=c(1,1), blocks=c(1,1), sparse=FALSE)
    foreach(i, 1:1,
		function(ps = splits(P),
				ms = splits(x$M),
				us = splits(x$U),
				rmse = splits(rmse_d)) {
			nnzP <- which(ps!=0, arr.ind=TRUE)
			realR <- ps[nnzP]
			predict_rating <- function(ind) {
				crossprod(us[,ind[1]], ms[,ind[2]])
			}

			predR <- apply(nnzP, 1, predict_rating)
			rmse <- as.matrix(sqrt(mean( (predR-realR)^2 ))) 
			update(rmse)
		}
	)
	cat("RMSE: ", getpartition(rmse_d), "\n")
}
