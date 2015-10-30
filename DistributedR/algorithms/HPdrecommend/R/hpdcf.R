hpdcf <- function(R, Rrow=NULL, method="ALS", parameters=NULL, seed=NULL, trace=FALSE, arma=TRUE, precompute=FALSE) {
	if(trace) {
		startTotalTime <- proc.time()
	}

	# check the input data
	if(is.null(R)) {
		stop("'R' is required argument")
	}
	if(!is.darray(R)) {
		stop("'R' should be distributed array")
	}

	# check the CF method
	if(!((method == "ALS") || (method == "RBM"))) {
		stop("Only ALS and RBM are supported for now")
	}

	# set parameters
	p <- .get_parameters(.ALS_param, parameters)
	
	if(trace) {
		ps <- distributedR_status()
		message("Number of executors:", ps$Int)
		message("System memory usage before training (MB): ", ps$MemUsed)
		message("Memory used to store darrays before training (MB): ", ps$DarrayUsed)
		startTrainTime <- proc.time()
	}

	if(arma) {
		message("Armadillo is being used...")
	}
	model <- eval(call(paste(".", method, "_train", sep=""), R, Rrow, p, seed, trace, arma, precompute))

	if(trace) {
		endTrainTime <- proc.time()
		message("Training time:", (endTrainTime - startTrainTime)[3], " sec")
		ps <- distributedR_status()
		message("System memory usage after training (MB):", ps$Memused)
		message("Memory used to stroe darrays after training (MB):", ps$DarrayUsed)
	}

	model <- c(model, list(method=method, parameters=p, ratings=R))
	class(model) <- "hpdcf"

	if(trace) {
		endTotalTime <- proc.time()
		totalTime <- endTotalTime - startTotalTime
		message("*****************************")
		message("Total running time:", (totalTime)[3], " sec")
	}

	return(model)
}

.ALS_train <- function(R, Rrow, parameters, seed, trace, arma, precompute) {
	# get ratings matrix partitioned row-wise
	if(is.null(Rrow)) {
		if(trace) {
			message("R is being row-wise partitioned")
			startTime <- proc.time()
		}

		Rrow <- .splitByRow(R)

		if(trace) {
			spentTime <- proc.time() - startTime
			message("Spent time: ", (spentTime)[3], " sec")
		}
	}

	# get parameters
	numFactors <- parameters$numFactors
	iterations <- parameters$iterations
	lambda <- parameters$lambda
	implicitPrefs <- parameters$implicitPrefs
	alpha <- parameters$alpha

	# get matrix info
	matrixInfo <- .get_matrixInfo(R)
	numPartitions <- matrixInfo$numPartitions
	numUsers <- matrixInfo$numUsers
	numItems <- matrixInfo$numItems
	userSplit <- matrixInfo$userSplit
	itemSplit <- matrixInfo$itemSplit

	# create user and item latent factor matrices
	#U <- darray(dim=c(numUsers, numFactors), blocks=c(userSplit, numFactors), sparse=FALSE)
	#M <- darray(dim=c(numItems, numFactors), blocks=c(itemSplit, numFactors), sparse=FALSE)
	U <- darray(dim=c(numFactors, numUsers), blocks=c(numFactors, userSplit), sparse=FALSE)
	M <- darray(dim=c(numFactors, numItems), blocks=c(numFactors, itemSplit), sparse=FALSE)

	if(precompute) {
		U_partInfo <- .gatherPartitionInfo(U, Rrow, numPartitions)
		M_partInfo <- .gatherPartitionInfo(M, R, numPartitions)
	}

	# initialize M
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

	startTime <- proc.time()
	if(implicitPrefs) {
		stop("Implicit preferences are not supported for now")
	}
	else {
		# iterating while alternating matrices
		for(iter in 1:iterations) {
			if(precompute) {
				cat("precompute used\n")
				# fix M and find optimal U
				U <- .computeFactors_new(U, M, U_partInfo, Rrow, itemSplit, numFactors, lambda, arma=arma)
				# fix U and find optimal M
				M <- .computeFactors_new(M, U, M_partInfo, R, userSplit,  numFactors, lambda, arma=arma)
			}
			else {
				# fix M and find optimal U
				U <- .computeFactors(U, M, Rrow, numFactors, lambda, arma=arma)
				# fix U and find optimal M
				M <- .computeFactors(M, U, R, numFactors, lambda, arma=arma)
			}
		}
	}
	cat("processing time: ", proc.time() - startTime, "\n")

	factors <- list(userFactors=U, itemFactors=M)

	return(factors)
}

.gatherPartitionInfo <- function(U, Rrow, numPartitions) {
	partInfo <- dlist(npartitions(U))
	foreach(k, 1:npartitions(U),
		function(us = splits(U, k),
				rs = splits(Rrow, k),
				dl = splits(partInfo, k),
				np = numPartitions) {
			library(HPdrecommend)
			p <- .Call('HPdrecommend_gatherPartitionInfo', PACKAGE = 'HPdrecommend', us, rs, np)
			p <- which(p == 1)
			dl <- list((p))
			update(dl)
		}
	)
	return(getpartition(partInfo))
}

.computeFactors <- function(learnedFactors, fixedFactors, R, numFactors, lambda, implicitPrefs=FALSE, alpha=0.1, arma=TRUE) {
	foreach(k, 1:npartitions(learnedFactors),
		function(ls = splits(learnedFactors, k),
				fs = splits(fixedFactors),
				rs = splits(R, k),
				lambda = lambda,
				numFactors = numFactors,
				arma = arma) {
			library(HPdrecommend)
			if(arma) {
				ls <- .Call('HPdrecommend_computeFactors', PACKAGE = 'HPdrecommend', ls, fs, rs, lambda, numFactors)
				update(ls)
			} else {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)
				
				for(i in 1:ncol(ls)) {
					ratings <- rs[,i]
					idx_rated <- which(ratings!=0)

					if(length(idx_rated) != 0) {
						Y_i <- matrix(nrow=numFactors, data=fs[,idx_rated])
						r_i <- ratings[idx_rated]
						#ch <- chol(crossprod(Y_i) + length(idx_rated) * lambda_I)
						#x_i <- backsolve(ch, forwardsolve(t(ch), crossprod(Y_i, r_i)))
						x_i <- qr.solve(Y_i %*% t(Y_i) + length(idx_rated) * lambda_I, Y_i %*% r_i)
					} else {
						x_i <- rep(0, numFactors)
					}

					ls[,i] <- x_i
				}
				update(ls)
			}
		}
	)

	return(learnedFactors)
}

.computeFactors_new <- function(learnedFactors, fixedFactors, partInfo, R, fsSplit, numFactors, lambda, implicitPrefs=FALSE, alpha=0.1, arma=TRUE) {
	foreach(k, 1:npartitions(learnedFactors),
		function(ls = splits(learnedFactors, k),
				fs = splits(fixedFactors, partInfo[[k]]),
				partInfo = partInfo[[k]],
				fsSplit = fsSplit,
				rs = splits(R, k),
				lambda = lambda,
				numFactors = numFactors,
				arma = arma) {
			library(HPdrecommend)
			if(arma) {
				fs <- do.call(cbind, fs)
				ls <- .Call('HPdrecommend_computeFactors_new', PACKAGE = 'HPdrecommend', ls, fs, rs, lambda, numFactors, partInfo, fsSplit)
				update(ls)
			} else {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)
				fs <- do.call(cbind, fs)
				
				for(i in 1:ncol(ls)) {
					ratings <- rs[,i]
					idx_rated <- which(ratings!=0)
					orig_idx_rated <- idx_rated
					
					for(j in 1:length(idx_rated)) {
						idx_in_part <- idx_rated[j] %% fsSplit
						orig_part <- floor((idx_rated[j] - 1) / fsSplit) + 1
						new_part <- which(partInfo == orig_part)
						idx_rated[j] = idx_rated[j] - (orig_part - new_part) * fsSplit
					}

					if(length(idx_rated) != 0) {
						Y_i <- matrix(nrow=numFactors, data=fs[,idx_rated])
						r_i <- ratings[orig_idx_rated]
						#ch <- chol(crossprod(Y_i) + length(idx_rated) * lambda_I)
						#x_i <- backsolve(ch, forwardsolve(t(ch), crossprod(Y_i, r_i)))
						x_i <- qr.solve(Y_i %*% t(Y_i) + length(idx_rated) * lambda_I, Y_i %*% r_i)
					} else {
						x_i <- rep(0, numFactors)
					}

					ls[,i] <- x_i
				}
				update(ls)
			}
		}
	)

	return(learnedFactors)
}

.get_matrixInfo <- function(R) {
	numPartitions <- npartitions(R)
	numUsers <- nrow(R)
	numItems <- ncol(R)
	userSplit <- ceiling(numUsers/numPartitions)
	itemSplit <- ceiling(numItems/numPartitions)
	if(R@npartitions[2] != ceiling(numItems/itemSplit)) {
		stop("'R' should be partitioned column-wise")
	}

	matrixInfo <- list(numPartitions = numPartitions,
					numUsers = numUsers,
					numItems = numItems,
					userSplit = userSplit,
					itemSplit = itemSplit
				)
	return(matrixInfo)
}

.splitByRow <- function(R) {
	matrixInfo <- .get_matrixInfo(R)
	numPartitions <- matrixInfo$numPartitions
	numUsers <- matrixInfo$numUsers
	numItems <- matrixInfo$numItems
	userSplit <- ceiling(numUsers/numPartitions)
	itemSplit <- ceiling(numItems/numPartitions)
	print(numPartitions)
	print(numUsers)
	print(numItems)
	print(userSplit)
	print(itemSplit)
	Rrow<- darray(dim=c(numUsers, numItems), blocks=c(userSplit, numItems), sparse=TRUE)

	for(k in 1:numPartitions) {
		foreach(i, 1:numPartitions,
			function(Rrow_i=splits(Rrow, i), R_k=splits(R, k), i=i, k=k, userSplit=userSplit, itemSplit=itemSplit) {
				start_row <- ((i - 1) * userSplit) + 1
				#num_rows <- ifelse(i * userSplit > nrow(R_k), userSplit - ((i * userSplit) - nrow(R_k)), userSplit)
				num_rows <- nrow(Rrow_i)
				end_row <- start_row + num_rows - 1

				start_col <- ((k - 1) * itemSplit) + 1
				#num_cols <- ifelse(k * itemSplit > ncol(Rrow_i), itemSplit - ((k * itemSplit) - ncol(Rrow_i)), itemSplit)
				num_cols <- ncol(R_k)
				end_col <- start_col + num_cols - 1

				#Rrow_i[, start_col:end_col] = R_k[start_row:end_row,]
				Rrow_i[ ,start_col:end_col] = R_k[start_row:end_row, ]

				update(Rrow_i)
			}
		)
	}

	return(Rrow)
}

print.hpdcf <- function(x, ...) {
	cat("\nNumber of factors: ", x$parameters$numFactors, "\n", sep="")
	cat("Number of iterations: ", x$parameters$iterations, "\n", sep="")
	cat("Lambda(regularization parameter): ", x$parameters$lambda, "\n", sep="")
	cat("Implicit preferences: ", x$parameters$implicitPrefs, "\n", sep="")
	cat("Alpha: ", x$parameters$alpha, "\n", sep="")
}

summary.hpdcf <- function() {
}

print.summary.hpdcf <- function() {
}

recommendItems <- function(model, user, k=10) {
}

.predict_rating <- function(model, user, item) {
	return(crossprod(model$U[user, ], model$M[item, ]))
}

predict.hpdcf <- function(x, testdata, ...) {
	if(is.null(testdata)) {
		stop("'testdata' is required argument")
	}

	rmse_d <- darray(dim=c(1,1), blocks=c(1,1), sparse=FALSE)
	foreach(i, 1:1, function(ps = splits(testdata),
							ms = splits(x$itemFactors),
							us = splits(x$userFactors),
							rmse = splits(rmse_d)) {
		nnzP <- which(ps!=0, arr.ind=TRUE)
		realR <- ps[nnzP]
		predict_rating <- function(ind) {
			crossprod(us[,ind[1]], ms[,ind[2]])
		}
		
		predR <- apply(nnzP, 1, predict_rating)
		rmse <- as.matrix(sqrt(mean( (predR-realR)^2 )))
		update(rmse)
	})

	cat("RMSE: ", getpartition(rmse_d), "\n")

	if(FALSE) {

	predicted_ratings <- array(0, dim = nrow(testdata))

	for (i in 1:nrow(testdata)) {
		predicted_ratings[i] <- .predict_rating(x, testdata[i, 1], testdata[i, 2])
	}

	if(ncol(testdata) > 2) {
		return(predicted_ratings)
	}

	actual_ratings <- testdata[i, 3]
	rmse <- sqrt(mean((predicted_ratings - actual_ratings)^2))
	cat("\nRMSE: ", rmse, "\n", sep="")
	return(predicted_ratings)

	}
}

.get_parameters <- function(p, parameter) {
	if(!is.null(parameter) && length(parameter) != 0) {
		o <- pmatch(names(parameter), names(p))

		if(any(is.na(o))) {
			stop(sprintf(ngettext(length(is.na(o)),
						"Unknown option: %s",
						"Unknown options: %s"),
						paste(names(parameter)[is.na(o)], collapse = " ")))
		}

		p[o] <- parameter
	}

	return(p)
}

.ALS_param <- list(
	numFactors = 10,
	iterations = 10,
	lambda = 0.13,
	implicitPrefs = FALSE,
	alpha = 1.0
)

########################################################################################

if(FALSE) {

hpdcf <- function(R, lambda=0.13, iterations=20, numFactors=20, trace=FALSE) {
	startTotalTime = proc.time()

	# check the input data
	if(is.null(R))
		stop("'ratings' are required arguments")
	if(!is.darray(R))
		stop("'ratings' should be distributed array")

	# get basic information about the size of the matrix, the number of partitions, the block size
	num_partitions <- npartitions(R)
	numUsers <- nrow(R)
	numItems <- ncol(R)
	userSplit <- ceiling(numUsers/num_partitions)
	itemSplit <- ceiling(numItems/num_partitions)

	# get ratings matrix partitioned row-wise
	Rrow <- .splitByRow(R)

	# create user and item laten factor matrices
	U <- darray(dim=c(numUsers, numFactors), blocks=c(userSplit, numFactors), sparse=FALSE)
	M <- darray(dim=c(numItems, numFactors), blocks=c(itemSplit, numFactors), sparse=FALSE)

	# training model
	model <- hpdcf.train_model(R, Rrow, U, M, lambda, iterations, numFactors)
	class(model) <- "hpdcf"
	model
}

hpdcf.train_model <- function(R, Rrow, U, M, lambda, iterations, numFactors) {
	# initialize M
	foreach(i, 1:length(splits(M)),
		function(ms = splits(M, i)) {
			rows <- seq(1, nrow(ms))
			ms[rows,] <- runif(nrow(ms) * ncol(ms))
			update(ms)
		}
	)

	# iterating while alternating matrices
	for(iter in 1:iterations) {
		# fix M and find optimal U
		foreach(k, 1:length(splits(U)),
			function(us = splits(U, k),
					ms = splits(M),
					rs = splits(Rrow, k),
					lambda = lambda,
					numFactors = numFactors) {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)

				for(i in 1:nrow(us)) {
					ratings <- rs[i,]
					idx_rated <- which(ratings != 0)

					if(length(idx_rated) != 0) {
						M_i <- matrix(ncol = numFactors, data = ms[idx_rated,])
						r_i <- ratings[idx_rated]
						ch <- chol(crossprod(M_i) + length(idx_rated) * lambda_I)
						u_i <- backsolve(ch, forwardsolve(t(ch), crossprod(M_i, r_i)))
					} else {
						u_i <- rep(0, numFactors)
					}
					
					us[i,] = u_i
				}
				update(us)
			}
		)

		# fix U and find optimal M
		foreach(k, 1:length(splits(M)),
			function(ms = splits(M, k),
					us = splits(U),
					rs = splits(R, k),
					lambda = lambda,
					numFactors = numFactors) {
				lambda_I <- diag(lambda, nrow=numFactors, ncol=numFactors)
				
				for(j in 1:nrow(ms)) {
					ratings <- rs[,j]
					idx_rated <- which(ratings!=0)
					
					if(length(idx_rated) != 0) {
						U_j <- matrix(ncol=numFactors, data=us[idx_rated,])
						r_j <- ratings[idx_rated]
						ch <- chol(crossprod(U_j) + length(idx_rated) * lambda_I)
						m_j <- backsolve(ch, forwardsolve(t(ch), crossprod(U_j, r_j)))
					} else {
						m_j <- rep(0, numFactors)
					}

					ms[j,] <- m_j
				}
				update(ms)
			}
		)
	}
}

}
