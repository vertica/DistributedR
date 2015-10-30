splitMatrixFile <- function(inputFile, npartitions, outputPath, row_wise = FALSE, isNFS = FALSE, transpose = FALSE) {
	if(!is.character(inputFile))
		stop("The name of the input file should be specified as a string")
	check <- system(paste("ls", inputFile), intern = TRUE)
	if(length(check) != 1)
		stop("cannot access the input file")
	inpuFile <- check
	npartitions <- as.integer(npartitions)
	if(npartitions <= 0)
		stop("npartitions should be a positive integer number")
	if(!is.character(outputPath))
		stop("The valid path for the output files should be specified as a string")
	if(isNFS) {
		check <- system(paste("ls", outputPath), intern = TRUE)
		if(!is.null(attributes(check)))
			stop("cannot access the output path")
	}

	tokens <- unlist(strsplit(inputFile, "/", fixed = TRUE))
	fileName <- tokens[length(tokens)]
	inputPath <- paste(tokens[-length(tokens)], collapse = "/")
	tempPath <- paste(inputPath, "/tempSGF", sep = "")
	system(paste("rm -r ", tempPath), ignore.stderr = TRUE)
	success <- system(paste("mkdir ", tempPath))
	if(success != 0)
		stop("cannot write the temporary files in ", inputPath)
	tempFile <- paste(tempPath, "/", fileName, sep = "")
	if(row_wise) 
		result <- .Call("hpdmatrixsplitter", inputFile, npartitions, 1, transpose, tempFile, PACKAGE = "HPdrecommend")
	else
		result <- .Call("hpdmatrixsplitter", inputFile, 1, npartitions, transpose, tempFile, PACKAGE = "HPdrecommend")
	if(is.null(result))
		stop("Split was unsuccessful")
	#nVertices <- result$nVertices
	if(row_wise) {
		nFiles <- result$rowsplits
		splitSize <- result$rowsplitsize
	}
	else {
		nFiles <- result$colsplits
		splitSize <- result$colsplitsize
	}
	if(isNFS) {
		commArg <- paste("mv ", tempFile, "?* ", outputPath, sep = "")
		success <- system(commArg)
	}
	else {
		workers <- levels(distributedR_status()$Workers)
		for(w in 1:length(workers)) {
			thisW <- workers[[w]]
			workerName <- strsplit(thisW, ":", fixed=TRUE)[[1]][[1]]
			cat("sending the files to ", workerName, "\n")
			if(npartitions == 1) {
				check <- system(paste("ssh ", workerName, " ls ", outputPath, sep = ""), intern = TRUE)
				success <- attributes(check)
				if(!is.null(success))
					break
			}
			commArg <- paste("scp ", tempFile, "?* ", workerName, ":", outputPath, sep = "")
			success <- system(commArg)
			if (success != 0) 
				break
		}
	}

	if (success != 0)
		stop("Split was unsuccessful. At least one of the nodes has problem to write on the specified outputPath.")
	commArg <- paste("rm -r ", tempPath)
	system(commArg)
	list(pathPrefix = paste(outputPath, "/", fileName, sep = ""), nRows = result$nRows, nCols = result$nCols,
		splitSize = splitSize, nFiles = nFiles, isWeighted = result$isWeighted)
}
