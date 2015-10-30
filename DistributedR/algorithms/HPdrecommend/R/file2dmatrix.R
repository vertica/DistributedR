file2dmatrix <- function(pathPrefix, nRows, nCols, splitSize, row_wise) {
	if (!is.character(pathPrefix)) 
		stop("The pathPrefix should be specified.")
	if (!is.numeric(splitSize)) 
		stop("The number of vectors considered in each split should be specified.")
	if (!is.numeric(nRows) || !is.numeric(nCols)) {
		if(row_wise && nRows < splitSize)
			stop("The total number of vectors is not correct.")
		if(!row_wise && nCols < splitSize)
			stop("The total number of vectors is not correct.")
	}
	if (!is.logical(row_wise)) 
		stop("It should be specified whether the files are row_wise partitioned or column_wise.")
	
	blockSize <- splitSize
	if (row_wise)
		X <- darray(dim = c(nRows, nCols), blocks = c(blockSize, nCols), sparse = TRUE)
	else
		X <- darray(dim = c(nRows, nCols), blocks = c(nRows, blockSize), sparse = TRUE)

	nFiles <- npartitions(X)
	cat("Openning files:\n", paste(pathPrefix, "0\n...until...\n", pathPrefix, nFiles - 1, "\n", sep = ""))
	errDL <- dlist(nFiles)
	
	foreach(i, 1:npartitions(X), function(x = splits(X, i),
										i = i,
										blockSize = blockSize,
										nRows = nRows,
										nCols = nCols,
										pathPrefix = pathPrefix,
										row_wise = row_wise,
										erri = splits(errDL, i)) {
			fname <- paste(pathPrefix, i - 1, sep = "")
			options(scipen = 10)
			f <- tryCatch({
				scan(file = fname, what = list(x = 0, y = 0, w = 0), multi.line = FALSE)
			}, error = function(e) {
				erri <- list(file = fname, error = e)
				update(erri)
				NULL
			})

			if (!is.null(f)) {
				if (min(f$x) < 0 || min(f$y) < 0 || max(f$x) > nRows|| max(f$y) > nCols) {
					erri <- list(file = fname, error = "Found out of the range vertices in the split file")
					update(erri)
				}
				else if (row_wise) {
					start <- (i - 1) * blockSize
					end <- start + nrow(x)
					if (min(f$x) < start || max(f$x) >= end) {
						erri <- list(file = fname, error = "Error in the range of vertices in the split files")
						update(erri)
					}
					else {
						x <- .Call("hpdsparseMatrix", iIndex = f$x - start, jIndex = f$y, xValue = f$w, d = dim(x), PACKAGE = "Executor")
						update(x)
					}
				}
				else {
					start <- (i - 1) * blockSize
					end <- start + ncol(x)
					if (min(f$y) < start || max(f$y) >= end) {
						erri <- list(file = fname, error = "Error in the range of vertices in the split files")
						update(erri)
					}
					else {
						x <- .Call("hpdsparseMatrix", iIndex = f$x, jIndex = f$y - start, xValue = f$w, d = dim(x), PACKAGE = "Executor")
						update(x)
					}
				}
			}
		}
	)
	errorList <- getpartition(errDL)
	if (!is.null(unlist(errorList))) {
		for (i in 1:length(errorList)) {
			print(paste(errorList[[i]]))
		}
		stop("Error in reading input files")
	}
	X
}
