# Copyright [2014] Hewlett-Packard Development Company, L.P.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

## inputFile: the input file which contains the complete adjacency list of a graph
## npartitions: the desired number of partitions in the graph. It may change based on the total number of vertices
## outputPath: it is the path for storing the output split files, it should be similar for master and workers
## isNFS: TRUE indicates that outputPath is on NFS; therefore, only one copy of files will be sent the destination.
##          When it is FALSE (defualt) a copy of the files will be sent to every node in the pool
# row_wise: when it is FALSE (defualt), the file is split based on the second vertices (target) of the edge list.
#           In contrast, when it is TRUE, the file is split based on the first vertices (source).
splitGraphFile <- function(inputFile, npartitions, outputPath, isNFS = FALSE, row_wise = FALSE) {
    # validating arguments
    if(!is.character(inputFile))
        stop("The name of the input file should be specified as a string")
    check <- system(paste("ls",inputFile), intern=TRUE)
    if(length(check) != 1)
        stop("cannot access the input file")
    inputFile <- check # it helps sometimes to make the address of inputFile absolute

    npartitions <- as.integer(npartitions)
    if(npartitions <= 0)
        stop("npartitions should be a positive integer number\n")

    if(!is.character(outputPath))
        stop("The valid path for the output files should be specified as a string\n")
    if(isNFS) {
        check <- system(paste("ls",outputPath), intern=TRUE)
        if(! is.null(attributes(check)))
            stop("cannot access the output path")
    }
    if(!is.logical(row_wise))
        stop("'row_wise' must be TRUE or FALSE.")
    
    # parsing inputPath and fileName
    tokens <- unlist(strsplit(inputFile, "/", fixed=TRUE))
    fileName <- tokens[length(tokens)]
    inputPath <- paste(tokens[-length(tokens)], collapse="/")
    # creating a temp subdirectory for generating splits
    tempPath <- paste(outputPath,"/tempSGF", sep="")
    system(paste("rm -r ", tempPath), ignore.stderr = TRUE) # to make sure that the temp subdirectory does not exist
    success <- system(paste("mkdir ", tempPath))
    if(success != 0)
        stop("Cannot write the temporary files in ", inputPath)
    tempFile <- paste(tempPath,"/",fileName, sep="")

    # calling split function
    if(row_wise) {
        result <- .Call("hpdsplitter", inputFile, npartitions, 1, tempFile, PACKAGE="HPdata")
    } else {
        result <- .Call("hpdsplitter", inputFile, 1, npartitions, tempFile, PACKAGE="HPdata")
    }
    if(is.null(result)) stop("Split was unsuccessful")
    nVertices <- result$nVertices   
    nFiles <- ifelse(row_wise, result$rowsplits, result$colsplits)

    if(isNFS) {
        commArg <- paste("mv ", tempFile, "?* ", outputPath, sep="")
        success <- system(commArg)
    } else {
        # copy the files to the workers one by one
        workers <- levels(distributedR_status()$Workers)
        for(w in 1:length(workers)) {
            thisW <- workers[[w]]
            workerName <- strsplit(thisW,":", fixed=TRUE)[[1]][[1]]
            message("Sending the files to ", workerName)
            if(npartitions == 1) {
                check <- system(paste("ssh ", workerName, " ls ", outputPath, sep=""), intern=TRUE)
                success <- attributes(check)
                if(! is.null(success)) break
            }
            commArg <- paste("scp ", tempFile, "?* ", workerName, ":", outputPath, sep="")
            success <- system(commArg)
            if(success != 0) break

        }
    }
    if(success != 0)
        stop("Split was unsuccessful. At least one of the nodes has problem to write on the specified outputPath.")
    # removing the temp subdirectory
    commArg <- paste("rm -r ", tempPath)
    system(commArg)

    list(pathPrefix=paste(outputPath,"/",fileName, sep=""), nVertices=nVertices, verticesInSplit= ifelse(row_wise, result$rowsplitsize, result$colsplitsize),
            nFiles=nFiles, isWeighted=result$isWeighted, row_wise=row_wise)
}

# Example:
# splitGraphFile(inputFile=="/home/userName/graph.txt", npartitions=7, outputPath="/home/userName/splitGraph/")
