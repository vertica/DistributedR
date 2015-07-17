\name{splitGraphFile}
\alias{splitGraphFile}
\title{A simple graph splitter}
\description{
\code{splitGraphFile} function is an example for splitting an edge-list file of a graph, and distributing the results among the active nodes of a cluster system. The result of this function can be fed to \code{file2dgraph} function to create a darray for adjacency matrix. The input edge-list is assumed zero-based; i.e., the id of the first edge is zero. The total number of vertices will be calculated according to the maximum vertex id in the input file, and for any missed vertex id a dummy vertex will be assumed; i.e., a vertex with no connected edge.
}
\usage{
splitGraphFile(inputFile, npartitions, outputPath, isNFS = FALSE, row_wise = FALSE)
}

\arguments{
  \item{inputFile}{
    the input file which contains the complete adjacency list of a graph. An absolute path should be used.
  }
  \item{npartitions}{
    the desired number of partitions in the graph. It may change based on the total number of vertices. It will be translated to the number of split files.
  }
  \item{outputPath}{
    it is the path for storing the output split files, it should be similar for master and workers. An absolute path should be used.
  }
  \item{isNFS}{
    TRUE indicates that outputPath is on NFS; therefore, only one copy of files will be sent to the destination. When it is FALSE (defualt) a copy of the files will be sent to every active node in the pool.
  }
  \item{row_wise}{
    when it is FALSE (defualt), the file is split based on the second vertices (target) of the edge list. In contrast, when it is TRUE, the file is split based on the first vertices (source).
  }
}

\details{
 Master should have read and write access to \code{inputPath}. Moreover, it should have write access to \code{outputPath}. When \code{outputPath} is not on NFS (the default assumption), Master distributes the files among all the workers which are active in the distributedR environment. \code{outputPath} should be already available on all the nodes of the cluster.

 The input file should be a text file containing the edge-list of a graph. Each edge should appear in a new line. Each edge should be represented by two integer numbers. The first integer would be the ID of the source vertex, and the second integer would be the ID of the target vertex. Edges can have floating point numbers for their weights which should appear right after the target vertex. These numbers should appear at the beginning of a line and delimited by space. Anything after these numbers will be ignored. Moreover, any line which starts with a non-number word will be assumed as a comment line and will be ignored. Existence of weight on the first edge determines if it the graph is weighted or not. Therefore, if the first edge appeared in the file does not have weight, weights on any other edges will be ignored. On the other hand, when the first edge has a weight, all other edges will be assumed weighted. In this case, the weight of the edges where it is not explicitly specified would be 1.

  The ID of the vertices starts from 0. The maximum ID number will be used to find the number of vertices in the graph. All missed ID numbers between the first ID (0) and the maximum ID will be assumed as vertices without any connected edge.

 The number of files at the result might not be exactly the same as desired number, but it will be close to it. The final number of files can be determined by the following routine. When a positive integer number is specified for npartitions, the number of vertices in each split file will be calculated as verticesInSplit <- ceiling(nVertices/npartitions) where nVertices is the number of vertices in the input graph. Then, the final number of files will be nFiles <- ceiling(nVertices/verticesInSplit) which might be different from npartitions. The files will be indexed from \code{0} to \code{nFiles - 1}.
}

\value{
  \item{pathPrefix}{the prefix name of the split files}
  \item{nVertices}{the total number of vertices in the graph}
  \item{verticesInSplit}{the number of vertices considered in each split file}
  \item{nFiles}{the number of split files}
  \item{isWeighted}{it indicates that there is weight on edges when it is TRUE}
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    require(HPdata)
    distributedR_start()

    # Wiki-Vote graph can be found in SNAP's repository
    # http://snap.stanford.edu/data/index.html
    ret <- splitGraphFile(inputFile=
            "/graphs/Wiki-Vote.txt", npartitions=7, 
            outputPath="/home/afard/temp/test", isNFS=TRUE)
 }
}

\keyword{ Distributed R }
\keyword{ Graph Analytics }
