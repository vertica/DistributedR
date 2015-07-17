\name{file2dgraph}
\alias{file2dgraph}
\title{A simple graph loader from a set of files}
\description{
\code{file2dgraph} function is an example for loading an adjacency matrix from an edge-list which is split in a set of files. It is assumed that edge-list is properly split among nfiles. Split files should correspond to column-wise partitioned adjacency matrix; i.e., each file will be used to load one partition of such a matrix. The files should be available from the same path from all the nodes; i.e., they should be located either on NFS or on the same path in different nodes of the cluster. Moreover, \code{isWeighted} input argument should be set properly according to the graph stored in the files.
}
\usage{
file2dgraph(pathPrefix, nVertices, verticesInSplit, isWeighted, row_wise = FALSE)
}

\arguments{
  \item{pathPrefix}{
    the path and prefix to the files. It should be an absolute path and the same on all nodes of the cluster.
  }
  \item{nVertices}{
    the total number of vertices in the graph
  }
  \item{verticesInSplit}{
    the number of vertices considered in each split file
  }
  \item{isWeighted}{
    it should be FALSE when the there is no weight information in the input files
  }
  \item{row_wise}{
    it should be TRUE if the files are split based on the first vertices (source) of the edge list; FALSE if they are split based on the second vertices (target).
  }
}

\details{
  The split files should be partitioned edge-lists of a graph. The vertices should be partitioned among the file in such a way that 
nFiles == ceiling(nVertices / verticesInSplit) where nFiles is the number of files, nVertices is the total number of vertices, and verticesInSplit is the number of vertice in each file. The last file may have less vertices. The ID of the vertices starts from 0. The maximum ID number must be equal to \code{nVertices - 1}. All missed ID numbers between the first ID (0) and the maximum ID will be assumed as vertices without any connected edge.


  \code{isWeighted} must be carefully set TRUE or FALSE. Either all the edges should be weighed or none of them. When there is no weight, the return value for \code{W} will be \code{NULL}.

  Any comment or extra character in the lines will generate error.

  All the nodes of the cluster should have access to all the split files, whether they are on a shared storage or they are distributed among the nodes. Nevertheless, only a portion of split files will be used by R-executors of a node. When the files are distributed among the nodes, file2dgraph function will be robust against corruption of the files which are not used by any R-executors. It should be noticed that this function does not detect the possible inconsistency of the distributed files, but it is assumed that they are the same from point of view of all the nodes. 
}

\value{
  \item{X}{the sparse darray of adjacency matrix}
  \item{W}{the sparse darray of weights on edges}
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
    ret <- splitGraphFile(inputFile="
                /home/afard/Downloads/graph/Wiki-Vote.txt",
                nSplits=7, outputPath="/home/afard/temp/test", isNFS=TRUE)

    dgraphF <- file2dgraph(ret$pathPrefix, ret$nVertices, 
                            ret$verticesInSplit, ret$isWeighted)
 }
}

\keyword{ Distributed R }
\keyword{ Graph Analytics }
