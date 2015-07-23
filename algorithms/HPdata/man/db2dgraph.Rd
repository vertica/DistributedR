\name{db2dgraph}
\alias{db2dgraph}
\title{A simple graph loader from a database}
\description{
db2dgraph function is an example for loading an adjacency matrix from a table of edge-list stored in a database. It is assumed that edge-list is stored in a single table. It is also assumed that the ID of vertices in the table starts from zero. As the returned darray will be column-wise partitioned, it would be more efficient when there is projection (index) on the 'to' column of the table.
}
\usage{
db2dgraph(tableName, dsn, from, to, weight, npartitions, row_wise = FALSE)
}

\arguments{
  \item{tableName}{
    it is the name of the table in the database in string format.
  }
  \item{dsn}{
    the name of configuration in ODBC.INI file for connecting to the database
  }
  \item{from}{
    the name of the column in the table which stores the source vertices
  }
  \item{to}{
    the name of the column in the table which stores the target vertices
  }
  \item{weight}{
    the name of the column in the table which stores the weights of the edges (it is an optional argument)
  }
  \item{npartitions}{
    this optional argument specifies the desired number of splits (partitions) in the distributed array. When it is not specified, it will become equal to the number of active instances in the distributed R environment. It should be noted that the number of splits in the returned result might not be exactly the same as npartitions. Please read the details for more information.
  }
  \item{row_wise}{
    if TRUE the returned darray will be row_wise partitioned.
  }
}

\details{
  The number of partitions at the result might not be exactly the same as desired number, but it will be close to it. The final number of partitions can be determined by the following routine.
When a positive integer number is specified for npartitions, the number of samples in each partition of the distributed structure will be calculated as
verticesInSplit <- ceiling(nVertices/npartitions) where nVertices is the number of vertices. As for building the distributed data structure, the value of verticesInSplit will be used, the final number of partitions will be ceiling(nVertices/verticesInSplit) which might be different from npartitions.

  The ID of the vertices starts from 0. The maximum ID number will be used to find the number of vertices in the graph. All missed ID numbers between the first ID (0) and the maximum ID will be assumed as vertices without any connected edge.

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
    # Assuming that unweighted edge-list is stored in a table
    #  named "graph", and the names of the columns are "x", "y". 
    dgraphDB <- db2dgraph("graph", from="x", to="y", dsn="RDev")
 }
}

\keyword{ Database }
\keyword{ Distributed R }
\keyword{ Graph Analytics }
