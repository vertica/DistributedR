\name{db2matrix}
\alias{db2matrix}
\title{A simple loader for loading a matrix from a database}
\description{
db2matrix function is an example for loading a set of unlabeled samples stored in a table to a matrix. It is assumed that samples are stored in a single table. All the rows of the table will be read, and each row will be a sample.
}
\usage{
db2matrix(tableName, dsn, features = list(...))
}

\arguments{
  \item{tableName}{
    it is the name of the table in the database in string format.
  }
  \item{dsn}{
    the Data Source Name(DSN) as provided in ODBC.INI file to connect to the database.
  }
  \item{features}{
    the list of the name of columns corresponding to the features of a sample.
  }
}

\value{
  \item{X}{the matrix}
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    # Assuming that centers are stored in a table named "centers",
    # and the names of the columns are "col1", "col2", "col3", and "col4". 
    loadedCenters <- db2matrix ("centers", list("col1", "col2", 
    "col3", "col4"), dsn="RDev")
 }
}

\keyword{ Database }
\keyword{ K-means }
