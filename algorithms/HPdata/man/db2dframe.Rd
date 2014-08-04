\name{db2dframe}
\alias{db2dframe}
\title{A simple loader for dframe}
\description{
db2dframe function is an example for loading a set of samples stored in a table to a dframe. It is assumed that samples are stored in a single table, and the table contains a column called 'rowid'. It is also assumed that 'rowid' starts from 0, and there is no missed 'rowid'.
}
\usage{
db2dframe(tableName, features = list(...), conf, nSplits)
}

\arguments{
  \item{tableName}{
    it is the name of the table in the database in string format.
  }
  \item{features}{
    the list of the name of columns corresponding to the features of a sample.
  }
  \item{conf}{
    the name of configuration in ODBC.INI file for connecting to the database.
  }
  \item{nSplits}{
    this optional argument specifies the desired number of splits (partitions) in the distributed frame. When it is not specified, it will become equal to the number of active instances in the distributed R environment. It should be noted that the number of splits in the returned result might not be exactly the same as nSplits. Please read the details for more information.
  }
}

\details{
The number of partitions at the result might not be exactly the same as desired number, but it will be close to it. The final number of partitions can be determined by the following routine.
When a positive integer number is specified for nSplits, the number of samples in each partition of the distributed structure will be calculated as
rowsInBlock <- ceiling(nobs/nSplits) where nobs is the number of observations (number of rows) in the table. As for building the distributed data structure, the value of rowsInBlock will be used, the final number of partitions will be ceiling(nobs/rowsInBlock) which clearly might be different from nSplits.
}

\value{
  \item{X}{the dframe of samples}
}

\author{
Arash Fard <afard@vertica.com>
}

\examples{
 \dontrun{
    require(HPdata)
    distributedR_start()
    # Assuming that samples are stored in a table named "samples",
    # and the names of the columns are "col1", "col2", "col3", and "col4". 
    loadedSamples <- db2dframe ("samples", list("col1", "col2", 
    "col3", "col4"), conf="RDev")
 }
}

\keyword{ Database }
\keyword{ Distributed R }
