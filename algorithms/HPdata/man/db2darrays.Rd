\name{db2darrays}
\alias{db2darrays}
\title{
    A Simple Data Loader which loads two aligned darrays from Database
}
\description{
    db2darrays function is a simple function to load a labeled dataset (a set of labeled samples) from a table in Vertica database to a pair of darrays which correspond to responses and predictors of a predictive model. It is assumed that samples (including responses and predictors) are stored in a single table, and the table contains a column called 'rowid'. It is also assumed that 'rowid' starts from 0, and there is no missed 'rowid'.
}
\usage{
db2darrays(tableName, resp = list(...), pred = list(...), conf, 
            nSplits)
}
\arguments{
  \item{tableName}{
    it is the name of the table in the database in string format.
  }
  \item{resp}{
    the list of the name of columns corresponding to responses.
  }
  \item{pred}{
    the list of the name of columns corresponding to predictors.
  }
  \item{conf}{
    the name of configuration in ODBC.INI file for connecting to the database.
  }
  \item{nSplits}{
    this optional argument specifies the desired number of splits (partitions) in the distributed arrays. When it is not specified, it will become equal to the number of active instances in the distributed R environment. It should be noted that the number of splits in the returned result might not be exactly the same as nSplits. Please read the details for more information.
  }
}

\details{
The number of partitions at the result might not be exactly the same as desired number, but it will be close to it. The final number of partitions can be determined by the following routine.
When a positive integer number is specified for nSplits, the number of samples in each partition of the distributed structure will be calculated as
rowsInBlock <- ceiling(nobs/nSplits) where nobs is the number of observations (number of rows) in the table. As for building the distributed data structure, the value of rowsInBlock will be used, the final number of partitions will be ceiling(nobs/rowsInBlock) which clearly might be different from nSplits.
}

\value{
  \item{Y }{the darray of responses}
  \item{X }{the darray of predictors}
}

\author{
    Arash Fard
}

\examples{
 \dontrun{
    require(HPdata)
    distributedR_start()
    # Assuming that samples are stored in a table named mortgage.
    # The name of response column is def,
    # and the names of predictive columns are mltvspline1, mltvspline2, 
    # agespline1, agespline2, hpichgspline, and ficospline.
    loadedData <- db2darrays ("mortgage", list("def"), list("mltvspline1", 
    "mltvspline2", "agespline1", "agespline2", "hpichgspline",
    "ficospline"), conf="RDev")
 }
}

\keyword{ Database }
\keyword{ Distributed R }
