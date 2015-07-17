\name{db2darrays}
\alias{db2darrays}
\title{
    A Simple Data Loader which loads two aligned darrays from Database
}
\description{
    db2darrays function is a simple function to load a labeled dataset (a set of labeled samples) from a table/view in Vertica database to a pair of darrays which correspond to responses and predictors of a predictive model. It is assumed that samples (including responses and predictors) are stored in a single table. By default, Vertica Connector for Distributed R is used to load data from a Vertica database to a pair of darrays. When it is selected not to use Vertica Connector for Distributed R, vRODBC or RODBC will be used to connect to the database and extract data from the table. In the latter case, the table must contain a column called 'rowid'. It is also assumed that 'rowid' starts from 0, and there is no missed 'rowid'.
}
\usage{
db2darrays(tableName, dsn, resp = list(...), pred = list(...), except=list(...),  
           npartitions, verticaConnector=TRUE, loadPolicy="local")
}
\arguments{
  \item{tableName}{
    it is the name of a table or a view in the database to be loaded into the darray.
  }
  \item{dsn}{
    the Data Source Name(DSN) as provided in ODBC.INI file to connect to the database.
  }
  \item{resp}{
    the list of the column names corresponding to responses.
  }
  \item{pred}{
    this is an optional argument to specify list of the column names corresponding to predictors. If this argument is not specfied or is empty, the function will load all columns of the table or view excluding the column specified in resp argument.
  }
  \item{except}{
    this is an optional argument to specify the list of column names that should be excluded from pred.
  }
  \item{npartitions}{
    this optional argument specifies the desired number of partitions in the distributed array. If not specified, it will be equal to the number of active R instances in the distributed R cluster. It should be noted that the number of splits in the returned result might not be exactly the same as npartitions. Please read the details for more information.
  }
  \item{verticaConnector}{
    when it is TRUE (default), Vertica Connector for Distributed R will be used to load data. Must install Distributed R Extensions package in Vertica to use Vertica Connector.
  }
  \item{loadPolicy}{
    it is the data loading policy used for Vertica Connector for Distributed R. It can have two values, "local" or "uniform". By default, "local" policy is used. Views and external tables will always be loaded with "uniform" policy. See details section for more information. 
  }
}

\details{
The number of partitions of the resultant darrays will be approximately equal to value provided in argument 'npartitions'.
When Vertica Connector for Distributed R is used, argument 'loadPolicy' defines the data loading policy between Distributed R and Vertica. loadPolicy "local" is employed when number of Distributed R worker is same as number for Vertica nodes containing data for table specified in argument 'tableName'. With this policy, there is one-to-one data load from a Vertica node to a Distributed R worker. For the alternate scenario, loadPolicy "uniform" is used where each Vertica nodes involves all Distributed R nodes in the data loading process. By default, loadPolicy "local" is used but it is automatically changed to "uniform" when the conditions for loadPolicy "local" are not met.

db2darrays supports numeric and logical data types.
}

\value{
  \item{Y }{the darray of responses}
  \item{X }{the darray of predictors}
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    require(HPdata)
    distributedR_start()
    # Assuming that samples are stored in a table named mortgage.
    # The name of response column is def,
    # and the names of predictive columns are mltvspline1, mltvspline2, 
    # agespline1, agespline2, hpichgspline, and ficospline.
    loadedData <- db2darrays ("mortgage", dsn="RDev", resp=list("def"), 
    pred=list("mltvspline1", "mltvspline2", "agespline1", "agespline2",
    "hpichgspline", "ficospline"))

    # Optionally user cannot specify pred argument in which case
    # all columns of the table mortgage by default excluding
    # the response column name (resp argument)
    loadedData <- db2darrays ("mortgage", dsn="RDev", resp=list("def")) 
 }
}

\keyword{ Database }
\keyword{ Distributed R }
