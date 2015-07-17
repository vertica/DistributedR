\name{db2darray}
\alias{db2darray}
\title{A simple loader for darray}
\description{
db2darray function is an example for loading a set of unlabeled samples stored in a table/view to a darray. It is assumed that samples are stored in a single table/view. By default, it uses Vertica Connector for Distributed R to load data from a table in Vertica database into a darray. When Vertica Connector for Distributed R is not selected, vRODBC or RODBC will be used to connect to the database and extract data from the table. In the latter case, the table must contain a column called 'rowid'. It is also assumed that 'rowid' starts from 0, and there is no missed 'rowid'.
}
\usage{
db2darray(tableName, dsn, features = list(...), except=list(...),
          npartitions, verticaConnector=TRUE, loadPolicy="local")
}

\arguments{
  \item{tableName}{
    it is the name of a table or a view in the database to be loaded into the darray.
  }
  \item{dsn}{
    the Data Source Name(DSN) as provided in ODBC.INI file to connect to the database.
  }
  \item{features}{
    this is an optional argument to specify list of columns names corresponding to the features of a sample. If this argument is not specfied or the argument is empty, the function will load all columns of the table or the view.
  }
  \item{except}{
    this is an optional argument to specify the list of column names that should be excluded from features.
  }
  \item{npartitions}{
    this optional argument specifies the desired number of partitions in the distributed array. If not specified, it will be equal to the number of active R instances in the distributed R cluster. It should be noted that the number of splits in the returned result might not be exactly the same as npartitions. Please read the details for more information.
  }
  \item{verticaConnector}{
    when it is TRUE (default),  Vertica Connector for Distributed R will be used to load data. Must install Distributed R Extensions package in Vertica to use Vertica Connector.
  }
  \item{loadPolicy}{
    it is the data loading policy used for Vertica Connector for Distributed R. It can have two values, "local" or "uniform". By default, "local" policy is used. Views and external tables will always be loaded with "uniform" policy. See details section for more information.
  }
}

\details{
The number of partitions of the resultant darray will be approximately equal to value provided in argument 'npartitions'.
When Vertica Connector for Distributed R is used, argument 'loadPolicy' defines the data loading policy between Distributed R and Vertica. loadPolicy "local" is employed when number of Distributed R worker is same as number for Vertica nodes containing data for table specified in argument 'tableName'. With this policy, there is one-to-one data load from a Vertica node to a Distributed R worker. For the alternate scenario, loadPolicy "uniform" is used where each Vertica nodes involves all Distributed R nodes in the data loading process. By default, loadPolicy "local" is used but it is automatically changed to "uniform" when the conditions for loadPolicy "local" are not met.
 
db2darray supports numeric and logical data types.
}

\value{
  \item{X}{the darray of samples}
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{
    require(HPdata)
    distributedR_start()
    # Assuming that samples are stored in a table named "samples", and
    # the names of the columns are "col1", "col2", "col3", and "col4". 
    # This call will load columns col1, col2, col3 and col4 into 
    # darray loadedSamples
    loadedSamples <- db2darray ("samples", dsn= "RDev", 
        features=list("col1", "col2", "col3", "col4"))

    # If features arguments is not used or is empty
    # All columns of the table sample will be loaded
    # in darray loadedSamples
    loadedSamples <- db2darray ("samples", dsn= "RDev")
    
 }
}

\keyword{ Database }
\keyword{ Distributed R }
