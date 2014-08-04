\name{vRODBC-internal}
\alias{odbcUpdate}
\alias{odbcCaseFlag}
\alias{odbcColumns}
\alias{odbcPrimaryKeys}
\alias{odbcSpecialColumns}
\alias{sqltablecreate}
\alias{sqlwrite}
\alias{odbcTableExists}
\alias{odbcClearResults}
\alias{odbcValidChannel}

\title{Internal vRODBC functions}
\description{
  Internal vRODBC functions: not exported from the \pkg{vRODBC} name space.
}
\usage{
odbcUpdate(channel, query, data, params, test = FALSE,
           verbose = FALSE, nastring = NULL)
odbcCaseFlag(channel)
odbcColumns(channel, table, catalog = NULL, schema = NULL, literal = FALSE)
odbcSpecialColumns(channel, table, catalog = NULL, schema = NULL)
odbcPrimaryKeys(channel, table, catalog = NULL, schema = NULL)
sqlwrite(channel, tablename, mydata, test = FALSE, fast = TRUE,
         nastring = NULL, verbose = FALSE)
sqltablecreate(channel, tablename, coldata = NULL, colspecs, keys = -1)
odbcTableExists(channel, tablename, abort = TRUE, forQuery = TRUE,
                allowDot = attr(channel, "interpretDot"))
odbcClearResults(channel)
odbcValidChannel(channel)
}
\keyword{internal}
