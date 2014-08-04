\name{vRODBC-package}
\alias{vRODBC}
\title{ODBC Database Connectivity}

\description{
  Package \pkg{vRODBC} implements ODBC database connectivity. vRODBC is a 
  modification to the Open-source RODBC package to provide improved data 
  loading time with Vertica database. No functionality native to RODBC, 
  for any other databases, has been hampered in any way.    
}

\details{
  Two groups of functions are provided.  The mainly internal
  \code{odbc*} commands implement low-level access to the ODBC functions
  of similar name.  The \code{sql*} functions operate at a higher level
  to read, save, copy and manipulate data between data frames and SQL
  tables.  Many connections can be open at once to any combination of
  DSN/hosts.
  
  It is recommended not to load both RODBC and vRODBC packages in the same
  session as both packages share same user function names and can lead to
  unexpected behaviour.
}

\author{  
  Michael Lapsley and Brian Ripley
  Modified by, Shreya Prasad
}

\keyword{IO}
\keyword{database}
