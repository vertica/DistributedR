# Copyright [2013] Hewlett-Packard Development Company, L.P.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

## A simple function for reading a matrix from a table in a database
## tableName: name of the table
## features: a list containing the name of columns corresponding to attributes of the matrix (features of samples)
## dsn: ODBC configuration
db2matrix <- function(tableName, dsn, features = list(...)) {

    if(!is.character(tableName))
        stop("The name of the table should be specified")
    if(is.null(dsn))
        stop("The ODBC configuration should be specified")

    if(missing(features) || length(features)==0 || features=="")
       features <- list("*")

    nFeatures <- length(features)  # number of features
    # loading vRODBC or RODBC library for master
    if (! require(vRODBC) )
        library(RODBC)
    # connecting to Vertica and reading the number of observations in the table
    qryString <- "select"
    if(nFeatures > 1) {
        for(i in 1:(nFeatures-1)) {
            qryString <- paste(qryString, "\"", features[i], '\",', sep="")
        }
    }
    qryString <- paste(qryString, "\"", features[nFeatures], "\"", sep="")
    qryString <- paste(qryString, " from", tableName)

    connect <- odbcConnect(dsn)
    segment<-sqlQuery(connect, qryString)
    odbcClose(connect)
    # check valid response from the database
    if (! is.data.frame(segment) )
        stop(segment)

    as.matrix(segment)
}
# Example:
# centers <- db2matrix("mortgage", dsn="RDev", list("mltvspline1", "mltvspline2", "agespline1", "agespline2", "hpichgspline", "ficospline"))
