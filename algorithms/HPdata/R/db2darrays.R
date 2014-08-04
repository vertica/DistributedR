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

## A simple function for reading responses and predictors from a table
## tableName: name of the table
## resp: a list containing the name of columns corresponding to responses
## pred: a list containing the name of columns corresponding to predictors
## conf: ODBC configuration
## nSplits: number of partitions in darrays (it is an optional argument)
db2darrays <- function(tableName, resp = list(...), pred = list(...), conf, nSplits) {

    nResponses <- length(resp)   # number of responses (1 for 'binomial logistic' and 'multiple linear' regression)
    nPredictors <- length(pred)  # number of predictors
    # when nSplits is not specified it should be calculated based on the number of executors
    if(missing(nSplits)) {
        ps <- distributedR_status()
        nExecuters <- sum(ps$Inst)   # number of executors asked from distributedR
        noBlock2Exc <- 1             # ratio of block numbers to executor numbers
        nparts <- nExecuters * noBlock2Exc  # number of partitions
    } else {
        nSplits <- round(nSplits)
        if(nSplits <= 0)
            stop("nSplits should be a positive integer number.")
        nparts <- nSplits
    }

    # loading RODBC library for master
    if (! require(vRODBC) )
        library(RODBC)
    # the columns of the tabel
    columns <- ""
    for(i in 1:nResponses) {
        columns <- paste(columns, resp[i], ',')
    }
    if(nPredictors > 1)
        for(i in 1:(nPredictors-1)) {
            columns <- paste(columns, pred[i], ',')
        }
    columns <- paste(columns, pred[nPredictors])
   
    # connecting to Vertica
    connect <- odbcConnect(conf)
    # checking availabilty of all columns
    qryString <- paste("select", columns, "from", tableName, "limit 1")
    oneLine <- sqlQuery(connect, qryString)
    # check valid response from the database
    if (! is.data.frame(oneLine) ) {
        odbcClose(connect)
        stop(oneLine)
    }

    # reading the number of observations in the table
    qryString <- paste("select count(*) from", tableName)
    nobs <- sqlQuery(connect, qryString)
    # check valid response from the database
    if (! is.data.frame(nobs) ) {
        odbcClose(connect)
        stop(nobs)
    }
    if(nobs == 0) stop("The table is empty!")

    # check valid number of rows based on rowid assumptions
    qryString <- paste("select count(distinct rowid) from", tableName, "where rowid >=0 and rowid <", nobs)
    distinct_nobs <- sqlQuery(connect, qryString)
    odbcClose(connect)
    if( nobs != distinct_nobs ) {
        stop("There is something wrong with rowid. Check the assumptions about rowid column in the manual.")
    }
    
    nobs <- as.numeric(nobs)
    rowsInBlock <- ceiling(nobs/nparts) # number of rows in each partition
  
    # creating darray for predictors
    X <- darray(dim=c(nobs, nPredictors), blocks=c(rowsInBlock,nPredictors), empty=TRUE)
    #Binary response vector: value one shows sucess and zero failure 
    Y <- darray(dim=c(nobs,nResponses), blocks=c(rowsInBlock,nResponses), empty=TRUE)

    if(!missing(nSplits)) {
        nparts <- npartitions(X)
        if(nparts != nSplits)
            warning("The number of splits changed to ", nparts)
    }

    #Load data from Vertica to darrays
    foreach(i, 1:npartitions(X), initArrays <- function(x = splits(X,i), y = splits(Y,i), myIdx = i, rowsInBlock = rowsInBlock, 
            nResponses=nResponses, nPredictors=nPredictors, tableName=tableName, resp=resp, pred=pred, conf=conf, columns=columns,
            size=partitionsize(X,i)) {

        # loading RODBC for each worker
        if (! require(vRODBC) )
            library(RODBC)
        start <- (myIdx-1) * rowsInBlock # start row in the block
        end <- size[1] + start   # end row in the block

        qryString <- paste("select", columns, "from", tableName, "where rowid >=", start,"and rowid <", end)

        # each worker connects to Vertica to load its partition of the darray 
        connect <- -1
        tryCatch(
            {
              connect <- odbcConnect(conf)
            }, warning = function(war) {
              if(connect == -1)
                stop(war)
            }
        )
        segment<-sqlQuery(connect, qryString, buffsize= end-start)
        odbcClose(connect)
        y <- NULL
        for(i in 1:nResponses) {
            #y <- cbind(y, as.numeric(unlist(segment[i])))
            y <- cbind(y, segment[[i]])
        }

        x <- NULL
        for(i in (nResponses+1):(nResponses+nPredictors)) {
            #x <- cbind(x, as.numeric(unlist(segment[i])))
            x <- cbind(x, segment[[i]])
        }

        update(x)
        update(y)
    })
    
    colnames(Y) <- resp
    colnames(X) <- pred

    list(Y=Y, X=X)
}

# Example:
# loadedData <- db2darrays("mortgage", list("def"), list("mltvspline1", "mltvspline2", "agespline1", "agespline2", "hpichgspline", "ficospline"),conf="RDev")
