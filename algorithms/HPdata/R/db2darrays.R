#####################################################################################
# Copyright [2013] Hewlett-Packard Development Company, L.P.                        # 
#                                                                                   #
# This program is free software; you can redistribute it and/or                     #
# modify it under the terms of the GNU General Public License                       #
# as published by the Free Software Foundation; either version 2                    #
# of the License, or (at your option) any later version.                            #
#                                                                                   #
# This program is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                    #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                      #
# GNU General Public License for more details.                                      #
#                                                                                   #
# You should have received a copy of the GNU General Public License                 #
# along with this program; if not, write to the Free Software                       #
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.    #
#####################################################################################


## A simple function for reading responses and predictors from a table
## tableName: name of the table
## dsn: ODBC DSN name
## resp: a list containing the name of columns corresponding to responses 
## pred: a list containing the name of columns corresponding to predictors (optional)
# except: the list of column names that should be excluded from pred (optional)
## npartitions: number of partitions in darrays (it is an optional argument)
# verticaConnector: when it is TRUE (default), Vertica Connector for Distributed R will be used
# loadPolicy: it determines the data loading policy of the Vertica Connector for Distributed R
db2darrays <- function(tableName, dsn, resp = list(...), pred = list(...), except=list(...), npartitions, verticaConnector=TRUE, loadPolicy="local") {

    if(!is.character(tableName))
        stop("The name of the table should be specified")
    if(is.null(dsn))
        stop("The ODBC DSN should be specified")
    if(missing(resp) || length(resp)==0 || resp=="")
        stop("Response column names should be specified")   
    if(!is.logical(verticaConnector))
        stop("verticaConnector can be either TRUE or FALSE")
    if(!is.character(loadPolicy) || (tolower(loadPolicy)!='local' && tolower(loadPolicy)!='uniform'))
        stop("loadPolicy can be either 'local' or 'uniform'")
   
    # loading vRODBC or RODBC library for master
    if (! require(vRODBC) )
        library(RODBC)

    # connecting to Vertica
    db_connect <- odbcConnect(dsn)
    loadPolicy <- tolower(loadPolicy)

    #Close the connection on exit
    on.exit(odbcClose(db_connect))

    #get projection_name
    table <- ""
    schema <- ""
    table_info <- unlist(strsplit(tableName, split=".", fixed=TRUE))
    if(length(table_info) > 2) {
       stop("Invalid table name. Table name should be in format <schema_name>.<table_name>. If the table is in 'public' schema, Schema name can be ignored while specifying table name")
    } else if(length(table_info) == 2){ 
       schema <- table_info[1]
       table <- table_info[2]
    } else {
       table <- table_info[1]
       schema <- "public"
    }  


    # get columns of the table/view
    pred_columns <- ""
    norelation <- FALSE
    relation_type <- ""
    `%notin%` <- function(x,y) !(tolower(x) %in% tolower(y))
    if(missing(pred) || length(pred)==0 || pred=="") {
      table_columns <- sqlQuery(db_connect, paste("select column_name from columns where table_schema ILIKE '", schema ,"' and table_name ILIKE '", table,"'", sep=""))
      if(!is.data.frame(table_columns)) {
        stop(table_columns)
      }

      if(nrow(table_columns) == 0) {
         ## check if its a view
         view_columns <- sqlQuery(db_connect, paste("select column_name from view_columns where table_schema ILIKE '", schema ,"' and table_name ILIKE '", table,"'", sep=""))
         if(!is.data.frame(view_columns)) {
           stop(view_columns)
         }

         if(nrow(view_columns) == 0)
           norelation <- TRUE
         else {
           relation_type <- "view"
           pred_columns <- as.list(as.character(view_columns[[1]]))
           pred_columns <- pred_columns[pred_columns %notin% resp]
           #match_idx = match(resp, pred_columns)#which(resp %in% pred_columns)
           #for(i in 1:length(match_idx)) 
           #  pred_columns[match_idx[[i]]-i+1] <- NULL
        
         } 
      } else {
        # get type of table - external or regular
        table_type <- sqlQuery(db_connect, paste("select table_definition from tables where table_schema ILIKE '", schema, "' and table_name ILIKE '", table, "'", sep=""))
        if(!is.data.frame(table_type)) {
           stop(table_columns)
        }
        relation_type <- ifelse((is.null(table_type[[1]][[1]]) || is.na(table_type[[1]][[1]])), "table", "external_table")
        pred_columns <- as.list(as.character(table_columns[[1]]))
        pred_columns <- pred_columns[pred_columns %notin% resp]
        #match_idx = match(resp, pred_columns)#which(resp %in% pred_columns)
        #for(i in 1:length(match_idx)) 
        #   pred_columns[match_idx[[i]]-i+1] <- NULL
      }
    } else {
      istable <- sqlQuery(db_connect, paste("select count(*) from tables where table_name ILIKE '", table, "' and table_schema ILIKE '", schema, "'", sep=""))
      if(istable == 0) {
         isview <- sqlQuery(db_connect, paste("select count(*) from views where table_name ILIKE '", table, "' and table_schema ILIKE '", schema, "'", sep=""))
         if(isview == 0)
           norelation <- TRUE
         else
           relation_type <- "view"
      } else {
         # get type of table - external or regular
        table_type <- sqlQuery(db_connect, paste("select table_definition from tables where table_schema ILIKE '", schema, "' and table_name ILIKE '", table, "'", sep=""))
        if(!is.data.frame(table_type)) {
           stop(table_columns)
        }
        relation_type <- ifelse((is.null(table_type[[1]][[1]]) || is.na(table_type[[1]][[1]])), "table", "external_table")
      }

      pred_columns <- pred
    }

    if(norelation) {
      stop(paste("Table/View ", schema, ".", table, " does not exist", sep=""))
    }

    # excluding the elements in the except list
    if(!missing(except) && length(except)!=0 && except!="")
        pred_columns <- pred_columns[sapply(pred_columns, function(x) !(x %in% except))]

    # we have columns, construct column string
    nResponses <- length(resp)   # number of responses (1 for 'binomial logistic' and 'multiple linear' regression)
    nPredictors <- length(pred_columns)  # number of predictors

    if(nResponses == 0) {
      stop("No response columns to fetch from table/view")
    }
    if(nPredictors == 0) {
       stop("No predictor columns to fetch from table/view")
    }

    # the columns of the tabel
    columns <- ""
    for(i in 1:nResponses) {
        columns <- paste(columns, "\"", resp[i], '\",', sep="")
    }
    if(nPredictors > 1)
        for(i in 1:(nPredictors-1)) {
            columns <- paste(columns, "\"", pred_columns[i], '\",', sep="")
        }
    columns <- paste(columns, "\"", pred_columns[nPredictors], "\"", sep="")

    # when npartitions is not specified it should be calculated based on the number of executors
    missingNparts <- FALSE
    if(missing(npartitions)) {
        ps <- distributedR_status()
        nExecuters <- sum(ps$Inst)   # number of executors asked from distributedR
        noBlock2Exc <- 1             # ratio of block numbers to executor numbers
        npartitions <- nExecuters * noBlock2Exc  # number of partitions
        missingNparts <- TRUE
    } else {
        npartitions <- round(npartitions)
        if(npartitions <= 0) {
            stop("npartitions should be a positive integer number.")
        }
    }

    # checking availabilty of all columns
    qryString <- paste("select", columns, "from", tableName, "limit 1")
    oneLine <- sqlQuery(db_connect, qryString)
    # check valid response from the database
    if (! is.data.frame(oneLine) ) {
        stop(oneLine)
    }
    if (! all(sapply(oneLine, function(x ) is.numeric(x) || is.logical(x))) ) {
        stop("Only numeric and logical types are supported for darray")
    }

    # reading the number of observations in the table
    qryString <- paste("select count(*) from", tableName)
    nobs <- sqlQuery(db_connect, qryString)
    # check valid response from the database
    if (! is.data.frame(nobs) ) {
        stop(nobs)
    }
    if(nobs == 0) {
        stop("The table is empty!")
    }
    dResult <- FALSE

    if (verticaConnector) {
        tryCatch ({
        #check for unsegmented projection
        .checkUnsegmentedProjections(schema, table, relation_type, db_connect) 

        if(relation_type == "table" && loadPolicy == "local") {
           #get projection name
           qryString <- paste("select projection_name from tables t, projections p where t.table_name ILIKE '", table, "' and t.table_schema ILIKE '", schema, "'and t.table_id=p.anchor_table_id and p.is_super_projection=true and is_up_to_date=true order by projection_name limit 1", sep="")
           projection_details <- sqlQuery(db_connect, qryString);

           if(nrow(projection_details) == 0)
              stop(paste("Table", tableName, "does not exist or the table has no super projections with data. \nRetry with loadPolicy='uniform'"))
           else
              projection_name <- as.character(projection_details$projection_name)
        } else
          loadPolicy <- "uniform"

        nRows <- as.numeric(nobs)
        # calculate approximate split_size 
        partition_size <- ceiling(nRows/npartitions)

        #start data loader thread in distributedR
        ret <- .startDataLoader(partition_size)
        if(!ret)
            stop("Vertica Connector aborted.")

        #decide what type of loadPolicy to run
        if(as.character(loadPolicy) == "local") {
            #get metadata - vertica_nodes
            qryString <- paste("select node_name from projection_storage where projection_name = '", projection_name, "' order by node_name", sep="")
            vertica_nodes <- sqlQuery(db_connect, qryString)
            if(!is.data.frame(vertica_nodes))
              stop(paste("Error in Vertica:", paste(vertica_nodes, sep="", collapse="")))

            #get parameter string
            udx_param <- .getUDxParameterLocalStr(vertica_nodes)
            if(!udx_param$success){
              if(udx_param$error_code=="ERR02") {
                stop(udx_param$parameter_str)
              } else {
                udx_param <- .getUDxParameterUniformStr() 
                type <- "uniform"
                if(!udx_param$success) {
                  stop(udx_param$parameter_str)
                } 
              }
            } else {
              type <- "local"
            }
          } else if(as.character(loadPolicy) == "uniform") {
            udx_param <- .getUDxParameterUniformStr()
            type <- "uniform"
            if(!udx_param$success) {
              stop(udx_param$parameter_str)
            } 
          } else {
            stop(paste("Invalid data load policy selection", as.character(loadPolicy)))
          }

          #disable retry using MaxQueryRetries.
          #sqlQuery(db_connect, "select set_vertica_options('BASIC', 'DISABLE_ERROR_RETRY');")
          retries_allowed <- sqlQuery(db_connect, "select get_config_parameter('MaxQueryRetries');")
          sqlQuery(db_connect, "select set_config_parameter('MaxQueryRetries', 0);")

          #issue UDx query
          qryString <- paste("select ExportToDistributedR(", columns, " USING PARAMETERS DR_worker_info='", udx_param$parameter_str,"', DR_partition_size=", partition_size, ", data_distribution_policy='", type, "', max_string_length = 0) over(PARTITION BEST) from ", tableName, sep="")
         
          message(paste("Loading total", nRows, "rows from", tableName, "from Vertica with approximate partition of", partition_size,"rows")) 
          load_result <- sqlQuery(db_connect, qryString)
          if(!is.data.frame(load_result)) {
            sqlQuery(db_connect, paste("select set_config_parameter('MaxQueryRetries', ", retries_allowed, ");"))
            error_msg <- .Call("HandleUDxError", load_result, PACKAGE="distributedR")
            stop(error_msg)
          }

          #clear retry
          #sqlQuery(db_connect, "select clr_vertica_options('BASIC', 'DISABLE_ERROR_RETRY');")
          sqlQuery(db_connect, paste("select set_config_parameter('MaxQueryRetries', ", retries_allowed, ");"))
          
          #get loader status from distributedR workers
          result <- .getLoaderResult(load_result)
          if(!is.list(result))
            stop(result)

          dResult <- .vertica.darrays(result, nResponses, nPredictors, pred_columns, resp)

          }, interrupt = function(e) {}
           , error = function(e) {
             .vertica.connector(e)
          } , finally = { 
             stopDataLoader()
          })

    # end of verticaConnector
    } else {
        # check valid number of rows based on rowid assumptions
        qryString <- paste("select count(distinct rowid) from", tableName, "where rowid >=0 and rowid <", nobs)
        distinct_nobs <- sqlQuery(db_connect, qryString)
        if( nobs != distinct_nobs ) {
            stop("There is something wrong with rowid. Check the assumptions about rowid column in the manual.")
        }
        
        nobs <- as.numeric(nobs)
        rowsInBlock <- ceiling(nobs/npartitions) # number of rows in each partition
      
        # creating darray for predictors
        X <- darray(dim=c(nobs, nPredictors), blocks=c(rowsInBlock,nPredictors), empty=TRUE)
        #Binary response vector: value one shows sucess and zero failure 
        Y <- darray(dim=c(nobs,nResponses), blocks=c(rowsInBlock,nResponses), empty=TRUE)

        if(length(pred_columns) > 0)
           X@dimnames[[2]] <- as.character(pred_columns)
        if(length(resp) > 0)
           Y@dimnames[[2]] <- as.character(resp)

        if(!missingNparts) {
            nparts <- npartitions(X)
            if(nparts != npartitions)
                warning("The number of splits changed to ", nparts)
        }

        #Load data from Vertica to darrays
        foreach(i, 1:npartitions(X), initArrays <- function(x = splits(X,i), y = splits(Y,i), myIdx = i, rowsInBlock = rowsInBlock, 
                nResponses=nResponses, nPredictors=nPredictors, tableName=tableName, resp=resp, pred=pred_columns, dsn=dsn, columns=columns,
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
                  connect <- odbcConnect(dsn)
                }, warning = function(war) {
                  if(connect == -1)
                    stop(war$message)
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

            colnames(x) <- pred
            colnames(y) <- resp

            update(x)
            update(y)
        })
        
        dResult <- list(Y=Y, X=X)

    } # if-else (verticaConnector)

    dResult
}

# Example:
# loadedData <- db2darrays("mortgage_1000", dsn="Dloader", list("def"), list("mltvspline1", "mltvspline2", "agespline1", "agespline2", "hpichgspline", "ficospline"))
