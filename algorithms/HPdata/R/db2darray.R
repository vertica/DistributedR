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

 
## A simple function for reading a darray from a table
# tableName: name of the table
# dsn: ODBC DSN name
# features: a list containing the name of columns corresponding to attributes of the darray (features of samples)
# except: the list of column names that should be excluded (optional)
# npartitions: number of partitions in the darray (it is an optional argument)
# verticaConnector: when it is TRUE (default), Vertica Connector for Distributed R will be used
# loadPolicy: it determines the policy of the Vertica Connector for Distributed R
db2darray <- function(tableName, dsn, features = list(...), except=list(...), npartitions, verticaConnector=TRUE, loadPolicy="local") {

    if(!is.character(tableName))
        stop("The name of the table should be specified")
    if(is.null(dsn))
        stop("The ODBC DSN should be specified")
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

    #Validate table name
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
    feature_columns <- ""
    norelation <- FALSE
    relation_type <- ""

    if(missing(features) || length(features)==0 || features=="") {
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
           feature_columns <- view_columns[[1]]
         }
      } else {
        # get type of table - external or regular
        table_type <- sqlQuery(db_connect, paste("select table_definition from tables where table_schema ILIKE '", schema, "' and table_name ILIKE '", table, "'", sep=""))
        if(!is.data.frame(table_type)) {
           stop(table_columns)
        }
        relation_type <- ifelse((is.null(table_type[[1]][[1]]) || is.na(table_type[[1]][[1]])), "table", "external_table")
        feature_columns <- table_columns[[1]]
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

      feature_columns <- features
    }

    if(norelation) {
      stop(paste("Table/View ", tableName, " does not exist", sep=""))
    }

    # excluding the elements in the except list
    if(!missing(except) && length(except)!=0 && except!="")
        feature_columns <- feature_columns[sapply(feature_columns, function(x) !(x %in% except))]

    # we have columns, construct column string
    nFeatures <- length(feature_columns)  # number of features
    columns <- ""
    if(nFeatures > 1) {
      for(i in 1:(nFeatures-1)) {
          columns <- paste(columns, "\"", feature_columns[i], '\",', sep="")
      }
    }
    columns <- paste(columns, "\"", feature_columns[nFeatures], "\"", sep="")

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
    X <- FALSE

    if (verticaConnector) {
        tryCatch ({
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
          
          X <- .vertica.darray(result, feature_columns)
          
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
  
        # creating darray for features
        X <- darray(dim=c(nobs, nFeatures), blocks=c(rowsInBlock,nFeatures), empty=TRUE)
        if(length(feature_columns) > 0)
           X@dimnames[[2]] <- as.character(feature_columns)

        if(!missingNparts) {
            nparts <- npartitions(X)
            if(nparts != npartitions)
                warning("The number of splits changed to ", nparts)
        }

        #Load data from Vertica to darray
        foreach(i, 1:npartitions(X), initArrays <- function(x = splits(X,i), myIdx = i, rowsInBlock = rowsInBlock, 
                nFeatures=nFeatures, tableName=tableName, dsn=dsn, columns=columns, size=partitionsize(X,i),
                feature_columns=feature_columns) {

            # loading RODBC for each worker
            if (! require(vRODBC) )
                library(RODBC)
        
            start <- (myIdx-1) * rowsInBlock # start row in the block, rowid starts from 0
            end <- size[1] + start   # end row in the block

            qryString <- paste("select",columns, " from", tableName, "where rowid >=", start,"and rowid <", end)

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

            x <- NULL
            for (i in 1:nFeatures) {
                x <- cbind(x, segment[[i]])
            }
            colnames(x) <- feature_columns

            update(x)
        })

    } # if-else (verticaConnector)

    X
}

# Example:
# loadedSamples <- db2darray("mortgage1e4", dsn="RTest", list("mltvspline1", "mltvspline2", "agespline1", "agespline2", "hpichgspline", "ficospline"))


