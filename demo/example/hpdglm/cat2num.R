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

## A simple function for converting a table with categorical data to a pure numerical table 
# srcTable: the name of the source table
# dsn: ODBC DSN name
# features: a list containing the name of columns from srcTable that are supposed to appear in the dstTable
# except: the list of column names that should be excluded from the dstTable
# dstTable: the name of the destination table
# view: when it is TRUE (default) a view otherwise a table is created
cat2num <- function(srcTable, dsn, features=list(...), except=list(...), dstTable, view=TRUE) {
    if(!is.character(srcTable))
        stop("The name of the source table is not specified correctly")
    if(!is.character(dstTable))
        stop("The name of the source table is not specified correctly")

    # loading vRODBC or RODBC library for master
    if (! require(vRODBC) )
        library(RODBC)

    # connecting to Vertica
    db_connect <- odbcConnect(dsn)

    #Validate table name
    src_table <- ""
    src_schema <- ""
    table_info <- unlist(strsplit(srcTable, split=".", fixed=TRUE))
    if(length(table_info) > 2) {
       odbcClose(db_connect)
       stop("Invalid name for the source table. Table name should be in format <schema_name>.<table_name>. If the table is in 'public' schema, Schema name can be ignored while specifying table name")
    } else if(length(table_info) == 2){
       src_schema <- table_info[1]
       src_table <- table_info[2]
    } else {
       src_table <- table_info[1]
       src_schema <- "public"
    }

    # get column names and types of the table/view
    feature_columns <- ""
    norelation <- FALSE
    relation_type <- ""

    table_columns <- sqlQuery(db_connect, paste("select column_name, data_type from columns where table_schema ILIKE '", src_schema ,"' and table_name ILIKE '", src_table,"'", sep=""))
    if(!is.data.frame(table_columns)) {
        odbcClose(db_connect)
        stop(table_columns)
     }

    if(nrow(table_columns) == 0) {
         ## check if its a view
         view_columns <- sqlQuery(db_connect, paste("select column_name, data_type from view_columns where table_schema ILIKE '", src_schema ,"' and table_name ILIKE '", src_table,"'", sep=""))
         if(!is.data.frame(view_columns)) {
           odbcClose(db_connect)
           stop(view_columns)
         } 

         if(nrow(view_columns) == 0) 
           norelation <- TRUE
         else {
           relation_type <- "view"
           feature_columns <- view_columns
         }
    } else {
        relation_type <- "table"
        feature_columns <- table_columns
    }

    if(norelation) {
      odbcClose(db_connect)
      stop(paste("Table/View ", srcTable, " does not exist", sep=""))
    }

    if(!missing(features) && length(features)!=0 && features!="") {
        if(! all(features %in% levels(feature_columns[,1])))
            stop("not all the specified features found in srcTable")
    } else {
        features <- levels(feature_columns[,1])
    }

    # excluding the elements in the except list
    if(!missing(except) && length(except)!=0 && except!="")
        features <- features[sapply(features, function(x) !(x %in% except))]

    # creating the select phrase of the final sql command
    create_columns <- ""
    inster_columns <- ""
    nFeatures <- length(features)  # number of features
    for(feature in features) { # any chategorical column with (l) levels will be converted to (l-1) binary columns
        if(grepl("char", feature_columns[feature_columns[,1]==feature, 2])) { # it is categorical
            lev <- sqlQuery(db_connect, paste("select distinct \"", feature, "\" from ", srcTable, sep=""))
            l <- levels(lev[[1]])
            nl <- length(l)
            nl <- nl - 1
            while(nl > 1) {
                inster_columns <- paste(inster_columns, "DECODE(\"", feature, "\", '", l[nl], "', 1, 0) as \"", feature, "_", nl, "\", ", sep="")
                create_columns <- paste(create_columns, "\"", feature, "_", nl, "\" int, ", sep="")
                nl <- nl - 1
            }
            if(nl == 1) {
                inster_columns <- paste(inster_columns, "DECODE(\"", feature, "\", '", l[nl], "', 1, 0) as \"", feature, "_", nl, "\" ", sep="")
                create_columns <- paste(create_columns, "\"", feature, "_", nl, "\" int ", sep="")
            } else if(nl == 0) {
                warning(paste("this column is neglected because it contains only a single category:", feature))
                next
            }
        } else { # it is not categorical
            inster_columns <- paste(inster_columns, " \"", feature, "\"", sep="")
            create_columns <- paste(create_columns, " \"", feature, "\" ", feature_columns[feature_columns[,1]==feature, 2], sep="")
        }
        if(nFeatures > 1) {
            inster_columns <- paste(inster_columns, ", ")
            create_columns <- paste(create_columns, ", ")
            nFeatures <- nFeatures -1
        }
    }

    if(view) {
        res <- sqlQuery(db_connect, paste("CREATE VIEW", dstTable, "AS (select", inster_columns, "from", srcTable, ")"))
    } else {
    #    sqlQuery(db_connect, paste("drop table", dstTable))
        res <- sqlQuery(db_connect, paste("create table", dstTable, "(", create_columns, ")"))    
        if(length(res) != 0) {
            odbcClose(db_connect)
            stop(res)
        }
        res <- sqlQuery(db_connect, paste("insert into", dstTable, "(select", inster_columns, "from", srcTable, ")"))
    }
    
    odbcClose(db_connect)
    if(length(res) != 0)
        stop(res)
}

