#########################################################################
# A scalable and high-performance platform for R.                       #
# Copyright (C) [2013] Hewlett-Packard Development Company, L.P.        #
                                                                        #
# This program is free software; you can redistribute it and/or modify  #
# it under the terms of the GNU General Public License as published by  #
# the Free Software Foundation; either version 2 of the License, or (at #
# your option) any later version.                                       #
                                                                        #
# This program is distributed in the hope that it will be useful, but   #
# WITHOUT ANY WARRANTY; without even the implied warranty of            #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU      #
# General Public License for more details.  You should have received a  #
# copy of the GNU General Public License along with this program; if    #
# not, write to the Free Software Foundation, Inc., 59 Temple Place,    #
# Suite 330, Boston, MA 02111-1307 USA                                  #
#########################################################################   

.get.file.prefix <- function() {
  pm <- get_pm_object()
  master_addr <- pm$get_master_addr()
  master_port <- as.integer(pm$get_master_port())
  asc <- function(x) {strtoi(charToRaw(x),16L)}
  mID <- master_port + sum(unlist(lapply(strsplit(master_addr, "")[[1]], asc)))
  paste("DR-loader-", as.character(mID), "-", sep="")
}

.startDataLoader <- function(split_size) {
  if(!is.numeric(split_size) || is.null(split_size)) stop("Specify numeric split_size")
  tryCatch({
    pm <- get_pm_object()
    file_prefix = .get.file.prefix()
    pm$start_dataloader(split_size, file_prefix)
  }, error = handle_presto_exception)
}

stopDataLoader <- function() {
  tryCatch({
    tryCatch(pm <- get_pm_object(), error=function(ex) {})
    if(exists("pm")) 
       pm$stop_dataloader()
  }, error = handle_presto_exception)
  TRUE
}

.getUDxParameterLocalStr <- function(vertica_nodes) {
  if(!is.data.frame(vertica_nodes))
   stop("Data Frame expected!")
  tryCatch({
    pm <- get_pm_object()
    ret <- .Call("GetLoaderParameter_local", pm, vertica_nodes, PACKAGE="distributedR")
  }, error = handle_presto_exception)
  ret
}

.getUDxParameterUniformStr <- function() {
  tryCatch({
    pm <- get_pm_object()
    ret <- .Call("GetLoaderParameter_uniform", pm, PACKAGE="distributedR")
  }, error = handle_presto_exception)
  ret 
}

.getLoaderResult <- function(x) {
  if(!is.data.frame(x))
    stop("x must be a data frame containing vRODBC result set")

  tryCatch({
    pm <- get_pm_object()
    ret <- .Call("GetLoaderResult", pm, x, PACKAGE="distributedR")
  }, error = handle_presto_exception)
  ret
}

.vertica.darray <- function(result, column_names=list()) {
  nparts <- result$npartitions
  file_ids <- result$file_ids
  file_prefix <- paste("/dev/shm/", .get.file.prefix(), sep="")

  d <- darray(npartitions=c(nparts,1), distribution_policy='custom')
  if(length(column_names) > 0) 
     d@dimnames[[2]] <- as.character(column_names)

  foreach(i, 1:npartitions(d), func <- function(dhs = splits(d,i), file_idx = file_ids[[i]],
                                                file_prefix = file_prefix, column_names = column_names) {
    library(data.table)
    file_name <- paste(file_prefix, file_idx,sep="")
    dhs_dt <- fread(file_name, header=FALSE, integer64='double')
    unlink(file_name)
    dhs <- data.matrix(dhs_dt)
    rm(dhs_dt)
    gc()

    colnames(dhs) <- column_names
    update(dhs)
  })
  d
}

.vertica.darrays <- function(result, nResponses, nPredictors, 
                             predictor_columns=list(), response_columns=list()) {
  nparts <- result$npartitions
  file_ids <- result$file_ids
  file_prefix <- paste("/dev/shm/", .get.file.prefix(), sep="")

  X <- darray(npartitions=c(nparts,1), distribution_policy='custom')
  Y <- darray(npartitions=c(nparts,1), distribution_policy='custom')
  if(length(predictor_columns) > 0)
     X@dimnames[[2]] <- as.character(predictor_columns)
  if(length(response_columns) > 0)
     Y@dimnames[[2]] <- as.character(response_columns) 

  foreach(i, 1:npartitions(X), func <- function(x = splits(X,i), y = splits(Y,i), file_idx = file_ids[[i]], 
            nResponses=nResponses, nPredictors=nPredictors, file_prefix = file_prefix,
            predictor_columns=predictor_columns, response_columns=response_columns) {
    library(data.table)
    file_name <- paste(file_prefix, file_idx,sep="")
    dhs_dt <- fread(file_name, header=FALSE, integer64='double')
    unlink(file_name)
 
    y <- NULL
    for(i in 1:nResponses) {
        y <- cbind(y, dhs_dt[[i]])
    }
    x <- NULL
    for(i in (nResponses+1):(nResponses+nPredictors)) {
        x <- cbind(x, dhs_dt[[i]])
    }
    
    rm(dhs_dt)
    gc()
    
    colnames(x) <- predictor_columns
    colnames(y) <- response_columns
    update(x)
    update(y)
  })
  list(Y=Y, X=X)
}


.vertica.dframe <- function(result, nFeatures, column_names=list()) {
  nparts <- result$npartitions
  file_ids <- result$file_ids
  file_prefix <- paste("/dev/shm/", .get.file.prefix(), sep="")

  d <- dframe(npartitions=c(nparts,1), distribution_policy='custom')
  if(length(column_names) > 0)  
     d@dimnames[[2]] <- as.character(column_names)

  foreach(i, 1:npartitions(d), func <- function(dhs = splits(d,i), file_idx = file_ids[[i]],
                                                file_prefix = file_prefix, nCols = nFeatures,
                                                column_names=column_names) {
    library(data.table)
    file_name <- paste(file_prefix, file_idx, sep="")
    if(nCols == 1)
       dhs <- fread(file_name, sep='\n', header=FALSE, stringsAsFactors=FALSE, integer64='double')
    else
       dhs <- fread(file_name, sep=',', header=FALSE, stringsAsFactors=FALSE, integer64='double')

    setattr(dhs,"class","data.frame")
    unlink(file_name)
    colnames(dhs) <- column_names
    update(dhs)
  })
  d
}

.vertica.connector <- function(error) {
  stop(error$message)
}

.checkUnsegmentedProjections <- function(schema, table, relation_type, db_connect) {
  #check for unsegmented projection
  unseg_tbls <- sqlQuery(db_connect, "select distinct(t.table_schema || '.' || t.table_name) from tables t, projections p where p.is_segmented=false and p.anchor_table_id=t.table_id;")
  unseg_projs <- sqlQuery(db_connect, "select distinct(projection_schema || '.' || projection_name) from projections where is_segmented=false;")
    
  if((relation_type == "table" || relation_type == "external_table") && nrow(unseg_tbls)>0 && nrow(unseg_projs)>0) {
    if(paste(schema, ".", table, sep="") %in% unseg_tbls[[1]] || paste(schema, ".", table, sep="") %in% unseg_projs[[1]])
      stop("Cannot have unsegmented projection on table")
  } 
  else if(relation_type == "view" && nrow(unseg_tbls)>0 && nrow(unseg_projs)>0) {
    view_defn <- sqlQuery(db_connect, paste("select view_definition from views where table_schema ILIKE '", schema, "' and table_name ILIKE '", table, "'", sep=""))[[1]]
    if(length(view_defn) > 0) {
    for(i in 1:nrow(unseg_tbls))
        if(any(gregexpr(paste(" ", unseg_tbls[[1]][[i]], ", ", sep=""), view_defn)[[1]] > 0) || 
           any(gregexpr(paste(", ", unseg_tbls[[1]][[i]], " ", sep=""), view_defn)[[1]] > 0) ||
           any(gregexpr(paste(" ", unseg_tbls[[1]][[i]], " ", sep=""), view_defn)[[1]] > 0) || 
           any(gregexpr(paste(" ", unseg_tbls[[1]][[i]], "$", sep=""), view_defn)[[1]] > 0)) 
           stop(paste("Cannot have unsegmented projection on table ", unseg_tbls[[1]][[i]], ", which is used in view ", schema, ".", table ,sep=""))

    for(i in 1:nrow(unseg_projs))
        if(any(gregexpr(paste(" ", unseg_projs[[1]][[i]], ", ", sep=""), view_defn)[[1]] > 0) || 
           any(gregexpr(paste(", ", unseg_projs[[1]][[i]], " ", sep=""), view_defn)[[1]] > 0) || 
           any(gregexpr(paste(" ", unseg_projs[[1]][[i]], " ", sep=""), view_defn)[[1]] > 0) || 
           any(gregexpr(paste(" ", unseg_projs[[1]][[i]], "$", sep=""), view_defn)[[1]] > 0)) 
           stop(paste("Cannot use an unsegmented projection", unseg_projs[[1]][[i]], "in view definition"))
    }
  }
}
