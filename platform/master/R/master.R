#####################################################################
#A scalable and high-performance platform for R.
#Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 2 of the License, or (at
#your option) any later version.

#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.  You should have received a
#copy of the GNU General Public License along with this program; if
#not, write to the Free Software Foundation, Inc., 59 Temple Place,
#Suite 330, Boston, MA 02111-1307 USA
#####################################################################

require(Rcpp)
require(Matrix)

NAMESPACE <- environment()
.onLoad <- function(libname, pkgname){
    require(methods)
    loadRcppModules()
    if (exists(".presto.shutdown.handle.reg", envir = .GlobalEnv) == FALSE) {
      assign(".presto.shutdown.handle.reg", TRUE, envir = .GlobalEnv)
      #reg.finalizer(.GlobalEnv, distributedR_shutdown, onexit = TRUE)

      #Detach the package before exiting. Avoids crashes (due to reference to old
      #variables) if workspace is saved. ".Last" is called during q(), but before
      #saving the workspace.
      assign(".Last", lastCleanupFunc, envir = .GlobalEnv)
    }
}

#In R<3.0.0, onDetach may need to be replaced with .Last.lib()
.onDetach <- function(libpath){
    distributedR_shutdown()
    remove(list=".Last", envir = .GlobalEnv)
    remove(list=".presto.shutdown.handle.reg", envir = .GlobalEnv)
}

#Function that is called as part of .Last (before quitting R session)
lastCleanupFunc <- function(){
    pkgname <- "distributedR"
    packagename <- paste("package:",pkgname,sep="")
    #First check if package is attached
    if(packagename %in% search()){
        #Now look for all dependent packages
        for(pkg in search()[-1L]) {
            if(grepl("^package:", pkg) &&
               exists(".Depends", pkg, inherits = FALSE) &&
               pkgname %in% get(".Depends", pkg, inherits = FALSE)){
                detach(name=pkg, unload=TRUE, force=TRUE, character.only=TRUE)
            }
        }
        detach(name=packagename, unload=TRUE, force=TRUE, character.only=TRUE)
    }
}

get_pm_object <- function(){
  pm <- mget(".PrestoMaster", envir = .GlobalEnv, ifnotfound=list(NULL))
  pm <- pm[[1]]
  if(is.null(pm)) {
    clear_presto_r_objs(FALSE)
    stop("distributedR is not running")  
  } else {
    if (pm$running() == FALSE) {
      clear_presto_r_objs(FALSE)
      stop("distributedR is not running")
    }
  }
  pm
}

stop_workers <- function(bin_name="R-worker-bin") {
  # Stop presto workers if active
  pm <- get_pm_object()
  if (!is.null(pm)) {
    hosts <- pm$worker_hosts()
    ports <- pm$worker_ports()
    rm(pm)

    for (i in 1:length(hosts)) {
      cmd = paste("ssh -n", hosts[[i]], "'", "killall", bin_name, "'", sep=" ")
      message(paste("Running ", cmd, sep=""))
      system(cmd, wait=FALSE, ignore.stdout=TRUE, ignore.stderr=TRUE)
    }
  }
  return(TRUE)
}

.pass_env_var <- function() {
  env_list <- c("ODBCINI", "VERTICAINI")
  ret_str <- ""
  for (el in env_list) {
    ev <- Sys.getenv(el)
    if (nchar(ev) > 0) {
      ret_str <- paste(ret_str, "-v ", el, ":", ev, " ", sep="")
    }
  }
  return (ret_str)
}

clean_string <- function(string) {
  clean <- string;
  clean <- gsub(" ","", clean, fixed=TRUE)
  clean <- gsub("\n","", clean, fixed=TRUE)
  clean <- gsub("\r","", clean, fixed=TRUE)
  clean <- gsub("\t","", clean, fixed=TRUE)
}

start_workers <- function(cluster_conf,
                          bin_path="./bin/start_proto_worker.sh",
                          inst=0, mem=0, rmt_home="", rmt_uid="", log=2) {
  if (rmt_home == ""){
    rmt_home <- getwd()
  }
  if (rmt_uid==""){
    rmt_uid = Sys.getenv(c("USER"))
  }

  library(XML)
  resourcePool <- data.frame()
  iscolocated <- isColocated(cluster_conf)
  if(isTRUE(iscolocated)){
	dsnName <- getDSN_Name(cluster_conf)
	if(is.null(dsnName) | dsnName == 'NULL'){
		stop("DSN Name in the config file cannot be null when Co-locating with Vertica.")
	}
	
	if(! require(vRODBC))
	     library(RODBC)
	connect <- odbcConnect(dsnName)
	resourcePool <- sqlQuery(connect, "select memory_size_kb, cpu_affinity_mask, cpu_affinity_mode from resource_pool_status where pool_name = 'distributedr' limit 1");
	close(connect)
	if(nrow(resourcePool)!=1){
		stop("Could not locate the distritubedr resource pool. Please create a resource pool named distributedr.")
	}
  }

  worker_conf <- conf2df(cluster_conf)
  master_addr <- get_pm_object()$get_master_addr()
  master_port <- get_pm_object()$get_master_port()

  #clean strings
  master_addr <- clean_string(master_addr)
  master_port <- clean_string(master_port)

  env_variables <- .pass_env_var()
  tryCatch({
    for(i in 1:nrow(worker_conf)) {
      r <- worker_conf[i,]

      #clean strings
      r$Hostname <- clean_string(r$Hostname)
      r$StartPortRange <- clean_string(r$StartPortRange)
      r$EndPortRange <- clean_string(r$EndPortRange)
      r$SharedMemory <- clean_string(r$SharedMemory)
      r$Executors <- clean_string(r$Executors)

      m <- ifelse(as.numeric(mem)>0, mem, ifelse(is.na(r$SharedMemory), 0, r$SharedMemory))
      e <- ifelse(as.numeric(inst)>0, inst, ifelse(is.na(r$Executors), 0, r$Executors))	
      if (as.numeric(e)>64)
      {
        message(paste("Warning: distributedR only supports 64 R instances per worker.\nNumber of R instances has been truncated from ", e, " to 64 for worker ", r$Hostname, "\n", sep=""))
        e <- 64
      }
      sp_opt <- getOption("scipen")  # This option value determines whether exponentional or fixed expression will be used (m and e should not not be expressed using exponentional expression)
      options("scipen"=100000)

      local_cmd <- paste("cd",rmt_home,";", bin_path,
                   "-m", m, "-e", e, "-p", r$StartPortRange, "-q", r$EndPortRange, "-l", log, "-a", master_addr, "-b", master_port,
                   "-w", r$Hostname, env_variables, sep=" ")
      ssh_cmd <- paste("ssh -n", paste(rmt_uid,"@",r$Hostname,sep=""), "'", local_cmd,"'", sep=" ")
      # do not use SSH if worker is running on localhost
      if(r$Hostname == "localhost" || r$Hostname == "127.0.0.1") {
          cmd <- local_cmd;
      }
      else {
          cmd <- ssh_cmd;
      }

      if(isTRUE(iscolocated)){
          cmd <- paste(cmd, "-c", iscolocated, "-o", resourcePool[1,1], "-k", resourcePool[1,2], "-d", resourcePool[1,3], sep=" ")
      }
      
      options("scipen"=sp_opt)
      ##print(sprintf("cmd: %s",cmd))
      system(cmd, wait=FALSE, ignore.stdout=TRUE, ignore.stderr=TRUE)
    }
  }, error = handle_presto_exception)
  return(TRUE)
}

distributedR_start <- function(inst=0, mem=0,
                         cluster_conf="",
                         log=2) {
  
  gcinfo(FALSE)
  gc()
  ret<-TRUE
  #helpful for debugging. Developers can set differnt values.
  presto_home=""
  workers=TRUE
  rmt_home=""
  rmt_uid=""
  yarn=FALSE
  if(!(is.numeric(inst) && floor(inst)==inst && inst>=0)) stop("Argument 'inst' should be a non-negative integral value")
  if(!(is.numeric(mem) && mem>=0)) stop("Argument 'mem' should be a non-negative number")

  pm <- NULL
  tryCatch(pm <- get_pm_object(), error=function(e){})
  if (!is.null(pm)){
    stop("distributedR is already running. Call distributedR_shutdown() to terminate existing session\n")
  }
  if(presto_home==""){
    presto_home<-ifelse(Sys.getenv(c("DISTRIBUTEDR_HOME"))=="", system.file(package='distributedR'), Sys.getenv(c("DISTRIBUTEDR_HOME")))
  }
  if (cluster_conf==""){
    cluster_conf <- paste(presto_home,"/conf/cluster_conf.xml",sep="")
  }
  bin_path <- "./bin/start_proto_worker.sh"
  # Normalize config to expand env vars etc
  normalized_config <- normalizePath(cluster_conf)
  pm <- new ( PrestoMaster, normalized_config )
  if (rmt_home == ""){
    rmt_home <- presto_home
  }
  # initialize a map which stores darray names -> pointers
  dobject_map <- new.env()
  # Set PrestoMaster pointer in global env
  assign(".PrestoMaster", pm , envir = .GlobalEnv)  
  assign(".PrestoDobjectMap", dobject_map, envir = .GlobalEnv)
  tryCatch({
    if (workers) {
      start_workers(cluster_conf=cluster_conf, bin_path=bin_path, 
        inst=inst, mem=mem, rmt_home=rmt_home, rmt_uid=rmt_uid, log=log)
    }
    else if (yarn){
      dr_path <- system.file(package = "distributedR")
      full_path <- paste(dr_path,'/yarn/yarn.R', sep='')
      source(full_path)

    }

    pm$start(log)
  },error = function(excpt){    
    pm <- get_pm_object()
    distributedR_shutdown(pm)
    gcinfo(FALSE)
    gc()
    ret<-FALSE
    stop(excpt$message)})
  cat(paste("Master address:port - ", pm$get_master_addr(),":",pm$get_master_port(),"\n",sep=""))
  ret<-ret && (check_dr_version_compatibility())
}

conf2df <- function(cluster_conf) {
  tryCatch({
    library(XML)
    if(file.access(cluster_conf,mode=4)==-1) stop("Cannot read configuration file. Check file permissions.")
    conf_xml <- xmlToList(cluster_conf)
    ## xmlToList adds a comment in a list element with a NULL value and we need to remove them.
    conf_xml$Workers[which(names(conf_xml$Workers) %in% c("comment"))] <- NULL
    for(i in 1:length(conf_xml$Workers)) {
        conf_xml$Workers[[i]][which(names(conf_xml$Workers[[i]]) %in% c("comment"))] <- NULL
    }
    conf_df <- lapply(conf_xml$Workers, data.frame, stringsAsFactors=FALSE)
    conf_df <- lapply(conf_df, function(X){
      nms <- c("Hostname", "StartPortRange", "EndPortRange", "Executors", "SharedMemory")
      missing <- setdiff(nms, names(X))
      X[missing] <- NA
      X <- X[nms]})
    conf_df <- do.call(rbind.data.frame, conf_df)
    row.names(conf_df) <- NULL
    return(conf_df)
  }, error = function(e) {
    message(paste("Fail to parse cluster configuration XML file. Start with default values\n", e, "\n", sep=""))
  })
  tryCatch({
    pm <- get_pm_object()
    if (!is.null(pm)) {
      hosts <- pm$worker_hosts()
      port_start <- pm$worker_start_port_range()
      port_end <- pm$worker_end_port_range()
      conf_df <- data.frame(Hostname=hosts, StartPortRange=port_start, EndPortRange=port_end, Executors=NA, SharedMemory=NA, stringsAsFactors=FALSE)
      return(conf_df)
  }}, error = handle_presto_exception)
}

# read the config file and determine if the flag isColocated is true or false.
isColocated <- function(cluster_conf){
tryCatch({
   if(file.access(cluster_conf,mode=4)==-1) stop("Cannot read configuration file. Check file permissions.")
   conf_xml <- xmlToList(cluster_conf)
   iscolocated <- FALSE
   if(!is.null(conf_xml$ServerInfo$isColocatedWithVertica)){
	iscolocated <- conf_xml$ServerInfo$isColocatedWithVertica
   }
   return(as.logical(iscolocated))
}, error = handle_presto_exception)
}

#read the config file and determine the DSN name.
getDSN_Name <- function(cluster_conf){
tryCatch({
   if(file.access(cluster_conf,mode=4)==-1) stop("Cannot read configuration file. Check file permissions.")
   conf_xml <- xmlToList(cluster_conf)
   return(conf_xml$ServerInfo$VerticaDSN)
}, error = handle_presto_exception)
}

distributedR_shutdown <- function(pm=NA, quiet=FALSE) {
  ret<-TRUE
  if(class(pm) != "Rcpp_PrestoMaster"){
    tryCatch(pm <- get_pm_object(), error=function(e){})
    if (is.null(pm)){
      return(NULL)
    }
  }
  tryCatch(pm$shutdown(),error = function(e){})
  ret<-ret && clear_presto_r_objs(FALSE)
}

clear_presto_r_objs <- function(darray_only=TRUE) {
  if(darray_only == FALSE) {
    rm_by_name <- c(".PrestoMaster", ".PrestoDobjectMap", "pm", ".__C__Rcpp_DistributedObject", ".__C__Rcpp_PrestoMaster")
    for(rl in rm_by_name){
      if(exists(rl, envir=.GlobalEnv) == TRUE){
        rm(list=c(rl), envir=.GlobalEnv)
      }
    }
    rm_by_class <- c("dobject", "splits", "Rcpp_PrestoMaster")
  } else {
    rm_by_class <- c("dobject", "splits")
  }
  vars <- ls(all=TRUE, envir=.GlobalEnv)
  for(v in vars){
    for (c in rm_by_class) {
      # if a variable is a list type, iterate it to check if it contains dobject
      eval_obj <- eval(as.name(v), envir=.GlobalEnv)
      if (inherits(eval_obj, "list") == TRUE) {
        lsize = length(eval_obj)
        if (lsize > 0) {
          # remove from the end to keep index intact
          for (i in lsize:1) {
            if (inherits(eval_obj[[i]], c) == TRUE) {
              eval(parse(text = paste(as.name(v), "[[", i, "]] <- NULL", sep="")), envir=.GlobalEnv)
            }
          }
        }
      } else {
        if(inherits(eval_obj, c) == TRUE){
          rm(list=c(eval(v)), envir=.GlobalEnv)
          break
        }
      }
    }
  }

  gcinfo(FALSE)
  gc() 
  TRUE
}

distributedR_status <- function(help=FALSE){
  stat_df <- NA
  tryCatch({
    pm <- get_pm_object()
    if (!is.null(pm)) {
      stat <- pm$presto_status()
      stat_df <- as.data.frame(t(data.frame(stat, check.names = FALSE)))
      stat_df <- data.frame(row.names(stat_df), stat_df)
      row.names(stat_df) <- NULL
      names(stat_df) <- c("Workers", "Inst", "SysMem", "MemUsed", "DarrayQuota", "DarrayUsed")
  }},error = handle_presto_exception)
  if(help==TRUE) {
    cat("\ndistributedR_status - help\nWorkers: list of workers\nInst: number of R instances on a worker\n")
    cat("SysMem: total available system memory (MB)\nMemUsed: memory currently used in a worker (MB)\n")
    cat("DarrayQuota:  memory for darrays (MB)\nDarrayUsed: memory currently used by darray (MB)\n\n")
  }
  stat_df
}

.distributedR_ls <- function(){
  tryCatch({
    pm <- get_pm_object()
    if (!is.null(pm)) {
      pm$presto_ls()
    }
  },error = handle_presto_exception)
}

distributedR_master_info <- function() {
  tryCatch({
    pm <- get_pm_object()
    if (!is.null(pm)) {
      master_addr <- pm$get_master_addr()
      master_port <- as.integer(pm$get_master_port())
      asc <- function(x) {strtoi(charToRaw(x),16L)}
      session_id <- master_port + sum(unlist(lapply(strsplit(master_addr, "")[[1]], asc)))
      list(address=master_addr, port=master_port, sessionID=session_id)
    }
  },error = handle_presto_exception)
}

handle_presto_exception <- function (excpt){
  excpt_class <- class(excpt)[1L]
  if (excpt_class=="presto::PrestoShutdownException") {
    message(excpt$message)
    pm <- get_pm_object()
    distributedR_shutdown(pm)
    gcinfo(FALSE)
    gc()
    stop()
  } else if (excpt_class=="presto::PrestoWarningException"){
    stop(excpt$message)
  } else {
    stop(excpt$message)
  }
}

#Check that the master and workers run the same version of distributed R
check_dr_version_compatibility<-function(){
  #Create dframe with num partitions = num workers. This can be buggy if scheduler does not create partitions in round robin
  #Correct, but higher overhead solution, is to create partitions=total_no_executors
  nworkers<-get_pm_object()$get_num_workers()
  temp<-dframe(c(nworkers,1),c(1,1))
  foreach(i, 1:npartitions(temp), function(x=splits(temp,i)){
  	 x=data.frame(v=packageVersion("distributedR"))
	 update(x)
  }, progress=FALSE)
  vers<-getpartition(temp)
  master_version<-packageVersion("distributedR")
  bad_workers<-which(master_version != vers$v)
  if(length(bad_workers)>0){
    message(paste("Error: Incompatible distributedR versions in the cluster (master=",master_version,", worker(s)=",vers$v[(bad_workers[1])],")\nInstall same version across cluster. Shutting down session.", sep=""))
    distributedR_shutdown() 
    return (FALSE)
  }
  return (TRUE)
}

