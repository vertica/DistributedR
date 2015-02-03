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
NAMESPACE <- environment()

.onLoad <- function(libname, pkgname){
    loadRcppModules()    
}
# Google protobuf allows sending upto 64MB of data at a time. 1MB less for safe.
.protobuf_max_msg_size = 63 * 1024*1024
.rcpp_obj_max_size = 2 * 1024 * 1024 * 1024 ## Rcpp allows upto 2GB of raw data

# The class 'splits' represents a collection of splits in a particular
# distributed array. It consists of a list of split-ids, a handle to the
# distributed array and optionally a name.
setClass("splits", representation(split_ids="numeric", 
                                  dobject_ptr="Rcpp_DistributedObject"), 
                   prototype(split_ids=numeric(), dobject_ptr=NULL),
                   S3methods=TRUE)

setMethod("initialize", "splits",
          function(.Object, split_ids, dobject_ptr) {
            .Object@split_ids = split_ids
            .Object@dobject_ptr = dobject_ptr
            .Object
          })

splits <- function(x, y,...)
    UseMethod("splits")

setMethod("splits", signature("dobject", "missing"),
          function(x, ...) {
            n <- x@dobject_ptr$num_splits()
            split_ids <- as.vector(0:(n-1))
            dobject <- x@dobject_ptr

            new("splits", split_ids, dobject)
          })

setMethod("splits", signature("dobject", "numeric"),
          function(x, y, ...) {
            if (y<=0||y>x@dobject_ptr$num_splits()) {
                stop("Split index must be larger than 0 and smaller than # of Splits")
	    }
            split_ids <- as.vector(y-1)
            dobject <- x@dobject_ptr
            new("splits", split_ids, dobject)
          })

#setMethod("splits", signature("dobject", "numeric", "numeric"),
#          function(x, y, z, ...) {
#            num.splits <- ceiling(x@dim/x@blocks)
#            if (y<=0||y>num.splits[1]||z<=0||z>num.splits[2]) {
#		stop("split index must be larger than 0 and smaller than # splits")
#	    }
#            idx <- (y-1)*num.splits[2] + z
#            splits(x, idx)
#          })

setMethod("length", signature("splits"),
  function(x) {
    length(x@split_ids)
  })

numSplits <- function(x)
    UseMethod("numSplits")

setMethod("numSplits", signature("dobject"),
  function(x) {
    x@dobject_ptr$num_splits()
  })

npartitions <- function(x)
    UseMethod("npartitions")

setMethod("npartitions", signature("dobject"),
  function(x) {
    x@dobject_ptr$num_splits()
  })

#Number of partitions expressed as a 2D mesh (e.g., (5,2) means there are total 10 partitions, 2 sets of 5 rows)
npartitions2D <- function(x)
    UseMethod("npartitions2D")

setMethod("npartitions2D", signature("dobject"),
  function(x) {
    x@npartitions
  })

getpartition <- function(x, y, z, ...){
  UseMethod("getpartition")
}

setMethod("getpartition", signature("darray", "missing", "missing"),
          function(x, ...) {
            if(((dim(x)[1]*dim(x)[2] > .Machine$integer.max) && !x@sparse) || all(dim(x)<=.Machine$integer.max) == FALSE) {
                stop(paste("Cannot perform getpartition on whole darray with dimension larger than", .Machine$integer.max,
                           "or number of elements more than",.Machine$integer.max))
            }
	    #If the array has not been initialized, let's just return 0 dim arrays.
	    if(is.invalid(x)){
		if(x@sparse){ 
			     return (new("dgCMatrix", i=as.integer({}),
                                      x=as.numeric({}), 
                                      p=as.integer(rep(0,1)),
                                      Dim=as.integer(c(0,0))))
    		}else{ return (array(dim=c(0,0)))}
	    }
            foreach(i, 1:1, function(comp = splits(x)) {}, progress=FALSE)
            tryCatch({
              pm <- get_pm_object()
              output <- .Call("DistributedObject_Get", pm, splits(x))
            },error = handle_presto_exception)
            tryCatch({
              if(!is.null(x@dimnames[[1]]) && length(x@dimnames[[1]]) != 0) {
                rownames(output) <- x@dimnames[[1]]
              }
              if(!is.null(x@dimnames[[2]]) && length(x@dimnames[[2]]) != 0) {
                colnames(output) <- x@dimnames[[2]]
              }
            }, error=function(e){})
            output
          })

setMethod("getpartition", signature("dframe", "missing", "missing"),
          function(x, ...) {
            if(all(dim(x)<=.Machine$integer.max) == FALSE) {
                stop(paste("Cannot perform getpartition on a dframe with dimension larger than",.Machine$integer.max))
            }
	    if(is.invalid(x)){ return (data.frame())}
            darr <- dframe(dim(x), dim(x))
            foreach(i, 1:1,
                    createcomposite <- function(comp = splits(x),
                                                da = splits(darr,1)) {
                      da <- comp
                      update(da)
                    }, progress=FALSE)
            output <- getpartition(darr, 1)
            rm(darr)
            gcinfo(FALSE)
            gc()
            output
          })


setMethod("getpartition", signature("dframe", "numeric", "missing"),
  function(x, y, ...) {
    y <- as.integer(y)
    # workaround to make sure split is in memory
    if(is.na(y) || y<=0 || y>x@dobject_ptr$num_splits()) {
	stop("getpartition index should be integer that is larger than 0 and smaller than #splits")
     }
#    foreach(i, 1, load <- function(sp = splits(x,y)) {}, progress=FALSE)
    tryCatch({
      pm <- get_pm_object()
      .Call("DistributedObject_Get", pm, new("splits", as.integer(y-1), x@dobject_ptr))
    },error = handle_presto_exception)
  })


setMethod("getpartition", signature("dlist", "missing", "missing"),
          function(x, ...) {
            if(all(x@dim<=.Machine$integer.max) == FALSE) {
                stop(paste("Cannot perform getpartition on a matrix with dimension larger than",.Machine$integer.max))
            }
            darr <- dlist(x@dim[1])
            foreach(i, 1:1,
                    createcomposite <- function(comp = splits(x),
                                                da = splits(darr,1)) {
                      da <- comp
                      update(da)
                    }, progress=FALSE)
            output <- getpartition(darr, 1)
            rm(darr)
            gcinfo(FALSE)
            gc()
            output
          })

setMethod("getpartition", signature("dlist", "numeric", "missing"),
  function(x, y, ...) {
    y <- as.integer(y)
    # workaround to make sure split is in memory
    if(is.na(y) || y<=0 || y>x@dobject_ptr$num_splits()) {
        stop("getpartition index should be integer that is larger than 0 and smaller than #splits")
     }
#    foreach(i, 1, load <- function(sp = splits(x,y)) {}, progress=FALSE)
    tryCatch({
      pm <- get_pm_object()
      .Call("DistributedObject_Get", pm, new("splits", as.integer(y-1), x@dobject_ptr))
    },error = handle_presto_exception)
  })

setMethod("getpartition", signature("darray", "numeric", "missing"),
  function(x, y, ...) {
    y <- as.integer(y)
    # workaround to make sure split is in memory
    if(is.na(y) || y<=0 || y>x@dobject_ptr$num_splits()) {
	stop("getpartition index should be integer that is larger than 0 and smaller than #splits")
     }
#    foreach(i, 1, load <- function(sp = splits(x,y)) {}, progress=FALSE)
    tryCatch({
      pm <- get_pm_object()
      ret <- .Call("DistributedObject_Get", pm, new("splits", as.integer(y-1), x@dobject_ptr))
      poffset<-dobject.getPartitionOffsets(x,y)
      psize<-partitionsize(x,y)
      tryCatch({
        if(!is.null(x@dimnames[[1]]) && length(x@dimnames[[1]]) != 0) {
          rownames(ret) <- (x@dimnames[[1]])[(poffset[1,1]+1):(poffset[1,1]+psize[1,1])]
        }
        if(!is.null(x@dimnames[[2]]) && length(x@dimnames[[2]]) != 0) {
          colnames(ret) <- (x@dimnames[[2]])[(poffset[1,2]+1):(poffset[1,2]+psize[1,2])]
        }
      }, error=function(e){})
      ret
    },error = handle_presto_exception)
  })

setMethod("getpartition", signature("dobject", "numeric", "numeric"),
          function(x, y, z, ...) {
            num.splits <- ceiling(x@dim/x@blocks)
            if (y<=0||y>num.splits[1]||z<=0||z>num.splits[2]) {
		stop("split index must be larger than 0 and smaller than # splits")
	    }
            idx <- (y-1)*num.splits[2] + z
            getpartition(x, idx)
          })


# The class 'rcall' encapsulates the function, arguments and names for a
# remote execution
setClass("rcall", representation(func="character", 
                                 names="list",
                                 splits="list",
                                 raw_names="list",
                                 raw_values="list"), 
                   prototype(func="", names=list(), splits=list(),
                             raw_names=list(), raw_values=list()),
                   S3methods=TRUE)

setMethod("initialize", "rcall",
          function(.Object, func, names, splits, raw_names, raw_values) {
            .Object@func = func
            .Object@names = names
            .Object@splits = splits
            .Object@raw_names = raw_names
            .Object@raw_values = raw_values
            .Object
          })

rcall <- function(func, names, splits, raw_names, raw_values,...)
  UseMethod("rcall")

setMethod("rcall", signature("character", "list", "list", "list", "list"),
  function(func, names, splits, raw_names, raw_values,...) {
    new ("rcall", func, names, splits, raw_names, raw_values)
  })

# foreach generic definition 
foreach <- function(index, range, func, progress=TRUE, scheduler=0, inputs=integer(0)) {
  options(error=dump.frames) #for debugging

  if (class(range) != "numeric" && class(range) != "integer") {
    stop("Foreach range must be a vector!")
  }
  if (class(func) != "function") {
    stop("Foreach func must be a function!")
  }
  if(!(scheduler==0 || scheduler ==1)){
    stop("Supported scheduler policies are 0 (minimize data movement) or 1 (send task to location of first argument)")
  }

start <- proc.time()[3]
  
  # Get function, convert it to string
  func_body <- deparse(body(func), width.cutoff=500);
  # Get list of argument names
  args <- formals(func)
  all_arg_names <- as.list(names(args))

  # Names of arguments
  arg_names = vector("list", 0)
  split_names = vector("list", 0)
  raw_arg_names = vector("list", 0)
  update_args = vector("list", 0)

  arg_vals = vector("list", 0)
  raw_arg_vals = vector("list", 0)

  assign(deparse(substitute(index)), range, envir=parent.frame())

  args.time <- 0
  raw.args.time <- 0
  a <- 0
  while ( a < length(args) ) {
#    eval_arg <- eval(eval(substitute(args[[a]]), envir=parent.frame()), envir=parent.frame())
    a <- a + 1
    if (length(args[[a]]) > 1 && deparse(args[[a]][[1]]) == "splits") {
st <- proc.time()[3]
      
      length(arg_names) <- length(arg_names) + 1
      arg_names[[length(arg_names)]] <- all_arg_names[[a]]

      length(split_names) <- length(split_names) + 1
      split_names[[length(split_names)]] <- eval(args[[a]][2][[1]], envir=parent.frame())@name

      length(arg_vals) <- length(arg_vals) + 1
      if (length(args[[a]]) == 3) { # single split
	arg_vals[[length(arg_vals)]] <- as.list(as.integer(eval(args[[a]][3][[1]], envir=parent.frame())-1))

	if (length(range)>1 && is.numeric(args[[a]][[3]]))  update_args = append(update_args, all_arg_names[[a]][1])

        nparts = eval(args[[a]][[2]], envir=parent.frame())@dobject_ptr$num_splits()  #validate splits arguments
	range.split<-(unlist(arg_vals[[length(arg_vals)]]))+1
        if (nparts < max(range.split))  {
           stop(paste("splits() failure: Object ",args[[a]][[2]]," does not have as many partitions as specified.\n",sep=""))
	}
        if (min(range.split)<1)  {
           stop(paste("splits() failure: Object ",args[[a]][[2]]," refers to partition with id < 1.\n",sep=""))
	}
      } else { # composite
        dobj <- eval(args[[a]][2][[1]], envir=parent.frame())
        if(is.darray(dobj)) {
           if(((dim(dobj)[1]*dim(dobj)[2] > .Machine$integer.max) && !dobj@sparse) || all(dim(dobj)<=.Machine$integer.max) == FALSE)
              stop(paste("splits() failure: Cannot execute function on whole darray with dimension larger than", .Machine$integer.max,
                         "or number of elements more than",.Machine$integer.max))
        }
        if (length(range)>1)  update_args = append(update_args, all_arg_names[[a]][1])

        num.splits <- eval(args[[a]][2][[1]], envir=parent.frame())@dobject_ptr$num_splits()
        arg_vals[[length(arg_vals)]] <- list(as.integer(0:(num.splits-1)))
      }

args.time <- args.time + proc.time()[3] - st
    } else {
st <- proc.time()[3]
      length(raw_arg_names) <- length(raw_arg_names) + 1
      raw_arg_names[[length(raw_arg_names)]] <- all_arg_names[[a]]

      length(raw_arg_vals) <- length(raw_arg_vals) + 1
      idx.string <- deparse(substitute(index))
      names.in.raw <- all.names(args[[a]])
      found <- FALSE
      if (length(names.in.raw) > 0) {
        for (i in 1:length(names.in.raw)) {
          if (names.in.raw[i] == idx.string) {
                                        # need to eval for all j in range
            found <- TRUE
            env <- new.env(parent = parent.frame())
            data <- lapply(lapply(range, function(i) { assign(idx.string, i, env); eval(args[[a]], envir=env) }),
                           serialize, connection = NULL)
            raw_arg_vals[[length(raw_arg_vals)]] <- data
            break
          }
        }
      }

      if (!found) {
        raw_arg_vals[[length(raw_arg_vals)]] <- list(serialize(eval(args[[a]], envir=parent.frame()), connection = NULL))
      }
      raw.args.time <- raw.args.time + proc.time()[3] - st
    }
  }

  # validate update() statements
  norm_func <- gsub("\\s+", "", func_body)
  update_match <- gregexpr('^update\\([a-zA-Z0-9]+)$', norm_func, ignore.case=TRUE)
  x <- 0
  while(x < length(norm_func)) {
    x <- x+1 
    if (update_match[[x]][[1]]>-1) {
        arg_name <- substr(norm_func[[x]], 8, gregexpr(")", norm_func[[x]])[[1]][[1]]-1)
        if (arg_name %in% update_args) stop(paste("Error in update(", arg_name, "): A split or composite array cannot be updated more than once", sep=""))
        mtch <- match(arg_name, all_arg_names)
        errorstring <- paste("Error in ",norm_func[[x]], ": Variable '", arg_name, "' is not mapped to any distributed object found in the foreach function argument\n", sep="") 
        if (!is.na(mtch)) {
           dobj_name <- if ( length(args[[mtch]])>1 && deparse(args[[mtch]][[1]])=="splits" ) args[[mtch]][[2]]
                         else NA
           #if ( !is.darray(eval(dobj_name, envir=parent.frame())) && !is.dframe(eval(dobj_name, envir=parent.frame())) 
           #      && !class(eval(dobj_name, envir=parent.frame()))=="dlist")  { 
           if (!inherits(eval(dobj_name, envir=parent.frame()), "dobject"))
              stop(errorstring)  
       }   
       else  {
        stop(errorstring)
       }   
    }   
  }

  max_argval_size = 0
  for (av in arg_vals) {
    max_argval_size = max_argval_size + max(unlist(lapply(av, object.size)))
  }
  for (rav in raw_arg_vals) {
    max_argval_size = max_argval_size + max(unlist(lapply(rav, object.size)))
  }

  if(max_argval_size >= .rcpp_obj_max_size) {
    stop(paste("Each function argument size cannot be larger than 2GB. Current size: ", max_argval_size, sep=""))
  }

  tryCatch({
    pm <- get_pm_object()
    status <- .Call("DistributedObject_ExecR",
        pm,
        func_body,
        length(range),
        arg_names,
        split_names,
        arg_vals,
        raw_arg_names,
        raw_arg_vals,
        TRUE,
        as.integer(scheduler),
        as.integer(inputs),
        progress,
        DUP=FALSE)
  },error = handle_presto_exception)

  #return(status)
}
