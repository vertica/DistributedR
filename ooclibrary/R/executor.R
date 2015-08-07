# setClass('big.matrix', representation(address='externalptr'))

# big.object <- function(){
# 	address <- .Call('allocBi')
# 	x <- new('big.matrix', address=address)
# }

# big.vector <- function(length, type){

# 	vec <- .Call("alloc_big_vector", type, as.integer(length))
# 	vec

# }


#' @export
spillexecute <- function(x=NULL, func, config=list()){

	if (class(func) != "function"){
		stop("function is needed for func")
	}

	UseMethod("execute")
}

#' @export
mmapexecute <- function(func){
	body = deparse(body(func))
	str = paste(body, collapse="\n");
	command = eval(parse(text=paste("quote({", str, "})")))
	print(command)
	.Call("EvalRCommand", command)
}

#' @export
execute.default <- function(x, func, config){
	body = deparse(body(func))
	str = paste(body, collapse="\n");
	command = eval(parse(text=paste("quote({", str, "})")))

	.Call("EvalRCommand", command)
}

#' @export
execute.in_session.executor <- function(x, func){
}

setClass("in_session.executor", representation(address='externalptr'))
