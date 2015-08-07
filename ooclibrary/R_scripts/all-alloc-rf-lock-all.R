generate_df <- function(n){
	a = c(1e5 + runif(n), -1e5 + runif(n))
	b = as.factor(c(rep(1, n), rep(-1, n)))
    data.frame(x=a, y=b)
}
library(ooclibrary)
EnableMMapHooks()
SetMemoryLimit(400)

library(randomForest)

LockAllPeriodically(1)
size = as.integer(commandArgs(TRUE)[1])

a = generate_df(size)
randomForest(a$y ~ a$x, ntree=1, do.trace=TRUE)
PrintMMapStatus()
