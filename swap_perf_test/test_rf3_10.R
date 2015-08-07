library(distributedR)

gb <- as.integer(commandArgs(TRUE)[1])
print(gb)
# row_num = gb * 1024 * 1024 * 1024 / 8 / 128

# distributedR_start()
distributedR_start(cluster_conf="/home/zhenche/distR/3node.xml", log=3)
status <- distributedR_status()
npart <- 48
per_split_row_num <- gb * 1024 * 1024 / npart

# a <- darray(dim=c(row_num, 100), blocks=c(as.integer(row_num / npart) + 1, 100))

dtest <- dframe(npartitions=npart)

foreach(i, 1:npartitions(dtest), init<-function(s=splits(dtest, i), nr=per_split_row_num){
  # s <- matrix(runif(nrow(s) * 100), nrow=nrow(s))
  s <- data.frame(x=matrix(runif(nr * 128), nrow=nr, ncol=128), y=as.factor(rbinom(nr, 1, 0.5)))
  update(s)
})

print(partitionsize(dtest))

colnames(dtest) <- c(paste0("x", 1:128), "y")

library(HPdclassifier)
hpdRF_parallelTree(y ~ ., data=dtest, ntree=10, do.trace=TRUE)


distributedR_shutdown()
