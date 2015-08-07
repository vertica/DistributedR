library(distributedR)

gb <- as.integer(commandArgs(TRUE)[1])
print(gb)
# row_num = gb * 1024 * 1024 * 1024 / 8 / 128

# distributedR_start()
distributedR_start(cluster_conf="/home/zhenche/distR/cluster_conf.xml", log=3)
status <- distributedR_status()
npart <- sum(status$Inst)
per_split_row_num <- gb * 1024 * 1024 / npart

# a <- darray(dim=c(row_num, 100), blocks=c(as.integer(row_num / npart) + 1, 100))

dtest <- darray(npartitions=npart)

foreach(i, 1:npartitions(dtest), init<-function(s=splits(dtest, i), nr=per_split_row_num){
  # s <- matrix(runif(nrow(s) * 100), nrow=nrow(s))
  s <- matrix(runif(nr * 128), nrow=nr, ncol=128)
  update(s)
})

print(partitionsize(dtest))

colnames(dtest) <- paste0("x", 1:128)

library(HPdcluster)
hpdkmeans(dtest, 10, trace=TRUE, iter.max=2)


distributedR_shutdown()
