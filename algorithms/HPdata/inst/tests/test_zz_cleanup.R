library(HPdata)
library(vRODBC)

connect <- odbcConnect("distr_regression")

system(paste("vsql -c \"DROP table table_10K_numeric\"", sep=""))
system(paste("vsql -c \"DROP table table_10K\"", sep=""))
system(paste("vsql -c \"DROP table table_graph\"", sep=""))

system(paste("vsql -c \"DROP VIEW view_10K_numeric\""))
system(paste("vsql -c \"DROP VIEW view_10K\""))
system(paste("vsql -c \"DROP VIEW view_graph\""))

system("rm -r /tmp/graphSplits")

odbcClose(connect)
