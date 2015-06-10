library(HPdata)
library(vRODBC)

data_path <- system.file("data", package="HPdata")
connect <- odbcConnect("distr_regression")

system(paste("vsql -f", data_path, "/schema.sql", sep=""))
print("Created schema");

system(paste("vsql -c \"COPY table_10K_numeric FROM '", data_path, "/data_10K_numeric.dat' DELIMITER '|' DIRECT\"", sep=""))
system(paste("vsql -c \"COPY table_10K FROM '", data_path, "/data_10K.dat' DELIMITER '|' DIRECT\"", sep=""))
system(paste("vsql -c \"COPY table_graph FROM '", data_path, "/graph3.dat' DELIMITER ' ' DIRECT\"", sep=""))
print("Uploaded data");

system(paste("vsql -c \"CREATE VIEW view_10K_numeric AS (SELECT * FROM table_10K_numeric)\""))
system(paste("vsql -c \"CREATE VIEW view_10K AS (SELECT * FROM table_10K)\""))
system(paste("vsql -c \"CREATE VIEW view_graph AS (SELECT * FROM table_graph)\""))

system("mkdir /tmp/graphSplits")
print("Created /tmp/graphSplits folder for the output files of splitGraphFile function")

context("Setup and validate setup")

test_that("Schema is created and data is loaded correctly", {
    res <- sqlQuery(connect, "select count(*) from tables where table_name in ('table_10K_numeric', 'table_10K', 'table_graph')")
    expect_that(res, is_a("data.frame"))
    expect_equal(res[[1]][[1]], 3)

    res <- sqlQuery(connect, "select count(*) from views where table_name in ('view_10K', 'view_10K_numeric', 'view_graph')")
    expect_that(res, is_a("data.frame"))
    expect_equal(res[[1]][[1]], 3)
})

test_that("DistributedR extension package is installed", {
    res <- sqlQuery(connect, "select count(*) from user_functions where function_name in ('DeployModelToVertica', 'ExportToDistributedR', 'DeleteModel')");
    expect_that(res, is_a("data.frame"))
    expect_equal(res[[1]][[1]], 3)
})

odbcClose(connect)
