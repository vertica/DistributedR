library(HPdata)
library(vRODBC)

data_path <- system.file("data", package="HPdata")
connect <- odbcConnect("distr_regression")

context("db2darray View: Check data loading")

test_that("db2darray() loads correctly", {

    ## 1: local + no specific columns
    A <- db2darray("view_10K_numeric", "distr_regression")
    expect_that(A, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 8)
    expect_equal(max(A), 9999)
    expect_equal(min(A), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,5], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))
    
    ## 2: uniform + no specific columns
    A <- db2darray("view_10K_numeric", "distr_regression", loadPolicy="uniform")
    expect_that(A, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 8)
    expect_equal(max(A), 9999)
    expect_equal(min(A), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,5], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))

    
    ## 3: local + specific columns
    A <- db2darray("view_10K_numeric", "distr_regression", features=list("rowid"))
    expect_that(A, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 1)
    expect_equal(max(A), 9999)
    expect_equal(min(A), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name) 
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))

    ## 4: uniform + specific columns
    A <- db2darray("view_10K_numeric", "distr_regression", features=list("col1"), loadPolicy="uniform")
    expect_that(A, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 1)
    expect_equal(max(A), 1)
    expect_equal(min(A), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='col1'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))

    ## 5: except argument
    A <- db2darray("view_10K_numeric", "distr_regression", except=list("rowid","col1"))
    expect_equal(colnames(A), c("col2", "col3", "col4", "col5", "col6", "col7"))
})
#Check columns


context("db2dframe View: Check data loading")

test_that("db2dframe() loads correctly", {

    ## 1:local + no specific columns
    A <- db2dframe("view_10K", "distr_regression")
    expect_that(A, is_a("dframe"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 11)
    minmax_da <- darray(c(npartitions(A), 1), c(1,1))
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(max(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 9999)
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(min(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(min(getpartition(minmax_da)), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))
    expect_that(part[,10], is_a("character"))


    ## 2: uniform + no specific columns
    A <- db2dframe("view_10K", "distr_regression", loadPolicy="uniform")
    expect_that(A, is_a("dframe"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 11) 
    minmax_da <- darray(c(npartitions(A), 1), c(1,1))
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(max(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 9999)
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(min(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(min(getpartition(minmax_da)), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))
    expect_that(part[,10], is_a("character"))


    ## 3: local + specific columns    
    A <- db2dframe("view_10K", "distr_regression", features=list("rowid"))
    expect_that(A, is_a("dframe"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 1) 
    minmax_da <- darray(c(npartitions(A), 1), c(1,1))
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(max(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 9999)
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), dframe_index=1) { m <- matrix(min(a[[dframe_index]]), 1, 1); update(m); })
    expect_equal(min(getpartition(minmax_da)), 0)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))


    ## 4: uniform + specific columns
    A <- db2dframe("view_10K", "distr_regression", features=list("col1", "col2"), loadPolicy="uniform")
    expect_that(A, is_a("dframe"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A), 10000)
    expect_equal(ncol(A), 2) 
    minmax_da <- darray(c(npartitions(A), 2), c(1,2))
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(max(a[[index]]), max(a[[index+1]])), nrow=1, ncol=2); update(m); })
    expect_equal(max(getpartition(minmax_da)[,1]), 1)
    expect_equal(max(getpartition(minmax_da)[,2]), 0.999855)
    foreach(i, 1:npartitions(A), function(a=splits(A, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(min(a[[index]]), min(a[[index+1]])), nrow=1, ncol=2); update(m); })
    expect_equal(min(getpartition(minmax_da)[,1]), 0)
    expect_equal(min(getpartition(minmax_da)[,2]), 1.8e-05)
    #Check columns
    table_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K' and column_name in ('col1', 'col2')", stringsAsFactors=FALSE)
    expect_equal(colnames(A), table_columns$column_name)
    #Check data type
    part <- getpartition(A, 1)
    expect_that(part[,1], is_a("integer"))
    expect_that(part[,2], is_a("numeric"))

    ## 5: except argument
    A <- db2dframe("view_10K", "distr_regression", except=list("rowid", "col1", "col8", "col9", "col10"))
    expect_equal(colnames(A), c("col2", "col3", "col4", "col5", "col6", "col7"))
})


context("db2darrays View: Check data loading")

test_that("db2darrays() loads correctly", {

    ## local + no specific columns
    A <- db2darrays("view_10K_numeric", "distr_regression", resp=list("rowid"))
    expect_that(A, is_a("list"))
    expect_that(A$X, is_a("darray"))
    expect_that(A$Y, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A$X), 10000)
    expect_equal(ncol(A$X), 7)
    expect_equal(nrow(A$Y), 10000)
    expect_equal(ncol(A$Y), 1)
    expect_equal(max(A$Y), 9999)
    expect_equal(min(A$Y), 0)
    minmax_da <- darray(c(npartitions(A$X), 1), c(1,1))
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(max(a[,index]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 1)
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(min(a[,index]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 0)
    #Columns
    resp_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A$Y), resp_columns$column_name)
    pred_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name in ('col1', 'col2', 'col3', 'col4', 'col5', 'col5', 'col6', 'col7')", stringsAsFactors=FALSE)
    expect_equal(colnames(A$X), pred_columns$column_name)
    #Check data type
    part <- getpartition(A$X, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))
    part <- getpartition(A$Y, 1)
    expect_that(part[,1], is_a("integer"))

    
    ## 2: uniform + no specific columns
    A <- db2darrays("view_10K_numeric", "distr_regression", resp=list("rowid"), loadPolicy="uniform")
    expect_that(A, is_a("list"))
    expect_that(A$X, is_a("darray"))
    expect_that(A$Y, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A$X), 10000)
    expect_equal(ncol(A$X), 7)
    expect_equal(nrow(A$Y), 10000)
    expect_equal(ncol(A$Y), 1)
    expect_equal(max(A$Y), 9999)
    expect_equal(min(A$Y), 0)
    minmax_da <- darray(c(npartitions(A$X), 1), c(1,1))
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(max(a[,index]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 1)
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(min(a[,index]), 1, 1); update(m); })
    expect_equal(max(getpartition(minmax_da)), 0)
    #Columns
    resp_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A$Y), resp_columns$column_name)
    pred_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name in ('col1', 'col2', 'col3', 'col4', 'col5', 'col5', 'col6', 'col7')", stringsAsFactors=FALSE)
    expect_equal(colnames(A$X), pred_columns$column_name)
    #Check data type
    part <- getpartition(A$X, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,3], is_a("numeric"))
    expect_that(part[,7], is_a("numeric"))
    part <- getpartition(A$Y, 1)
    expect_that(part[,1], is_a("integer"))


    ## 3: local + specific columns
    A <- db2darrays("view_10K_numeric", "distr_regression", resp=list("rowid"), pred=list("col1", "col2"))
    expect_that(A, is_a("list"))
    expect_that(A$X, is_a("darray"))
    expect_that(A$Y, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A$X), 10000)
    expect_equal(ncol(A$X), 2)
    expect_equal(nrow(A$Y), 10000)
    expect_equal(ncol(A$Y), 1)
    expect_equal(max(A$Y), 9999)
    expect_equal(min(A$Y), 0)
    minmax_da <- darray(c(npartitions(A$X), 2), c(1,2))
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(max(a[,index]), max(a[,index+1])), nrow=1, ncol=2); update(m); })
    expect_equal(max(getpartition(minmax_da)[,1]), 1)
    expect_equal(max(getpartition(minmax_da)[,2]), 0.999801)
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(min(a[,index]), min(a[,index+1])), nrow=1, ncol=2); update(m); })
    expect_equal(min(getpartition(minmax_da)[,1]), 0)
    expect_equal(min(getpartition(minmax_da)[,2]), 0.000125)
    #Columns
    resp_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A$Y), resp_columns$column_name)
    pred_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name in ('col1', 'col2')", stringsAsFactors=FALSE)
    expect_equal(colnames(A$X), pred_columns$column_name)
    #Check data type
    part <- getpartition(A$X, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,2], is_a("numeric"))

    

    ## 4: uniform + specific columns
    A <- db2darrays("view_10K_numeric", "distr_regression", resp=list("rowid"), pred=list("col1", "col2"), loadPolicy="uniform")
    expect_that(A, is_a("list"))
    expect_that(A$X, is_a("darray"))
    expect_that(A$Y, is_a("darray"))
    expect_equal(system("ls -lh | grep DR-loader- | wc -l", intern=TRUE), "0")
    expect_equal(nrow(A$X), 10000)
    expect_equal(ncol(A$X), 2)
    expect_equal(nrow(A$Y), 10000)
    expect_equal(ncol(A$Y), 1)
    expect_equal(max(A$Y), 9999)
    expect_equal(min(A$Y), 0)
    minmax_da <- darray(c(npartitions(A$X), 2), c(1,2))
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(max(a[,index]), max(a[,index+1])), nrow=1, ncol=2); update(m); })
    expect_equal(max(getpartition(minmax_da)[,1]), 1)
    expect_equal(max(getpartition(minmax_da)[,2]), 0.999801)
    foreach(i, 1:npartitions(A$X), function(a=splits(A$X, i), m=splits(minmax_da, i), index=1) { m <- matrix(c(min(a[,index]), min(a[,index+1])), nrow=1, ncol=2); update(m); })
    expect_equal(min(getpartition(minmax_da)[,1]), 0)
    expect_equal(min(getpartition(minmax_da)[,2]), 0.000125)
    #Columns
    resp_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name='rowid'", stringsAsFactors=FALSE)
    expect_equal(colnames(A$Y), resp_columns$column_name)
    pred_columns <- sqlQuery(connect, "select column_name from view_columns where table_name='view_10K_numeric' and column_name in ('col1', 'col2')", stringsAsFactors=FALSE)
    expect_equal(colnames(A$X), pred_columns$column_name)
    #Check data type
    part <- getpartition(A$X, 1)
    expect_that(part[,1], is_a("numeric"))
    expect_that(part[,2], is_a("numeric"))

    ## 5: except argument
    A <- db2darrays("view_10K_numeric", "distr_regression", resp=list("rowid"), except=list("col1"), loadPolicy="uniform")
    expect_equal(colnames(A$X), c("col2", "col3", "col4", "col5", "col6", "col7"))

})

context("db2dgraph View: Check data loading")

test_that("db2dgraph() loads correctly", {

    dg <- db2dgraph("view_graph", "distr_regression", "u", "v", "weight")
    expect_equal(sum(dg$X), 10)
    expect_equal(sum(dg$W), 17.3)
    expect_equal(dim(dg$X), c(8, 8))
    expect_equal(dim(dg$W), c(8, 8))
    expect_equal(partitionsize(dg$X)[1, 1], 8)
    expect_equal(partitionsize(dg$W)[1, 1], 8)

    dg <- db2dgraph("view_graph", "distr_regression", "u", "v", "weight", row_wise=TRUE)
    expect_equal(sum(dg$X), 10)
    expect_equal(sum(dg$W), 17.3)
    expect_equal(dim(dg$X), c(8, 8))
    expect_equal(dim(dg$W), c(8, 8))
    expect_equal(partitionsize(dg$X)[1, 2], 8)
    expect_equal(partitionsize(dg$W)[1, 2], 8)

})

odbcClose(connect)
