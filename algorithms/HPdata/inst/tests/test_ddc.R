library(HPdata)
distributedR_start()

test_that("ex001.csv", {
  df <- csv2dframe(paste(getwd(),'/data/ex001.csv',sep=''), schema='a:int64,b:string')
  localdf <- getpartition(df)
  expect_true(is.data.frame(localdf))
  expect_equal(nrow(localdf), 128)
  expect_equal(ncol(localdf), 2)
})

test_that("ex002.csv", {
  df <- csv2dframe(paste(getwd(),'/data/ex002.csv',sep=''), schema='a:int64,b:string,c:int64,d:string')
  localdf <- getpartition(df)
  expect_true(is.data.frame(localdf))
  expect_equal(nrow(localdf), 128)
  expect_equal(ncol(localdf), 4)
})

test_that("TestOrcFile.test1.orc", {
  df <- orc2dframe(paste(getwd(),'/data/TestOrcFile.test1.orc',sep=''), selectedStripes='0')
  localdf <- getpartition(df)
  expect_true(is.data.frame(localdf))
  expect_equal(nrow(localdf), 2)
  expect_equal(ncol(localdf), 12)
})



