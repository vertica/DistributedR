
library(HPdutility)

context("Negative Tests: hpdsample()")

##############################################################################
# Generate test data
##############################################################################

a <- as.darray(as.matrix(1:100))  
adf <- as.dframe(as.data.frame(1:100))  

##############################################################################
# Argument checks
##############################################################################

test_that("data1 must be a darray/dframe", {
  e <- "'data1' must be a darray or dframe"
  expect_error(hpdsample(data1 = NA, nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = 1, nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = data.frame(c(1, 2)), 
                         nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = "c", 
                         nSamplePartitions = 1, samplingRatio = 0.1), e)
})

test_that("data2 must be a darray/dframe", {
  e <- "'data2' must be a darray or dframe"
  expect_error(hpdsample(data1 = a, data2 = NA, nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = a, data2 = 1, nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = a, data2 = data.frame(c(1, 2)), 
                         nSamplePartitions = 1, samplingRatio = 0.1), e)
  expect_error(hpdsample(data1 = a, data2 = "c", 
                         nSamplePartitions = 1, samplingRatio = 0.1), e)
})

test_that("Errors are thrown when required arguments are missing", {
  # data1
  expect_error(hpdsample(data2 = a, nSamplePartitions = 1, samplingRatio = 0.1),
               'argument "data1" is missing, with no default')
  # nSamplePartitions
  expect_error(hpdsample(data1 = a, samplingRatio = 0.1),
               'argument "nSamplePartitions" is missing, with no default')

  # samplingRatio
  expect_error(hpdsample(data1 = a, nSamplePartitions = 2),
               'argument "samplingRatio" is missing, with no default')
})

test_that("nSamplePartitions must be a positive integer", {
  e <- "'nSamplePartitions' must be a positive integer"
  expect_error(hpdsample(a, nSamplePartitions = -1,      samplingRatio = 0.1), e)
  expect_error(hpdsample(a, nSamplePartitions = 0,       samplingRatio = 0.1), e)
  expect_error(hpdsample(a, nSamplePartitions = 1.5,     samplingRatio = 0.1), e)
  expect_error(hpdsample(a, nSamplePartitions = NA,      samplingRatio = 0.1), e)
  expect_error(hpdsample(a, nSamplePartitions = 'c',     samplingRatio = 0.1), e)
  expect_error(hpdsample(a, nSamplePartitions = c(1, 2), samplingRatio = 0.1), e)
})

test_that("samplingRatio must be a positive number", {
  e <- "'samplingRatio' must be a positive number"
  expect_error(hpdsample(a, nSamplePartitions = 6, samplingRatio = -1), e)
  expect_error(hpdsample(a, nSamplePartitions = 6, samplingRatio = 0), e)
  expect_error(hpdsample(a, nSamplePartitions = 6, samplingRatio = NA), e)
  expect_error(hpdsample(a, nSamplePartitions = 6, samplingRatio = c(1, 2)), e)
  expect_error(hpdsample(a, nSamplePartitions = 6, samplingRatio = c('a')), e)
})

test_that("data1 and data2 must have the same partition structure", {
  e <- "'data1' and 'data2' must have the same number of partitions, and corresponding partitions must have the same size"
  # Diff # rows
  d1 <- as.darray(as.matrix(1:100))
  d2 <- as.darray(as.matrix(1:101))
  expect_error(hpdsample(d1, d2, 6, 0.1), e)

  # Same number of rows, different # partitions
  d1 <- darray(npartition = 1)
  foreach(i, 1:npartitions(d1), function(ds = splits(d1, i)) {
    ds <- as.matrix(1:100)
    update(ds)
  }, progress = F)
  d2 <- as.darray(as.matrix(1:100))
  expect_true(nrow(d1) == nrow(d2))
  expect_error(hpdsample(d1, d2, 6, 0.1), e)

  # Same number of rows, same number of partitions, different partition sizes
  d1 <- darray(npartition = 6)
  foreach(i, 1:npartitions(d1), function(ds = splits(d1, i),
                                         i = i) {
    ds <- as.matrix(1:i)
    update(ds)
  }, progress = F)
  d2 <- as.darray(as.matrix(1:21)) 
  expect_true(nrow(d1) == nrow(d2))
  expect_error(hpdsample(d1, d2, 6, 0.1), e)
})

test_that("data1 must be row-wise partitioned", {
  e <- "'data1' must be partitioned row-wise"
  d1 <- darray(dim = c(9, 9), blocks = c(3, 3), data = 1)
  d2 <- as.darray(as.matrix(1:9))
  expect_error(hpdsample(d1, nSamplePartitions = 4, samplingRatio = 0.1), e)
  expect_error(hpdsample(d1, d2, nSamplePartitions = 4, samplingRatio = 0.1), e)
})

test_that("data2 must be row-wise partitioned", {
  e <- "'data2' must be partitioned row-wise"
  d1 <- as.darray(as.matrix(1:9))
  d2 <- darray(dim = c(9, 9), blocks = c(3, 3), data = 1)
  expect_error(hpdsample(d1, d2, nSamplePartitions = 4, samplingRatio = 0.1), e)
})

context("Positive Tests: hpdsample()")

test_that("number of output partitions is correct", {
  nSamplePartitions <- 1:5
  for (n in nSamplePartitions) {
    sa <- hpdsample(a, nSamplePartitions = n, samplingRatio = 0.5)
    expect_equal(n, npartitions(sa), label = paste0("nSamplePartitions = ",n))
    sadf <- hpdsample(adf, nSamplePartitions = n, samplingRatio = 0.5)
    expect_equal(n, npartitions(sadf), label = paste0("nSamplePartitions = ",n))
  }
})

test_that("sample mean and variance are similar to global mean and variance", {
  rawdata    <- as.matrix(1:100000)
  data       <- as.darray(rawdata)
  nSamplePartitions <- 6
  sdata      <- hpdsample(data, nSamplePartitions = nSamplePartitions, samplingRatio = 0.1)
  globalMean <- mean(rawdata)
  globalVar  <- var(rawdata)
  dlocMeans  <- darray(npartitions = nSamplePartitions)
  dlocVars   <- darray(npartitions = nSamplePartitions)
  foreach(i, 1:nSamplePartitions, function(lms = splits(dlocMeans, i), 
                                    lvs = splits(dlocVars, i),
                                    ss = splits(sdata, i)) {
    lms <- mean(ss)
    lvs <- var(ss)
    update(lms)
    update(lvs)
  }, progress = F)
  locMeans <- getpartition(dlocMeans)
  locVars  <- getpartition(dlocVars)

#  print(paste('glob mean:', globalMean))
#  print(paste('loc means:', paste(locMeans, collapse = ", ")))
#  print(paste('glob var:', globalVar))
#  print(paste('loc vars:', paste(locVars, collapse = ", ")))

  expect_true(all((locMeans - globalMean)/globalMean < 0.1))
  expect_true(all((as.vector(locVars) - globalVar)/globalVar < 0.1))
})
 
test_that("when data1 and data2 provided, samples correspond", {
  d1 <- as.darray(as.matrix(1:1001))
  d2 <- as.darray(as.matrix(1:1001) + 1)
  nSamplePartitions <- 1:6
  samplingRatios <- seq(0.1, 1, 0.2)
  for (n in nSamplePartitions) {
    for (sr in samplingRatios) {
      sd <- hpdsample(d1, d2, nSamplePartitions = n, samplingRatio = sr)
      s1 <- getpartition(sd[[1]])
      s2 <- getpartition(sd[[2]])
      expect_true(all(s1 + 1 == s2))
    }
  }
})

test_that("when data1 and data2 provided, works with all combo of dframe/darray", {
  d1 <- as.darray(as.matrix(1:1001))
  d2 <- as.dframe(as.data.frame(1:1001) + 1)
  n  <- 6
  sr <- 0.1
  sd <- hpdsample(d1, d1, nSamplePartitions = n, samplingRatio = sr)
  sd <- hpdsample(d2, d1, nSamplePartitions = n, samplingRatio = sr)
  sd <- hpdsample(d1, d2, nSamplePartitions = n, samplingRatio = sr)
  sd <- hpdsample(d2, d2, nSamplePartitions = n, samplingRatio = sr)
})

test_that("works with diff data types", {
  d1 <- as.darray(as.matrix(1:1001))
  d2 <- as.dframe(data.frame(a = rep('c', 1001), 
                             b = 1:1001, 
                             c = runif(1001), 
                             d = c(rep('a', 501), rep('b', 500))))
  print(str(getpartition(d2)))
  n  <- 6
  sr <- 0.1
  # Shouldn't break
  sd <- hpdsample(d1, d1, nSamplePartitions = n, samplingRatio = sr)

  sd <- hpdsample(d2, nSamplePartitions = n, samplingRatio = sr)
  expect_true(all(sapply(getpartition(sd), class) == 
      sapply(getpartition(d2), class)))

  sd <- hpdsample(d2, d1, nSamplePartitions = n, samplingRatio = sr)

  # Make sure the classes of the output columns are the same as the ones of the
  # input columns
  expect_true(all(sapply(getpartition(sd[[1]]), class) == 
      sapply(getpartition(d2), class)))
  print(str(getpartition(sd[[1]])))

  sd <- hpdsample(d1, d2, nSamplePartitions = n, samplingRatio = sr)
  expect_true(all(sapply(getpartition(sd[[2]]), class) == 
      sapply(getpartition(d2), class)))
  sd <- hpdsample(d2, d2, nSamplePartitions = n, samplingRatio = sr)
  expect_true(all(sapply(getpartition(sd[[2]]), class) == 
      sapply(getpartition(d2), class)))
})

test_that("all output partitions have the same size", {
  nSamplePartitions <- 1:5
  for (n in nSamplePartitions) {
    sa <- hpdsample(a, nSamplePartitions = n, samplingRatio = 0.5)
    expect_true(all(partitionsize(sa)[,1] == partitionsize(sa)[1,1]))
  }
})

