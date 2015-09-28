
library(distributedR)
library(HPdutility)
distributedR_start()

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

})

test_that("data2 must be a darray/dframe", {

})

test_that("Errors are thrown when required arguments are missing", {
  # data1
  # nSampParts
  # nSampPerPartition
})

test_that("nSampParts must be a positive integer", {
  e <- "'nSampParts' must be a positive integer"
  expect_error(hpdsample(a, nSampParts = -1,      sampRatio = 0.1), e)
  expect_error(hpdsample(a, nSampParts = 0,       sampRatio = 0.1), e)
  expect_error(hpdsample(a, nSampParts = 1.5,     sampRatio = 0.1), e)
  expect_error(hpdsample(a, nSampParts = NA,      sampRatio = 0.1), e)
  expect_error(hpdsample(a, nSampParts = 'c',     sampRatio = 0.1), e)
  expect_error(hpdsample(a, nSampParts = c(1, 2), sampRatio = 0.1), e)
})

test_that("sampRatio must be a positive number", {
  e <- "'sampRatio' must be a positive number"
  expect_error(hpdsample(a, nSampParts = 6, sampRatio = -1), e)
  expect_error(hpdsample(a, nSampParts = 6, sampRatio = 0), e)
  expect_error(hpdsample(a, nSampParts = 6, sampRatio = NA), e)
  expect_error(hpdsample(a, nSampParts = 6, sampRatio = c(1, 2)), e)
  expect_error(hpdsample(a, nSampParts = 6, sampRatio = c('a')), e)
})

context("Positive Tests: hpdsample()")


test_that("number of output partitions is correct", {
  nSampParts <- 1:5
  for (n in nSampParts) {
    sa <- hpdsample(a, nSampParts = n, sampRatio = 0.5)
    expect_equal(n, npartitions(sa), label = paste0("nSampParts = ",n))
    sadf <- hpdsample(adf, nSampParts = n, sampRatio = 0.5)
    expect_equal(n, npartitions(sadf), label = paste0("nSampParts = ",n))
  }
})

test_that("sample mean and variance are similar to global mean and variance", {
  rawdata    <- as.matrix(1:100000)
  data       <- as.darray(rawdata)
  nSampParts <- 6
  sdata      <- hpdsample(data, nSampParts = nSampParts, sampRatio = 0.1)
  globalMean <- mean(rawdata)
  globalVar  <- var(rawdata)
  dlocMeans  <- darray(npartitions = nSampParts)
  dlocVars   <- darray(npartitions = nSampParts)
  foreach(i, 1:nSampParts, function(lms = splits(dlocMeans, i), 
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
  nSampParts <- 1:6
  sampRatios <- seq(0.1, 2, 0.5)
  for (n in nSampParts) {
    for (sr in sampRatios) {
      sd <- hpdsample(d1, d2, nSampParts = n, sampRatio = sr)
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
  sd <- hpdsample(d1, d1, nSampParts = n, sampRatio = sr)
  sd <- hpdsample(d2, d1, nSampParts = n, sampRatio = sr)
  sd <- hpdsample(d1, d2, nSampParts = n, sampRatio = sr)
  sd <- hpdsample(d2, d2, nSampParts = n, sampRatio = sr)
})


test_that("works with diff data types", {
  d1 <- as.darray(as.matrix(1:1001))
  d2 <- as.dframe(data.frame(a = rep('c', 1001), 
                             b = 1:1001, 
                             c = runif(1001), 
                             d = rep('a', 1001)))
  n  <- 6
  sr <- 0.1
  sd <- hpdsample(d1, d1, nSampParts = n, sampRatio = sr)

  sd <- hpdsample(d2, d1, nSampParts = n, sampRatio = sr)
  # Make sure the classes of the output columns are the same as the ones of the
  # input columns
  all(sapply(getpartition(sd[[1]]), class) == 
      sapply(getpartition(d2), class))

  sd <- hpdsample(d1, d2, nSampParts = n, sampRatio = sr)
  all(sapply(getpartition(sd[[2]]), class) == 
      sapply(getpartition(d2), class))
  sd <- hpdsample(d2, d2, nSampParts = n, sampRatio = sr)
  all(sapply(getpartition(sd[[2]]), class) == 
      sapply(getpartition(d2), class))
})

test_that("all output partitions have the same size", {
  nSampParts <- 1:5
  for (n in nSampParts) {
    sa <- hpdsample(a, nSampParts = n, sampRatio = 0.5)
    expect_true(all(partitionsize(sa)[,1] == partitionsize(sa)[1,1]))
  }
})


distributedR_shutdown()
