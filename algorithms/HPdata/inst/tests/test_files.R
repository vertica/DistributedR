library(HPdata)

data_path <- system.file("data", package="HPdata")

context("splitGraphFile function validations")

test_that("Test splitGraphFile() arguments validation", {
    expect_error(ret <- splitGraphFile (inputFile= "wrongPath", outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS=TRUE))
    expect_error(ret <- splitGraphFile (inputFile= paste(data_path, "/graph3.dat", sep=""), outputPath= "wrongPath" , npartitions=2 , isNFS=TRUE))
    expect_error(ret <- splitGraphFile (inputFile= paste(data_path, "/graph3.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=10 , isNFS=TRUE))
    expect_error(ret <- splitGraphFile (inputFile= paste(data_path, "/graph3.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS='Yes'))
    expect_error(ret <- splitGraphFile (inputFile= paste(data_path, "/graph3.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS=TRUE, row_wise='No'))
})

test_that("Test splitGraphFile() functionality", {

    ret <- splitGraphFile (inputFile= paste(data_path, "/graph1.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=3 , isNFS=TRUE)
    expect_equal(ret$pathPrefix, "/tmp/graphSplits/graph1.dat")
    expect_equal(ret$nVertices, 8)
    expect_equal(ret$verticesInSplit, 3)
    expect_equal(ret$nFiles, 3)
    expect_false(ret$isWeighted)
    expect_false(ret$row_wise)

    ret <- splitGraphFile (inputFile= paste(data_path, "/graph2.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS=TRUE)
    expect_equal(ret$pathPrefix, "/tmp/graphSplits/graph2.dat")
    expect_equal(ret$nVertices, 8)
    expect_equal(ret$verticesInSplit, 4)
    expect_equal(ret$nFiles, 2)
    expect_true(ret$isWeighted)
    expect_false(ret$row_wise)

    ret <- splitGraphFile (inputFile= paste(data_path, "/graph3.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS=TRUE)
    expect_equal(ret$pathPrefix, "/tmp/graphSplits/graph3.dat")
    expect_equal(ret$nVertices, 8)
    expect_equal(ret$verticesInSplit, 4)
    expect_equal(ret$nFiles, 2)
    expect_true(ret$isWeighted)
    expect_false(ret$row_wise)

    ret <- splitGraphFile (inputFile= paste(data_path, "/graph4.dat", sep=""), outputPath= "/tmp/graphSplits" , npartitions=2 , isNFS=TRUE, row_wise=TRUE)
    expect_equal(ret$pathPrefix, "/tmp/graphSplits/graph4.dat")
    expect_equal(ret$nVertices, 8)
    expect_equal(ret$verticesInSplit, 4)
    expect_equal(ret$nFiles, 2)
    expect_true(ret$isWeighted)
    expect_true(ret$row_wise)

})

context("file2dgraph function validations")

test_that("Test file2dgraph() arguments validation", {

    expect_error(dg1 <- file2dgraph(pathPrefix="wrongPath", nVertices=8, verticesInSplit=3, isWeighted=FALSE))
    expect_error(dg1 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph1.dat", nVertices=10, verticesInSplit=3, isWeighted=FALSE))
    expect_error(dg1 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph1.dat", nVertices=8, verticesInSplit=4, isWeighted=FALSE))
    expect_error(dg1 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph1.dat", nVertices=8, verticesInSplit=3, isWeighted=TRUE))
    expect_error(dg2 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph2.dat", nVertices=8, verticesInSplit=4, isWeighted=FALSE))

})

test_that("Test file2dgraph() functionality", {

    dg1 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph1.dat", nVertices=8, verticesInSplit=3, isWeighted=FALSE)
    expect_equal(sum(dg1$X), 10)
    expect_equal(dim(dg1$X), c(8,8))
    expect_true(is.null(dg1$W))
    expect_equal(partitionsize(dg1$X)[1, 1], 8)

    dg2 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph2.dat", nVertices=8, verticesInSplit=4, isWeighted=TRUE)
    expect_equal(sum(dg2$X), 10)
    expect_equal(dim(dg2$X), c(8,8))
    expect_equal(sum(dg2$W), 13.3)
    expect_equal(dim(dg2$W), c(8,8))
    expect_equal(partitionsize(dg2$X)[1, 1], 8)
    expect_equal(partitionsize(dg2$W)[1, 1], 8)

    dg3 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph3.dat", nVertices=8, verticesInSplit=4, isWeighted=TRUE)
    expect_equal(sum(dg3$X), 10)
    expect_equal(dim(dg3$X), c(8,8))
    expect_equal(sum(dg3$W), 17.3)
    expect_equal(dim(dg3$W), c(8,8))
    expect_equal(partitionsize(dg3$X)[1, 1], 8)
    expect_equal(partitionsize(dg3$W)[1, 1], 8)

    dg4 <- file2dgraph(pathPrefix="/tmp/graphSplits/graph4.dat", nVertices=8, verticesInSplit=4, isWeighted=TRUE, row_wise=TRUE)
    expect_equal(sum(dg4$X), 10)
    expect_equal(dim(dg4$X), c(8,8))
    expect_equal(sum(dg4$W), 17.3)
    expect_equal(dim(dg4$W), c(8,8))
    expect_equal(partitionsize(dg4$X)[1, 2], 8)
    expect_equal(partitionsize(dg4$W)[1, 2], 8)

})

