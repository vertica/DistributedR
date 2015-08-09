library(distributedR)
library(Matrix)
data_path<-system.file("extdata",package="distributedR")

context("Darray argument check")

mx <- matrix(c(1:16), nrow=4,ncol=4)
test_that("Darray: Invalid arguments", {
  mx <- matrix(c(1:16), nrow=4,ncol=4)
  expect_error(darray(1,1))
  expect_error(darray(c(1,1), c(2,2)))
  expect_error(darray(c(10,10),c(-1,5)))
  expect_error(darray(c("a","b"), c(4,4)))
  expect_error(darray(c(1,2,3),c(4,2)))
  expect_error(darray(c(10,10),c(10,0)))
  expect_error(darray(wrongarg=c(2,4)))

})

context("Sparse Darray")

da1 <- darray(c(4,4), c(2,4), sparse=TRUE, data=0)

test_that("Create and Fetch works", {
  da1gp <- getpartition(da1)
  da1gp1 <- getpartition(da1, 1)

  expect_that(da1gp, is_a("dgCMatrix"))
  expect_equal(0, length(da1gp@x))
  expect_equal(5, length(da1gp1@p))
  expect_equal(0, length(da1gp1@i))
  expect_equal(rep(0,5), da1gp1@p)
  expect_equal(2, length(splits(da1)))
  expect_equal(1, splits(da1)@split_ids[2])
  expect_equal(0, splits(da1,1)@split_ids[1])
  
  expect_error(getpartition(mx))
  expect_error(getpartition(da1, 0))
  expect_error(getpartition(da1, 3))
  expect_error(getpartition(da1, -100))
  expect_error(getpartition(da1, mx))
  expect_error(splits(da1,0))
  expect_error(splits(da1,3))
})

context("Dense Darray")

da2 <- darray(c(4,4), c(2,4), sparse=FALSE, data=1)
da3 <- darray(c(4,4), c(4,2), sparse=FALSE, data=2)

test_that("Create and Fetch works", {
  da2gp <- getpartition(da2)
  da2gp2 <- getpartition(da2, 2)
  da3gp2 <- getpartition(da3, 2)

  expect_that(da2gp, is_a("matrix"))
  expect_equal(c(4,4), dim(da2gp))
  expect_equal(da2gp[4],1)
  expect_equal(c(2,4), dim(da2gp2))
  expect_equal(2, npartitions(da2))
  expect_equal(1, splits(da2)@split_ids[2])
  expect_equal(0, splits(da2,1)@split_ids[1])

  expect_equal(2, npartitions(da3))
  expect_equal(da3gp2[2],2)
  expect_equal(c(4,2), dim(da3gp2))

})


context("Loading Darrays using load.darray()")

load.darray(da1,paste(data_path,"/A",sep=""))
load.darray(da2,paste(data_path,"/A",sep=""))
load.darray(da3,paste(data_path,"/B",sep=""))
da4 <- darray(c(4,4),c(2,2),sparse=FALSE, data=1)
da5 <- darray(c(4,4),c(2,2),sparse=TRUE, data=0)
load.darray(da4, paste(data_path,"/C",sep=""))
load.darray(da5, paste(data_path,"/C",sep=""))
mtx1 <- matrix(c(1:16), nrow=4, ncol=4)

test_that("Basic load.darray", {
  da1gp<-getpartition(da1)
  da2gp<-getpartition(da2)
  da3gp<-getpartition(da3)
  sample_data <- c(0,0,0,0,0,1,0,1,0,1,0,1,0,1,0,0)

  expect_equal(sample_data, as.numeric(as.matrix(da1gp)))  #da1 is Sparse darray
  expect_equal(sample_data, as.numeric(da2gp))
  expect_equal(sample_data, as.numeric(da3gp))
  expect_equal(as.numeric(mtx1), as.numeric(as.matrix(getpartition(da4))))
  expect_equal(as.numeric(mtx1), as.numeric(as.matrix(getpartition(da5))))
})


context("as.darray()")

mtx<-matrix(c(1:100), nrow=20)

test_that("Column-partitioned darray: works", {
  da<-as.darray(mtx, blocks=dim(mtx))  
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(20,5))
  expect_equal(as.matrix(getpartition(da)), mtx)

  da<-darray(c(20,5), c(20,1), sparse=FALSE, data=1)
  da<-as.darray(mtx,c(20,1))
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(20,1))
  expect_equal(as.numeric(getpartition(da,2)), c(21:40))
  expect_equal(as.matrix(getpartition(da)), mtx)
})

test_that("Row-partitioned darray: works", {
  da<-darray(c(20,5), c(5,5), sparse=FALSE, data=1)  #row-partitioned
  da<-as.darray(mtx,c(5,5))
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(5,5))
  expect_equal(as.numeric(getpartition(da,3)), c(11:15, 31:35, 51:55, 71:75, 91:95))
  expect_equal(as.matrix(getpartition(da)), mtx)
})

test_that("Block-partitioned darray: works", {
  da<-darray(c(20,5), c(3,3), sparse=FALSE, data=1)  #block-partitioned
  da<-as.darray(mtx, c(3,3))
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(3,3))
  expect_equal(as.matrix(getpartition(da,2)), matrix(c(61:63,81:83), nrow=3))
  expect_equal(as.matrix(getpartition(da,13)), matrix(c(19,20,39,40,59,60), nrow=2))
  expect_equal(as.matrix(getpartition(da,14)), matrix(c(79,80,99,100), nrow=2))
  expect_equal(as.matrix(getpartition(da)), mtx)
})

test_that("More tests", {
  da<-as.darray(mtx, c(2,5))
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(2,5))
  expect_equal(as.matrix(getpartition(da,10)), matrix(c(19,20,39,40,59,60,79,80,99,100), nrow=2))
  expect_equal(as.matrix(getpartition(da)), mtx)
  da<-as.darray(mtx, c(20,1))
  expect_equal(da@dim, c(20,5))
  expect_equal(da@blocks, c(20,1))
  expect_equal(as.matrix(getpartition(da,3)), matrix(c(41:60), ncol=1))
  expect_equal(as.matrix(getpartition(da)), mtx)
})

test_that("Expected errors returned", {
  da<-darray(c(10,5), c(2,2), sparse=TRUE, data=0)
  expect_error(as.darray(mtx, out_darray=da)) # output darray dimension do not match
  expect_error(as.darray(mtx, da_dim=c(30,1))) #desired block dimension does not match
  expect_error(as.darray(mtx, da_dim=c(20,10))) #darray block dim does not match
  expect_error(as.darray(mtx, da_dim=c(20,10)))
})

test_that("as.darray support for input sparse matrix", {
  input1 <- Matrix(data=10, nrow=5, ncol=4, sparse=TRUE)
  input2 <- Matrix(data=10, nrow=5, ncol=5, sparse=TRUE)

  output <- as.darray(input1)
  expect_equal(output@dim, c(5,4))
  expect_equal(output@blocks, c(1,4))
  expect_equal(output@sparse, TRUE)

  output <- as.darray(input2)
  expect_equal(output@dim, c(5,5))
  expect_equal(output@blocks, c(1,5))
  expect_equal(output@sparse, TRUE)
})


context("as.darray() with large data")

large_mat <- matrix(runif(3000*3000), 3000, 3000)
test_that("Dense darrays: works", {
  da<-as.darray(large_mat, blocks=dim(large_mat))
  gpa <- getpartition(da)

  expect_equal(da@dim, c(3000,3000))
  expect_equal(da@blocks, c(3000,3000))
  expect_equal(gpa, large_mat)

  da<-as.darray(large_mat,c(2900,2800))  # mixture of external transfer and protobuf msg
  gpa <- getpartition(da)
  expect_equal(gpa, large_mat)
})


context("Partitionsize")

r <- sample(1:100, 1)
c <- sample(1:100, 1)
rpart <- sample(1:r, 1)
cpart <- sample(1:c, 1)
test_that("Dense darrays: works", {
  da<-darray(dim=c(r,c), blocks=c(rpart, cpart), empty=TRUE)
  expect_equal(nrow(da), 0, info="check darray nrow")
  expect_equal(ncol(da), 0, info="check darray col")
  expect_equal(partitionsize(da,1), matrix(c(rpart,cpart),nrow=1), info="check size of partition 1")
  expect_equal(is.invalid(da), TRUE, info="check validity of empty dense darray after declaration")
})


context("Invalid darrays")
val<-runif(1,0:1)
mat<-array(val, dim=c(r,c))

test_that("Dense darrays: works", {
  da<-darray(dim=c(r,c), blocks=c(rpart, cpart), empty=TRUE)
  foreach(i, 1:npartitions(da), initArrays<-function(y=splits(da,i), v=val, size=partitionsize(da,i)) {
    y <- matrix(v,nrow=size[1],ncol=size[2])
    update(y)
  })

  expect_equal(is.invalid(da), FALSE, info="check validity of empty dense darray after writing content")
  expect_equal(getpartition(da), mat, info="check uninitialized darray")
  expect_equal(nrow(da), r, info="check darray nrow") 
  expect_equal(ncol(da), c, info="check darray col") 
})

test_that("Sparse darrays: works", {
  da <- darray(dim=c(r, c), blocks=c(rpart,c), sparse=TRUE, empty=TRUE)
  expect_equal(is.invalid(da), TRUE, info="check validity of empty sparse darray after declaration")
  expect_equal(nrow(da), 0, info="check sparse empty darray nrow")
  expect_equal(ncol(da), 0, info="check sparse empty darray col")

  foreach(i, 1:npartitions(da), initArrays<-function(y=splits(da,i), index=i-1, dsize=partitionsize(da,i) ) {
        y<-sparseMatrix(i=1,
                        j=1,
                        x=index+1,
                        dims=dsize)
        update(y)
  })
  expect_equal(is.invalid(da), FALSE, info="check validity of empty sparse darray after writing content")
  index<-head(c(1,cumsum(partitionsize(da)[,1])+1),-1)
  y <- sparseMatrix(i=index, j=rep(1,length(index)), dims=c(r,c), x=1:length(index))
  gy <- getpartition(da)
  expect_equal(y, gy)
})
