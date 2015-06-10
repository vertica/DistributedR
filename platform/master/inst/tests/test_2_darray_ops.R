library(Matrix)
data_path<-system.file("extdata",package="distributedR")


context("Equality and Clone")

mat<-matrix(1:100,10,10)
test_that("Dense darrays: Expected errors are returned", {
  da<-darray(c(10,10),c(5,10),FALSE, data=1)
  db<-darray(c(10,100),c(5,10),FALSE, data=1)
  expect_error(da==db, info="check if two darrays same: different dimension size has to raise an exception")
  db<-darray(c(100,10),c(5,10),FALSE, data=1)
  expect_error(da==db, info="check if two darrays same: different dimension size has to raise an exception")
  db<-darray(c(10,10),c(10,10),FALSE,data=1)
  expect_error(da==db, info="check if two darrays same: different block size has to raise an exception")

  da<-as.darray(mat,c(5,10))
  expect_error(clone(mat), info="clone.dobject: Input other than darray has to raise an exception")
})

test_that("Dense darrays: works", {
  da<-as.darray(mat,c(5,10))
  db<-clone(da)
  expect_true(da==db, info="clone has to return a same value darray")

  db<-darray(c(10,10),c(5,10),FALSE,data=1)
  expect_true(!(da==db), info="check if two darray same: it has to rerutn false")

  da<-darray(c(3,3),c(1,3), data=NA)
  db<-clone(da, ncol=1, data=NA)
  expect_true(all(is.na(getpartition(db))) && (nrow(db)==nrow(da)) && (ncol(db)==1), info="clone has to return a same value for darray (NA case)")

})

test_that("Sparse darrays: works", {
  da<-as.darray(mat,c(5,10))  
  db<-darray(c(10,10),c(5,10),sparse=TRUE, data=0)
  expect_true(!(da==db))
})


context("Basic operations: min/max/head etc")

r <- sample(1:100, 1)
c <- sample(1:100, 1)
rpart <- sample(1:r, 1)
cpart <- sample(1:c, 1)
mat <- matrix(sample(1:r*c,r*c,replace=TRUE),c(r,c))
da <- as.darray(mat, c(rpart,cpart))

test_that("Dense darray: works", {
  da <- as.darray(mat, c(rpart,cpart))
  expect_equal(max(da), max(mat), info="check max operator on darray")
  expect_equal(min(da), min(mat), info="check min operator on darray")
  expect_equal(sum(da), sum(mat), info="check sum operator on darray")
  expect_equal(mean(da), mean(mat), info="check mean operator on darray")
  expect_equal(colSums(da), colSums(mat), info="check colSums operator on darray")
  expect_equal(rowSums(da), rowSums(mat), info="check rowSums operator on darray")
  expect_equal(colMeans(da), colMeans(mat), info="check colMeans operator on darray")
  expect_equal(rowMeans(da), rowMeans(mat), info="check rowMeans operator on darray")
  expect_equal(head(da), head(mat), info="check head operator on darray")
  expect_equal(as.numeric(tail(da)), as.numeric(tail(mat)), info="check tail operator on darray")  
  expect_equal(norm(da), norm(mat,"F"), info="check norm operator on darray")
})


context("Basic operations: sum/minus")

test_that("Dense darray: works", {
  expect_equal(getpartition(da+da), (mat+mat), info="check + operator on darray")
  expect_equal(getpartition(da-22), (mat-22), info="check + operator on darray")
  expect_equal(getpartition(22-da), (22-mat), info="check + operator on darray")
  
})

ds <- darray(c(20,3), c(3,3), sparse=TRUE)
dd <- darray(c(20,3), c(3,3))
mat1 <- matrix(data=5, nrow=20, ncol=3)
mat2 <- matrix(data=-5, nrow=20, ncol=3)

test_that("Sparse darray: works", {
  expect_equal(getpartition(ds+5), mat1, info="check numeric + sparse darray")
  expect_equal(getpartition(5+ds), mat1, info="check numeric + sparse darray")
  expect_equal(getpartition(5-ds), mat1, info="check numeric - sparse darray")
  expect_equal(getpartition(ds-5), mat2, info="check numeric - sparse darray")  
})

test_that("Sparse and Dense darray: works", {
  foreach(i,1:npartitions(ds), function(a = splits(dd,i),b = splits(ds,i), i = i) {
    a = matrix(data = 5, ncol = ncol(a), nrow = nrow(a))
    library(Matrix)
    b = Matrix(data = 5, ncol = ncol(b), nrow = nrow(b), sparse=TRUE)
    update(a)
    update(b)
  })

  expect_equal(getpartition(ds+dd), matrix(10, 20, 3), info="check dense + sparse darray")
  expect_equal(getpartition(dd+ds), matrix(10, 20, 3), info="check dense + sparse darray")
  expect_equal(getpartition(ds-dd), matrix(0, 20, 3), info="check dense - sparse darray")
  expect_equal(getpartition(dd-ds), matrix(0, 20, 3), info="check dense - sparse darray")
  expect_equal(getpartition(dd-ds), matrix(0, 20, 3), info="check dense - sparse darray")
  expect_equal(as.numeric(getpartition(ds*5)), as.numeric(matrix(25, 20, 3)), info="check numeric * sparse darray")
})


context("Matrix multiplication")

da1 <- darray(c(4,4), c(2,4), sparse=TRUE, data=0)
da2 <- darray(c(4,4), c(2,4), sparse=FALSE, data=1)
da3 <- darray(c(4,4), c(4,2), sparse=FALSE, data=2)
load.darray(da1,paste(data_path,"/A",sep=""))
load.darray(da2,paste(data_path,"/A",sep=""))
load.darray(da3,paste(data_path,"/B",sep=""))
da4 <- darray(c(4,4),c(2,2),sparse=FALSE, data=1)
da5 <- darray(c(4,4),c(2,2),sparse=TRUE, data=0)
load.darray(da4, paste(data_path,"/C",sep=""))
load.darray(da5, paste(data_path,"/C",sep=""))
da6 <- darray(c(6,6),c(6,2), data=1)
da7 <- darray(c(4,4), c(4,2), sparse=TRUE, data=0)
da8<-darray(c(5,5),c(1,5), sparse=TRUE, data=0)
load.darray(da7,paste(data_path,"/B",sep=""))
load.darray(da8, paste(data_path,"/pr_ex1_",sep=""))
mtx1 <- matrix(c(1:16), nrow=4, ncol=4)

test_that("Sparse and Dense: Error handling", {
  expect_error(da4 %*% da5) # darray should be splitted row-wise %*% column-wise
  expect_error(da1 %*% da6) # dimensions of da1 and da6 do not match
  expect_error(da3 %*% da2) # x%*%y. at least x is row-partitioned or y is column-partitioned
  expect_error(da3 %*% da1)
})

test_that("Sparse and Dense: works", {
  damul <- da1 %*% da3

  expect_equal(dim(damul), c(dim(da1)[1], dim(da3)[2]))

  da1gp <- getpartition(da1)
  da2gp <- getpartition(da2)
  da3gp <- getpartition(da3)
  da4gp <- getpartition(da4)
  da5gp <- getpartition(da5)
  da6gp <- getpartition(da6)
  da7gp <- getpartition(da7)
  da8gp <- getpartition(da8)

  expect_equal(as.numeric(da1gp%*%da3gp), as.numeric(getpartition(damul)))
  expect_equal(da1gp%*%da7gp, getpartition(da1 %*% da7)) #multiplication of sparse and sparse
  expect_equal(da2gp%*%da3gp, getpartition(da2 %*% da3)) #multiplication of dense and dense
})

test_that("Sparse: More tests", {

  da9 <- darray(dim=c(5,1), blocks=c(5,1), sparse=FALSE, data=0.2)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))
  da9 <- darray(dim=c(5,1), blocks=c(1,1), sparse=FALSE, data=2)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))
  da9 <- darray(dim=c(5,2), blocks=c(5,2), sparse=FALSE, data=0.1)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))
  da9 <- darray(dim=c(5,2), blocks=c(2,2), sparse=FALSE, data=0.5)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))
  da9 <- darray(dim=c(5,2), blocks=c(1,1), sparse=FALSE, data=3)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))
  da9 <- darray(dim=c(5,2), blocks=c(5,1), sparse=FALSE, data=10)
  expect_equal(as.numeric(getpartition(da8%*%da9)), as.numeric(getpartition(da8)%*%getpartition(da9)))

})
