library(Matrix)

context ("Basic foreach")
test_that("Dense darray: works", {
  da <- darray(c(10,1), c(2,1), data=1)  # column vectpr
  db <- darray(c(1,10), c(1,3), data=1)  # row vector
  dc <- darray(c(10,10), c(2,2), data=1)
  foreach(i,1:1,function(sa=splits(da), sb=splits(db), sc=splits(dc)){
    sa = matrix(5, 10, 1)
    sb = matrix(10, 1, 10)
    sc = matrix(20, 10, 10)
    update(sa)
    update(sb)
    update(sc)
  },progress=FALSE)
  expect_equal(getpartition(da),matrix(5,10,1))
  expect_equal(getpartition(db),matrix(10,1,10))
  expect_equal(getpartition(dc),matrix(20,10,10))
})


test_that("Sparse darrays: works", {
  w <- runif(15)
  vNum <- 10
  nColBlock <- 3
  el <- matrix(nrow=15, ncol=2, c(1,1,1,2,2,2,3,3,4,4,4,4,5,7,9,6,9,2,5,7,3,4,8,1,2,6,9,3,2,1))
  wGF <- darray(dim=c(vNum, vNum), blocks=c(nColBlock,vNum), TRUE, data=0)
  foreach(i, 1, initArrays<-function(y=splits(wGF), v=vNum, w=w, el=el) {
    y <- sparseMatrix(i=el[,1], j=el[,2], dims=c(v,v), x=w)
    update(y)
  },progress=FALSE)
  y <- sparseMatrix(i=el[,1], j=el[,2], dims=c(vNum,vNum), x=w)
  gy <- getpartition(wGF)
  expect_equal(y, gy)

  wGF <- darray(dim=c(vNum, vNum), blocks=c(vNum, nColBlock), TRUE, data=0)
  foreach(i, 1, initArrays<-function(y=splits(wGF), v=vNum, w=w, el=el) {
    y <- sparseMatrix(i=el[,1], j=el[,2], dims=c(v,v), x=w)
    update(y)
  },progress=FALSE)
  gy <- getpartition(wGF)
  expect_equal(y, gy)
})

context("Enhanced foreach")

test_that("List of splits: works and handles corner cases", {
  produce.indices <- function(index,length,list=FALSE){
    indices = c(index:(index+length-1))
    if(list) indices = as.list(indices)
    indices
}

  #creates a reference matrix which counts up in blocks n*n
  create.reference.matrix <- function(length,width,n){
    current = 0
    result = matrix(1,0,n*width)
    for(row in 1:length){
      cur_row = matrix(1,n,0)
      for(col in 1:width){
        current = current + 1
        mat = matrix(current,n,n)
        cur_row = cbind(cur_row,mat)
    }
    result = rbind(result,cur_row)
    }
    result
  }

  assign("da6", darray(c(9,9), c(3,3), sparse=FALSE, data=0), envir=.GlobalEnv)
  assign("da7", darray(c(3,6), c(3,3), sparse=FALSE, data=0), envir=.GlobalEnv)
 
  #test single-split, non-list case
  foreach(i,1:9,function(a=splits(da6,produce.indices(i,1,FALSE)),data=i){
        a = matrix(data,nrow(a),ncol(a))
        update(a)},progress=FALSE)
  expect_equal(as.matrix(getpartition(da6)),create.reference.matrix(3,3,3))

  #test single-split, list case
  foreach(i,1:9,function(a=splits(da6,produce.indices(i,1,TRUE)),data=i){
        a[[1]] = matrix(data,nrow(a[[1]]),ncol(a[[1]]))
        update(a)
        },progress=FALSE)
  expect_equal(as.matrix(getpartition(da6)),create.reference.matrix(3,3,3))

  #test multi-split, non-list case
  foreach(i,1:3,function(a=splits(da6,produce.indices(3*(i-1)+1,3)),data=3*(i-1)+1){
        a[[1]] = matrix(data,ncol(a[[1]]),nrow(a[[1]]))
        a[[2]] = matrix(data+1,ncol(a[[2]]),nrow(a[[2]]))
        a[[3]] = matrix(data+2,ncol(a[[3]]),nrow(a[[3]]))
        update(a)
        },progress=FALSE)

  expect_equal(as.matrix(getpartition(da6)),create.reference.matrix(3,3,3))

  # test multi-split, list case
  foreach(i,1:3,function(a=splits(da6,produce.indices(3*(i-1)+1,3,TRUE)),data=3*(i-1)+1){
        a[[1]] = matrix(data,ncol(a[[1]]),nrow(a[[1]]))
        a[[2]] = matrix(data+1,ncol(a[[2]]),nrow(a[[2]]))
        a[[3]] = matrix(data+2,ncol(a[[3]]),nrow(a[[3]]))
        update(a)
        },progress=FALSE)
})

context("Fault tolerance")

test_that("Errors are thrown when they are supposed to be", {
  # null split index should be handled accordingly
  expect_error(foreach(i,1:npartitions(da1),function(b=splits(da1,NULL)){}), silent=TRUE, progress=FALSE)
  expect_error(foreach(i,1:10), silent=TRUE)
  expect_error(foreach(i,f<-function(i=i){},f<-function(i=i){}), silent=TRUE, progress=FALSE)
  expect_error(foreach(f<-function(i=i){},"a",f<-function(i=i){}), silent=TRUE, progress=FALSE)

  # Unit test for various checks made before a foreach executes
  expect_error(foreach(i, 1:npartitions(da1), function(a=splits(da1, 1)){update(a)}), silent=TRUE, progress=FALSE)
  expect_error(foreach(i, 1:npartitions(da1), function(a=splits(da1)){update(a)}), silent=TRUE, progress=FALSE)
  expect_error(foreach(i, 1:npartitions(da1), function(a=split(da1, i)){update(a)}), silent=TRUE, progress=FALSE)
  expect_error(foreach(i, 1:npartitions(da1), function(a=splits(da1, i)){update(b)}), silent=TRUE, progress=FALSE)
  expect_error(foreach(i, 1:npartitions(da1), function(a=splits(da1, npartitions(da1)+1)){update(a)}), silent=TRUE, progress=FALSE)

  # Check error for updating overlapping split lists
  expect_error(foreach(i, 1:npartitions(da1), function(a=splits(da1, i:(i+1))){update(a)}), silent=TRUE, progress=FALSE)

  expect_error(numSplits(splits(da1)), silent=TRUE)
  expect_error(numSplits(mtx), silent=TRUE)
  })

test_that("Dense darrays: works", {
  assign("da1", darray(c(4,4), c(2,4), data=1), envir=.GlobalEnv)
  assign("dl1", dlist(5), envir=.GlobalEnv)

  foreach(i, 1:npartitions(da1), function(a=splits(da1, i)){
  a <- list()
  update(a)},progress=FALSE)

  expect_that(distributedR_status(), is_a('data.frame'))

  foreach(i, 1:npartitions(da1), function(a=splits(da1, i)){
  a <- matri(5, 2, 4)
  update(a)},progress=FALSE)

  expect_that(distributedR_status(), is_a('data.frame'))

  # Array size mismatch
  foreach(i, 1:npartitions(da1), function(a=splits(da1, i)){
  a <- matrix(5, 10, 20)
  update(a)},progress=FALSE)

  expect_that(distributedR_status(), is_a('data.frame'))

  #Multiple updates
  foreach(i, 1:npartitions(da1), function(a=splits(da1,i), b=splits(dl1,i)) {
  a <- matrix(6,5,5)
  update(a)
  b <- list("a")
  update(b)})

  expect_that(distributedR_status(), is_a('data.frame'))

  })

