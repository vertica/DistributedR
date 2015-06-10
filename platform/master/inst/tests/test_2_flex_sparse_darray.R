library(Matrix)

context("Flexible Sparse darray")

rblocks <- sample(1:8, 1)
cblocks <- sample(1:8, 1)
rsize <- sample(1:10, rblocks)
csize <- sample(1:10, cblocks)
da<-darray(npartitions=c(rblocks,cblocks), sparse=TRUE)

test_that("Creation and Fetch works", {
  expect_equal(nrow(da), 0, info="check darray nrow")
  expect_equal(ncol(da), 0, info="check darray col")
  expect_equal(is.invalid(da), TRUE, info="check validity of flex sparse darray after declaration")   #invalid check
})

foreach(i, 1:npartitions(da), initArrays<-function(y=splits(da,i), index=i-1, rs = rsize, cs= csize, cb=cblocks ) { 
        nrow=rs[floor(index/cb)+1]
        ncol=cs[(index%%cb)+1]
        y<-sparseMatrix(i=1,
                        j=1,
                        x=index+1, 
                        dims=c(nrow,ncol))
        update(y)
  })  

  rindex<-c(1,(cumsum(rsize)+1)[1:length(rsize)-1])
  rindex<-rep(rindex, each=cblocks)
  cindex<-c(1,(cumsum(csize)+1)[1:length(csize)-1])
  cindex<-rep(cindex, rblocks)
  mat<- sparseMatrix(i=rindex,
                        j=cindex,
                        x=1:length(rindex), 
                        dims=c(sum(rsize),sum(csize)))


test_that("Creation and Fetch works", {
  expect_equal(is.invalid(da), FALSE, info="check validity of flex sparse darray after data write")
  expect_equal(nrow(da), nrow(mat), info="check nrow of flex sparse darray")
  expect_equal(ncol(da), ncol(mat), info="check ncol of flex sparse darray")
  expect_equal(getpartition(da), mat, info="check flex sparse darray contents")
  expect_equal(dim(da), dim(mat), info="check dimension of flex sparse darray")
  expect_equal(dim(da), dim(mat), info="check dimension of flex sparse darray")
})


context("Flexible Sparse darray operations")

test_that("Operatons: max, min, head, tail works", {
  expect_equal(max(da), max(mat), info="check max of flex sparse darray")
  expect_equal(min(da), min(mat), info="check min of flex sparse darray")
  expect_equal(sum(da), sum(mat), info="check sum of flex sparse darray")
  expect_equal(mean(da), mean(mat), info="check mean of flex sparse darray")
  expect_equal(colSums(da), colSums(mat), info="check colSums of flex sparse darray")
  expect_equal(rowSums(da), rowSums(mat), info="check rowSums of flex sparse darray")
  expect_equal(colMeans(da), colMeans(mat), info="check colMeans of flex sparse darray")
  expect_equal(rowMeans(da), rowMeans(mat), info="check rowMeans of flex sparse darray")
  expect_equal(head(da), head(mat), info="check head operator on flex sparse darray")
  expect_equal(as.numeric(tail(da)), as.numeric(tail(mat)), info="check tail operator on flex sparse darray")
})

db<-clone(da)

test_that("Clone works", {
  expect_true(da==db, info="clone has to return a same value darray")
  expect_equal(getpartition(da+da), (mat+mat), info="check + operator on flex sparse darray")  
})

