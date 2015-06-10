library(Matrix)

context("Flexible Dense darray")

rblocks <- sample(1:8, 1)
cblocks <- sample(1:8, 1)
rsize <- sample(1:10, rblocks)
csize <- sample(1:10, cblocks)

da<-darray(npartitions=c(rblocks,cblocks))
db<-darray(npartitions=c(rblocks,cblocks))

test_that("Creation and Fetch works", {
  expect_equal(nrow(da), 0, info="check darray nrow")
  expect_equal(ncol(da), 0, info="check darray col")
  expect_equal(is.invalid(da), TRUE, info="check validity of flex darray after declaration")   #invalid check
})

 
#Update flex darray

mat=NULL
  mat2=NULL
  matsize=NULL
  for(rid in 0:(rblocks-1)){
    cmat=NULL
    cmat2=NULL
    for(cid in 0:(cblocks-1)){
            index <- (rid*cblocks)+cid

            cmat <-cbind(cmat,matrix(index, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
            matsize<-rbind(matsize,c(rsize[floor(index/cblocks)+1],csize[(index%%cblocks)+1]))

            #For mat2
            v<-NULL
            if(index%%3 ==0){v<-TRUE}
            if(index%%3 ==1){v<-NA}
            if(index%%3 ==2){v<-22}
            cmat2 <-cbind(cmat2,matrix(v, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
    }
    mat <-rbind(mat,cmat)
    mat2 <-rbind(mat2,cmat2)
  }

  foreach(i, 1:npartitions(da), initArrays<-function(y=splits(da,i), index=i-1, rs = rsize, cs= csize, cb=cblocks ) {
     y<-matrix(index, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1])
     update(y)
  })

  #test arrays with different types of entries in each partition (NA, logical, numeric)
  foreach(i, 1:npartitions(db), initArrays<-function(y=splits(db,i), index=i-1, rs = rsize, cs= csize, cb=cblocks ) {
     v<-NULL
     if(index%%3 ==0){v<-TRUE}
     if(index%%3 ==1){v<-NA}
     if(index%%3 ==2){v<-22}
     y<-matrix(v, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1])
     update(y)
  })

test_that("Creation and Fetch works", {
  expect_equal(nrow(da), nrow(mat), info="check nrow of flex darray")
  expect_equal(ncol(da), ncol(mat), info="check ncol of flex darray")
  expect_equal(getpartition(da), mat, info="check flex darray contents")
  expect_equal(partitionsize(da,1), matrix(c(rsize[1],csize[1]),nrow=1), info="check size of first partition")
  expect_equal(partitionsize(da), matsize, info="check size of all partitions")
  expect_equal(dim(da), dim(mat), info="check dimension of flex darray")
})


context("Flexible Dense darray operations")

test_that("Operatons: max, min, head, tail works", {
  expect_equal(max(da), max(mat), info="check max of flex darray")
  expect_equal(min(da), min(mat), info="check min of flex darray")
  expect_equal(sum(da), sum(mat), info="check sum of flex darray")
  expect_equal(mean(da), mean(mat), info="check mean of flex darray")
  expect_equal(colSums(da), colSums(mat), info="check colSums of flex darray")
  expect_equal(rowSums(da), rowSums(mat), info="check rowSums of flex darray")
  expect_equal(colMeans(da), colMeans(mat), info="check colMeans of flex darray")
  expect_equal(rowMeans(da), rowMeans(mat), info="check rowMeans of flex darray")
  expect_equal(head(da), head(mat), info="check head operator on flex darray")
  expect_equal(as.numeric(tail(da)), as.numeric(tail(mat)), info="check tail operator on flex darray")
  expect_equal(norm(da), norm(mat,"F"), info="check norm operator on flex darray")
})

da1<-clone(da)
val<-42
mat2<-array(val, dim=c(nrow(mat),ncol(mat)))
foreach(i, 1:1, initArrays<-function(y=splits(da), v=val) {
  y<-matrix(v, nrow=nrow(y),ncol=ncol(y))
  update(y)
})
 
test_that("Operations: sum, minus works", {
  expect_equal(getpartition(da1+da1), (mat+mat), info="check self + operator on flex darray")
  expect_equal(getpartition(da), mat2, info="check flex darray contents after update to full array")
  expect_equal(getpartition(da+da1), (mat+mat2), info="check + operator on flex darray")
  expect_equal(getpartition(da1-da), (mat-mat2), info="check - operator on flex darray")
  expect_equal(getpartition(da1+22), (mat+22), info="check - operator on flex darray")
})


da2<-clone(da,nrow=ncol(da1),data=val)
mat3<-array(val, dim=c(ncol(mat),ncol(mat)))

test_that("Operations: multiplication works", {
  expect_equal(getpartition(da1%*%da2), (mat%*%mat3), info="check %*% operator on flex darray")  
})

db<-clone(da, data=NA)

test_that("Clone works", {
  expect_true(all(is.na(getpartition(db))) && (nrow(db)==nrow(mat)) && (ncol(db)==ncol(mat)), info="clone has to return a same value for flex darray (NA case)")

  val<-val+1
  db<-clone(da, ncol=1, data=val)
  mat3<-array(val,dim=c(nrow(mat),1))
  expect_equal(getpartition(db), mat3, info="check clone with variable ncol on flex darray")
  
  db<-clone(da, nrow=1, data=val)
  mat3<-array(val,dim=c(1,ncol(mat)))
  expect_equal(getpartition(db), mat3, info="check clone with variable nrow on flex darray")
})
