library(distributedR)
library(Matrix)
data_path<-system.file("extdata",package="distributedR")

context("checking invalid creating of dframes")

test_that("Dframes: Invalid arguments creation", {
  
 expect_error(dframe(1,1))
 expect_error(dframe(c(1,1),c(2,2)))
 expect_error(dframe(c(-10,-10),c(1,5)))
 expect_error(dframe(c("a",1),c(4,"c")))
 expect_error(dframe(c(1,2,3),c(4,2)))
 expect_error(dframe(c(10,10),c(0,10)))
 expect_error(dframe(wrongarg=c(2,4)))
 expect_error(dframe(c(4,4), c(2,4), sparse=TRUE))

})

context("Testing valid creation of dframes and basic checks")

test_that("Dframes: Valid argument creation",{
 mdf<- data.frame()
 nr <- 4
 nc <- 4
 df1 <- dframe(c(nr,nc), c(2,4))
 df1gp <- getpartition(df1)
 df2gp <- getpartition(df1,2)
 expect_error(getpartition(df1,3))
 expect_that(df1gp, is_a("data.frame"))
 expect_equal(nc, length(names(df1gp)))
 expect_equal(nr, length(row.names(df1gp)))
 expect_equal(4, length(names(df2gp)))
 expect_equal(2, length(row.names(df2gp)))
 expect_equal(1, splits(df1)@split_ids[2])
 expect_equal(0, splits(df1,1)@split_ids[1])
 expect_error(getpartition(df1, 0))
 expect_error(getpartition(df1, 3))
 expect_error(getpartition(df1, -100))
 expect_error(getpartition(df1, mdf))
 expect_error(splits(df1,0))
 expect_error(splits(df1,3))
})

context("Testing row/column name and value setup")

test_that("Dframes: Row/column name and value setup", {
  df2 <- dframe(c(10,10), c(4,3))
  expect_equal(12, length(splits(df2)@split_ids))
  df2 <- dframe_set_values(df2, 10)
  df2gp <- getpartition(df2)
  df2gp3 <- getpartition(df2, 3)
  df2gp12 <- getpartition(df2, 12)
  expect_equal(10, length(names(df2gp)))
  expect_equal(10, length(row.names(df2gp)))
  expect_equal(3, length(names(df2gp3)))
  expect_equal(4, length(row.names(df2gp3)))
  expect_equal(1, length(names(df2gp12)))
  expect_equal(2, length(row.names(df2gp12)))
  expect_true(all(df2gp==10))

  wrong_name <- as.character(sample(1:9))
  expect_error(dimnames(df2)[[2]] <- wrong_name)
  expect_error(dimnames(df2)[[1]] <- wrong_name)
  wrong_name <- as.character(sample(1:11))
  expect_error(dimnames(df2)[[2]] <- wrong_name)
  expect_error(dimnames(df2)[[1]] <- wrong_name)

  name_sample <- as.character(sample(1:10))
  dimnames(df2)[[2]] <- name_sample
  expect_true(all(df2@dimnames[[2]]==name_sample))
  df2gp <- getpartition(df2)
  expect_true(all(names(df2gp)==name_sample))
  df2gp6 <- getpartition(df2, 6)
  expect_true(all(names(df2gp6)==name_sample[4:6]))
  df2gp12 <- getpartition(df2, 12)
  expect_true(all(names(df2gp12)==name_sample[10]))

  dimnames(df2)[[1]] <- name_sample
  expect_true(all(df2@dimnames[[1]]==name_sample))
  df2gp <- getpartition(df2)
  expect_true(all(names(df2gp)==name_sample))
  expect_true(all(row.names(df2gp)==name_sample))

  df2gp6 <- getpartition(df2, 6)
  expect_true(all(row.names(df2gp6)==name_sample[5:8]))
  df2gp12 <- getpartition(df2, 12)
  expect_true(all(row.names(df2gp12)==name_sample[9:10]))

  df3 <- dframe(c(20,4),c(10,2))
  expect_equal(20, nrow(df3))
  expect_equal(4, ncol(df3))
  file_path <- paste(data_path,"/df_data",sep="")
  foreach(i, 1:numSplits(df3), function(sf=splits(df3,i),ii=i,path=file_path){
    sf<-read.table(paste(path,ii,sep=""), as.is=TRUE)
    update(sf)
  })
  expect_equal(20, nrow(df3))
  expect_equal(4, ncol(df3))
  local_df <- read.table(paste(data_path,"/df_data_all",sep=""))
  df3gp <- getpartition(df3)
  expect_true(all(df3gp == local_df))
  expect_true(all(head(df3)==head(local_df)))
  expect_true(all(tail(df3)==tail(local_df)))
  expect_true(all(head(df3, 3)==head(local_df, 3)))
  expect_true(all(tail(df3, 3)==tail(local_df, 3)))
  expect_true(all(head(df3, 8)==head(local_df, 8)))
  expect_true(all(tail(df3, 8)==tail(local_df, 8)))  
  df3gp1 <- getpartition(df3, 1)
  df3gp2 <- getpartition(df3, 2)
  df3gp3 <- getpartition(df3, 3)
  df3gp4 <- getpartition(df3, 4)
  expect_true(all(cbind(rbind(df3gp1,df3gp3),rbind(df3gp2,df3gp4)) == local_df))
  expect_true(all(rbind(cbind(df3gp1,df3gp2),cbind(df3gp3,df3gp4)) == local_df))
  sum_da <- darray(c(2,1),c(1,1))
  foreach(i, 1:numSplits(sum_da), function(sf=splits(df3, i*2-1), sa=splits(sum_da,i), ii=i){
    sa<-matrix(sum(sf[2]),1,1)
    update(sa)
  })
  sum <- getpartition(sum_da)
  sum <- sum(sum)
  expect_equal(sum, sum(local_df[2]))

  expect_error(sum(df3))    # error message should be returned
  expect_error(min(df3))    # error message should be returned
  expect_error(max(df3))    # error message should be returned
  foreach(i, 1:numSplits(df3), function(sf=splits(df3,i),ii=2,path=file_path){
    sf<-read.table(paste(path,ii,sep=""), as.is=TRUE)
    update(sf)
  })
  df3gp <- getpartition(df3)
  expect_equal(min(df3gp), min(df3))
  expect_equal(max(df3gp), max(df3))
  expect_equal(sum(df3gp), sum(df3))

})

context("check data.frame operations such as colSums on mixed col. types")

test_that("Dframe: check data.frame operations such as colSums on mixed col. types",{
  df4<-dframe(c(3,3), c(3,1))
  
  foreach(i, 1:npartitions(df4), initArrays<-function(y=splits(df4,i), index=i) {
     if(index==3){
          y<-c(1,2,3)
      }else{
       y<-c(TRUE,FALSE,as.integer(4))
   }
     y<-data.frame(y)
     update(y)
   })
  expect_equal(is.invalid(df4), FALSE, msg="check validity of dframe after contents are written")
  df4gp <- getpartition(df4)
  expect_equal(as.numeric(colSums(df4gp)), as.numeric(colSums(df4)))
  expect_equal(as.numeric(rowSums(df4gp)), as.numeric(rowSums(df4)))
  expect_equal(as.numeric(colMeans(df4gp)), as.numeric(colMeans(df4)))
  expect_equal(as.numeric(rowMeans(df4gp)), as.numeric(rowMeans(df4)))
})

  rblocks <- sample(1:8, 1)
  cblocks <- sample(1:8, 1)
  rsize <- sample(1:10, rblocks)
  csize <- sample(1:10, cblocks)

  da<-dframe(npartitions=c(rblocks,cblocks))

context("Testing flexible data frames")

test_that("Dframes: test flexible data frames",{

  expect_equal(nrow(da), 0)
  expect_equal(ncol(da), 0)
  expect_true(is.invalid(da))
})

context("Fill a matrix partition-by-partition for later comparison with darray. We fill from left-right and then top-bottom.")

test_that("Dframe: Fill a matrix partition-by-partition for later comparison with darray. We fill from left-right and then top-bottom.",{

  mat=NULL
  matsize=NULL
  for(rid in 0:(rblocks-1)){
    cmat=NULL
    for(cid in 0:(cblocks-1)){
            index <- (rid*cblocks)+cid
            cmat <-cbind(cmat,matrix(index, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
	    matsize<-rbind(matsize,c(rsize[floor(index/cblocks)+1],csize[(index%%cblocks)+1]))
    }
    mat <-rbind(mat,cmat)
  }
  mat<-data.frame(mat)

  foreach(i, 1:npartitions(da), initArrays<-function(y=splits(da,i), index=i-1, rs = rsize, cs= csize, cb=cblocks ) {
     tmp<-matrix(index, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1])
     y<-data.frame(tmp)
     update(y)
  })
  cnames<-as.character(sample(1:ncol(mat)))
  expect_false(is.invalid(da))
  expect_equal(nrow(da), nrow(mat))
  expect_equal(ncol(da), ncol(mat))
  expect_true(all(getpartition(da)== mat))
  expect_equal(partitionsize(da,1), matrix(c(rsize[1],csize[1]),nrow=1))	
  expect_equal(partitionsize(da), matsize)	
  colnames(da)<-cnames
  expect_true(all(colnames(getpartition(da))== cnames))
 
  expect_equal(dim(da), dim(mat))
  expect_equal(max(da), max(mat))
  expect_equal(min(da), min(mat))
  expect_equal(sum(da), sum(mat))
  expect_equal(as.numeric(colSums(da)), as.numeric(colSums(mat)))
  expect_equal(as.numeric(rowSums(da)), as.numeric(rowSums(mat)))
  expect_equal(as.numeric(rowMeans(da)), as.numeric(rowMeans(mat)))
  expect_equal(as.numeric(colMeans(da)), as.numeric(colMeans(mat)))
  
  expect_true(all(head(da)== head(mat)))
  expect_true(all(tail(da)== tail(mat)))
  db<-clone(da)
  expect_true(da==db)
  db<-clone(da, ncol=1, data=5)
  mat2<-data.frame(array(5, dim=c(nrow(mat),1)))
  expect_true(all(getpartition(db)== mat2))
  dc<-clone(da, nrow=1, data=5)
  mat2<-data.frame(array(5, dim=c(1, ncol(mat))))
  expect_true(all(getpartition(dc)== mat2))
})

context("Testing addition and subtraction of dframes")

test_that("Dframe: Testing addition and subtraction of dframes",{
 df<-data.frame(matrix(data=1,ncol=5, nrow=5))
  ddf<-dframe(c(5,5),c(1,5))
  foreach(i,1:npartitions(ddf),copy<-function(lddf=splits(ddf,i),ldf=df,i=i){
	lddf <- ldf[i,]
	update(lddf)
  })

  df <- df +1
  ddf<- ddf +1

  expect_true(all(getpartition(ddf)==df))

  df <- 1+df
  ddf <- 1+ddf

  expect_true(all(getpartition(ddf)==df))
 
  df <- df+df
  ddf<- ddf+ddf
 
  expect_true(all(getpartition(ddf)==df))

  df <- df-df
  ddf<- ddf-ddf
  
  expect_true(all(getpartition(ddf)==df))
  
  df <- df -1
  ddf<- ddf -1

  expect_true(all(getpartition(ddf)==df))

  df <- 1-df
  ddf <- 1-ddf
  
  expect_true(all(getpartition(ddf)==df))

})

context("Testing as.dframe")

test_that("Dframe: Testing as.dframe",{
mtx <- matrix(c(1:100),nrow=20)
  
  #checking base case with giving dframe dimensions
  df <- as.dframe(mtx,blocks=dim(mtx))
  expect_equal(df@dim, c(20,5))
  expect_equal(df@blocks, c(20,5))
  expect_true(all(as.matrix(getpartition(df))==mtx))
  expect_error(as.dframe(mtx,blocks=c(20,10)))

  #checking base case without giving dframe dimensions
  df <- as.dframe(mtx)
  expect_equal(df@dim, c(20,5))
  expect_true(all(as.matrix(getpartition(df))==mtx))

  #testing creating of dframe with data.frame
  dfa <- c(2,3,4)
  dfb <- c("aa","bb","cc")
  dfc <- c(TRUE,FALSE,TRUE)
  df <- data.frame(dfa,dfb,dfc)

  # creating dframe with default block size
  ddf <- as.dframe(df, blocks=c(1,3))
  expect_equal(ddf@dim, c(3,3))
  expect_equal(ddf@blocks, c(1,3))
  expect_equal(colnames(ddf), colnames(df))
  
  # creating dframe with 1x1 block size
  ddf <- as.dframe(df,c(1,1))
  expect_equal(ddf@dim, c(3,3))
  expect_equal(ddf@blocks, c(1,1))
  expect_equal(colnames(ddf), colnames(df))

  # testing large darray
  large_mat <- matrix(runif(3000*3000), 3000,3000)
  df <- as.dframe(large_mat, blocks=dim(large_mat))
  gpdf <- getpartition(df)
  
  expect_equal(df@dim, c(3000,3000))
  expect_equal(df@blocks, c(3000,3000))
  expect_true(all(as.matrix(gpdf)== large_mat))

  df <- as.dframe(large_mat, c(2900,2800))
  gpdf <- getpartition(df)
  expect_true(all(as.matrix(gpdf)== large_mat))

})
