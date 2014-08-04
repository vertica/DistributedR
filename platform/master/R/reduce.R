#####################################################################
#A scalable and high-performance platform for R.
#Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 2 of the License, or (at
#your option) any later version.

#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.  You should have received a
#copy of the GNU General Public License along with this program; if
#not, write to the Free Software Foundation, Inc., 59 Temple Place,
#Suite 330, Boston, MA 02111-1307 USA
#####################################################################

# if f is an associative, commutative op. denoted by %,
# with signature f(split,split) --> split,
# then reduce.internal(f, d) will return d1 % d2 % ... % dn,
# where di are the splits of d
# reduce(f, d, idx) will return didx[1] % didx[2] % ...
reduce <- function(f,d,idx=1:length(splits(d))) {
  tmp <- darray(d@dim, d@blocks, d@sparse)
  foreach(i, 1:length(splits(tmp)),
          init.tmp <- function(ds = splits(d,i),
                               ts = splits(tmp,i)) {
            ts <- ds
            update(ts)
          }, progress=FALSE) # TODO(erik): this is inefficient when using idx!

  reduce.internal(f,tmp,idx)

  result <- darray(d@blocks, d@blocks, d@sparse)
  foreach(i, 1,
          init.result <- function(ts = splits(tmp,idx[1]),
                                  rs = splits(result,1)) {
            rs <- ts
            update(rs)
          }, progress=FALSE)
  return(result)
}

# if f is an associative, commutative op. denoted by %,
# with signature f(split,split) --> split,
# then reduce.internal(f, d) will do d1 <- d1 % d2 % ... % dn,
# where di are the splits of d
# reduce(f, d, idx) will do didx[1] <- didx[1] % didx[2] % ...
reduce.internal <- function(f,d,idx=1:length(splits(d))) {
  i <- 1
  n <- length(idx)
  repeat {
    step <- 2*i
    reducers <- floor((n-i-1)/step)+1
    foreach(j, 1:reducers,
            reduce.pair <- function(s1 = splits(d,idx[(j-1)*step+1]),
                                    s2 = splits(d,idx[(j-1)*step+1+i]),
                                    fun = f) {
              s1 <- fun(s1,s2)
              update(s1)
            }, progress=FALSE)
    
    i <- i*2
    if (1+i > n) {
      break
    }
  }
}
