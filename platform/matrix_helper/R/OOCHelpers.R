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

bfs.merge <- function(a,b) {
  return(.Call("bfs_merge",a,b,DUP=FALSE))
}

bfs.step <- function(g,f,d,start) {
  return(.Call("bfs_step",g,f,d,start,DUP=FALSE))
}

bfs.step2 <- function(g,f,d,start) {
  return(.Call("bfs_step",g,f,d,start,DUP=FALSE))
}

bfs.getpart <- function(v,start,size) {
  return(.Call("bfs_getpart",v,start,size,DUP=FALSE))
}

bfs.dense.step <- function(g,p,done) {
  return(.Call("bfs_dense_step",g,p,done,DUP=FALSE))
}

unpack <- function(a) {
  return(.Call("unpack",a,DUP=FALSE))
}

cc.step <- function(g,labels,labels.receive,s,p) {
  return(.Call("cc_step",g,labels,labels.receive,s,p,DUP=FALSE))
}
