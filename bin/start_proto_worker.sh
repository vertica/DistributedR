#!/bin/bash

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

R_LD_LIB_PATH=$(echo "cat(Sys.getenv(\"LD_LIBRARY_PATH\"))" | R --slave --vanilla)
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$R_LD_LIB_PATH
export R_LIBS_USER=`pwd`/install
##ODBC file can be put in the path by either using .bashrc or specifying below
##Any path specified here will override the declartion in .bashrc
#export ODBCINI=`dirname $0`/<dir>/ODBC.INI
#export VERTICAINI=`dirname $0`/<dir>/VERTICA.INI 
R_WORKER_BINARY=`dirname $0`/R-worker-bin
org_arg=$@
## parse the arguments and get a list of environemnt variables (-v option)
while [[ $# > 1 ]]
do
key="$1"
shift
case $key in
    -v)
    name=`echo $1 | awk 'BEGIN { FS = ":" };{print $1}'`
    value=`echo $1 | awk 'BEGIN { FS = ":" };{print $2}'`
    # if the environment variable is not set, set the value
    if [ -z ${!name} ]; then
      export $name=$value
    fi 
    shift
    ;;
esac
done

$R_WORKER_BINARY $org_arg
