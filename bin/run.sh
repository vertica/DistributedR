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

# Syntax: ./run.sh XMLFILE PROJECTSRCFILE

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage $0 <workers.xml> <r-file> [worker-config.xml]";
  exit 1;
fi

XMLFILE="$1"
shift
SRCFILE="$1"
shift
WORKERXMLFILE="$1"
shift
PROJECTDIR=`pwd`
userid=$(id -un 2>&1)
# INTERACTIVE=1

export R_LIBS_USER=$PROJECTDIR/install
#WORKERIPS=(`grep Hostname $XMLFILE | tail --lines=+2 | egrep -o [^[:space:]]+ | sed 's/<Hostname>//' | sed 's|</Hostname>||'`)
#WORKERPORTS=(`grep Port $XMLFILE | tail --lines=+2 | egrep -o [[:digit:]]+`)
#LAST=`expr ${#WORKERIPS[@]} - 1`
#userid=$(id -un 2>&1)

#for i in `seq 1 $LAST`
#do
#echo "Starting worker ${WORKERIPS[$i]}:${WORKERPORTS[$i]}"
#    ssh -x -n ${WORKERIPS[$i]} "cd $PROJECTDIR ; ulimit -n 32768 ; ulimit -c unlimited; ./bin/start_proto_worker.sh $WORKERXMLFILE &> /tmp/R_make_tests_${userid}.out" &
#done

#start master
if [[ -z "$INTERACTIVE" || "$INTERACTIVE" -ne 1 ]]
then
    echo -e "library(distributedR) ; pm <- distributedR_start(cluster_conf=\"$XMLFILE\") ; source(\"$SRCFILE\") ; distributedR_shutdown(pm) ; Sys.sleep(5) ; q(save=\"no\")" | ${PROJECTDIR}/bin/master-bin $@ #valgrind --suppressions=./valgrind.supp R --no-save

#    echo -e "library(distributedR) ; pm <- distributedR_start(\"$XMLFILE\", workers=F) ; source(\"$SRCFILE\") ; distributedR_shutdown(pm) ; cat(\"shutdown issued, waiting 5 secs for workers to shut down\") ; Sys.sleep(5)" | valgrind --tool=memcheck --suppressions=./valgrind.supp ./master-bin
# --leak-check=full --track-fds=yes 
else
    echo -e "\n\nCOMMANDS:\nlibrary(distributedR) \n pm <- distributedR_start(cluster_conf=\"$XMLFILE\") \n source(\"$SRCFILE\") \n distributedR_shutdown(pm) \n Sys.sleep(5) \n q(save=\"no\")"
    R
fi  

#kill workers & ssh connections
#for i in `seq 0 $LAST`
#do
#    ssh ${WORKERIPS[$i]} "pkill 'R-worker-bin' ; pkill 'R-executor-bin' ; rm -f /dev/shm/*_*_*"
#done
