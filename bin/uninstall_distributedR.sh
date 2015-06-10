#!/bin/bash

if [[ $# -gt 2 ]]; then
  echo "Usage $0 dependency_flag[0/1] r_flag[0/1]";
  exit 1;
fi

REMOVE_DEPENDENCY="$1"
REMOVE_R="$2"
INSTALLBASE=/opt/hp/distributedR

echo -e "==== Uninstalling algorithm packages"
sudo R CMD REMOVE HPdata         > /dev/null 2>&1
sudo R CMD REMOVE HPdregression  > /dev/null 2>&1
sudo R CMD REMOVE HPdcluster     > /dev/null 2>&1
sudo R CMD REMOVE HPdclassifier  > /dev/null 2>&1
sudo R CMD REMOVE HPdgraph       > /dev/null 2>&1

echo -e "==== Uninstalling DistributedR"
sudo R CMD REMOVE distributedR   > /dev/null 2>&1
sudo R CMD REMOVE Executor       > /dev/null 2>&1
sudo R CMD REMOVE MatrixHelper   > /dev/null 2>&1

if [[ "REMOVE_DEPENDENCY" -eq 1 ]]; then
  echo -e "==== Removing dependent R packages ===="
  sudo R CMD REMOVE Rcpp
  sudo R CMD REMOVE RInside
  sudo R CMD REMOVE data.table
  sudo R CMD REMOVE XML
  sudo R CMD REMOVE randomForest
  sudo R CMD REMOVE plyr
  sudo R CMD REMOVE chron
  sudo R CMD REMOVE stringr
  sudo R CMD REMOVE reshape2
fi

if [[ "REMOVE_R" -eq 1 ]]; then
  if [[ `cat /etc/*-release | egrep '(CentOS|Red Hat)' | wc -l` > 0 ]]; then
    echo -e "==== CentOS/RedHat: Uninstalling R ===="
    sudo rpm -e R
    sudo rpm -e R-devel
    sudo rpm -e R-core-devel
    sudo rpm -e R-core
    sudo rpm -e R-java
    sudo rpm -e R-java-devel
  fi

  else if [[ `cat /etc/*-release | egrep '(Ubuntu|Debian)' | wc -l` > 0 ]]; then
    echo -e "==== Ubuntu/Debian: Uninstalling R ===="
    sudo dpkg -r R
    sudo dpkg --purge R
    sudo dpkg -r R-devel
    sudo dpkg --purge R-devel
    sudo dpkg -r R-core-devel
    sudo dpkg --purge R-core-devel
    sudo dpkg -r R-core
    sudo dpkg --purge R-core
    sudo dpkg -r R-java
    sudo dpkg --purge R-java
    sudo dpkg -r R-java-devel
    sudo dpkg --purge R-java-devel
  fi
fi

sudo rm -rf $INSTALLBASE
