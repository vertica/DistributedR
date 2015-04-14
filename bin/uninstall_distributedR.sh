#!/bin/bash

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

sudo rm -rf $INSTALLBASE
