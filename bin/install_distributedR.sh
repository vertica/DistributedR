#!/bin/bash


# Check for distributed R rpm installation and 
#uninstall before installation from source
if [[ `cat /etc/*-release | egrep '(CentOS|Red Hat)' | wc -l` > 0 ]]; then
  echo -e "\n==== Checking for installed rpm ===="
  if [[ `rpm -qa | grep 'hp-distributedR' | wc -l` > 0 ]]; then
     echo "Uninstalling hp-distributedR RPM"
     sudo rpm -e hp-distributedR
     if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
     fi
  fi

else if [[ `cat /etc/*-release | egrep '(Ubuntu|Debian)' | wc -l` > 0 ]]; then
  echo -e "\n==== Checking for installed deb ===="
  if [[ `dpkg -l | grep 'hp-distributedr' | wc -l` > 0 ]]; then
     echo "Uninstalling hp-distributedr DEB"
     sudo dpkg -r hp-distributedr
     sudo dpkg --purge hp-distributedr
     if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
     fi
  fi
fi

fi

echo -e "\n==== Installing Distributed R and parallel algorithm packages from source"
INSTALLBASE=/opt/hp/distributedR
ALGORITHMS=$PWD/algorithms

rm -rf $INSTALLBASE
install -d $INSTALLBASE/third_party/R_addons
install -d $INSTALLBASE/third_party/lib/atomicio

rsync -ax --exclude=.svn $ALGORITHMS/HPdregression $INSTALLBASE/third_party/R_addons/
rsync -ax --exclude=.svn $ALGORITHMS/HPdcluster    $INSTALLBASE/third_party/R_addons/
rsync -ax --exclude=.svn $ALGORITHMS/HPdgraph      $INSTALLBASE/third_party/R_addons/
rsync -ax --exclude=.svn $ALGORITHMS/HPdclassifier $INSTALLBASE/third_party/R_addons/
rsync -ax --exclude=.svn $ALGORITHMS/HPdata        $INSTALLBASE/third_party/R_addons/
rsync -ax --exclude=.svn $PWD/third_party/boost_threadpool  $INSTALLBASE/third_party/

rsync -ax $PWD/install --exclude=.svn $INSTALLBASE
rsync -ax $PWD/bin --exclude=.svn $INSTALLBASE
rsync -ax $PWD/lib --exclude=.svn $INSTALLBASE
rsync -ax $PWD/conf --exclude=.svn $INSTALLBASE
rsync -ax $PWD/third_party/install/*  $INSTALLBASE/third_party/

install -m 755 $PWD/third_party/atomicio/libatomicio.so $INSTALLBASE/third_party/lib/atomicio
install -m 444 $PWD/third_party/atomicio/LICENSE        $INSTALLBASE/third_party/lib/atomicio

/sbin/ldconfig -v > /dev/null 2>&1

for thing in distributedR Executor MatrixHelper; do
    echo -e "\nInstalling $thing"
    R CMD INSTALL --no-html $INSTALLBASE/install/$thing
    if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
    fi
    /sbin/ldconfig -v > /dev/null 2>&1
done

for ADDON in HPdregression HPdcluster HPdgraph HPdclassifier HPdata; do
    echo -e "\nInstalling $ADDON"
    R CMD INSTALL $INSTALLBASE/third_party/R_addons/$ADDON
    if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
    fi
    /sbin/ldconfig -v > /dev/null 2>&1
done
