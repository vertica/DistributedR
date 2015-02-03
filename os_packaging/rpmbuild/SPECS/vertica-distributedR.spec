#####################################################################
#Copyright (C) [2014] Hewlett-Packard Development Company, L.P.

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

Name:    vertica-distributedR
Version: 1.0.0
Release: 0.%{?build_number}%{?dist}
Summary: An environment for executing R functions in a distributed manner.

Group:   Applications/Databases
Vendor:  Hewlett-Packard Company
License: proprietary       
URL:     http://www.vertica.com/  

%define ldconfig_file %{name}-x86_64.conf     
%define HPdregression HPdregression
%define HPdcluster HPdcluster
%define HPdgraph HPdgraph
%define HPdclassifier HPdclassifier
%define HPdata HPdata

Source0: %{name}-%{version}.tar.gz       
Source1: %{ldconfig_file}
Source6: %{HPdregression}
Source7: %{HPdcluster}
Source8: %{HPdgraph}
Source9: %{HPdclassifier}
Source11: %{HPdata}

BuildRoot:   %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

# This turns off automatic dependency generation for shared libs (among other things.
# We do this because rpm gets confused about R module dependencies.
AutoReqProv: no

#BuildRequires:  protobuf-devel, protobuf-compiler, libxml2-devel
#BuildRequires:  zlib-devel, zeromq-devel libaio-devel
#Requires:       protobuf, libxml2, libxml2-devel, libaio, zeromq

%description
R is a single-threaded process.  This limits its utility for big data analytics.  Vertica Distributed-R allows Vertica users to write programs which will be executed in distributed fashion, that is, parts of programs as specified by the developer can be run in multiple, parallel single-threaded R-processes.  The result is dramatically reduced execution times for big data  analytical algorithms.

Build revision: %{scm_revision}
%prep
%setup -q
cp -pv %SOURCE1 .
cp -rpv %SOURCE6 .
cp -rpv %SOURCE7 .
cp -rpv %SOURCE8 .
cp -rpv %SOURCE9 .
cp -rpv %SOURCE11 .

%build
make

%install
%define _prefix /opt/hp/distributedR
%define _install_base $RPM_BUILD_ROOT/%{_prefix}
rm -rf $RPM_BUILD_ROOT
install -d %{_install_base}/third_party/R_addons
install -d %{_install_base}/third_party/lib/atomicio
install -d $RPM_BUILD_ROOT/%_sysconfdir/ld.so.conf.d/

install -m 644 %{ldconfig_file}   $RPM_BUILD_ROOT/%_sysconfdir/ld.so.conf.d/

rsync -axv --exclude=.svn %{HPdregression} %{_install_base}/third_party/R_addons/
rsync -axv --exclude=.svn %{HPdcluster}    %{_install_base}/third_party/R_addons/
rsync -axv --exclude=.svn %{HPdgraph}      %{_install_base}/third_party/R_addons/
rsync -axv --exclude=.svn %{HPdclassifier} %{_install_base}/third_party/R_addons/
rsync -axv --exclude=.svn %{HPdata}        %{_install_base}/third_party/R_addons/
rsync -axv --exclude=.svn third_party/boost_threadpool  %{_install_base}/third_party/

rsync -axv install                %{_install_base}
rsync -axv bin                    %{_install_base}
rsync -axv lib                    %{_install_base}
rsync -axv conf                   %{_install_base}
rsync -axv third_party/install/*  %{_install_base}/third_party/

install -m 755 third_party/atomicio/libatomicio.so %{_install_base}/third_party/lib/atomicio
install -m 444 third_party/atomicio/LICENSE        %{_install_base}/third_party/lib/atomicio

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_prefix}
%_sysconfdir/ld.so.conf.d/%{ldconfig_file}

%pre
%define _logdir /tmp/distributedR_addons
rm -rf  %{_logdir}/
mkdir -pv %{_logdir} > /dev/null 2>&1

which R > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo 
    echo "     R is either not installed or cannot be found on the command search path."
    echo "     R is required for installation of this software.  Please install R and/or "
    echo "     update your command search path."
    echo
    exit 1
fi

##Remove old packages in case of upgrades
if [ "$1" -eq "2" ]; then 
echo "Removing old distributedR packages" >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdata >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdregression >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdcluster >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdgraph >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdclassifier >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE MatrixHelper >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE Executor >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE distributedR >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
fi

%post
mkdir -p /tmp/distributedR_addons
chmod 777 /dev/shm

/sbin/ldconfig -v > %{_logdir}/ldconfig.log 2>&1

for thing in distributedR Executor MatrixHelper; do
    echo "Installing $thing"
    echo "Installing $thing" >>  %{_logdir}/vertica_distributedR_addon_install.log
    R CMD INSTALL --no-html /opt/hp/distributedR/install/$thing >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
    if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
    fi
    /sbin/ldconfig -v >> %{_logdir}/ldconfig.log 2>&1
done

for ADDON in HPdregression HPdcluster HPdgraph HPdclassifier HPdata; do
    echo "Installing $ADDON"
    echo "Installing $ADDON" >>  %{_logdir}/vertica_distributedR_addon_install.log
    R CMD INSTALL /opt/hp/distributedR/third_party/R_addons/$ADDON >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
    if [ $? -ne 0 ]; then
        echo "... error"
        exit 1
    fi
    /sbin/ldconfig -v >> %{_logdir}/ldconfig.log 2>&1
done

%preun
mkdir -p /tmp/distributedR_addons
##Remove old packages only in case of uninstallation
if [ "$1" -eq "0" ]; then
echo "Uninstalling distributedR" >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdata >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdregression >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdcluster >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdgraph >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE HPdclassifier >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE MatrixHelper >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE Executor >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
R CMD REMOVE distributedR >> %{_logdir}/vertica_distributedR_addon_install.log 2>&1
fi

%postun
/sbin/ldconfig

%changelog
* Mon Dec 01 2014 Rich Zeliff <richard.zeliff@hp.com> 1.0.0
  - Removed unnecessary R language addon packs
* Wed Jan 29 2014 Arash Fard <arash.jalal-zadeh-fard@hp.com> 0.4.0
  Added HPdata to the RPM 
* Tue Jan 21 2014 Arash Fard <arash.jalal-zadeh-fard@hp.com> 0.3.0
  Added HPdclassifier and randomForest to the RPM 
* Sat Jan 4 2014 Shreya Prasad <shreya.prasad@hp.com> 0.3.0
  Added HPdcluster and HPdgraph to the RPM 
* Fri Nov 8 2013 Shreya Prasad <shreya.prasad@hp.com> 0.3.0
- RPM for Beta 2. Integrated HPDGLM to the RPM
* Tue Sep 24 2013 Rich Zeliff <richard_zeliff@hp.com> 0.2.0
- Initial RPM release

