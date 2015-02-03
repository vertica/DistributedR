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

# Top level Makefile for Presto

include vars.mk
all: third_party ${ATOMICIO_LIB} ${WORKER_BIN} ${MASTER_BIN} ${MASTER_RLIB} ${EXECUTOR_BIN} ${MATRIX_HELPER_RLIB}

install-algo:
	$(MAKE)
	Rscript $(PRESTO_INSTALL_LOCALLIB_SCRIPT)

lint:
	tools/lint.sh ${PRESTO_WORKER_SRC} ${PRESTO_MASTER_SRC} ${PRESTO_WORKER_HEADERS} ${PRESTO_MASTER_HEADERS} ${PRESTO_COMMON_HEADERS} ${PRESTO_COMMON_SRC} ${PRESTO_EXECUTOR_HEADERS} ${PRESTO_EXECUTOR_SRC}

test:
	$(MAKE)
	$(PRESTO_RUN) $(PRESTO_DEF_WORKER_LIST) $(PRESTO_UNITTEST_SCRIPT)

algotest:
	$(MAKE)
	$(PRESTO_RUN) $(PRESTO_DEF_WORKER_LIST) $(PRESTO_ALGOTEST_SCRIPT)

stresstest:
	$(MAKE)
	$(PRESTO_RUN) $(PRESTO_DEF_WORKER_LIST) $(PRESTO_STRSTEST_SCRIPT)

.PHONY: clean third_party test boost docs manual tutorial faq distclean 

${ATOMICIO_LIB}:
	$(MAKE) -C third_party/atomicio

boost:
	$(MAKE) -C third_party boost

third_party:
	$(MAKE) -C third_party -j8 all

docs:
	mkdir -p $(DOC_DIR_PLATFORM)
	$(MAKE) manual tutorial faq
	mkdir -p $(DOC_DIR_ALGORITHMS)
	$(MAKE) algorithm_docs

manual:
	-${R_HOME}/bin/R CMD Rd2pdf --no-preview --batch --force --title="Distributed R Manual" --output=$(MAN_OUTPUT) $(MAN_FILES)

#Tutorial requires Presto and R 'igraph' installed.
tutorial:
	cd $(TUTORIAL_DIR); ${R_HOME}/bin/R CMD Sweave $(TUTORIAL_FILE).Rnw; \
	pdflatex $(TUTORIAL_FILE).tex; \
	rm $(TUTORIAL_FILE).out $(TUTORIAL_FILE).log $(TUTORIAL_FILE).aux $(TUTORIAL_FILE).tex $(TUTORIAL_DATA_FILE)?
	mv $(TUTORIAL_DIR)/$(TUTORIAL_FILE).pdf $(DOC_DIR_PLATFORM)/Distributed-R-UserGuide.pdf

faq:
	cd $(TUTORIAL_DIR); \
	pdflatex $(FAQ_FILE).tex; \
	rm $(FAQ_FILE).log $(FAQ_FILE).aux  
	mv $(TUTORIAL_DIR)/$(FAQ_FILE).pdf $(DOC_DIR_PLATFORM)/Distributed-R-FAQ.pdf

algorithm_docs:
	mkdir -p $(DOC_DIR_ALGORITHMS)/HPdregression
	R CMD Rd2pdf --no-preview --force --output=$(DOC_DIR_ALGORITHMS)/HPdregression/HPdregression-Manual.pdf $(PWD)/algorithms/HPdregression
	mkdir -p $(DOC_DIR_ALGORITHMS)/HPdcluster
	R CMD Rd2pdf --no-preview --force --output=$(DOC_DIR_ALGORITHMS)/HPdcluster/HPdcluster-Manual.pdf $(PWD)/algorithms/HPdcluster
	mkdir -p $(DOC_DIR_ALGORITHMS)/HPdclassifier
	R CMD Rd2pdf --no-preview --force --output=$(DOC_DIR_ALGORITHMS)/HPdclassifier/HPdclassifier-Manual.pdf $(PWD)/algorithms/HPdclassifier
	mkdir -p $(DOC_DIR_ALGORITHMS)/HPdgraph
	R CMD Rd2pdf --no-preview --force --output=$(DOC_DIR_ALGORITHMS)/HPdgraph/HPdgraph-Manual.pdf $(PWD)/algorithms/HPdgraph
	mkdir -p $(DOC_DIR_ALGORITHMS)/HPdata
	R CMD Rd2pdf --no-preview --force --output=$(DOC_DIR_ALGORITHMS)/HPdata/HPdata-Manual.pdf $(PWD)/algorithms/HPdata
	

clean: clean-installer-tarball
	$(MAKE) -C third_party/atomicio clean
	-${R_HOME}/bin/R CMD REMOVE -l ${R_INSTALL_DIR} distributedR
	-${R_HOME}/bin/R CMD REMOVE -l ${R_INSTALL_DIR} Executor
	-${R_HOME}/bin/R CMD REMOVE -l ${R_INSTALL_DIR} MatrixHelper
	rm -rf ${GEN_DIR} ${PRESTO_PROTO}
	rm -rf ${PRESTO_COMMON} ${PRESTO_COMMON_OBJS} ${LIB_DIR}
	rm -rf ${MASTER_RLIB} ${PRESTO_MASTER_OBJS} ${MASTER_BIN} ${PRESTO_MASTER_DIR}/src/*.so
	rm -rf ${WORKER_RLIB} ${PRESTO_WORKER_OBJS} ${WORKER_BIN} ${PRESTO_WORKER_DIR}/src/*.so
	rm -rf ${EXECUTOR_RLIB} ${PRESTO_EXECUTOR_OBJS} ${EXECUTOR_BIN} ${PRESTO_EXECUTOR_DIR}/src/*.so
	rm -rf ${PRESTO_MATRIX_HELPER_OBJS} ${PRESTO_MATRIX_HELPER_DIR}/src/*.so
	rm -rf ${MAN_OUTPUT}

clean-installer-tarball:
	rm -rf vertica-distributedR-1.0.0-centos.tar.gz vertica-distributedR-1.0.0-ubuntu.tar.gz installer_exported installer/dr/binaries

#delete everything including boost installation
distclean: clean
	$(MAKE) -C third_party clean

rpm:
	$(MAKE) -C os_packaging rpm
	cp os_packaging/rpmbuild/RPMS/x86_64/*.rpm .
	$(MAKE) -C os_packaging rpm-clean

debian:
	$(MAKE) -C os_packaging debian
	cp os_packaging/debbuild/DEBS/*.deb .
	$(MAKE) -C os_packaging deb-clean

include build.mk

BINARIES=installer_exported/installer/binaries/epel-release-6-8.noarch.rpm installer_exported/installer/binaries/epel-release-7-2.noarch.rpm installer_exported/installer/binaries/R_addons/RInside_0.2.11.tar.gz installer_exported/installer/binaries/R_addons/RODBC_1.3-10.tar.gz installer_exported/installer/binaries/R_addons/RUnit_0.4.27.tar.gz installer_exported/installer/binaries/R_addons/Rcpp_0.11.2.tar.gz installer_exported/installer/binaries/R_addons/XML_3.98-1.1.tar.gz installer_exported/installer/binaries/R_addons/data.table_1.8.10.tar.gz installer_exported/installer/binaries/R_addons/randomForest_4.6-10.tar.gz installer_exported/installer/binaries/vertica-distributedR-latest.rpm installer_exported/installer/binaries/vertica-distributedR-latest.deb  installer_exported/installer/binaries/vRODBC.tar.gz installer_exported/installer/binaries/vertica-odbc-7.1.1-0.x86_64.linux.tar.gz
VDEV_IP_ADDRESS = 10.10.10.16
installer-tarball: centos-installer-tarball ubuntu-installer-tarball
centos-installer-tarball: installer-download-binaries vertica-distributedR-1.0.0-centos.tar.gz
ubuntu-installer-tarball: installer-download-binaries vertica-distributedR-1.0.0-ubuntu.tar.gz

create-binaries-dir: installer_exported/installer/binaries/R_addons/
installer_exported/installer/binaries/R_addons/:
	mkdir -p installer_exported/installer/binaries/R_addons

installer-download-binaries: export-subversion create-binaries-dir $(BINARIES)

installer_exported/installer/binaries/epel-release-6-8.noarch.rpm: 
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/epel-release-6-8.noarch.rpm http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/epel-release-6-8.noarch.rpm
installer_exported/installer/binaries/epel-release-7-2.noarch.rpm: 
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/epel-release-7-2.noarch.rpm http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/epel-release-7-2.noarch.rpm
installer_exported/installer/binaries/R_addons/RInside_0.2.11.tar.gz: 
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/RInside_0.2.11.tar.gz http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/RInside_0.2.11.tar.gz
installer_exported/installer/binaries/R_addons/RODBC_1.3-10.tar.gz:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/RODBC_1.3-10.tar.gz  http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/RODBC_1.3-10.tar.gz
installer_exported/installer/binaries/R_addons/RUnit_0.4.27.tar.gz:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/RUnit_0.4.27.tar.gz  http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/RUnit_0.4.27.tar.gz 
installer_exported/installer/binaries/R_addons/Rcpp_0.11.2.tar.gz:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/Rcpp_0.11.2.tar.gz  http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/Rcpp_0.11.2.tar.gz 
installer_exported/installer/binaries/R_addons/XML_3.98-1.1.tar.gz: 
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/XML_3.98-1.1.tar.gz    http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/XML_3.98-1.1.tar.gz  
installer_exported/installer/binaries/R_addons/data.table_1.8.10.tar.gz:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/data.table_1.8.10.tar.gz   http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/data.table_1.8.10.tar.gz 
installer_exported/installer/binaries/R_addons/randomForest_4.6-10.tar.gz:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/R_addons/randomForest_4.6-10.tar.gz   http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/DependantPackages/R_addons/randomForest_4.6-10.tar.gz 
installer_exported/installer/binaries/vertica-distributedR-latest.rpm:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/vertica-distributedR-latest.rpm http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/builds/vertica-distributedR-1.0.0-0.651.el6.x86_64.rpm
installer_exported/installer/binaries/vertica-distributedR-latest.deb:  
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/vertica-distributedR-latest.deb http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/builds/vertica-distributedR-1.0.0-0.651.DEBIAN7.amd.deb
installer_exported/installer/binaries/vRODBC.tar.gz: 
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/vRODBC.tar.gz http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/builds/vRODBC_1.0.0-b569.RHEL5.tar.gz
installer_exported/installer/binaries/vertica-odbc-7.1.1-0.x86_64.linux.tar.gz:
	http_proxy='' https_proxy='' curl -o installer_exported/installer/binaries/vertica-odbc-7.1.1-0.x86_64.linux.tar.gz http://$(VDEV_IP_ADDRESS)/kits/betas/distributedR/installer/vertica-odbc-7.1.1-0.x86_64.linux.tar.gz
export-subversion: installer_exported/
installer_exported/:
	svn export installer/ installer_exported
vertica-distributedR-1.0.0-centos.tar.gz:  $(BINARIES)
	tar cfz vertica-distributedR-1.0.0-centos.tar.gz  --exclude=installer_exported/installer/binaries/r-ubuntu-12.04-64bit.tgz --exclude=installer_exported/installer/binaries/r-centos-6.5-64bit.tgz --exclude=installer_exported/installer/binaries/vertica-distributedR-latest.deb --exclude=.svn installer_exported/
vertica-distributedR-1.0.0-ubuntu.tar.gz:  $(BINARIES)
	tar cfz vertica-distributedR-1.0.0-ubuntu.tar.gz  --exclude=installer_exported/installer/binaries/r-ubuntu-12.04-64bit.tgz --exclude=installer_exported/installer/binaries/r-centos-6.5-64bit.tgz --exclude=installer_exported/installer/binaries/vertica-distributedR-latest.rpm --exclude=.svn  installer_exported/


