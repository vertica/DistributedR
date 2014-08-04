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


#Makefile variables
include package_vars.mk

SDK?=/opt/vertica/sdk

INSTALLBASE   = $(DEBIANBASE)/SPEC/opt/hp/distributedR
THIRD_PARTY   = $(DEBIANBASE)/SOURCE/third_party
VERTICA_PKGS  = $(DEBIANBASE)/SOURCE/algorithms
DISTR_PKGS    = $(DEBIANBASE)/SOURCE
CONFIG_FILES  = $(DEBIANBASE)/SPEC

debian: deb-clean deb-prep deb-build deb-install deb-pkg

deb-prep:
	@echo "Preparing the debian build area"
	@mkdir -pv $(DEBIANBASE)/SOURCE $(DEBIANBASE)/DEBS
	@rsync -ax --exclude='os_packaging' --exclude='.svn' $(SOURCE_BASE)/* $(DEBIANBASE)/SOURCE
	@chmod 755 $(DEBIANBASE)/SOURCE

deb-build:
	@echo "Building source"
	cd $(DEBIANBASE)/SOURCE; \
        $(MAKE) SDK=$(SDK)

deb-install:
	@echo "Installing source into debian package"
	@rm -rf $(DEBIANBASE)/SPEC/opt
	@install -d $(INSTALLBASE)
	@install -d $(INSTALLBASE)/third_party/R_addons
	@install -d $(INSTALLBASE)/third_party/lib/atomicio
	
	@install -m 444 $(THIRD_PARTY)/RInside_0.2.10.tar.gz         $(INSTALLBASE)/third_party/R_addons/
	@install -m 444 $(THIRD_PARTY)/Rcpp_0.10.6.tar.gz            $(INSTALLBASE)/third_party/R_addons/
	@install -m 444 $(THIRD_PARTY)/XML_3.98-1.1.tar.gz           $(INSTALLBASE)/third_party/R_addons/
	@install -m 444 $(THIRD_PARTY)/data.table_1.8.10.tar.gz      $(INSTALLBASE)/third_party/R_addons/
	@install -m 444 $(THIRD_PARTY)/randomForest_4.6-7.tar.gz     $(INSTALLBASE)/third_party/R_addons/
	@rsync -axv --exclude=.svn $(THIRD_PARTY)/boost_threadpool   $(INSTALLBASE)/third_party/

	@rsync -axv --exclude=.svn $(VERTICA_PKGS)/HPDGLM            $(INSTALLBASE)/third_party/R_addons/
	@rsync -axv --exclude=.svn $(VERTICA_PKGS)/HPdcluster        $(INSTALLBASE)/third_party/R_addons/
	@rsync -axv --exclude=.svn $(VERTICA_PKGS)/HPdgraph          $(INSTALLBASE)/third_party/R_addons/
	@rsync -axv --exclude=.svn $(VERTICA_PKGS)/HPdclassifier     $(INSTALLBASE)/third_party/R_addons/
	@rsync -axv --exclude=.svn $(VERTICA_PKGS)/HPdata            $(INSTALLBASE)/third_party/R_addons/

	@rsync -axv --exclude=.svn $(DISTR_PKGS)/install             	$(INSTALLBASE)
	@rsync -axv --exclude=.svn $(DISTR_PKGS)/bin			$(INSTALLBASE)
	@rsync -axv --exclude=.svn $(DISTR_PKGS)/lib			$(INSTALLBASE)
	@rsync -axv --exclude=.svn $(DISTR_PKGS)/conf			$(INSTALLBASE)
	@rsync -axv --exclude=.svn $(THIRD_PARTY)/install/*		$(INSTALLBASE)/third_party	

	@install -m 755 $(THIRD_PARTY)/atomicio/libatomicio.so 	     $(INSTALLBASE)/third_party/lib/atomicio
	@install -m 444 $(THIRD_PARTY)/atomicio/LICENSE        	     $(INSTALLBASE)/third_party/lib/atomicio

deb-pkg:
	@echo "Building debian package"
	@rm -rf $(DEBIANBASE)/DEBS
	@mkdir -p $(DEBIANBASE)/DEBS
	@rm -rf $(CONFIG_FILES)/.svn $(CONFIG_FILES)/DEBIAN/.svn $(CONFIG_FILES)/etc/.svn
	@sudo dpkg -b $(DEBIANBASE)/SPEC $(DEBIANBASE)/DEBS/vertica-distributedR-$(VERSION)-0.$(BUILD_NUMBER).0.$(PLATFORM).x86_64.deb

deb-clean:
	@rm -rf $(DEBIANBASE)/SOURCE
	@rm -rf $(DEBIANBASE)/SPEC/opt
	@rm -rf $(DEBIANBASE)/DEBS
