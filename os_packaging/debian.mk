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

PLATFORM     ?= DEBIAN6

DEBIANBASE    = $(WORKSPACE)/debbuild
BUILD         = $(DEBIANBASE)/build
INSTALLBASE   = $(DEBIANBASE)/install/opt/hp/distributedR

THIRD_PARTY   = $(BUILD)/SOURCE/third_party
VERTICA_PKGS  = $(BUILD)/SOURCE/algorithms
DISTR_PKGS    = $(BUILD)/SOURCE

debian: deb-clean deb-prep deb-build deb-install deb-pkg

deb-prep:
	@echo "Preparing the debian build area"
	@mkdir -pv $(BUILD)
	@chmod 755 $(BUILD)
	@rsync -ax --exclude='os_packaging' --exclude='.svn' $(SOURCE_BASE)/* $(BUILD)/

deb-build:
	@echo "Building source"
	cd $(BUILD); \
        $(MAKE) SDK=$(SDK)

deb-install:
	@echo "Installing source into debian package staging area"
	@rm -rf $(INSTALLBASE)
	@install -d $(INSTALLBASE)
	@install -d $(DEBIANBASE)/install/etc/ld.so.conf.d/
	@install -d $(INSTALLBASE)/third_party/R_addons
	@install -d $(INSTALLBASE)/third_party/lib/atomicio

        # Add Debian control files	
	@rsync -ax --exclude='.svn' $(DEBIANBASE)/DEBIAN $(DEBIANBASE)/install
	@sed -i "s/@@SCM_VERSION@@/$(SVNREVISION)/" $(DEBIANBASE)/install/DEBIAN/control

	# Add Distributed R code
	@rsync -ax --exclude=.svn $(BUILD)/third_party/boost_threadpool $(INSTALLBASE)/third_party/
	@rsync -ax --exclude=.svn $(BUILD)/algorithms/HPdregression     $(INSTALLBASE)/third_party/R_addons/
	@rsync -ax --exclude=.svn $(BUILD)/algorithms/HPdcluster        $(INSTALLBASE)/third_party/R_addons/
	@rsync -ax --exclude=.svn $(BUILD)/algorithms/HPdgraph          $(INSTALLBASE)/third_party/R_addons/
	@rsync -ax --exclude=.svn $(BUILD)/algorithms/HPdclassifier     $(INSTALLBASE)/third_party/R_addons/
	@rsync -ax --exclude=.svn $(BUILD)/algorithms/HPdata            $(INSTALLBASE)/third_party/R_addons/

	@rsync -ax --exclude=.svn $(BUILD)/install                      $(INSTALLBASE)
	@rsync -ax --exclude=.svn $(BUILD)/bin		                $(INSTALLBASE)
	@rsync -ax --exclude=.svn $(BUILD)/lib		                $(INSTALLBASE)
	@rsync -ax --exclude=.svn $(BUILD)/conf		                $(INSTALLBASE)
	@rsync -ax --exclude=.svn $(BUILD)/third_party/install/*        $(INSTALLBASE)/third_party	

	@install -m 755 $(BUILD)/third_party/atomicio/libatomicio.so    $(INSTALLBASE)/third_party/lib/atomicio
	@install -m 444 $(BUILD)/third_party/atomicio/LICENSE     	$(INSTALLBASE)/third_party/lib/atomicio

	@install    $(WORKSPACE)/$(NAME)-x86_64.conf                    $(DEBIANBASE)/install/etc/ld.so.conf.d/

deb-pkg:
	@echo "Building debian package"
	@sudo dpkg -b $(DEBIANBASE)/install $(DEBIANBASE)/vertica-distributedR-$(VERSION)-0.$(BUILD_NUMBER).$(PLATFORM).amd.deb

deb-clean:
	@rm -rf $(BUILD)
	@rm -rf $(DEBIANBASE)/install
