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


include package_vars.mk

all: rpm

.PHONY: rpm-prep rpm-tarball rpm rpm-clean 

rpm-prep:
	@echo "Preparing the rpm build area"
	@mkdir -pv $(RPMBASE)/{BUILD,SOURCES,SRPMS,RPMS}
	@cp -v $(WORKSPACE)/$(NAME)-x86_64.conf                $(RPMBASE)/SOURCES/
	@cp -v $(SOURCE_BASE)/third_party/RInside_0.2.10.tar.gz $(RPMBASE)/SOURCES/
	@cp -v $(SOURCE_BASE)/third_party/Rcpp_0.10.6.tar.gz    $(RPMBASE)/SOURCES/
	@cp -v $(SOURCE_BASE)/third_party/XML_3.98-1.1.tar.gz  $(RPMBASE)/SOURCES/
	@cp -v $(SOURCE_BASE)/third_party/data.table_1.8.10.tar.gz  $(RPMBASE)/SOURCES/
	@cp -v $(SOURCE_BASE)/third_party/randomForest_4.6-7.tar.gz  $(RPMBASE)/SOURCES/
	@cp -rv $(SOURCE_BASE)/algorithms/HPDGLM  $(RPMBASE)/SOURCES/
	@cp -rv $(SOURCE_BASE)/algorithms/HPdcluster $(RPMBASE)/SOURCES/
	@cp -rv $(SOURCE_BASE)/algorithms/HPdgraph $(RPMBASE)/SOURCES/
	@cp -rv $(SOURCE_BASE)/algorithms/HPdclassifier $(RPMBASE)/SOURCES/
	@cp -rv $(SOURCE_BASE)/algorithms/HPdata $(RPMBASE)/SOURCES/

rpm-tarball: 
	@echo "Preparing RPM source tarball"
	@rm -rf $(TEMPDIR)
	@mkdir -p $(TARBALL_STAGE)
	@rsync -ax --exclude-from=rsync.excludes $(SOURCE_BASE) $(TARBALL_STAGE)
	@mkdir -p $(TARBALL_STAGE)/third_party
	@rsync -ax $(SOURCE_BASE)/third_party/Makefile $(SOURCE_BASE)/third_party/boost*.tar.gz $(SOURCE_BASE)/third_party/atomicio $(SOURCE_BASE)/third_party/boost_threadpool $(TARBALL_STAGE)/third_party/
	@tar -C $(TEMPDIR) -czf $(RPMBASE)/SOURCES/$(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	@rm -rf $(TEMPDIR)

rpm: rpm-clean rpm-prep rpm-tarball
	rpmbuild --define "_topdir $(RPMBASE)" --define "build_number $(BUILD_NUMBER)" -ba $(RPMBASE)/SPECS/$(NAME).spec

# This target used for debugging the RPM specfile installation phase
rpm-install: 
	rpmbuild --define "_topdir $(RPMBASE)" -bi --short-circuit $(RPMBASE)/SPECS/$(NAME).spec

rpm-clean:
	@rm -rf $(RPMBASE)/{BUILD,SOURCES,SRPMS,RPMS,BUILDROOT}
	@rm -rf $(RPMBASE)/\{BUILD\,SOURCES\,SRPMS\,RPMS\}/

include debian.mk
