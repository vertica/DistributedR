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


NAME = vertica-distributedR
VERSION = 0.7.0

# This allows the Jenkins build number to be included in the RPM release value.
BUILD_NUMBER  ?= 1

WORKSPACE     = $(CURDIR)
SOURCE_BASE   = $(WORKSPACE)/..
RPMBASE       = $(WORKSPACE)/rpmbuild
DEBIANBASE    = $(WORKSPACE)/debbuild
TEMPDIR       = /tmp/distributedR-tarball-stage
TARBALL_STAGE = $(TEMPDIR)/$(NAME)-$(VERSION)
