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

# makefile for packages - algorithm packages, vRODBC and Data Loader UDX

.PHONY: vrodbc algorithms data_loader_udx clean

vrodbc:
	rm -rf third_party/vRODBC_*.tar.gz
	R CMD build third_party/vRODBC
	mv vRODBC*.tar.gz third_party/

algorithms:
	rm -rf algorithms/HPDGLM*.tar.gz
	rm -rf algorithms/HPdcluster*.tar.gz
	rm -rf algorithms/HPdgraph*.tar.gz
	rm -rf algorithms/HPdclassifier*.tar.gz
	rm -rf algorithms/HPdata*.tar.gz
	R CMD build algorithms/HPDGLM
	mv HPDGLM*.tar.gz algorithms/      
	R CMD build algorithms/HPdcluster
	mv HPdcluster*.tar.gz algorithms/
	R CMD build algorithms/HPdgraph
	mv HPdgraph*.tar.gz algorithms/
	R CMD build algorithms/HPdclassifier
	mv HPdclassifier*.tar.gz algorithms/
	R CMD build algorithms/HPdata
	mv HPdata*.tar.gz algorithms/

data_loader_udx:
	$(MAKE) clean -C algorithms/Native_data_connnector
	$(MAKE) -C algorithms/Native_data_connnector

## Cleaning all package build tarballs
clean:
	rm -rf third_party/vRODBC_*.tar.gz
	rm -rf algorithms/HPDGLM*.tar.gz
	rm -rf algorithms/HPdcluster*.tar.gz
	rm -rf algorithms/HPdgraph*.tar.gz
	rm -rf algorithms/HPdclassifier*.tar.gz
	rm -rf algorithms/HPdata*.tar.gz

	
