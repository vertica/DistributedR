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

# Top level Makefile for Distributed R


## === Build targets


.PHONY: test docs manual tutorial faq

## === Test targets
TEST_OUTPUT_FILES=$(PWD)/test_platform.out

test:
	$(MAKE) test_platform

test_platform:
	echo "library(distributedR); library(testthat); sink(paste(getwd(),'/test_platform.out',sep=''), type='output'); distributedR_start(); test_package('distributedR'); distributedR_shutdown()" | R --vanilla --slave --no-save
	@printf "\n----- Test Report -----\n\n"
	@cat $(PWD)/test_platform.out

test_clean:
	rm -rf ${TEST_OUTPUT_FILES}

## === Targets to generate documentation
DOC_DIR = $(PWD)/doc
DOC_DIR_PLATFORM = $(DOC_DIR)/platform
DOC_DIR_ALGORITHMS = $(DOC_DIR)/algorithms
MAN_OUTPUT = $(PWD)/doc/platform/Distributed-R-Manual.pdf
MAN_DIR = $(PWD)/platform/master/man
MAN_FILES = $(MAN_DIR)/package.Rd $(MAN_DIR)/start.Rd $(MAN_DIR)/shutdown.Rd $(MAN_DIR)/status.Rd $(MAN_DIR)/darray.Rd $(MAN_DIR)/dframe.Rd $(MAN_DIR)/dlist.Rd $(MAN_DIR)/as.darray.Rd $(MAN_DIR)/as.dframe.Rd $(MAN_DIR)/is.darray.Rd $(MAN_DIR)/is.dframe.Rd $(MAN_DIR)/is.dlist.Rd $(MAN_DIR)/as.factor.dframe.Rd $(MAN_DIR)/factor.dframe.Rd $(MAN_DIR)/unfactor.dframe.Rd $(MAN_DIR)/levels.dframe.Rd $(MAN_DIR)/npartitions.Rd $(MAN_DIR)/partitionsize.Rd $(MAN_DIR)/getpartition.Rd $(MAN_DIR)/clone.Rd $(MAN_DIR)/foreach.Rd $(MAN_DIR)/splits.Rd $(MAN_DIR)/update.Rd
TUTORIAL_DIR = $(PWD)/platform/master/vignettes
TUTORIAL_FILE = Tutorial
TUTORIAL_DATA_FILE = Data
FAQ_FILE = FAQ

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

install:
	R CMD INSTALL platform/executor
	R CMD INSTALL platform/master
	R CMD INSTALL platform/matrix_helper

uninstall:
	R CMD REMOVE platform/executor
	R CMD REMOVE platform/master
	R CMD REMOVE platform/matrix_helper

clean:
	cd platform/master/src; make -f Makevars clean; cd ../../../

dist-clean:
	cd platform/master/src; make -f Makevars clean-third-party; cd ../../../

