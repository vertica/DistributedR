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

include vars.mk

## === Build targets
all: third_party ${ATOMICIO_LIB} ${WORKER_BIN} ${MASTER_BIN} ${MASTER_RLIB} ${EXECUTOR_BIN} ${MATRIX_HELPER_RLIB}

lint:
	tools/lint.sh ${PRESTO_WORKER_SRC} ${PRESTO_MASTER_SRC} ${PRESTO_WORKER_HEADERS} ${PRESTO_MASTER_HEADERS} ${PRESTO_COMMON_HEADERS} ${PRESTO_COMMON_SRC} ${PRESTO_EXECUTOR_HEADERS} ${PRESTO_EXECUTOR_SRC}

.PHONY: clean third_party test boost docs manual tutorial faq distclean install blkin trace_build

${ATOMICIO_LIB}:
	$(MAKE) -C third_party/atomicio

boost:
	$(MAKE) -C third_party boost

third_party:
	$(MAKE) -C third_party -j8 all

install:
	$(MAKE)
	sudo bin/install_distributedR.sh

blkin: 
	$(MAKE) -C third_party -j8 blkin

trace_build: GCC_FLAGS += -I ${BLKIN_INCLUDE} -DPERF_TRACE ${BLKIN_LINKER_FLAGS}

trace_build: clean blkin all

## === Test targets

test:
	$(MAKE) test_platform

algotest:
	$(MAKE)
	$(PRESTO_RUN) $(PRESTO_DEF_WORKER_LIST) $(PRESTO_ALGOTEST_SCRIPT)

stresstest:
	$(MAKE)
	$(PRESTO_RUN) $(PRESTO_DEF_WORKER_LIST) $(PRESTO_STRSTEST_SCRIPT)

test_platform:
	echo "library(distributedR); library(testthat); sink(paste(getwd(),'/test_platform.out',sep=''), type='output'); distributedR_start(); test_package('distributedR'); distributedR_shutdown()" | R --vanilla --slave --no-save
	@printf "\n----- Test Report -----\n\n"
	@cat $(PWD)/test_platform.out

test_clean:
	rm -rf ${TEST_OUTPUT_FILES}

## === Targets to generate documentation

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


## === Uninstall and Clean targets

uninstall:
	sudo bin/uninstall_distributedR.sh ${ARGS}

clean:
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

#delete everything including boost installation from third_party
distclean: clean
	$(MAKE) -C third_party clean

include build.mk
