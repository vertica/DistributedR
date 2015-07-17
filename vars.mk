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

# Makefile variables 
BOOST_DIR = $(PWD)/third_party/install/include
BOOST_LIB_DIR = $(PWD)/third_party/install/lib

BLKIN_INCLUDE = $(PWD)/third_party/install/include/blkin
BLKIN_LINKER_FLAGS = -lzipkin-c -lblkin-front -lzipkin-cpp

PROTOC_BIN=$(PWD)/third_party/install/bin/protoc
PROTOBUF_STATIC_LIB=$(PWD)/third_party/install/lib/libprotobuf.a
#order matters
ZMQ_STATIC_LIB=$(PWD)/third_party/install/lib/libzmq.a $(PWD)/third_party/install/lib/libuuid.a 

BOOST_THREADPOOL_DIR = $(PWD)/third_party/boost_threadpool/threadpool
ATOMICIO_DIR = $(PWD)/third_party/atomicio

# CGROUP VARIABLES
CGROUP_DIR = $(PWD)/third_party/install/include
CGROUP_LIB_DIR = $(PWD)/third_party/install/lib

GEN_DIR = $(PWD)/platform/messaging/gen-cpp
LIB_DIR = $(PWD)/lib
BIN_DIR = $(PWD)/bin
CONF_DIR = $(PWD)/conf
R_INSTALL_DIR = $(PWD)/install

R_HOME = $(shell R RHOME | grep -v WARNING)
R_INCLUDE_FLAGS = `${R_HOME}/bin/R CMD config --cppflags`
R_LD_FLAGS = `${R_HOME}/bin/R CMD config --ldflags`

RCPP_INCLUDE_FLAGS = `Rscript -e "Rcpp:::CxxFlags()"`
RCPP_LD_FLAGS = `Rscript -e "Rcpp:::LdFlags()"`

RINSIDE_INCLUDE_FLAGS = `Rscript -e "RInside:::CxxFlags()"`
RINSIDE_LD_FLAGS = `Rscript -e "RInside:::LdFlags()"`

PROTO_SRC = $(wildcard platform/messaging/*.proto)
GEN_PROTO_SRC_TMP = $(PROTO_SRC:.proto=.pb.cc)
GEN_PROTO_SRC = $(subst platform/messaging,${GEN_DIR},${GEN_PROTO_SRC_TMP})

PRESTO_COMMON_DIR = platform/common
PRESTO_COMMON_HEADERS = $(wildcard ${PRESTO_COMMON_DIR}/*.h) 
PRESTO_COMMON_SRC = $(wildcard ${PRESTO_COMMON_DIR}/*.cpp) 
PRESTO_COMMON_OBJS = $(PRESTO_COMMON_SRC:.cpp=.o)

PRESTO_WORKER_DIR = platform/worker
PRESTO_WORKER_HEADERS = $(wildcard ${PRESTO_WORKER_DIR}/src/*.h)
PRESTO_WORKER_SRC = $(wildcard ${PRESTO_WORKER_DIR}/src/*.cpp) 
PRESTO_WORKER_OBJS = $(PRESTO_WORKER_SRC:.cpp=.o)

PRESTO_MASTER_DIR = platform/master
PRESTO_MASTER_HEADERS = $(wildcard ${PRESTO_MASTER_DIR}/src/*.h)
PRESTO_MASTER_SRC = $(wildcard ${PRESTO_MASTER_DIR}/src/*.cpp) 
PRESTO_MASTER_OBJS = $(PRESTO_MASTER_SRC:.cpp=.o)
PRESTO_MASTER_RFILES = $(wildcard ${PRESTO_MASTER_DIR}/R/*.R)

PRESTO_EXECUTOR_DIR = platform/executor
PRESTO_EXECUTOR_HEADERS = $(wildcard ${PRESTO_EXECUTOR_DIR}/src/*.h)
PRESTO_EXECUTOR_SRC = $(wildcard ${PRESTO_EXECUTOR_DIR}/src/*.cpp) 
PRESTO_EXECUTOR_OBJS = $(PRESTO_EXECUTOR_SRC:.cpp=.o)
PRESTO_EXECUTOR_RFILES = $(wildcard ${PRESTO_EXECUTOR_DIR}/R/*.R)

PRESTO_MATRIX_HELPER_DIR = platform/matrix_helper
PRESTO_MATRIX_HELPER_SRC = $(wildcard ${PRESTO_MATRIX_HELPER_DIR}/src/*.cpp) 
PRESTO_MATRIX_HELPER_OBJS = $(PRESTO_MATRIX_HELPER_SRC:.cpp=.o)
PRESTO_MATRIX_HELPER_RFILES = $(wildcard ${PRESTO_MATRIX_HELPER_DIR}/R/*.R)

PRESTO_RUN = ${PWD}/tests/run.sh
PRESTO_DEF_WORKER_LIST = ${CONF_DIR}/cluster_conf.xml
PRESTO_INSTALL_LOCALLIB_SCRIPT = $(PWD)/tools/InstallLocalLib.R
PRESTO_UNITTEST_SCRIPT = $(PWD)/tests/RunPrestoUnitTest.R
PRESTO_ALGOTEST_SCRIPT = $(PWD)/tests/RunPrestoAlgoTest.R
PRESTO_STRSTEST_SCRIPT = $(PWD)/tests/RunPrestoStressTest.R

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
TEST_OUTPUT_FILES=$(PWD)/test_platform.out

BLAS_LIBS = $(shell R CMD config BLAS_LIBS | grep -v WARNING)
INCLUDE_FLAGS = -DBOOST_LOG_DYN_LINK -DHAVE_NETINET_IN_H -DHAVE_INTTYPES_H -I ${CGROUP_DIR} -I ${BOOST_DIR} -I ${GEN_DIR} -I ${BOOST_THREADPOOL_DIR} -I ${ATOMICIO_DIR} -I ${PRESTO_COMMON_DIR} ${R_INCLUDE_FLAGS} ${RCPP_INCLUDE_FLAGS} ${RINSIDE_INCLUDE_FLAGS}
LINK_FLAGS = -lm -L $(CGROUP_LIB_DIR) -Wl,-rpath,$(CGROUP_LIB_DIR) -lcgroup -rdynamic -L ${LIB_DIR} -Wl,-rpath,${LIB_DIR} ${R_LD_FLAGS} -lpthread -L$(BOOST_LIB_DIR) -Wl,-rpath,${BOOST_LIB_DIR} -lboost_thread -lboost_system -lboost_log -lboost_log_setup -lboost_chrono -lboost_filesystem -lboost_date_time -L ${ATOMICIO_DIR} -Wl,-rpath,${ATOMICIO_DIR} -latomicio ${RCPP_LD_FLAGS} ${RINSIDE_LD_FLAGS} -lrt $(BLAS_LIBS) #-laio 

DEBUG = -g
#PROFILING = -DPROFILING
#SCHEDULER_LOGGING = -DSCHEDULER_LOGGING
#OOC_SCHEDULER = -DOOC_SCHEDULER
#USE_MMAP_AS_SHMEM = -DUSE_MMAP_AS_SHMEM
#USE_DYNAMIC_PARTITION = -DDYNAMIC_PARTITION
# updates are processed faster at master for darrays with many splits;
# interferes with onchange!, may interfere with repartitioning! (?)
#FAST_UPDATE = -DFAST_UPDATE
# increase R heap size to 4GB min (reduces number of GCs)
#INCREASE_R_HEAP = -DINCREASE_R_HEAP
# executor log names have worker pid in them
#UNIQUE_EXECUTOR_LOG_NAMES = -DUNIQUE_EXECUTOR_LOG_NAMES
#EXECUTOR_TRYCATCH = -DEXECUTOR_TRYCATCH
#MULTITHREADED_SCHEDULER = -DMULTITHREADED_SCHEDULER  # locking in multi-threaded scheduler is not 100% tested; use single thread until it affects performance
GCC_FLAGS = -std=c++0x ${DEBUG} -O2 -fopenmp -finline-limit=10000 -DNDEBUG ${INCLUDE_FLAGS} ${LINK_FLAGS} -Wno-deprecated-declarations -DSTRICT_R_HEADERS ${USE_MMAP_AS_SHMEM} ${SCHEDULER_LOGGING} ${OOC_SCHEDULER} ${USE_DYNAMIC_PARTITION} ${PROFILING} ${FAST_UPDATE} ${INCREASE_R_HEAP} ${UNIQUE_EXECUTOR_LOG_NAMES} ${EXECUTOR_TRYCATCH}

CXXFLAGS=${GCC_FLAGS} -fPIC


ATOMICIO_LIB=${ATOMICIO_DIR}/libatomicio.so
PRESTO_PROTO=${LIB_DIR}/libR-proto.so
PRESTO_COMMON=${LIB_DIR}/libR-common.so

MASTER_RLIB=${R_INSTALL_DIR}/distributerR/libs/distributedR.so
MASTER_BIN=${BIN_DIR}/master-bin

WORKER_RLIB=${R_INSTALL_DIR}/PrestoWorker/libs/PrestoWorker.so
WORKER_BIN=${BIN_DIR}/R-worker-bin

EXECUTOR_RLIB=${R_INSTALL_DIR}/Executor/libs/Executor.so
EXECUTOR_BIN=${BIN_DIR}/R-executor-bin

MATRIX_HELPER_RLIB=${R_INSTALL_DIR}/MatrixHelper/libs/MatrixHelper.so

# Uncomment to get timing and debugging information
#-DPROFILE_TRANSFER_TIME 
# Uncomment to randomize darray locations
#-DRANDOMIZE_DARRAY_LOCATIONS 

