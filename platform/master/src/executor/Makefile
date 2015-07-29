# --- COMMON ---

R_CXXFLAGS = $(shell R CMD config --cppflags)  $(shell Rscript -e "Rcpp:::CxxFlags()") $(shell Rscript -e "RInside:::CxxFlags()")
#R_LDFLAGS =  $(shell R CMD config --ldflags)  $(shell Rscript -e "Rcpp:::LdFlags()") $(shell Rscript -e "RInside:::LdFlags()")
R_LDFLAGS = -L$(shell Rscript -e "cat(R.home('home'), '/lib', sep='')") -lR -L$(shell Rscript -e "cat(system.file(package='RInside'),'/lib',sep='')") -Wl,-static -lRInside  -Wl,-Bdynamic -Wl,-rpath,$(shell Rscript -e "cat(system.file(package='RInside'),'/lib',sep='')")

CXXFLAGS = -fPIC -std=c++0x -g -O2 -fopenmp -finline-limit=10000 -DNDEBUG -I../third_party/install/include   -I./ -I../common/ -I../messaging/gen-cpp -DSTRICT_R_HEADERS -I../third_party/boost_1_50_0/ -I..

LDFLAGS = -L../third_party/install/lib -lpthread -lboost_thread -lboost_system -lboost_log -lboost_log_setup -lboost_chrono -lboost_filesystem -lboost_date_time -latomicio -lR-common -lzmq -luuid -lrt

# To enable zipkin-based tracing add the following to CXXFLAGS and LDFLAGS
#   BLKIN_INCLUDE = -I../third_party/install/include/blkin -DPERF_TRACE
#   BLKIN_LINKER_FLAGS = -lzipkin-c -lblkin-front -lzipkin-cpp

#PRESTO_COMMON_DIR = .
#PRESTO_COMMON_HEADERS = $(wildcard ${PRESTO_COMMON_DIR}/*.h)
#PRESTO_COMMON_SRC = $(wildcard ${PRESTO_COMMON_DIR}/*.cpp)
#PRESTO_COMMON_OBJS = $(PRESTO_COMMON_SRC:.cpp=.o)
PRESTO_COMMON_OBJS = executor.o

all: $(PRESTO_COMMON_OBJS) ../../inst/bin/R-executor-bin

# Common objects
%.o: %.cpp
	g++ -c $< $(CXXFLAGS) $(R_CXXFLAGS) -o $@

# Common library
../../inst/bin/R-executor-bin: $(PRESTO_COMMON_OBJS)
	mkdir -p ../../inst/bin
	g++  $(PRESTO_COMMON_OBJS) $(LDFLAGS) $(R_LDFLAGS) -o $@

clean:
	rm -rf *.o ../../inst/bin/R-executor-bin
