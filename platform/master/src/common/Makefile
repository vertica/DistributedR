# --- COMMON ---


R_CXXFLAGS = $(shell R CMD config --cppflags)  $(shell Rscript -e "Rcpp:::CxxFlags()") $(shell Rscript -e "RInside:::CxxFlags()")

CXXFLAGS = -fPIC -std=c++0x -g -O2 -fopenmp -finline-limit=10000 -DNDEBUG -I../third_party/install/include -I../messaging/gen-cpp/  -I./ -I../third_party/boost_1_50_0/

LDFLAGS = -L../third_party/install/lib

PRESTO_COMMON_DIR = .
PRESTO_COMMON_HEADERS = $(wildcard ${PRESTO_COMMON_DIR}/*.h)
PRESTO_COMMON_SRC = $(wildcard ${PRESTO_COMMON_DIR}/*.cpp)
PRESTO_COMMON_OBJS = $(PRESTO_COMMON_SRC:.cpp=.o)

all: $(PRESTO_COMMON_OBJS) ../third_party/install/lib/libR-common.a

# Common objects
%.o: %.cpp $(PRESTO_COMMON_HEADERS)
	g++ -c $< $(CXXFLAGS) $(R_CXXFLAGS) -o $@

# Common library
../third_party/install/lib/libR-common.a: ../third_party/install/lib/libR-proto.a $(PRESTO_COMMON_OBJS)
	mkdir -p ../third_party/install/lib
	ar rcs $@ $(PRESTO_COMMON_OBJS)

clean:
	rm -rf *.o ../third_party/install/lib/libR-common.a
