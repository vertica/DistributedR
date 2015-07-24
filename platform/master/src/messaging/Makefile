# --- COMMON ---

CXXFLAGS = -fPIC -std=c++0x -g -O2 -fopenmp -finline-limit=10000 -DNDEBUG -I../third_party/install/include
LDFLAGS = -L../third_party/install/lib

PROTO_SRC = $(wildcard *.proto)
GEN_PROTO_SRC_TMP = $(PROTO_SRC:.proto=.pb.cc)
GEN_PROTO_DIR = ./gen-cpp
GEN_PROTO_HEADERS = $(wildcard ${GEN_PROTO_DIR}/*.h)
GEN_PROTO_SRC = $(subst .,${GEN_PROTO_DIR},${GEN_PROTO_SRC_TMP})
GEN_PROTO_OBJS = $(GEN_PROTO_SRC:.cc=.o)

INSTALLDIR=$(abspath ../third_party/install/)
PROTOC_BIN = $(INSTALLDIR)/bin/protoc

# --- PROTOCOL BUFFERS ---

all:  ../third_party/install/lib/libR-proto.a

# Protocol Buffers C++ src files
${GEN_PROTO_DIR}/master.pb.cc ${GEN_PROTO_DIR}/master.pb.h: ./master.proto
	mkdir -p ${GEN_PROTO_DIR}
	${PROTOC_BIN} --cpp_out=${GEN_PROTO_DIR} --proto_path=. $^

${GEN_PROTO_DIR}/worker.pb.cc ${GEN_PROTO_DIR}/worker.pb.h: ./worker.proto
	mkdir -p ${GEN_PROTO_DIR}
	${PROTOC_BIN} --cpp_out=${GEN_PROTO_DIR} --proto_path=. $^

${GEN_PROTO_DIR}/shared.pb.cc ${GEN_PROTO_DIR}/shared.pb.h: ./shared.proto
	mkdir -p ${GEN_PROTO_DIR}
	${PROTOC_BIN} --cpp_out=${GEN_PROTO_DIR} --proto_path=. $^

${GEN_PROTO_DIR}/master.pb.o: ${GEN_PROTO_DIR}/master.pb.cc ${GEN_PROTO_DIR}/master.pb.h ${GEN_PROTO_DIR}/shared.pb.h
	g++ -c $< $(CXXFLAGS) -o $@

${GEN_PROTO_DIR}/worker.pb.o: ${GEN_PROTO_DIR}/worker.pb.cc ${GEN_PROTO_DIR}/worker.pb.h ${GEN_PROTO_DIR}/shared.pb.h
	g++ -c $< $(CXXFLAGS) -o $@

${GEN_PROTO_DIR}/shared.pb.o: ${GEN_PROTO_DIR}/shared.pb.cc ${GEN_PROTO_DIR}/shared.pb.h
	g++ -c $< $(CXXFLAGS) -o $@


# Protocol Buffers library
../third_party/install/lib/libR-proto.a: ${GEN_PROTO_DIR}/shared.pb.o ${GEN_PROTO_DIR}/worker.pb.o ${GEN_PROTO_DIR}/master.pb.o
	mkdir -p ../third_party/install/lib/
	ar rcs $@ ${GEN_PROTO_DIR}/shared.pb.o ${GEN_PROTO_DIR}/worker.pb.o ${GEN_PROTO_DIR}/master.pb.o


clean:
	rm -rf *.o gen-cpp/ ../third_party/install/lib/libR-proto.a
