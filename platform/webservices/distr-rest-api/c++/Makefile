INCDIRS=-Icasablanca/Release/include -Iinclude
LIBS=-Lcasablanca/Release/build.release/Binaries/ -lcpprest
CXXFLAGS=$(INCDIRS) -std=c++11
LDFLAGS=$(LIBS)
CXX=g++-4.8

all: build/server build/client
build/server: build/exampleserver1.o 
	$(CXX) -o build/server build/exampleserver1.o $(LDFLAGS)
build/exampleserver1.o: src/exampleserver1.cpp
	$(CXX) $(CXXFLAGS) -o build/exampleserver1.o -c src/exampleserver1.cpp

build/client: build/exampleclient1.o
	$(CXX) -o build/client build/exampleclient1.o $(LDFLAGS)
build/exampleclient1.o: src/exampleclient1.cpp
	$(CXX) $(CXXFLAGS) -o build/exampleclient1.o -c src/exampleclient1.cpp
