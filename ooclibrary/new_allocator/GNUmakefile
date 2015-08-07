## -*- mode: make; tab-width: 8; -*-
##
## Simple Makefile
##
## TODO: 
##  proper configure for non-Debian file locations,   [ Done ]
##  allow RHOME to be set for non-default R etc

## comment this out if you need a different version of R, 
## and set set R_HOME accordingly as an environment variable
R_HOME := 		$(shell R RHOME)

sources := 		$(wildcard *.cpp)
programs := 	test_access_performance


## include headers and libraries for R 
RCPPFLAGS := 		$(shell $(R_HOME)/bin/R CMD config --cppflags)
RLDFLAGS := 		$(shell $(R_HOME)/bin/R CMD config --ldflags)
RBLAS := 		$(shell $(R_HOME)/bin/R CMD config BLAS_LIBS)
RLAPACK := 		$(shell $(R_HOME)/bin/R CMD config LAPACK_LIBS)

RCPPINCL := 		$(shell echo 'Rcpp:::CxxFlags()' | $(R_HOME)/bin/R --vanilla --slave)
RCPPLIBS := 		$(shell echo 'Rcpp:::LdFlags()'  | $(R_HOME)/bin/R --vanilla --slave)

RINSIDEINCL := 		$(shell echo 'RInside:::CxxFlags()' | $(R_HOME)/bin/R --vanilla --slave)
RINSIDELIBS := 		$(shell echo 'RInside:::LdFlags()'  | $(R_HOME)/bin/R --vanilla --slave)

# INCLUDE := -I../include
LIBS := -luuid -ldl -lpthread
LDLIBS := 		$(LIBS) $(RLDFLAGS) $(RRPATH) $(RCPPLIBS) $(RINSIDELIBS)

# LIBS := LDLIBS := $(LIBS) $(RCPPLIBS) $(RINSIDELIBS) $(LAPACK_LIBS) $(BLAS_LIBS) $(FLIBS)

ALL_FLAGS = -g -pg -I./include
# ALL_FLAGS = -O3

CFLAGS=$(INCLUDE) -fPIC -DPIC $(ALL_FLAGS) -Wall -Wextra
CXXFLAGS=$(INCLUDE) -std=c++0x -fPIC -DPIC $(ALL_FLAGS) -Wall -Wextra $(RCPPFLAGS) $(RCPPINCL) $(RINSIDEINCL)
CPPFLAGS=-Wall -fPIC -DPIC $(ALL_FLAGS) -Wall -Wextra

# .SILENT: *.o

TEST_PROGRAM := test_all test_basic test_basic_original \
				test_seq test_seq_original profile_signal \
				init_shm test_all_shm \
				test_shm

PROGRAM := unemalloc.so 

HELPER_OBJECTS := helper.o profiling.o gprintf.o

ALLOCATOR_OBJECTS := new_allocator.o dlmalloc-mem.o inplace_mem_allocator.o multi_reservation_object_allocator.o vm_manager.o object_io.o $(HELPER_OBJECTS)

all: $(PROGRAM)

test: $(TEST_PROGRAM)

unemalloc.so: dlmalloc-dym.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -fPIC -O3 -W -Wall -shared -Wl,-export-dynamic -o $@ $^ -lc $(LDLIBS)

profile_signal: profile_signal.o $(HELPER_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

profile_timer: profile_timer.o $(HELPER_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_all: test_all.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_all_shm: test_all_shm.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_shm: test_shm.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_resize: test_resize.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_basic: test_basic.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_basic_original: test_basic_original.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_seq: test_seq.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_seq_original: test_seq_original.o wrapper.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_meta_allocator: test_meta_allocator.o dlmalloc-mem.o
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_gprintf: test_gprintf.o gprintf.o
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

init_shm: init_shm.o $(ALLOCATOR_OBJECTS)
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

dlmalloc-dym.o: external/dlmalloc.c
	$(CC) $(CFLAGS) $(INCLUDE) -DDYNAMIC_LIB -fPIC -DPIC -c -o $@ $<  $(LIBS)

dlmalloc-mem.o: external/dlmalloc.c
	$(CC) $(CFLAGS) $(INCLUDE) -DALL_IN_MEM -fPIC -DPIC -c -o $@ $<  $(LIBS)

test_basic_original.o: test/test_basic.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -DUSE_ORIGINAL -fPIC -DPIC -c -o $@ $<  $(LIBS)

test_seq_original.o: test/test_seq.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -DUSE_ORIGINAL -fPIC -DPIC -c -o $@ $<  $(LIBS)

test_all_shm.o: test/test_all.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -DUSE_SHM -fPIC -DPIC -c -o $@ $<  $(LIBS)


# %.o: %.c
# 	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

# %.o: %.cpp
# 	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

%.o: src/%.c
	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

%.o: src/%.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

%.o: test/%.c
	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

%.o: test/%.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)

%.o: external/%.c
	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -DPIC -c -o $@ $<  $(LIBS)


clean:
			rm -vf $(PROGRAM) $(TEST_PROGRAM)
			rm -vrf *.dSYM
			rm -vf *.o *.so
