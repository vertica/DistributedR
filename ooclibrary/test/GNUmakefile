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

INCLUDE := -I../include
LIBS := -luuid -ldl -lpthread
LDLIBS := 		$(LIBS) $(RLDFLAGS) $(RRPATH) $(RCPPLIBS) $(RINSIDELIBS)

# LIBS := LDLIBS := $(LIBS) $(RCPPLIBS) $(RINSIDELIBS) $(LAPACK_LIBS) $(BLAS_LIBS) $(FLIBS)


CFLAGS=$(INCLUDE) -fPIC -dPIC -g
CXXFLAGS=$(INCLUDE) -fPIC -dPIC -g $(RCPPFLAGS) $(RCPPINCL) $(RINSIDEINCL)
CPPFLAGS=-Wall -fPIC -dPIC -g

all: test_access_performance test_random_action


test_access_performance: helper.o large_malloc.o allocator.o test_access_performance.o
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)

test_random_action: helper.o large_malloc.o allocator.o test_random_action.o
	$(CXX) -o $@ $^ $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) $(LDLIBS)


%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -dPIC -c -o $@ $<  $(LIBS)

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -fPIC -dPIC -c -o $@ $<  $(LIBS)

%.o: ../src/%.c
	$(CC) $(CFLAGS) $(INCLUDE) -fPIC -dPIC -c -o $@ $<  $(LIBS)

%.o: ../src/%.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDE) -fPIC -dPIC -c -o $@ $<  $(LIBS)

clean:
			rm -vf $(programs)
			rm -vrf *.dSYM
			rm -vf *.o *.so