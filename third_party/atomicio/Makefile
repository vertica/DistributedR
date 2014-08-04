# Makefile for building atomicio shared library

.PHONY: atomicio all

all: libatomicio.so

libatomicio.so: atomicio.o
	g++ -O3 -fPIC -shared -o $@ $^

libatomicio.a: atomicio.o
	ar rcs $@ $^

%.o: %.cpp
	g++ -O3 -fPIC -o $@ -c $^

clean:
	rm -rf *.o *.so *.a
