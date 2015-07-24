# Makefile for building atomicio shared library


INSTDIR=../../third_party/install/lib/

all: $(INSTDIR)/libatomicio.a $(INSTDIR)/../include/atomicio.h

#libatomicio.so: atomicio.o
#	g++ -O3 -fPIC -shared -o $@ $^

$(INSTDIR)/libatomicio.a: atomicio.o
	mkdir -p $(INSTDIR)
	ar rcs $@ $^

$(INSTDIR)/../include/atomicio.h:
	mkdir -p $(INSTDIR)/../include
	cp atomicio.h $@

%.o: %.cpp
	g++ -O3 -fPIC -o $@ -c $^

clean:
	rm -rf *.o  $(INSTDIR)/libatomicio.a $(INSTDIR)/../include/atomicio.h
