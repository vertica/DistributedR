
ZMQ_VERSION=2.2.0
PROTOBUF_VERSION=2.3.0

all: boost protobuf zmq  uuid
boost: $(PWD)/third_party/install/lib/libboost_thread.so

$(PWD)/third_party/boost_1_50_0/bootstrap.sh:
	tar -zxf boost_1_50_0.tar.gz
	cd boost_1_50_0; ./bootstrap.sh --with-libraries=thread,system,log,chrono --prefix=$(PWD)/third_party/install

$(PWD)/third_party/install/lib/libboost_thread.so: $(PWD)/third_party/boost_1_50_0/bootstrap.sh
	cd boost_1_50_0; ./b2 -j8 --with-thread --with-log --with-chrono --with-system link=shared runtime-link=shared define=BOOST_LOG_DYN_LINK install

protobuf: $(PWD)/third_party/install/lib/libprotobuf.a

$(PWD)/third_party/install/lib/libprotobuf.a:
	tar -zxf protobuf-$(PROTOBUF_VERSION).tar.gz
	cd protobuf-$(PROTOBUF_VERSION); CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(PWD)/third_party/install && make -j8 && make install

zmq: $(PWD)/third_party/install/lib/libzmq.a

$(PWD)/third_party/install/lib/libzmq.a: $(PWD)/third_party/install/lib/libuuid.a
	tar -zxf zeromq-$(ZMQ_VERSION).tar.gz
	mkdir -p install/include
	cd zeromq-$(ZMQ_VERSION); CFLAGS="-fPIC -I`pwd`/../install/include" CXXFLAGS="-fPIC -I`pwd`/../install/include" LDFLAGS="-fPIC -L`pwd`/../install/lib" ./configure --enable-static --enable-shared=no --prefix=$(PWD)/third_party/install && make -j8 && make install


uuid: $(PWD)/third_party/install/lib/libuuid.a
$(PWD)/third_party/install/lib/libuuid.a:
	tar -zxf libuuid-1.0.3.tar.gz
	cd libuuid-1.0.3; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure  --enable-static --enable-shared=no --prefix=$(PWD)/third_party/install && make -j8 && make install

clean: 
	rm -rf $(PWD)/third_party/install/
	rm -rf $(PWD)/third_party/boost_1_50_0/
	rm -rf $(PWD)/third_party/protobuf-2.5.0/
	rm -rf $(PWD)/third_party/zeromq-4.0.4
	rm -rf $(PWD)/third_party/libxml2-2.9.1
