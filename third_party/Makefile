
boost: $(PWD)/third_party/install/lib/libboost_thread.a

$(PWD)/third_party/install/lib/libboost_thread.a:
	tar -zxf boost_1_50_0.tar.gz
	cd boost_1_50_0; ./bootstrap.sh --prefix=$(PWD)/third_party/install
	cd boost_1_50_0; ./b2 --without-python --without-mpi link=static,shared runtime-link=shared define=BOOST_LOG_DYN_LINK install

clean: 
	rm -rf $(PWD)/third_party/install/
	rm -rf $(PWD)/third_party/boost_1_50_0/
