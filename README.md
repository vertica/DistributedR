DistributedR
============

Distributed R is a scalable high-performance platform for the R language. It enables and accelerates large scale machine learning, statistical analysis, and graph processing.

The Distributed R platform exposes data structures, such as distributed arrays, to store data across a cluster. Arrays act as a single abstraction to efficiently express both machine learning algorithms, which primarily use matrix operations, and graph algorithms, which manipulate the graph’s adjacency matrix. In addition to distributed arrays, the platform also provides distributed data frames, lists and loops.

Using Distributed R constructs, data can be loaded in parallel from any data source. Distributed R also provides a parallel data loader from the Vertica database. Please see vRODBC repository.

Distributed R is delivered in a single, easy-to-install rpm file. The rpm installs the platform and all parallel algorithm R packages. Please download it from from www.vertica.com/marketplace.
