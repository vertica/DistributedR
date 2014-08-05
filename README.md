DistributedR
============

Distributed R is a scalable high-performance platform for the R language. It enables and accelerates large scale machine learning, statistical analysis, and graph processing.

The Distributed R platform exposes data structures, such as distributed arrays, to store data across a cluster. Arrays act as a single abstraction to efficiently express both machine learning algorithms, which primarily use matrix operations, and graph algorithms, which manipulate the graph’s adjacency matrix. In addition to distributed arrays, the platform also provides distributed data frames, lists and loops.

Using Distributed R constructs, data can be loaded in parallel from any data source. Distributed R also provides a parallel data loader from the Vertica database. Please see vRODBC repository.

Distributed R is delivered in a single, easy-to-install rpm file. The rpm installs the platform and all parallel algorithm R packages. Please download it from from www.vertica.com/marketplace.

Contributor Agreement
============
In order to contribute the code base of this project, you must agree to this Developer Certificate of Origin for this project under GPLv2.
By making a contribution to this project, I certify that:
  1. The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or
  2. The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or
  3. The contribution was provided directly to me by some other person who certified (a), (b) or (c) and I have not modified it.
  4. I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.
