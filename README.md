# DistributedR

Distributed R is a scalable high-performance platform for the R language. It enables and accelerates large scale machine learning, statistical analysis, and graph processing.

The Distributed R platform exposes data structures, such as distributed arrays, to store data across a cluster. Arrays act as a single abstraction to efficiently express both machine learning algorithms, which primarily use matrix operations, and graph algorithms, which manipulate the graph’s adjacency matrix. In addition to distributed arrays, the platform also provides distributed data frames, lists and loops.

Using Distributed R constructs, data can be loaded in parallel from any data source. Distributed R also provides a parallel data loader from the Vertica database. Please see vRODBC repository.

## Installing from binaries

Distributed R is delivered in a single, easy-to-install tar file. The installation tool "distributedR_install" installs the platform and all parallel algorithm R packages. You can register and get the tar file [here](http://www.vertica.com/hp-vertica-products/hp-vertica-distributed-r/).

You can also get a Virtual Machine with everything installed [here](http://www.vertica.com/hp-vertica-products/hp-vertica-distributed-r/).

## Installing from source

1. Install dependencies:  
  * On Ubuntu:  

          $ sudo apt-get install -y make gcc g++ libxml2-dev rsync bison byacc flex

  * On CentOS:

          $ sudo yum install -y make gcc gcc-c++ libxml2-devel rsync bison byacc flex


2. Install R:
  * On Ubuntu:

          $ echo "deb http://cran.r-project.org//bin/linux/ubuntu trusty/" | sudo tee /etc/apt/sources.list.d/r.list
          $ sudo apt-get update
          $ sudo apt-get install -y --force-yes r-base-core

  * On CentOS:

          $ curl -O http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
          $ sudo rpm -i epel-release-latest-7.noarch.rpm
          $ sudo yum update
          $ sudo yum install R R-devel


3. Install R dependencies:

        $ sudo R  # to install globally
        R> install.packages(c('Rcpp','RInside','XML','randomForest','data.table'))

4. Compile and install Distributed R:
    
        $ R CMD INSTALL platform/executor
        $ R CMD INSTALL platform/master

5. Or directly from the R console:
        
        R> devtools::install_github('vertica/DistributedR',subdir='platform/executor')
        R> devtools::install_github('vertica/DistributedR',subdir='platform/master')

6. Open R and run an example:

        library(distributedR)
        distributedR_start()  # start DR
        distributedR_status()

        B <- darray(dim=c(9,9), blocks=c(3,3), sparse=FALSE) # create a darray
        foreach(i, 1:npartitions(B),
          init<-function(b = splits(B,i), index=i) {
          b <- matrix(index, nrow=nrow(b), ncol=ncol(b))
          update(b)
        })  # initialize it

        getpartition(B) # collect darray data

        distributedR_shutdown() # stop DR

## How to Contribute

You can help us in different ways:

1. Reporting [issues](https://github.com/vertica/distributedr/issues).
2. Contributing code and sending a [Pull Request](https://github.com/vertica/DistributedR/pulls).

In order to contribute the code base of this project, you must agree to the Developer Certificate of Origin (DCO) 1.1 for this project under GPLv2+:

    By making a contribution to this project, I certify that:
    
    (a) The contribution was created in whole or in part by me and I have the 
        right to submit it under the open source license indicated in the file; or
    (b) The contribution is based upon previous work that, to the best of my 
        knowledge, is covered under an appropriate open source license and I 
        have the right under that license to submit that work with modifications, 
        whether created in whole or in part by me, under the same open source 
        license (unless I am permitted to submit under a different license), 
        as indicated in the file; or
    (c) The contribution was provided directly to me by some other person who 
        certified (a), (b) or (c) and I have not modified it.
    (d) I understand and agree that this project and the contribution are public and
        that a record of the contribution (including all personal information I submit 
        with it, including my sign-off) is maintained indefinitely and may be 
        redistributed consistent with this project or the open source license(s) involved.

To indicate acceptance of the DCO you need to add a `Signed-off-by` line to every commit. E.g.:

    Signed-off-by: John Doe <john.doe@hisdomain.com>

To automatically add that line use the `-s` switch when running `git commit`:

    $ git commit -s
