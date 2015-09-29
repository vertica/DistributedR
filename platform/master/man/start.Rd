\name{distributedR_start}
\alias{distributedR_start}

\title{distributedR_start}
\description{Starts distributedR in single-machine or cluster mode. By
default, distributedR starts on the local machine with number of R executors
equal to the number of physical cores. For cluster mode, worker
details should be present in cluster_conf file. After successful distributedR_start call,
the master address and port number is displayed. This value is useful when a user wants to
reference log files in workers.}

\usage{
distributedR_start (executors = 0, mem=0, cluster_conf="", log=2)
}
\arguments{

  \item{executors}{ number of executors to launch at each worker. Default
    value is zero and will automatically start executors equal to the
    number of physical cores in each machine. This default value (zero) is
    ignored if Executors field is defined in cluster_conf file}

  \item{mem}{ allocated memory size of a worker node. The default
    value is zero and will use all the space present in /dev/shm on
    each worker. The default value (zero) is ignored if SharedMemory
    field is defined in cluster_conf file}

    \item{cluster_conf}{path to XML file describing configuration of
      master and workers. In the XML file, ServerInfo contains the IP address (hostname)
      and port number range of the master.  StartPortRange and EndPortRange
      specifies the port number range that will be used by a master.
      The runtime will select a random open port within the range.
      In a master, the port number range has to contain
      at least two open ports.

      Within the Workers field, information about multiple workers can be included.
      The worker field contains IP address (hostname) and port
      range. In a multi-worker cluster, loop-back address (127.0.0.1
      or localhost) should not be used as it prohibits inter-node
      communication. StartPortRange and EndPortRange specifies the
      port number range that will be used by a worker.  This range
      should contain at least 2*(number of workers)+1 open ports.

     The Executor field sets the number of R executors. If the value
     is 0, the runtime will automatically start as many executors as the
     number of physical cores in each machine.  SharedMemory determines the
     memory size assigned to each worker in MB. For the default value of 0, the runtime
     will automatically use the size of mounted shared memory region.
     Currently, memory constraints are not enforced.  

     An example configuration file is present in
     (/path/to/distributedR)/conf/cluster_conf.xml

     The distributedR_start function can fetch the cluster_conf file in multiple ways with
     the priority as follows:

     1. cluster_conf parameter specified in the distributedR_start function.

     2. fetch cluster_conf file from DR_CLUSTER_CONF environment variable. This should contain 
        complete path to the cluster configuration file.

     3. Fetch cluster_conf file from (DISTRIBUTEDR_HOME)/conf/cluster_conf.xml. Here 
        DISTRIBUTEDR_HOME is an environment variable holding path where 
        DistributedR is installed.

     4. Default path where DistributedR is installed.} 

   \item{log}{sets level of information generated in log files. 
    The four severity levels are: 0 (ERROR), 1 (WARNING), 2
    (INFOR) or 3 (DEBUG). 

    Severity level 0 (ERROR): only error messages are logged.    

    Severity level 1 (WARNING): error and warning messages are logged.    

    Severity level 2 (INFOR): additionally logs helpful
    messages. Set as default level.

    Severity level 3 (DEBUG): verbose logging. Mainly 
    applicable for debugging.}

   \item{inst}{deprecated. number of executors to launch at each worker. Default
    value is zero and will automatically start executors equal to the
    number of physical cores in each machine. This default value (zero) is
    ignored if Executors field is defined in cluster_conf file}

%  \item{presto_home}{ root path of Presto. If missing, the current
%  directory is set as home directory. Relative paths will be resolved
%  from presto_home. When running in cluster mode, the path has to be set
%  to the Presto home folder of remote machines.}

%  \item{workers}{ primarily used during debugging. If set to \code{FALSE},
%workers will have to be started manually.}

%\item{rmt_home}{primarily used during debugging.  Specifies location of
%  Presto home folder in remote machines. Used if remote home directory
%  is different the home directory on local (master) machine.}

%\item{rmt_uid}{primarily used during debugging.  In cluster mode,
%  specifies the user-id on remote machine if the user-id of master
%  (local machine) and worker nodes are different. User-id is related to
%  ssh login.}

}

\details{distributedR execution generates three types of log files:

- Master log (\code{R_master_<username>_<master_addr>.<master_port>.log}) : contains Master level log
messages on \code{foreach} functions received, task requests created and sent to Worker nodes for
execution etc. It is created in the /tmp/ folder of the Master node.

- Worker log (\code{R_worker_<username>_<master_addr>.<master_port>.log}) : contains
Worker level messages on requests received from Master node and other Worker nodes etc. It
is created in /tmp/ folder of each Worker node.

- Executor log (\code{R_executor_<username>_<master_addr>.<master_port>_<executorID>.log}) : Each executor in
each Worker node has its own log file. Normal execution log messages or Executor exceptions
(depending on severity level chosen by user) are logged here. It is created in /tmp/ folder of each
Worker node.

Review the Master and Executor Master logs for complete exception details if an Executor exception is encountered.

Argument 'inst' has been deprecated. Argument 'executors' should be used instead to      
set the number of executors to be launched per worker.}

\value{}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
  Learning and Graph Processing with Sparse Matrices. \emph{EuroSys'13},
  197--210.
  \item Homepage: http://www.vertica.com/distributedr
%  \item Mailing list: presto-dev@external.groups.hp.com
}
}

\author{HP Vertica Development Team}

\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{distributedR_shutdown}}, \code{\link{distributedR_status}}, \code{\link{distributedR_master_info}}
}

\examples{
\dontrun{
library(distributedR)
##Start worker process
distributedR_start()
distributedR_status()
distributedR_master_info()
distributedR_shutdown()
## Cluster mode. Assumes location of configuration file
conf.dir = getwd()
distributedR_start(cluster_conf=paste(conf.dir,"/conf/cluster_conf.xml",sep=""))
distributedR_shutdown()
}
}
