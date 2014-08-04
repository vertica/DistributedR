\name{distributedR_start}
\alias{distributedR_start}

\title{distributedR_start}
\description{Starts distributedR in single-machine or cluster mode. By
default, distributedR starts on the local machine with number of R instances
equal to one less than the number of CPU cores. For cluster mode, worker
details should be present in cluster_conf file. After successful distributedR_start call,
the master address and port number is displayed. This value is useful when a user wants to
reference log files in workers.}

\usage{
distributedR_start (inst = 0, mem=0, cluster_conf="", log=3)
%presto_home = "", workers=TRUE, rmt_home="", rmt_uid = "")
}
\arguments{

  \item{inst}{ number of R instances to launch at each
    worker. Setting this to zero will automatically start R instances one less than
    the number of CPU cores in each machine. This value is ignored if
    Executors field is defined in cluster_conf file}

  \item{mem}{ allocated memory size of a worker node. This value is
    ignored if SharedMemory field is defined in cluster_conf file}

    \item{cluster_conf}{path to XML file describing configuration of
      master and workers. File should contain hostname (or IP address) and
      port number of master and workers. In Workers field, 
      Executors field determines the number of executors in a worker,
      and SharedMemory determines the size of shared memory. 
      Executors and SharedMemory fileds are optional, and default value (0) will be used
      unless inst or mem are specified in the arguments.
      Example configuration file is in $distributedR_HOME /conf/cluster_conf.xml}

   \item{log}{sets level of information generated in log files. 
    The four severity levels are: 0 (ERROR), 1 (WARNING), 2
    (INFOR) or 3 (DEBUG). 

    Severity level 0 (ERROR): only error messages are logged.    

    Severity level 1 (WARNING): error and warning messages are logged.    

    Severity level 2 (INFOR): additionally logs helpful
    messages. Set as default level.

    Severity level 3 (DEBUG): verbose logging. Mainly 
    applicable for debugging.}

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

- Master log file (\code{R_master_<username>_<master_address>.<master_port_number>.log}) : contains Master level log
messages on \code{foreach} functions received, task requests created and sent to Worker nodes for
execution etc. It is created in the /tmp/ folder of the Master node.

- Worker log file (\code{R_worker_<username>_<master_address>.<master_port_number>.log}) : contains
Worker level messages on requests received from Master node and other Worker nodes etc. It
is created in /tmp/ folder of each Worker node.

- Executor log file (\code{R_executor_<username>_<master_address>.<master_port_number>_<executorID>.log}) : Each executor in
each Worker node has its own log file. Normal execution log messages or Executor exceptions
(depending on severity level chosen by user) are logged here. It is created in /tmp/ folder of each
Worker node.

Review the Master and Executor Master logs for complete exception details if an Executor exception is encountered.}

\value{}

\references{
  \itemize{
    \item Venkataraman, S., Bodzsar, E., Roy, I.,
  AuYoung, A., and Schreiber, R. (2013) Presto: Distributed Machine
  Learning and Graph Processing with Sparse Matrices. \emph{EuroSys'13},
  197--210.
  \item Homepage: http://www.hpl.hp.com/research/distributedr.htm
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
