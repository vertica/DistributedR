\name{hpdsample}
\alias{hpdsample}
\title{A sampling algorithm for darrays/dframes}
\description{
    Samples data from data1 (and when present, data2, maintaining the same
    order of samples between them) into a new darray/dframe with nSamplePartitions
    partitions, each of which contains approximately samplingRatio * nrow(data1)
    elements.
}
\usage{
hpdsample(data1, data2, nSamplePartitions, samplingRatio, trace = FALSE)
}

\arguments{
  \item{data1}{
      a darray/dframe to sample. It must be row-wise partitioned.
  }
  \item{data2}{
      (optional) a darray/dframe to sample along with data1. Must have the same number 
      of rows and partitions as data1. It must also be row-wise partitioned. Each row of data2 is assumed to correspond with the same row of data1, and this correspondence will be maintained when sampling.
  }
  \item{nSamplePartitions}{
      the number of output partitions for the sample data
  }
  \item{samplingRatio}{
      the fraction of data from data1/data2 to be sampled
      in each output partition. Must be a positive number. E.g. if 
      nrow(data1) = 100, and samplingRatio is 0.1, each output partition 
      will contain approximately 100 * 0.1 = 10 rows.
  }
  \item{trace}{
      a boolean that states whether or not to print the
      progress of the algorithm  
  }
}

\details{
This function is used to sample (with replacement) one or two darrays/dframes into new darrays/dframes. The type of the output object is the same as that of the input object, and when possible, the class types of columns are maintained (one exception is ordered factors; these will be converted to ordinary factors in the output). It serves any combination of darray/dframe objects for data1 and data2 so long as they have the same number of rows and partitions (and the same number of rows in each
partition). 
    
The output array will have a number of partitions equal to nSamplePartitions, each of which will contain approximately samplingRatio * nrow(data1) rows. When both data1 and data2 are provided, they are sampled according to the same index; i.e. if row 5 from data1 is placed on the 10th row of the third output partition, row 5 from data2 will also be placed in the same position. Sampling is performed such that each output partition will obtain approximately an equal percentage (samplingRatio * 100
percent) of the data from each input partition. So if samplingRatio is 0.1, then each output partition consists of about 10\% of each of the input data partitions.
}

\value{
  \item{sdata}{
      If only data1 is present, an output dobject of the same type as data1
      with nSamplePartitions partitions, each of which contains samplingRatio *
      nrow(data1) elements sampled randomly from data1. If data2 is present, a
      list with elements sdata1 (containing the data sampled from data1) and
      sdata2 (containing the data sampled from data2)
  }
}

\author{
    HP Vertica Analytics Team
}

\examples{
 \dontrun{

    library(HPdutility)

    distributedR_start()

    d1 <- as.darray(as.matrix(1:1001))
    d2 <- as.dframe(data.frame(a = rep('c', 1001), 
                               b = 1:1001, 
                               c = runif(1001), 
                               d = c(rep('a', 501), rep('b', 500))))

    # sd will be a list of two elements. The first element, sdata1, will be a
    # darray with 4 partitions, each of which contains about 1001*0.1 elements 
    # sampled from d1. The second, sdata2, will be a dframe with 4 partitions, 
    # each of which contains about 1001*0.1 elements.
    sd <- hpdsample(d1, d2, nSampParts = 4, samplingRatio = 0.1)
    sda1 <- sd$sdata1
    sdf2 <- sd$sdata2
 }
}

\keyword{ Database }
\keyword{ Distributed R }
