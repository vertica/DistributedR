#####################################################################################
# Copyright [2013] Hewlett-Packard Development Company, L.P.                        # 
#                                                                                   #
# This program is free software; you can redistribute it and/or                     #
# modify it under the terms of the GNU General Public License                       #
# as published by the Free Software Foundation; either version 2                    #
# of the License, or (at your option) any later version.                            #
#                                                                                   #
# This program is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                    #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                      #
# GNU General Public License for more details.                                      #
#                                                                                   #
# You should have received a copy of the GNU General Public License                 #
# along with this program; if not, write to the Free Software                       #
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.    #
#####################################################################################


hpdsample <- function(data1, data2, nSamplePartitions, samplingRatio, trace = FALSE) {
  # Samples data from data1 (and when present, data2, maintaining the same
  # order of samples between them) into a new darray/dframe with nSamplePartitions
  # partitions, each of which contains approximately samplingRatio * nrow(data1)
  # elements. 
  #
  # Args:
  #   data1:            A darray/dframe to sample
  #   data2 (optional): A darray/dframe to sample along with data1. Must have
  #                     the same number of rows and partitions as data1. Each
  #                     row of data2 is assumed to correspond with the same row
  #                     of data1, and this correspondence will be maintained
  #                     when sampling.
  #   nSamplePartitions:       The number of output partitions for the sample data
  #   samplingRatio:        The percentage of data from data1/data2 to be sampled
  #                     in each output partition. E.g. if nrow(data1) = 100,
  #                     and samplingRatio is 0.1, each output partition will
  #                     contain approximately 100 * 0.1 = 10 rows.
  #   trace:            A boolean that states whether or not to print the
  #                     progress of the distributed foreach calls
  # Returns:
  #   If only data1 is present, an output dobject of the same type as data1
  #   with nSamplePartitions partitions, each of which contains samplingRatio *
  #   nrow(data1) elements sampled randomly from data1. If data2 is present, a
  #   list with elements sdata1 (containing the data sampled from data1) and
  #   sdata2 (containing the data sampled from data2)

  if (!is.numeric(nSamplePartitions) || length(nSamplePartitions) != 1 || nSamplePartitions <= 0 ||
      nSamplePartitions %% 1 != 0) 
    stop("'nSamplePartitions' must be a positive integer")

  if (!is.numeric(samplingRatio) || length(samplingRatio) != 1 || 
      samplingRatio <= 0) 
    stop("'samplingRatio' must be a positive number")
  
  if (!(is.darray(data1) || is.dframe(data1))) 
    stop("'data1' must be a darray or dframe")

  if (!.isRowPartitioned(data1))
    stop("'data1' must be partitioned row-wise")

  if (missing(data2)) {
    # Sample only data1
    .hpdsamplesingle(data1, nSamplePartitions, samplingRatio,  trace)
  } else {
    # Further argument checks, then sample both data1 and data2 in conjunction
    if (!(is.darray(data2) || is.dframe(data2))) 
      stop("'data2' must be a darray or dframe")

    if (!.isRowPartitioned(data2))
      stop("'data2' must be partitioned row-wise")

    partsize1 <- partitionsize(data1)
    partsize2 <- partitionsize(data2)

    if (!all(dim(partsize1) == dim(partsize2)) || !all(partsize1[,1] ==
                                                       partsize2[,1]))
      stop("'data1' and 'data2' must have the same number of partitions, and corresponding partitions must have the same size")

    .hpdsampledual(data1, data2, nSamplePartitions, samplingRatio, trace)
  }
}

.hpdsamplesingle <- function(data, nSamplePartitions, samplingRatio, trace) {
  # Function used to perform sampling on a single darray/dframe

  ndcol     <- ncol(data)
  # Contains the amount of data to be sampled from each data partition into the
  # output partitions
  psizes    <- ceiling(partitionsize(data)[,1] * samplingRatio)
  finalnspp <- sum(psizes) # Number of samples per partition
  ddim      <- c(finalnspp * nSamplePartitions, ndcol)
  dblocks   <- c(finalnspp, ndcol)
  # Contains sample data to be returned
  sdata     <- if      (is.darray(data)) darray(dim = ddim, blocks = dblocks)
               else if (is.dframe(data)) dframe(dim = ddim, blocks = dblocks)

  # We need metadata about which of the columns are factors so that we can
  # keep track of factors locally within each partition.  If data is a dframe,
  # dLevels is an array of the global levels for each column of data that is a
  # factor. Otherwise, it is NULL and isn't used later
  dLevels <- if(is.dframe(data)) .getGlobalLevels(data)

  for (k in 1:npartitions(data)) { # For each partition dk of the input data
    # Iterate over the output partitions si in parallel and copy some portion of
    # the data from partition dk to sample partition si. This copied data will
    # be appended to the data copied from previous partitions
    foreach(i, 1:nSamplePartitions, function(si = splits(sdata, i),
                                      dk = splits(data, k),
                                      psizes = psizes,
                                      dLevels = dLevels,
                                      k = k, 
                                      .coerceFactorsToGlobalLevels =
                                        .coerceFactorsToGlobalLevels, 
                                      .copyClassStructure = .copyClassStructure) {
      # Create a random index to choose which data items to copy from dk to si
      idx <- sample(1:nrow(dk), psizes[k], replace = TRUE)

      # Based on the value of k we decide where to copy the data in splits si
      # since we don't want to overwrite data that was previously copied.
      # Effectively appends this data to previous data
      startRow <- if (k == 1) 1 else sum(psizes[1:(k - 1)]) + 1 
      endRow   <- startRow + length(idx) - 1

      if (is.data.frame(dk)) {
        # If dLevels is non-null, need to convert its factor columns to use the
        # same levels map as the global levels in case the levels aren't
        # consistent from partition to partition of 'data'
        if (!is.null(dLevels)) 
          dk <- .coerceFactorsToGlobalLevels(dk, dLevels)

        # First iteration, no data has been copied yet. Ensure all the types of
        # the sample data are the same as the originals
        if (k == 1) 
          si <- .copyClassStructure(dk, si, dLevels)
      }

      # Copy data items into sampled data
      si[startRow:endRow,] <- dk[idx,,drop = F]

      update(si)
    }, progress = trace, scheduler = 1)
  }
  colnames(sdata) <- colnames(data)
  sdata 
}

.hpdsampledual <- function(data1, data2, nSamplePartitions, samplingRatio, trace) {
  # Function to sample two darrays/dframes in conjunction
  
  ndcol1    <- ncol(data1)
  ndcol2    <- ncol(data2)
  # Contains the amount of data to be sampled from each data partition into the
  # output partitions
  psizes    <- ceiling(partitionsize(data1)[,1] * samplingRatio)
  finalnspp <- sum(psizes) # Number of samples per partition 

  d1dim     <- c(finalnspp * nSamplePartitions, ndcol1)
  d1blocks  <- c(finalnspp, ndcol1)
  d2dim     <- c(finalnspp * nSamplePartitions, ndcol2)
  d2blocks  <- c(finalnspp, ndcol2)
  # dobject to contain the sampled data from data1
  sdata1 <- if (is.darray(data1))      darray(dim = d1dim, blocks = d1blocks) 
            else if (is.dframe(data1)) dframe(dim = d1dim, blocks = d1blocks) 
  # dobject to contain the sampled data from data2 
  sdata2 <- if (is.darray(data2))      darray(dim = d2dim, blocks = d2blocks)
            else if (is.dframe(data2)) dframe(dim = d2dim, blocks = d2blocks)

  # We need metadata about which of the columns are factors so that we can
  # keep track of factors locally within each partition: 
  d1Levels <- if(is.dframe(data1)) .getGlobalLevels(data1)
  d2Levels <- if(is.dframe(data2)) .getGlobalLevels(data2)

  for (k in 1:npartitions(data1)) { # For each partition dk of the input data
    # Iterate over the output partitions si in parallel and copy some portion of
    # the data from partition dk to sample partition si. This copied data will
    # be appended to the data copied from previous partitions
    foreach(i, 1:nSamplePartitions, function(si1 = splits(sdata1, i),
                                      si2 = splits(sdata2, i),
                                      dk1 = splits(data1, k),
                                      dk2 = splits(data2, k),
                                      psizes = psizes,
                                      k = k, 
                                      d1Levels = d1Levels,
                                      d2Levels = d2Levels,
                                      .coerceFactorsToGlobalLevels =
                                        .coerceFactorsToGlobalLevels,
                                      .copyClassStructure = .copyClassStructure) {
      # Create a random index to choose which data items to copy from dk to si
      idx <- sample(1:nrow(dk1), psizes[k], replace = TRUE)

      # Based on the value of k we decide where to copy the data in splits si1
      # and si2, since we don't want to overwrite data that was previously
      # copied. Effectively appends this to the previous sampled data
      startRow <- if (k == 1) 1 else sum(psizes[1:(k - 1)]) + 1 
      endRow   <- startRow + length(idx) - 1

      # If dLevels1/2 are non-null, need to convert its factor columns to use the
      # same levels map as the global levels in case the levels aren't
      # consistent from partition to partition of 'data'
      if (is.data.frame(dk1)) {
        if (!is.null(d1Levels))
          dk1 <- .coerceFactorsToGlobalLevels(dk1, d1Levels)

        # First iteration, no data has been copied yet. Ensure all the types
        # of the sample data are the same as the originals
        if (k == 1)  
          si1 <- .copyClassStructure(dk1, si1, d1Levels)
      }

      if (is.data.frame(dk2)) {
        if (!is.null(d2Levels))
          dk2 <- .coerceFactorsToGlobalLevels(dk2, d2Levels)

        # First iteration, no data has been copied yet. Ensure all the types of
        # the sample data are the same as the originals
        if (k == 1)            
          si2 <- .copyClassStructure(dk2, si2, d2Levels)
      }

      # Copy data items into sampled data
      si1[startRow:endRow,] <- dk1[idx,,drop = F]
      si2[startRow:endRow,] <- dk2[idx,,drop = F]

      update(si1)
      update(si2)
    }, progress = trace, scheduler = 1)
  }

  # Ensure output data has same column names as corresponding input data
  colnames(sdata1) <- colnames(data1)
  colnames(sdata2) <- colnames(data2)
  list(sdata1 = sdata1, sdata2 = sdata2)
}

.getGlobalLevels <- function(data) {
  # Finds the columns of data that are factors and returns the global levels of
  # each column
  # 
  # Args:
  #   data: A dframe object
  # 
  # Returns: 
  #   An object returned by levels.dframe  

  dfactorCols <- darray(npartition = 1)
  # Find out which columns of data are factors by looking at the first
  # partition
  foreach(i, 1:1, function(di = splits(data, i),
                           fs = splits(dfactorCols, i)) {
    fs <- as.matrix(which(sapply(di, is.factor)))
    update(fs)
  }, progress = F)

  factorCols <- as.vector(getpartition(dfactorCols))
  # Finds the global levels of these columns (if there are any)
  dLevels <- if (length(factorCols) > 0) {
    levels.dframe(data, colID = factorCols)
  } else NULL
  return (dLevels)
}

.coerceFactorsToGlobalLevels <- function(df, dLevels) {
  # Converts the columns of df corresponding to factors into new factors using
  # the levels specified in dLevels
  #
  # Args:
  #   df: A data frame
  #   dLevels: An object returned by levels.dframe containing the global
  #            levels for a dframe
  # Returns:
  #   The modified df
     
  for (j in seq_along(dLevels$columns)) {
    c <- dLevels$columns[j]
    df[,c] <- factor(df[,c], levels = dLevels$Levels[[j]])
  }
  return (df)
}

.copyClassStructure <- function(dk, si, dLevels) {
  # Coerces the columns of si to have the same class types as dk
  #
  # Args:
  #   dk: A data frame
  #   si: A data frame (presumed full of NAs)
  #   dLevels: An object returned by levels.dframe, or NULL
  #
  # Returns:
  #  The modified si

  # For each non-factor class, convert class type of columns of si to be the
  # same as class type of dk. Right now ordered factors aren't supported, which
  # is why we handle factors separately
  nonFactorCols <- if (is.null(dLevels)) 1:ncol(si) 
                   else (1:ncol(si))[-dLevels$columns]
  for (ci in nonFactorCols) {
    si[,ci] <- as(si[,ci], class(dk[,ci]))
  }

  if (!is.null(dLevels)) {
    # For every factor column in dk, make sure that column in si is also a
    # factor with the same levels
    for (ci in seq_along(dLevels$columns)) {
      c <- dLevels$columns[ci]
      si[,c] <- as.factor(si[,c])
      levels(si[,c]) <- dLevels$Levels[[ci]]
    }
  }
  return (si)
}

.isRowPartitioned <- function(d) {
  # Tells whether d is row-wise partitioned or not
  #
  # Args:
  #   d: A darray/dframe
  #
  # Returns:
  #   A boolean that is TRUE if d is row-wise partitioned and FALSE otherwise
  return (all(partitionsize(d)[,2] == ncol(d)))
}
