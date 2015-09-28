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


hpdsample <- function(data1, data2, nSampParts, sampRatio, trace = FALSE) {
  # Samples data from data1 (and when present, data2, maintaining the same
  # order of samples between them) into a new darray/dframe with nSampParts
  # partitions, each of which contains approximately sampRatio * nrow(data1)
  # elements. 
  #
  # Args:
  #   data1:            A darray/dframe to sample
  #   data2 (optional): A darray/dframe to sample along with data1. Must have
  #                     the same number of rows and partitions as data1. Each
  #                     row of data2 is assumed to correspond with the same row
  #                     of data1, and this correspondence will be maintained
  #                     when sampling.
  #   nSampParts:       The number of output partitions for the sample data
  #   sampRatio:        The percentage of data from data1/data2 to be sampled
  #                     in each output partition. E.g. if nrow(data1) = 100,
  #                     and sampRatio is 0.1, each output partition will
  #                     contain approximately 100 * 0.1 = 10 rows.
  #   trace:            A boolean that states whether or not to print the
  #                     progress of the distributed foreach calls
  # Returns:
  #   If only data1 is present, an output dobject of the same type as data1
  #   with nSampParts partitions, each of which contains sampRatio *
  #   nrow(data1) elements sampled randomly from data1. If data2 is present, a
  #   list with elements sdata1 (containing the data sampled from data1) and
  #   sdata2 (containing the data sampled from data2)

  if (!is.numeric(nSampParts) || length(nSampParts) != 1 || nSampParts <= 0 ||
      nSampParts %% 1 != 0) 
    stop("'nSampParts' must be a positive integer")

  if (!is.numeric(sampRatio) || length(sampRatio) != 1 || sampRatio <= 0) 
    stop("'sampRatio' must be a positive number")

  if (missing(data2)) .hpdsamplesingle(data1, nSampParts, sampRatio,  trace)
  else                .hpdsampledual(data1, data2, nSampParts, sampRatio, trace)
}

.hpdsamplesingle <- function(data, nSampParts, sampRatio, trace) {
  # Function used to perform sampling on a single darray/dframe
  
  if (!(is.darray(data) || is.dframe(data))) 
    stop("'data' must be a darray or dframe")

  ndcol     <- ncol(data)
  # Contains the amount of data to be sampled from each data partition into the
  # output partitions
  psizes    <- ceiling(partitionsize(data)[,1] * sampRatio)
  finalnspp <- sum(psizes) # Number of samples per partition
  ddim      <- c(finalnspp * nSampParts, ndcol)
  dblocks   <- c(finalnspp, ndcol)
  # Contains sample data to be returned
  sdata     <- if      (is.darray(data)) darray(dim = ddim, blocks = dblocks)
               else if (is.dframe(data)) dframe(dim = ddim, blocks = dblocks)

  # We need metadata about which of the columns are factors so that we can
  # keep track of factors locally within each partition.  If data is a dframe,
  # dLevels is an array of the global levels for each column of data that is a
  # factor. Otherwise, it is NULL and isn't used later
  # TODO ask about style
  dLevels <- if(is.dframe(data)) .getGlobalLevels(data)

  for (k in 1:npartitions(data)) { # For each partition dk of the input data
    # Iterate over the output partitions si in parallel and copy some portion of
    # the data from partition dk to sample partition si. This copied data will
    # be appended to the data copied from previous partitions
    foreach(i, 1:nSampParts, function(si = splits(sdata, i),
                                      dk = splits(data, k),
                                      psizes = psizes,
                                      dLevels = dLevels,
                                      k = k, 
                                      .coerceFactorsToGlobalLevels =
                                        .coerceFactorsToGlobalLevels) {
      # Create a random index to choose which data items to copy from dk to si
      idx <- sample(1:nrow(dk), psizes[k], replace = TRUE)
      # Based on the value of k we decide where to copy the data in splits si
      # since we don't want to overwrite data that was previously copied.
      # Effectively appends this data to previous data
      startRow <- if (k == 1) 1 else sum(psizes[1:(k - 1)]) + 1 
      endRow   <- startRow + length(idx) - 1

      # If dLevels is non-null, need to convert its factor columns to use the
      # same levels map as the global levels in case the levels aren't
      # consistent from partition to partition of 'data'
      if (!is.null(dLevels)) 
        .coerceFactorsToGlobalLevels(dk, dLevels)

      # Copy data items into sampled data
      si[startRow:endRow,] <- dk[idx,,drop = F]

      update(si)
    }, progress = trace, scheduler = 1)
  }
  colnames(sdata) <- colnames(data)
  sdata 
}

.hpdsampledual <- function(data1, data2, nSampParts, sampRatio, trace) {
  # Function to sample two darrays/dframes in conjunction
  
  if (!(is.darray(data1) || is.dframe(data1))) 
    stop("'data1' must be a darray or dframe")

  if (!(is.darray(data2) || is.dframe(data2))) 
    stop("'data2' must be a darray or dframe")

  if (nrow(data1) != nrow(data2)) 
    stop("'data1' and 'data2' must be the same length")

  if (npartitions(data1) != npartitions(data2)) 
    stop("'data1' and 'data2' must have the same number of partitions")

  ndcol1    <- ncol(data1)
  ndcol2    <- ncol(data2)
  # Contains the amount of data to be sampled from each data partition into the
  # output partitions
  psizes    <- ceiling(partitionsize(data1)[,1] * sampRatio)
  finalnspp <- sum(psizes) # Number of samples per partition 

  d1dim     <- c(finalnspp * nSampParts, ndcol1)
  d1blocks  <- c(finalnspp, ndcol1)
  d2dim     <- c(finalnspp * nSampParts, ndcol2)
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
    foreach(i, 1:nSampParts, function(si1 = splits(sdata1, i),
                                      si2 = splits(sdata2, i),
                                      dk1 = splits(data1, k),
                                      dk2 = splits(data2, k),
                                      psizes = psizes,
                                      k = k, 
                                      d1Levels = d1Levels,
                                      d2Levels = d2Levels,
                                      .coerceFactorsToGlobalLevels =
                                        .coerceFactorsToGlobalLevels) {
      if (nrow(dk1) != nrow(dk2)) 
        stop("nrow mismatch between data1 split and data2 split in foreach")

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
      if (is.data.frame(dk1))
        .coerceFactorsToGlobalLevels(dk1, d1Levels)

      if (is.data.frame(dk2))
        .coerceFactorsToGlobalLevels(dk2, d2Levels)

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
# Value: 
#   An object returned by levels.dframe  

  dfactorCols <- darray(npartition = 1)
  # Find out which columns of data are factors by looking at the first
  # partition
  foreach(i, 1:1, function(di = splits(data, i),
                           fs = splits(dfactorCols, i)) {
    fs <- as.matrix(which(sapply(di, is.factor)))
    update(fs)
  }, progress = F)

  factorCols <- getpartition(dfactorCols)
  # Finds the global levels of these columns (if there are any)
  dLevels <- if (length(factorCols) > 1) {
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
#   
  for (j in seq_along(dLevels$columns)) {
    c <- dLevels$columns[j]
    df[,c] <- factor(df[,c], levels = dLevels$Levels[[j]])
  }
}
