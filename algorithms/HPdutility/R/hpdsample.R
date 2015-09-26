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
  
  if (!(is.darray(data) || is.dframe(data))) 
    stop("'data' must be a darray or dframe")

  ndrow     <- nrow(data)
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
  # keep track of factors locally within each partition: 
  # These should only have values if data1 is a dframe. Otherwise they are NULL
  dp1        <- if (is.dframe(data)) getpartition(data, 1) 
  factorCols <- if (is.dframe(data)) which(sapply(dp1, is.factor))  
  dLevels    <- if (is.dframe(data)) lapply(dp1[,factorCols], levels) 

  for (k in 1:npartitions(data)) { # For each partition dk of the input data
    # Iterate over the output partitions si in parallel and copy some portion of
    # the data from partition dk to sample partition si. This copied data will
    # be appended to the data copied from previous partitions
    foreach(i, 1:nSampParts, function(si = splits(sdata, i),
                                      dk = splits(data, k),
                                      psizes = psizes,
                                      factorCols = factorCols,
                                      dLevels = dLevels,
                                      k = k) {
      # When vars are factors, must convert them to chars
#      if (is.data.frame(dk)) {
#        factorCols      <- which(sapply(dk, is.factor))
#        dk[,factorCols] <- as.character(dk[,factorCols])
#      }
      if (k == 1 && is.data.frame(si)) {
        for (ci in seq_along(factorCols)) {
          c <- factorCols[ci]
          si[,c] <- as.factor(si[,c])
          levels(si[,c]) <- dLevels[[ci]]
        }
      }
      # Create a random index to choose which data items to copy from dk to si
      idx <- sample(1:nrow(dk), psizes[k], replace = TRUE)
      # Based on the value of k we decide where to copy the data in splits si1
      # and si2, since we don't want to overwrite data that was previously
      # copied
      startRow <- if (k == 1) 1 else sum(psizes[1:(k - 1)]) + 1 
      endRow   <- startRow + length(idx) - 1
      si[startRow:endRow,] <- dk[idx,] 
      update(si)
    }, progress = trace, scheduler = 1)
  }
  colnames(sdata) <- colnames(data)
  sdata 
}

.hpdsampledual <- function(data1, data2, nSampParts, sampRatio, trace) {
  
  if (!(is.darray(data1) || is.dframe(data1))) 
    stop("'data1' must be a darray or dframe")

  if (!(is.darray(data2) || is.dframe(data2))) 
    stop("'data2' must be a darray or dframe")

  if (nrow(data1) != nrow(data2)) 
    stop("'data1' and 'data2' must be the same length")

  if (npartitions(data1) != npartitions(data2)) 
    stop("'data1' and 'data2' must have the same number of partitions")

  ndrow     <- nrow(data1)
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
  # These should only have values if data1 is a dframe. Otherwise they are NULL
  d1p1        <- if (is.dframe(data1)) getpartition(data1, 1) 
  factorCols1 <- if (is.dframe(data1)) which(sapply(d1p1, is.factor))  
  d1Levels    <- if (is.dframe(data1)) lapply(d1p1[,factorCols1], levels) 

  # These should only have values if data2 is a dframe. Otherwise they are NULL
  d2p1        <- if (is.dframe(data2)) getpartition(data2, 1) 
  factorCols2 <- if (is.dframe(data2)) which(sapply(d2p1, is.factor)) 
  d2Levels    <- if (is.dframe(data2)) lapply(d2p1[,factorCols2], levels)

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
                                      factorCols1 = factorCols1,
                                      factorCols2 = factorCols2,
                                      d1Levels = d1Levels,
                                      d2Levels = d2Levels, i = i) {
      if (nrow(dk1) != nrow(dk2)) 
        stop("nrow mismatch between data1 split and data2 split in foreach")

#      # When vars are factors, must convert them to chars
#      if (is.data.frame(dk1)) {
#        #factorCols1       <- which(sapply(dk1, is.factor))
#        dk1[,factorCols1] <- as.character(dk1[,factorCols1])
#      }
#      if (is.data.frame(dk2)) {
#        #factorCols2       <- which(sapply(dk2, is.factor))
#        dk2[,factorCols2] <- as.character(dk2[,factorCols2])
#      }
      if (k == 1) {
        if (is.data.frame(si1)) {
          for (ci in seq_along(factorCols1)) {
            c <- factorCols1[ci]
            si1[,c] <- as.factor(si1[,c])
            levels(si1[,c]) <- d1Levels[[ci]]
          }
        }
        if (is.data.frame(si2)) {
          for (ci in seq_along(factorCols2)) {
            c <- factorCols2[ci]
            si2[,c] <- as.factor(si2[,c])
            levels(si2[,c]) <- d2Levels[[ci]]
          }
        }
      }


      # Create a random index to choose which data items to copy from dk to si
      idx <- sample(1:nrow(dk1), psizes[k], replace = TRUE)
      # Based on the value of k we decide where to copy the data in splits si1
      # and si2, since we don't want to overwrite data that was previously
      # copied
      startRow <- if (k == 1) 1 else sum(psizes[1:(k - 1)]) + 1 
      endRow   <- startRow + length(idx) - 1
      si1[startRow:endRow,] <- dk1[idx,, drop = F]
      si2[startRow:endRow,] <- dk2[idx,, drop = F]

      update(si1)
      update(si2)
    }, progress = trace, scheduler = 1)
  }

  # Ensure output data has same column names as corresponding input data
  colnames(sdata1) <- colnames(data1)
  colnames(sdata2) <- colnames(data2)
  list(sdata1 = sdata1, sdata2 = sdata2)
}

