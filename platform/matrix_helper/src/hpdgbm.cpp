/*
 * CPP functions called by R in hpdgbm.R
 * Author: Hao Peng (penghao888@gmail.com)
 */

#include <cstdlib>
#include <math.h>
#include <queue>

using namespace std;
#define DEBUG
#define EPS 0 // a threshold for deciding whether an improvement is significant

extern "C" {
#include <R.h>
#include <Rinternals.h>
#include "hpdtree.h"
#include "hphistogram.h"

/************************ shared **************************/
/*
 * Compute the sufficient statistics to initalize outputs
 */
SEXP hpComputeInitFStats(SEXP Rdistribution, // loss function
                         SEXP Ry, // original responses
                         // output variables
                         SEXP RinitFStats) // sufficient statistics
{
   
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double* y = REAL(Ry);
   double* initFStats = REAL(RinitFStats);
   int nSamples = length(Ry);

   double sum = 0.0;
   if (strcmp(distribution,"gaussian")==0)
   {
      for (int iSample =0; iSample < nSamples; iSample++)
         sum += y[iSample];
      initFStats[0] = sum;
      initFStats[1] = nSamples;
   }
   else 
   {
      error("Distribution is not defined.");
   }
   // return nothing
   return R_NilValue;
}
SEXP hpComputeInitF(SEXP Rdistribution, // loss function
                    SEXP RinitFStats, // sufficient statistics
                    SEXP RnPartitions) // number of partitions
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double* initFStats = REAL(RinitFStats);
   int nPartitions = INTEGER(RnPartitions)[0];

   double initF = 0.0;
   if (strcmp(distribution,"gaussian")==0)
   {
      double sum = 0.0;
      double count = 0.0;
      for (int iPartition = 0; iPartition < nPartitions; iPartition++)
      {
         sum += initFStats[iPartition];
         count += initFStats[nPartitions+iPartition];
      }

      initF = sum / count;
   }
   else 
   {
      error("Distribution is not defined.");
   }

   // allocate memory for return value
   SEXP RinitF = R_NilValue;
   PROTECT(RinitF = allocVector(REALSXP,1));
   REAL(RinitF)[0] = initF;
   UNPROTECT(1);

   return RinitF;
}
/*
 * compute the gradients
 */
void hpComputeGradient(char const* distribution, // loss function
                       int nSamples, // number of samples
                       double *y, // original responses
                       double *outputs, // current outputs
                       // output variables
                       double *gradients) // the gradients
{
   if (strcmp(distribution,"gaussian") == 0)
   {
      for (int iSample = 0; iSample < nSamples; iSample++)
      {
         gradients[iSample] = y[iSample]-outputs[iSample];
      }
   }
   else 
   {
      error("Distribution is not defined.");
   }
}
/*
 * return a node id of which the sample belongs to
 */
inline int hpFindNodeForSample(hpDecisionTree &tree, // the pointer to the tree structure
                               double* x, // all features values 
                               int nSamples, // total number of samples
                               int iSample)  // which sample to compute
{
   // inintial set the sample to root
   int iNode = 0;

   // Loop if iNode is not a terminal
   while(!IS_TERMINAL_NODE(tree, iNode))
   {
      int offset = tree.nodes[iNode].splitFeature*nSamples+iSample;
      double xValue = x[offset];

      if (ISNA(xValue)) // go missing
         iNode = tree.nodes[iNode].missingChild;
      else if (xValue < tree.nodes[iNode].splitValue) // go left
         iNode = tree.nodes[iNode].leftChild;
      else // go right
         iNode = tree.nodes[iNode].rightChild;
      #ifdef DEBUG
      if (iNode == 0)
         error("There is something wrong with the tree.");
      #endif
   }

   return iNode;
}
/*
 * compute incrementals and update outputs
 * compute new gradients
 */
SEXP hpEnsembleATree(SEXP Rdistribution, // loss function
                     SEXP Rtree, // tree to add
                     SEXP Rx, // features
                     SEXP Ry, // orginal responses
                     // output variables
                     SEXP Routputs, // current outputs
                     SEXP Rgradients) // new gradients
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));

   double* x = REAL(Rx);
   int nSamples = nrows(Rx);
   double* y = REAL(Ry);

   double* outputs = REAL(Routputs);
   double* gradients = REAL(Rgradients);

   // update the current outputs if there is a tree chosen last iteration
   if (!isNull(Rtree))
   {
      hpDecisionTree tree = hpGetDecisionTree(Rtree);

      // compute incrementals and update outputs
      for (int iSample = 0; iSample < nSamples; iSample++)
      {
         // find which node the sample belongs to
         int iNode = hpFindNodeForSample(tree, x, nSamples, iSample);
         // now we arrive at a terminal node
         // update the outputs
         outputs[iSample] += tree.nodes[iNode].prediction;
      }
   }

   // compute the gradients
   hpComputeGradient(distribution,nSamples,y,outputs,gradients);

   // return nothing
   return R_NilValue;
}
/*
 * Computing sufficient statistics for fitting the best constant
 * The suffcient statistics differ among different loss functions, so
 * RterminalStatus has different meanings:
 * 1. For "gaussian" distribution:
 *    The (i*2*max.n.terminals+j)'th element corresponds to the local sum 
 *    for the j'th terminal of the tree from i'th partition.
 *    The (i*2*max.n.terminals+k+1)'th element corresponds to the local 
 *    count for the j'th terminal of the tree from i'th partition
 */
SEXP hpPrepareForFitting(SEXP Rdistribution, // loss function
                         SEXP Rx, // feature values
                         SEXP Ry, // original responses
                         SEXP Routputs, // current outputs
                         SEXP RnTrees, // number of trees
                         SEXP Rtrees, // all trees. one tree per partition.
                         //output variables
                         SEXP RterminalStats) // suffcient statistic for computing best fitting for data on this partition
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double* x = REAL(Rx);
   double* y = REAL(Ry);
   double* outputs = REAL(Routputs);
   int nSamples = length(Ry);
   int nTrees = INTEGER(RnTrees)[0];
   hpDecisionTree *trees = (hpDecisionTree*) malloc(sizeof(hpDecisionTree)*nTrees);
   hpGetDecisionTrees(Rtrees, nTrees, trees);
   double* terminalStats = REAL(RterminalStats);
   int maxNNodes = trees[0].maxNNodes;
   int maxNTerminals =  (2*maxNNodes+1)/3;


   // reset terminalStats
   memset(terminalStats, 0, sizeof(double)*length(RterminalStats));

   // allocate mermory for node to terminal mapping
   int *nodeToTerminal = (int*) malloc(sizeof(int)*maxNNodes*nTrees);

   // compute the node to terminal Mapping
   for (int iTree = 0; iTree < nTrees; iTree++)
      hpComputeNodeToTerminal(trees[iTree],
                              nodeToTerminal+iTree*maxNNodes);

   if (strcmp(distribution,"gaussian")==0)
   {
      #ifdef DEBUG
      if (maxNTerminals*2*nTrees != length(RterminalStats))
         error("length of RterminalStats is wrong");
      #endif

      // iterate over each tree and compute the statistics
      for (int iTree = 0; iTree < nTrees; iTree++)
      {
         int offset = iTree*2*maxNTerminals;

         hpDecisionTree &tree = trees[iTree];

         for (int iSample = 0; iSample < nSamples; iSample++)
         {
            // assign sample to node
            int iNode = hpFindNodeForSample(tree, x, nSamples, iSample);
            // which terminal it belongs to
            int iTerminal = nodeToTerminal[iTree*maxNNodes+iNode];
            // update the terminal statistics;
            terminalStats[offset+2*iTerminal] += y[iSample]-outputs[iSample];   // sum
            terminalStats[offset+2*iTerminal+1] += 1; // count
         }
      }
   }
   else
   {
      error("Distribution is not defined.");
   }

   // free memories
   free(nodeToTerminal);
   free(trees);

   // return nothing
   return R_NilValue;
}
/*
 * fit the best constant according to the loss function
 * and statatics gathered from all partitions
 */
SEXP hpFitBestConstant(SEXP Rdistribution, // loss function
                       SEXP RiPartition, // current partition
                       SEXP RnPartitions, // number of paritions
                       SEXP RnTrees, // number of trees in total
                       SEXP RterminalStats, //sufficient statistics for fitting the best constant computed from all paritions
                       // input and output varibles
                       SEXP Rtree) // the tree built on this partition
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   int maxNNodes = tree.maxNNodes;
   int maxNTerminals = (2*maxNNodes+1)/3;
   int iPartition = INTEGER(RiPartition)[0]-1; // R is 1 based
   int nPartitions = INTEGER(RnPartitions)[0];
   int nTrees = INTEGER(RnTrees)[0];
   double *terminalStats = REAL(RterminalStats);

   // allocate mermory for node to terminal mapping for this tree only
   int *nodeToTerminal = (int*) malloc(sizeof(int)*maxNNodes);

   // compute the node to terminal Mapping
   hpComputeNodeToTerminal(tree, nodeToTerminal);

   if (strcmp(distribution,"gaussian")==0)
   {
      for (int iNode = 0; iNode < *tree.nNodesP; iNode++)
      {
         if (IS_TERMINAL_NODE(tree,iNode)) // terminal node
         {
            int iTerminal = nodeToTerminal[iNode];
            double sum = 0.0;
            double count = 0.0;
            // combine the sufficient statistics from all partitions
            for (int ip = 0; ip < nPartitions; ip++)
            {
               int offset = ip*2*nTrees*maxNTerminals+// from which partition
                            iPartition*maxNTerminals*2+ // which partition is needed
                            iTerminal*2; // at which terminal
               sum += terminalStats[offset];
               count += terminalStats[offset+1];
            }
            // If there is no sample for this terminal
            // used the old prediction,
            // otherwise, update the prediction 
            if (count)
               tree.nodes[iNode].prediction = sum/count;
         }
      }
   }
   else
   {
      error("Distribution is not defined.");
   }

   // free memories
   free(nodeToTerminal);

   // return nothing
   return R_NilValue;
}
/*
 * Shink the tree
 */
SEXP hpShrinkTree(SEXP Rshrinkage, // factor of the shrinkage: 1 means no shrinkage
                  // input and output variables
                  SEXP Rtree) // the tree to be shrinked

{
   double shrinkage = REAL(Rshrinkage)[0];
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   #ifdef DEBUG
   if (shrinkage <= 0 || shrinkage > 1)
      error("shrinkage should be between 0 and 1.");
   #endif

   for (int iNode = 0; iNode < *tree.nNodesP; iNode++)
      if (IS_TERMINAL_NODE(tree, iNode)) // terminal node
         tree.nodes[iNode].prediction *= shrinkage;
   // return nothing
   return R_NilValue;
}
/*
 * Compute the improvement
 * Difference between the original sum of squared errors 
 * and the sum of squared errors after split
 */
inline double hpComputeImprovement(double leftCount, // left count after split
                                   double leftSum, // left sum after split
                                   double rightCount, //right count after split
                                   double rightSum, // right sum after split
                                   double missingCount, // count for missing values
                                   double missingSum, // sum for missing values
                                   double count, // total count
                                   double sum) // total sum
{
   double improvement = 0;
   double diff = 0;
   if (missingCount == 0)
   {
      diff = leftSum/leftCount-rightSum/rightCount;
      improvement = leftCount*rightCount*diff*diff/count;
   }
   else
   {
      diff = leftSum/leftCount-rightSum/rightCount;
      improvement += leftCount*rightCount*diff*diff;
      diff = leftSum/leftCount-missingSum/missingCount;
      improvement += leftCount*missingCount*diff*diff;
      diff = missingSum/missingCount-rightSum/rightCount;
      improvement += missingCount*rightCount*diff*diff;
      improvement /= count;
   }
   return improvement;
}


/*
 * find best splits points for each terminal nodes
 */
inline void hpFindBestSplitPoints(double* x,
                                  double* xSortedIndex,
                                  double* gradients,
                                  double* sums,
                                  double* counts,
                                  double* sampleToTerminal,
                                  int nTerminals,
                                  int nFeatures,
                                  int nSamples,
                                  double *currentLeftSums,
                                  double *currentLeftCounts,
                                  double *currentMissingSums,
                                  double *currentMissingCounts,
                                  double *currentRightSums,
                                  double *currentRightCounts,
                                  double *lastXValues,
                                  // output variables
                                  double* bestSplitFeatures,
                                  double* bestSplitValues,
                                  double* bestImprovements,
                                  double* bestLeftSums,
                                  double* bestLeftCounts,
                                  double* bestMissingSums,
                                  double* bestMissingCounts,
                                  double* bestRightSums,
                                  double* bestRightCounts)
{

   // initialize best statistics
   memset(bestSplitFeatures, 0, sizeof(double)*nTerminals);
   memset(bestSplitValues, 0, sizeof(double)*nTerminals);
   memset(bestImprovements, 0, sizeof(double)*nTerminals);
   memset(bestLeftSums, 0, sizeof(double)*nTerminals);
   memset(bestLeftCounts, 0, sizeof(double)*nTerminals);
   memset(bestMissingSums, 0, sizeof(double)*nTerminals);
   memset(bestMissingCounts, 0, sizeof(double)*nTerminals);
   memset(bestRightSums, 0, sizeof(double)*nTerminals);
   memset(bestRightCounts, 0, sizeof(double)*nTerminals);

   // compute the best features and values to split for each terminal nodes
   // iterate over features
   for (int iFeature = 0; iFeature < nFeatures; iFeature++)
   {
      // reset current statists of terminal nodes
      for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      {
         currentLeftSums[iTerminal] = 0.0;
         currentLeftCounts[iTerminal] = 0;
         currentMissingSums[iTerminal] = 0.0;
         currentMissingCounts[iTerminal] = 0;
         currentRightSums[iTerminal] = sums[iTerminal];
         currentRightCounts[iTerminal] = counts[iTerminal];
         lastXValues[iTerminal] = -HUGE_VAL;
      }
      // scan samples according to the sorted order by current feature
      for (int ind = 0; ind < nSamples; ind++)
      {
         // get which sample (Suppose xSortedIndex is 0-based)
         int iSample = xSortedIndex[iFeature*nSamples+ind];

         // get which terminal the sample is assigned to (Suppose it is 0-based)
         int iTerminal = (int) sampleToTerminal[iSample];

         // current feature value of the sample
         double xValue = x[iFeature*nSamples+iSample];

         if (ISNA(xValue)) // missing value
         {
            currentMissingSums[iTerminal] += gradients[iSample];
            currentMissingCounts[iTerminal] += 1;
            currentRightSums[iTerminal] -= gradients[iSample];
            currentRightCounts[iTerminal] -= 1;
         }
         else // xValue is a valid value
         {
            #ifdef DEBUG
            if (lastXValues[iTerminal] > xValue)
               error("Wrong order\n");
            #endif

            // update only if the current value is a new value and it is not on the boundary
            if (lastXValues[iTerminal] != xValue &&
                  currentLeftCounts[iTerminal] && currentRightCounts[iTerminal]) // currentRightCounts[iTerminal] is true all the time
            {
               // compute the current split value
               double splitValue = 0.5*(lastXValues[iTerminal]+xValue);

               // fix numerical issue
               // assume that sample goes left if < split value and goes right if >= split value
               if (splitValue == lastXValues[iTerminal])
                  splitValue = xValue;

               // compute the current improvement
               double currentImprovement = hpComputeImprovement(
                  currentLeftCounts[iTerminal],currentLeftSums[iTerminal],
                  currentRightCounts[iTerminal],currentRightSums[iTerminal],
                  currentMissingCounts[iTerminal],currentMissingSums[iTerminal],
                  counts[iTerminal],sums[iTerminal]);

               // update the best statistics if the current improvement is better
               if (currentImprovement > bestImprovements[iTerminal])
               {
                  bestSplitFeatures[iTerminal] = iFeature;
                  bestSplitValues[iTerminal] = splitValue;
                  bestLeftSums[iTerminal] = currentLeftSums[iTerminal];
                  bestLeftCounts[iTerminal] = currentLeftCounts[iTerminal];
                  bestMissingSums[iTerminal] = currentMissingSums[iTerminal];
                  bestMissingCounts[iTerminal] = currentMissingCounts[iTerminal];
                  bestRightSums[iTerminal] = currentRightSums[iTerminal];
                  bestRightCounts[iTerminal] = currentRightCounts[iTerminal];
                  bestImprovements[iTerminal] = currentImprovement;
               }
            }

            // now move the current observation to the left
            currentLeftSums[iTerminal] += gradients[iSample];
            currentLeftCounts[iTerminal] += 1;
            currentRightSums[iTerminal] -= gradients[iSample];
            currentRightCounts[iTerminal] -= 1;
            lastXValues[iTerminal] = xValue;
         } // end of !ISNA(xValue)
      }
   }
}
/*
 * Make prediction based on the current model
 */
SEXP hpDGBMPred(SEXP Rdistribution, // loss function
                SEXP RinitF, // initial output value
                SEXP Rtrees, // all trees,
                SEXP RnTreesUsed, // number of trees to be used
                SEXP Rx, // feature values for the data to be predicted
                // output variables
                SEXP Rpred) // predictions
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double initF = REAL(RinitF)[0];
   int nTrees = length(Rtrees);
   int nTreesUsed = INTEGER(RnTreesUsed)[0];
   #ifdef DEBUG
   if (nTreesUsed < 0 || nTreesUsed > nTrees)
      error("Number of trees usesd is not in the right range.");
   #endif
   double* x = REAL(Rx);
   int nSamples = nrows(Rx);
   double* pred = REAL(Rpred);

   // loop over samples
   for (int iSample = 0; iSample < nSamples; iSample++)
   {
      pred[iSample] = initF;
      for (int iTree = 0; iTree < nTreesUsed; iTree++)
      {
         // get the current tree strucutre
         SEXP Rtree = VECTOR_ELT(Rtrees, iTree);
         hpDecisionTree tree = hpGetDecisionTree(Rtree);

         // find which node this sample goes to
         int iNode = hpFindNodeForSample(tree, x, nSamples, iSample);

         // update prediction
         pred[iSample] += tree.nodes[iNode].prediction;
      }
   }

   if (strcmp(distribution, "gaussian") == 0)
   {
      // do nothing
   }
   else {
      error("Distribution is not defined.");
   }

   // return nothing
   return R_NilValue;
}

/************** distributed Regression Tree ****************/

/*
 * A structure that store the local best statistics
 */
struct __attribute__((__packed__)) hpBestResult {
   double splitTerminal;
   double splitFeature;
   double splitValue;
   double improvement;
   double leftSum;
   double leftCount;
   double missingSum;
   double missingCount;
   double rightSum;
   double rightCount;
};

/* 
 * search for the best split terminal and the corresponding infomration
 * return 0 if no improvement
 * otherwies return the best split point
 * */
SEXP hpFindABestSplit(SEXP Rx,   // a matrix of features
                      SEXP RxSortedIndex, // a sorted index of x by features
                      SEXP Rgradients, // responses
                      SEXP RsampleToTerminal, // sample to teminal node mapping
                      SEXP RnTerminals, // number of terminal nodes
                      SEXP Rsums, // sum of response for each terminal
                      SEXP Rcounts, // sample count for each terminal
                      // output variable
                      SEXP RbestResult) // statistic of best result
{
   // convert the data
   double *x = REAL(Rx);
   double *xSortedIndex = REAL(RxSortedIndex);
   double *gradients = REAL(Rgradients);
   double *sampleToTerminal = REAL(RsampleToTerminal);
   int nTerminals = INTEGER(RnTerminals)[0];
   double *sums = REAL(Rsums);
   double *counts = REAL(Rcounts);
   // current statistcs for each terminal
   double *currentLeftSums = (double*) malloc(sizeof(double)*nTerminals);
   double *currentLeftCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *currentMissingSums = (double*) malloc(sizeof(double)*nTerminals);
   double *currentMissingCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *currentRightSums = (double*) malloc(sizeof(double)*nTerminals);
   double *currentRightCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *lastXValues = (double*) malloc(sizeof(double)*nTerminals);
   // best statistics for each terminal
   double *bestSplitFeatures = (double*) malloc(sizeof(double)*nTerminals);
   double *bestSplitValues = (double*) malloc(sizeof(double)*nTerminals);
   double *bestImprovements = (double*) malloc(sizeof(double)*nTerminals);
   double *bestLeftSums = (double*) malloc(sizeof(double)*nTerminals);
   double *bestLeftCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *bestMissingSums = (double*) malloc(sizeof(double)*nTerminals);
   double *bestMissingCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *bestRightSums = (double*) malloc(sizeof(double)*nTerminals);
   double *bestRightCounts = (double*) malloc(sizeof(double)*nTerminals);

   SEXP Rdim = getAttrib(Rx, R_DimSymbol);
   int nSamples = INTEGER(Rdim)[0];
   int nFeatures = INTEGER(Rdim)[1];

   // some checkings
   #ifdef DEBUG
   Rdim = getAttrib(RxSortedIndex, R_DimSymbol);
   if (nSamples != INTEGER(Rdim)[0])
      error("Row dimensions of x and xSortedIndex do not match\n");
   if (nFeatures != INTEGER(Rdim)[1])
      error("Col dimensions of x and xSortedIndex do not match\n");
   if (nSamples != length(Rgradients))
      error("Dimensions of x %d and %d gradients do not match\n", nSamples, length(Rgradients));
   #endif

   hpFindBestSplitPoints(x,xSortedIndex,gradients,sums,counts,
      sampleToTerminal,nTerminals,nFeatures,nSamples,
      currentLeftSums,currentLeftCounts,currentMissingSums,
      currentMissingCounts,currentRightSums,currentRightCounts,lastXValues,
      bestSplitFeatures,bestSplitValues,bestImprovements,bestLeftSums,
      bestLeftCounts,bestMissingSums,bestMissingCounts,bestRightSums,bestRightCounts);

   // find which terminal has the best improvement
   int bestTerminal = -1;
   double bestImprovement = 0.0;
   for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
   {
      if (bestImprovements[iTerminal] > bestImprovement)
      {
         bestImprovement = bestImprovements[iTerminal];
         bestTerminal = iTerminal;
      }
   }

   // get return values
   hpBestResult *bestResult = (hpBestResult *) REAL(RbestResult);
   bestResult->splitTerminal = bestTerminal; // suppose 0-based
   bestResult->splitFeature = bestTerminal==-1?0:bestSplitFeatures[bestTerminal]; // suppose 0-based
   bestResult->splitValue = bestTerminal==-1?0:bestSplitValues[bestTerminal];
   bestResult->improvement = bestImprovement;
   bestResult->leftSum = bestTerminal==-1?0:bestLeftSums[bestTerminal];
   bestResult->leftCount =  bestTerminal==-1?0:bestLeftCounts[bestTerminal];
   bestResult->missingSum = bestTerminal==-1?0:bestMissingSums[bestTerminal];
   bestResult->missingCount =  bestTerminal==-1?0:bestMissingCounts[bestTerminal];
   bestResult->rightSum = bestTerminal==-1?0:bestRightSums[bestTerminal];
   bestResult->rightCount = bestTerminal==-1?0:bestRightCounts[bestTerminal];
   
   // free memories
   free(currentLeftSums);
   free(currentLeftCounts);
   free(currentMissingSums);
   free(currentMissingCounts);
   free(currentRightSums);
   free(currentRightCounts);
   free(lastXValues);
   free(bestSplitFeatures);
   free(bestSplitValues);
   free(bestImprovements);
   free(bestLeftSums);
   free(bestLeftCounts);
   free(bestMissingSums);
   free(bestMissingCounts);
   free(bestRightSums);
   free(bestRightCounts);

   // return nothing
   return R_NilValue;
}
/* 
 * search for samples which are assigned to the new terminal
 * For a split, 2(3) new terminals will be created.
 * One replaces the original terminal. The other will be added to the end.
 * The terminal with less samples will be put the end.
 * Only the sampls belongs to that terminal will be updated.
 * The list of samples to update (goes to left/right child) is stored in Rupdates 
 * The list of sample to missing child is stored in RupdatesToMissing
 */

SEXP hpFindSamplesToSplit(SEXP Rx,   // a matrix of features
                          SEXP RsampleToTerminal, // sample to teminal node mapping
                          SEXP RbestResult, // best result from findBestSplit
                          // output variables
                          // updates list is built as:
                          // [to left/right child]-> .... <- [to missing child]
                          SEXP Rupdates) // samples to update (0-based)
{
   double *x = REAL(Rx);
   double *sampleToTerminal = REAL(RsampleToTerminal);
   hpBestResult *bestResult = (hpBestResult *) REAL(RbestResult);
   double *updates = REAL(Rupdates);
   int updatesLength = length(Rupdates);

   SEXP Rdim = getAttrib(Rx, R_DimSymbol);
   int nSamples = INTEGER(Rdim)[0];

   int nUpdates = 0;
   int nUpdatesToMissing = 0;

   // whether the left child goes to the end
   int isLeftChild = bestResult->leftCount < bestResult->rightCount;

   // the offset used for x (for the best feature column)
   unsigned offset = ((unsigned) bestResult->splitFeature)*nSamples;
   // loop over all samples
   for (int iSample = 0; iSample < nSamples; iSample++) 
   {
      // if this sample was in the best split terminal
      if (sampleToTerminal[iSample] == bestResult->splitTerminal)
      {
         double xValue = x[offset+iSample];
         if (ISNA(xValue)) // missing value
         {
            nUpdatesToMissing++;
            updates[updatesLength-nUpdatesToMissing] = iSample;
         }
         // whether the sample goes the child that goes to the end of list
         else if ((x[offset+iSample]<bestResult->splitValue) == isLeftChild)
         {
            updates[nUpdates] = iSample; // Suppose updates is 0-based
            nUpdates++;
         }
      }
   }
   #ifdef DEBUG
   if (nUpdates + nUpdatesToMissing != updatesLength)
      error("updates length does not match(%d + %d != %d)", nUpdates, nUpdatesToMissing, updatesLength);
   #endif

   // return nothing
   return R_NilValue; 
}

/*
 * Perform the split by reassigning the sample to the right terminal or the missing terminal
 */
SEXP hpSplitSamples(SEXP RnTerminals,   // number of terminals
                    SEXP RnUpdates,   // number of updates
                    SEXP RnUpdatesToMissing,   // number of updates to missing terminals
                    SEXP Rupdates, // samples to update (0-based)
                    // output variables
                    SEXP RsampleToTerminal) // sample to teminal node mapping
{
   int nTerminals = INTEGER(RnTerminals)[0];
   int nUpdates = INTEGER(RnUpdates)[0];
   int nUpdatesToMissing = INTEGER(RnUpdatesToMissing)[0];
   int updatesLength = length(Rupdates);
   #ifdef DEBUG
   if (nUpdates+nUpdatesToMissing > updatesLength)
      error("number of updates exceeds the size of updates");
   #endif
   double *updates = REAL(Rupdates);
   double *sampleToTerminal = REAL(RsampleToTerminal);

   for (int iUpdate = 0; iUpdate < nUpdates; iUpdate++)
   {
      // assign to the terminal at the last of list
      // Suppose updates is 0-based
      sampleToTerminal[(int)updates[iUpdate]] = nTerminals-1; // suppose 0-based
   }
   
   for (int iUpdate = 0; iUpdate < nUpdatesToMissing; iUpdate++)
   {
      // assign to the terminal at the second last of the list
      // Suppose updatesToMissing is 0-based
      sampleToTerminal[(int)updates[updatesLength-iUpdate-1]] = nTerminals-2; // suppose 0-based
   }

   // return nothing
   return R_NilValue; 
}

/*
 * Perform the split by adding two children to the best terminal to split
 * Updating the terminal node lists
 */
SEXP hpSplitANode(SEXP RnPartitions, // number of partitions
                  SEXP RbestResults, // locally best result of each partition
                  SEXP RfeatureCounts, // start feature of each partition
                  // input and output variables
                  SEXP Rtree, // the decision tree
                  SEXP RnTerminals, // number of terminals
                  SEXP RterminalToNode, // node id of each terminal
                  SEXP Rsums, // total sum of each terminal
                  SEXP Rcounts, // total count of each terminal
                  // output variables
                  // the partition with best improvment
                  // which is set to be 0 if there is no improvement
                  SEXP RbestPartition,
                  // number of updates for samples to be reassigned to
                  // the new terminal (left/right child)
                  SEXP RnUpdates,
                  // number of updates for samples to be reassigned to
                  // the new missing terminal
                  SEXP RnUpdatesToMissing) 
{
   int nPartitions = INTEGER(RnPartitions)[0];
   hpBestResult* bestResults = (hpBestResult *) REAL(RbestResults);
   double* featureCounts = REAL(RfeatureCounts);
   hpDecisionTree tree = hpGetDecisionTree(Rtree);

   int* nTerminals = INTEGER(RnTerminals);
   int* terminalToNode = INTEGER(RterminalToNode);
   double* sums = REAL(Rsums);
   double* counts = REAL(Rcounts);
   int* bestPartition = INTEGER(RbestPartition);
   int* nUpdates = INTEGER(RnUpdates);
   int* nUpdatesToMissing = INTEGER(RnUpdatesToMissing);

   // find the global best partition and the improvement
   bestPartition[0] = -1; // it is set to be -1 if no improvement
   double bestImprovement = 0;
   for (int iPartition = 0; iPartition < nPartitions; iPartition++)
   {
      if (bestResults[iPartition].improvement > bestImprovement)
      {
         bestPartition[0] = iPartition; // suppose 0-based
         bestImprovement = bestResults[iPartition].improvement; 
      }
   }

   // if there is no improvement, do nothing and return
   if (bestImprovement == 0) return R_NilValue;

   // BestResult
   hpBestResult *bestResult = &bestResults[bestPartition[0]]; // suppose bestPartition is 0-based
   // find which terminal to split
   int bestSplitTerminal = bestResult->splitTerminal; // suppose 0-based
   // find which node is the terminal
   int bestNode = terminalToNode[bestSplitTerminal]; // suppose 0-based

   hpSplitADecisionTreeNode(tree, bestNode,
      bestResult->splitFeature+featureCounts[bestPartition[0]],
      bestResult->splitValue,
      bestResult->leftCount?bestResult->leftSum/bestResult->leftCount:
         sums[bestSplitTerminal]/counts[bestSplitTerminal],
      bestResult->missingCount?bestResult->missingSum/bestResult->missingCount:
         sums[bestSplitTerminal]/counts[bestSplitTerminal],
      bestResult->rightCount? bestResult->rightSum/bestResult->rightCount:
         sums[bestSplitTerminal]/counts[bestSplitTerminal]);

   // add three terminal nodes and remove one terminal node
   nTerminals[0] += 2;
   // the new termianl with less samples goes to the last of terminal list
   // the new termianl for missing node goes to the second last of terminal list
   // the new terminal with more samples replaces the original terminal
   int isLeftChild = bestResult->leftCount < bestResult->rightCount;
   if (isLeftChild) // the left child has less samples
   {
      // left child
      terminalToNode[nTerminals[0]-1] = tree.nodes[bestNode].leftChild;
      sums[nTerminals[0]-1] = bestResult->leftSum;
      counts[nTerminals[0]-1] = bestResult->leftCount;
      // missing child
      terminalToNode[nTerminals[0]-2] = tree.nodes[bestNode].missingChild;
      sums[nTerminals[0]-2] = bestResult->missingSum;
      counts[nTerminals[0]-2] = bestResult->missingCount;
      // right child
      terminalToNode[bestSplitTerminal] = tree.nodes[bestNode].rightChild;
      sums[bestSplitTerminal] = bestResult->rightSum;
      counts[bestSplitTerminal] = bestResult->rightCount;
      // new updates
      nUpdates[0] = (int) bestResult->leftCount;
      nUpdatesToMissing[0] = (int) bestResult->missingCount;
   }
   else // the right child has less samples
   {
      // left child
      terminalToNode[bestSplitTerminal] = tree.nodes[bestNode].leftChild;
      sums[bestSplitTerminal] = bestResult->leftSum;
      counts[bestSplitTerminal] = bestResult->leftCount;
      // missing child
      terminalToNode[nTerminals[0]-2] = tree.nodes[bestNode].missingChild;
      sums[nTerminals[0]-2] = bestResult->missingSum;
      counts[nTerminals[0]-2] = bestResult->missingCount;
      // right child
      terminalToNode[nTerminals[0]-1] = tree.nodes[bestNode].rightChild;
      sums[nTerminals[0]-1] = bestResult->rightSum;
      counts[nTerminals[0]-1] = bestResult->rightCount;
      // new updates
      nUpdates[0] = (int) bestResult->rightCount;
      nUpdatesToMissing[0] = (int) bestResult->missingCount;
   }

   // return nothing
   return R_NilValue;
}
/*
 * Update the tree using the new prediction values
 */
SEXP hpUpdateTree(SEXP Rtree,
                  SEXP Rpredictions)
{
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   double * predictions = REAL(Rpredictions);

   for (int iNode = 0; iNode < *tree.nNodesP; iNode++)
      tree.nodes[iNode].prediction = predictions[iNode];

   // return nothing
   return R_NilValue;
}
/*
 * initialize the gradients
 */
SEXP hpInitGradient(SEXP Rdistribution, // loss function
                    SEXP Ry, // original responses
                    SEXP Routputs, // current outputs
                    // output variables
                    SEXP Rgradients) // gradients
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double* y = REAL(Ry);
   double* outputs = REAL(Routputs);
   double* gradients = REAL(Rgradients);
   int nSamples = length(Ry);

   hpComputeGradient(distribution, nSamples, y, outputs, gradients);

   return R_NilValue;
}

/*
 * Fit the best constant according the loss function
 * Compute the incrementals
 * Update outputs
 * Compute new gradients
 */
SEXP hpFitAndEnsembleATree(SEXP Rdistribution, // loss function
                           SEXP Rshrinkage, // shrinkage factor
                           SEXP RterminalToNode, // terminal to node mapping
                           SEXP Rtree, // orginal regression tree
                           SEXP Ry, // original responses,
                           //input and output variables
                           SEXP Routputs, //outputs
                           SEXP RsampleToTerminal, // a mapping from each sample to its crossponding terminal
                           //output variables
                           SEXP RnewPredictions, // new predictions based on the loss function
                           SEXP Rgradients)
{
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   double shrinkage = (double) REAL(Rshrinkage)[0];
   int* terminalToNode = INTEGER(RterminalToNode);
   int nTerminals = length(RterminalToNode);
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   double* y = REAL(Ry);
   int nSamples = length(Routputs);
   double* outputs = REAL(Routputs);
   double* sampleToTerminal = REAL(RsampleToTerminal);
   double* newPredictions = REAL(RnewPredictions);
   double* gradients = REAL(Rgradients);

   // fit the best constant according to the loss function
   memset(newPredictions, 0, sizeof(double)*(*tree.nNodesP));
   if (strcmp(distribution, "gaussian") == 0)
   {
      // just use the original predictions
      for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      {
         int iNode = terminalToNode[iTerminal]; // suppose 0-based
         newPredictions[iNode] = tree.nodes[iNode].prediction;
      }
   }
   else
   {
      error("Distribution is not defined.");
   }
   // times a shrinkage factor
   for (int iNode = 0; iNode < *tree.nNodesP; iNode++)
      newPredictions[iNode] *= shrinkage;

   // compute the incrementals and update outputs
   for (int iSample = 0; iSample < nSamples; iSample++)
   {
      // find which terminal the sample belongs to
      int iTerminal = sampleToTerminal[iSample]; // suppose 0-based
      // find which node the sample belongs to
      int iNode = terminalToNode[iTerminal]; // suppose 0-based
      // compute the incremental
      double incremental = newPredictions[iNode];
      // update the outputs with a shrinkage
      outputs[iSample] += incremental;
   }

   // compute new gradients
   hpComputeGradient(distribution, nSamples, y, outputs, gradients); 

   // reset sample to terminal to root
   for (int iSample = 0; iSample < nSamples; iSample++)
      sampleToTerminal[iSample] = 0;
   
   return R_NilValue;
}

/*********************** dLambdaMART ****************************/


/*
 * build a regression tree based on the local information
 * build level by level
 */
SEXP hpBuildOneTree(SEXP Rx, // features
                    SEXP RxSortedIndex, // sorted row index by each feature
                    SEXP Rgradients, // responses variables (gradients in GBM)
                    SEXP RinteractionDepth, // number of depths
                    SEXP RsampleToTerminal, // sample to node mapping
                    // output variables
                    SEXP Rtree) // tree information
{
   // convert R objects to C arrays
   double* x = REAL(Rx);
   double* xSortedIndex = REAL(RxSortedIndex);
   double* gradients = REAL(Rgradients);
   int nSamples = length(Rgradients);
   int nFeatures = length(Rx)/nSamples;
   int interactionDepth = INTEGER(RinteractionDepth)[0];
   int maxNNodes = (pow(3,interactionDepth+1)-1)/2;
   int maxNTerminals = pow(3,interactionDepth);
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   double* sampleToTerminal = REAL(RsampleToTerminal);

   // allocate memory for building a tree
   // total sums and counts at each terminal
   double* sums = (double*) malloc(sizeof(double)*maxNTerminals);
   memset(sums, 0, sizeof(double)*maxNTerminals);
   double* counts = (double*) malloc(sizeof(double)*maxNTerminals);
   memset(counts, 0, sizeof(double)*maxNTerminals);
   // current statistics for each terminal
   double *currentLeftSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double *currentLeftCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   double *currentMissingSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double *currentMissingCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   double *currentRightSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double *currentRightCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   double *lastXValues = (double*) malloc(sizeof(double)*maxNTerminals);
   // best statistics for each terminal
   double *bestSplitFeatures = (double*) malloc(sizeof(double)*maxNTerminals);
   double *bestSplitValues = (double*) malloc(sizeof(double)*maxNTerminals);
   double *bestImprovements = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestLeftSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestLeftCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestMissingSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestMissingCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestRightSums = (double*) malloc(sizeof(double)*maxNTerminals);
   double* bestRightCounts = (double*) malloc(sizeof(double)*maxNTerminals);
   // two way mapping between terminals and nodes
   int* terminalToNode = (int*) malloc(sizeof(int)*maxNTerminals);
   int* nodeToTerminal = (int*) malloc(sizeof(int)*maxNNodes);

   // sum of gradients
   double gradientSum = 0.0;
   for (int iSample = 0; iSample < nSamples; iSample++)
      gradientSum += gradients[iSample];

   // add the root to the tree
   hpInitializeDecisionTree(tree, gradientSum/nSamples);
   // set root as the first terminal
   int nTerminals = 1;
   sums[0] = gradientSum;
   counts[0] = nSamples;
   // set the correponding mapping between 1st node and 1st terminal
   nodeToTerminal[0] = 0;
   terminalToNode[0] = 0;

   // assign all samples to root
   for (int iSample =0; iSample < nSamples; iSample++)
      sampleToTerminal[iSample] = 0;

   // build a tree level by level
   for (int iDepth = 0; iDepth < interactionDepth; iDepth++)
   {
      hpFindBestSplitPoints(x,xSortedIndex,gradients,sums,counts,
         sampleToTerminal,nTerminals,nFeatures,nSamples,
         currentLeftSums,currentLeftCounts,currentMissingSums,
         currentMissingCounts,currentRightSums,currentRightCounts,lastXValues,
         bestSplitFeatures,bestSplitValues,bestImprovements,bestLeftSums,
         bestLeftCounts,bestMissingSums,bestMissingCounts,bestRightSums,bestRightCounts);

      // If no terminal gets improvement, stop building the tree
      int noImprovement = 1;
      for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
         if (bestImprovements[iTerminal] > EPS)
            noImprovement = 0;
      if (noImprovement)
         break;

      // split every terminal
      int oldNTerminals = nTerminals;
      for (int iTerminal = 0; iTerminal < oldNTerminals; iTerminal++)
      {
         if (bestImprovements[iTerminal] > EPS)
         {
            // which node is the terminal
            int iNode = terminalToNode[iTerminal];

            // perform the split
            hpSplitADecisionTreeNode(tree, iNode,
               bestSplitFeatures[iTerminal],
               bestSplitValues[iTerminal],
               bestLeftCounts[iTerminal]?
                  bestLeftSums[iTerminal]/bestLeftCounts[iTerminal]:
                  sums[iTerminal]/counts[iTerminal],
               bestMissingCounts[iTerminal]?
                  bestMissingSums[iTerminal]/bestMissingCounts[iTerminal]:
                  sums[iTerminal]/counts[iTerminal],
               bestRightCounts[iTerminal]?
                  bestRightSums[iTerminal]/bestRightCounts[iTerminal]:
                  sums[iTerminal]/counts[iTerminal]);

            // add three terminal nodes, one of which will replace the current split node
            nTerminals += 2;

            // the left child replaces the current terminal
            terminalToNode[iTerminal] = tree.nodes[iNode].leftChild;
            nodeToTerminal[tree.nodes[iNode].leftChild] = iTerminal;
            sums[iTerminal] = bestLeftSums[iTerminal];
            counts[iTerminal] = bestLeftCounts[iTerminal];

            // the missing child is placed at the second last of terminal list
            terminalToNode[nTerminals-2] = tree.nodes[iNode].missingChild;
            nodeToTerminal[tree.nodes[iNode].missingChild] = nTerminals-2;
            sums[nTerminals-2] = bestMissingSums[iTerminal];
            counts[nTerminals-2] = bestMissingCounts[iTerminal];

            // the right child is placed at the last of terminal list
            terminalToNode[nTerminals-1] = tree.nodes[iNode].rightChild;
            nodeToTerminal[tree.nodes[iNode].rightChild] = nTerminals-1;
            sums[nTerminals-1] = bestRightSums[iTerminal];
            counts[nTerminals-1] = bestRightCounts[iTerminal];
         }
      }

      // split samples by reasigning the samples to the new terminals
      for (int iSample = 0; iSample < nSamples; iSample++)
      {
         int iNode = hpFindNodeForSample(tree, x, nSamples, iSample);
         sampleToTerminal[iSample] = nodeToTerminal[iNode];
      }
   }

   // free memories
   free(nodeToTerminal);
   free(terminalToNode);
   free(currentLeftSums);
   free(currentLeftCounts);
   free(currentMissingSums);
   free(currentMissingCounts);
   free(currentRightSums);
   free(currentRightCounts);
   free(lastXValues);
   free(bestRightCounts);
   free(bestRightSums);
   free(bestMissingCounts);
   free(bestMissingSums);
   free(bestLeftCounts);
   free(bestLeftSums);
   free(bestImprovements);
   free(bestSplitValues);
   free(bestSplitFeatures);
   free(counts);
   free(sums);

   // return nothing
   return R_NilValue;
}
/*
 * compute the losses for all trees on this partition
 * the i'th element of Rlosses corresponds to the local loss
 * on this partition for the tree from i'th partition
 */
SEXP hpComputeLosses(SEXP Rdistribution,
                     SEXP Rx, // feature values
                     SEXP Ry, // original responses
                     SEXP Routputs, // current outputs
                     SEXP RnPartitions, // number of partitions
                     SEXP Rtrees, // all trees, one tree per partition
                     //output varbiles
                     SEXP Rlosses) // the current loss for each tree
{
   
   char const* distribution = CHAR(STRING_ELT(Rdistribution,0));
   int nPartitions = INTEGER(RnPartitions)[0];
   double* x = REAL(Rx);
   double* y = REAL(Ry);
   double* outputs = REAL(Routputs);
   int nSamples = length(Ry);
   hpDecisionTree *trees = (hpDecisionTree*) malloc(sizeof(hpDecisionTree)*nPartitions);
   hpGetDecisionTrees(Rtrees, nPartitions, trees);
   double *losses = REAL(Rlosses);

   #ifdef DEBUG
   if (length(Rlosses) != nPartitions)
      error("wrong size of losses");
   #endif

   if (strcmp(distribution,"gaussian")==0)
   {
      // iterate over each tree and compute the losses
      for (int iPartition = 0; iPartition < nPartitions; iPartition++)
      {  
         // initialize loss
         losses[iPartition] = 0;

         // get the current tree structure
         hpDecisionTree &tree = trees[iPartition];

         for (int iSample = 0; iSample < nSamples; iSample++)
         {
            // assign sample to node
            int iNode = hpFindNodeForSample(tree, x, nSamples, iSample);

            // add the loss for that sample to the total loss
            double diff = y[iSample]-outputs[iSample]-tree.nodes[iNode].prediction;
            losses[iPartition] += diff*diff; // square loss
         }
      }
   }
   else
   {
      error("Distribution is not defined.");
   }
   
   // free memories
   free(trees);
   // return nothing
   return R_NilValue;
}
/*********************** histogram ****************************/
/*
 * Compute a histogram for each terminal and each feature
 * Structure of Rhistograms:
 * let f = # of features
 * let t = max # of terminals
 * let h_ij = the histogram for the i'th feature and the j'th terminal
 * the Rhistgorams has the structure:
 * (h_11, ..., h1t, ..., h_f1, ... h_ft)
 * Each histogram has the strcutre:
 * let b = # of bins
 * Each histogram has the following structure:
 * (p_0, m_0, r_0), (p_1, m_1, r_1), ..., (p_b, m_b, r_b)
 * For the k'th bin:
 * p_k is the center
 * m_k is the count
 * r_k is hte cummulative responses
 */
SEXP hpComputeHistograms(SEXP Rx, // feature values
                         SEXP RxSortedIndex, // index of x sorted by each feature
                         SEXP Rgradients, // responses (gradients in GBM)
                         SEXP RsampleToTerminal, // sample to termial mapping
                         SEXP RnTerminals, // number of terminals,
                         SEXP RmaxNBins, // number of bins per histogram
                         // output variables
                         SEXP Rhistograms) //histogram for each terminal and each feature
{
   double *x = REAL(Rx);
   int nSamples = nrows(Rx);
   int nFeatures = ncols(Rx);
   double *xSortedIndex = REAL(RxSortedIndex);
   double *gradients = REAL(Rgradients);
   int nTerminals = INTEGER(RnTerminals)[0];
   int maxNBins = INTEGER(RmaxNBins)[0];
   double *sampleToTerminal = REAL(RsampleToTerminal);
   double *histograms = REAL(Rhistograms);
   int currentMaxNTerminals = length(Rhistograms)/3/nFeatures/(maxNBins+1);

   // reset the output so that the end of histogram will has count 0
   memset(histograms, 0, sizeof(double)*length(Rhistograms));

   // allocate statistics for missing values
   double *missingCounts = (double*) malloc(sizeof(double)*nTerminals);
   double *missingSums = (double*) malloc(sizeof(double)*nTerminals);

   // allocate histogram structures
   hpHistogram **hists = (hpHistogram**) malloc(sizeof(hpHistogram*)*nTerminals);
   for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      hists[iTerminal] = hpAllocateHistogram(maxNBins);

   // iterate over features
   for (int iFeature = 0; iFeature < nFeatures; iFeature++)
   {
      // reset missing values/counts
      memset(missingCounts, 0, sizeof(double)*nTerminals);
      memset(missingSums, 0, sizeof(double)*nTerminals);

      // reset the histograms
      for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
         hpResetHistogram(hists[iTerminal]);

      // scan samples according to the order of current feature
      for (int ind = 0; ind < nSamples; ind++)
      {
         // get which sample (suppose xSortedIndex is 0-based)
         int iSample = xSortedIndex[iFeature*nSamples+ind];
         
         // get which terminal the sample is assigned to (suppose sampleToTerminal is 0-based)
         int iTerminal = (int) sampleToTerminal[iSample];

         // feature value of the current sample
         double xValue = x[iFeature*nSamples+iSample];

         // missing value
         if (ISNA(xValue))
         {
            missingSums[iTerminal] += gradients[iSample];
            missingCounts[iTerminal] += 1;
         }
         else // real value
         {
            hpInsertBin(hists[iTerminal],xValue,1, gradients[iSample]);
         }
         
      }
      // write to output buffer
      // first bin for each terminal is for missing values
      for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      {
         int offset = iFeature*3*(maxNBins+1)*currentMaxNTerminals +
                      iTerminal*3*(maxNBins+1);
         // For missing values
         histograms[offset] = NAN;
         histograms[offset+1] = missingCounts[iTerminal];
         histograms[offset+2] = missingSums[iTerminal];

         hpSerializeHistogram(hists[iTerminal],&histograms[offset+3]);
      }

   }

   // free memories
   for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      hpFreeHistogram(hists[iTerminal]);
   free(hists);
   free(missingSums);
   free(missingCounts);

   // return nothing
   return R_NilValue;
}
/*
 * Represents a bin in a histogram
 * It is simpler than struct hpBin
 */
struct __attribute__((__packed__)) hpBinItem
{
   int iPartition;
   double center;
   double count;
   double response;
   hpBinItem(int ip, double ctr, double cnt, double r):
       iPartition(ip), center(ctr), count(cnt), response(r){}
   bool operator< (const hpBinItem& b) const {return center > b.center;} 
};
/*
 * For a feature and a terminal,
 * merge the histograms from all partitions and
 * sort it by center of bins.
 */
inline void hpMergeHistograms(double *histograms, //all histograms
                              int nPartitions,
                              int iFeature,
                              int nFeatures,
                              int iTerminal,
                              int currentMaxNTerminals,
                              int maxNBins,
                              int* iBins,
                              priority_queue<hpBinItem> &currentBins,
                              // output variables
                              double* bins, // merged histograms
                              int* nBinsP, // pointer to number of bins
                              double *countP, // pointer to total count
                              double *sumP, // pointer to total sum
                              double *currentMissingCountP,
                              double *currentMissingSumP)
{
   // initialized the indices of the bins and currentBins
   // skip one bin for missing values
   for (int iPartition = 0; iPartition < nPartitions; iPartition++)
   {
      int iBin = 1;
      iBins[iPartition] = iBin;
      int offset = iPartition*3*(maxNBins+1)*currentMaxNTerminals*nFeatures+
                   iFeature*3*(maxNBins+1)*currentMaxNTerminals+
                   iTerminal*3*(maxNBins+1)+
                   iBin*3;
      // if there is a bin in the list from i'th partition
      if (iBin <= maxNBins && histograms[offset+1])
      {
         // add to currentBins
         currentBins.push(hpBinItem(iPartition,
                                    histograms[offset],
                                    histograms[offset+1],
                                    histograms[offset+2]));
      }
   }
   
   // initiatlization
   *nBinsP = 0; // number of bins
   *countP = 0; // total count in the merged histogram
   *sumP = 0; // total sum of responses in the merged histogram

   // do until there is no bins in the current bin list
   while (!currentBins.empty())
   {
      // find the unprocessed bin with the smallest center
      hpBinItem binItem = currentBins.top();
      currentBins.pop();

      // add the bin to the merged bin list
      int binOffset = (*nBinsP)*3;

      //merge bins if they have the same center
      if (*nBinsP > 0 &&  bins[binOffset-3] == binItem.center)
      {
         bins[binOffset-2] += binItem.count;
         bins[binOffset-1] += binItem.response;
      }
      else // if the new bin is different, add it to list
      {
         bins[binOffset] = binItem.center;
         bins[binOffset+1] = binItem.count;
         bins[binOffset+2] = binItem.response;
         (*nBinsP)++;
      }
         
      // compute the total count and sum
      *countP += binItem.count;
      *sumP += binItem.response;

      int iPartition = binItem.iPartition;
      int iBin = ++iBins[iPartition]; // increase the index by one
      int offset = iPartition*3*(maxNBins+1)*currentMaxNTerminals*nFeatures+
                   iFeature*3*(maxNBins+1)*currentMaxNTerminals+
                   iTerminal*3*(maxNBins+1)+
                   iBin*3;

      // if there is a bin in the list from i'th partition
      if (iBin <= maxNBins && histograms[offset+1])
      {
         // add to currentBins
         currentBins.push(hpBinItem(iPartition,
                                    histograms[offset],
                                    histograms[offset+1],
                                    histograms[offset+2]));
      }
   }

   // compute the total missing count and sum for this terminal
   // and this feature
   *currentMissingCountP = 0;
   *currentMissingSumP = 0;
   for (int iPartition = 0; iPartition < nPartitions; iPartition++)
   {
      int offset = iPartition*3*(maxNBins+1)*currentMaxNTerminals*nFeatures+
                   iFeature*3*(maxNBins+1)*currentMaxNTerminals+
                   iTerminal*3*(maxNBins+1);
      #ifdef DEBUG
      if (!ISNAN(histograms[offset]))
         error("The first bin of histogram is not NAN.");
      #endif

      *currentMissingCountP += histograms[offset+1];
      *currentMissingSumP += histograms[offset+2];
   }
}
/*
 * For a terminal,
 * find the best split point from a merged histograms for a feature.
 * If it is better than the best splits given (from another feature),
 * then update the best split points.
 */
inline void hpGetBestSplits(double *bins, // merged histograms
                            int nBins, // number of bins in the histogram
                            int iFeature, // current feature
                            int iTerminal, // current terminal
                            double count, // total count for the terminal
                            double sum,  // total sum for the terminal
                            double currentMissingCount,
                            double currentMissingSum,
                            // input and output variables
                            double *bestSplitFeatures,
                            double *bestSplitValues,
                            double *bestImprovements,
                            double *bestLeftCounts,
                            double *bestLeftSums,
                            double *bestMissingCounts,
                            double *bestMissingSums,
                            double *bestRightCounts,
                            double *bestRightSums)
{
   /*
    * Naively use the middle points between bins as the split point.
    * Current implementation is different but seems to be better
    * tha Algorithm 4 in "A streaming parallel decision tree algorithm"
    * by Yael Ben-Haim and Elad Tom-Tov.
    */
   double currentLeftCount = 0;
   double currentLeftSum = 0;
   for (int iSplit = 1; iSplit < nBins; iSplit++)
   {
      currentLeftCount += bins[(iSplit-1)*3+1];
      currentLeftSum += bins[(iSplit-1)*3+2];
      double currentRightCount = count - currentLeftCount;
      double currentRightSum = sum - currentLeftSum;
      double currentSplitValue = 0.5*(bins[(iSplit-1)*3]+bins[iSplit*3]);
      // compute the improvement
      double currentImprovement = hpComputeImprovement(
            currentLeftCount,currentLeftSum,
            currentRightCount,currentRightSum,
            currentMissingCount,currentMissingSum,
            count,sum);

      // fix numerical error
      if (currentImprovement < EPS)
         currentImprovement = 0;

      // update the best improvement
      if (currentImprovement > bestImprovements[iTerminal])
      {
         bestSplitFeatures[iTerminal] = iFeature;
         bestSplitValues[iTerminal] = currentSplitValue;
         bestImprovements[iTerminal] = currentImprovement;
         bestLeftCounts[iTerminal] = currentLeftCount;
         bestLeftSums[iTerminal] = currentLeftSum;
         bestMissingCounts[iTerminal] = currentMissingCount;
         bestMissingSums[iTerminal] = currentMissingSum;
         bestRightCounts[iTerminal] = currentRightCount;
         bestRightSums[iTerminal] = currentRightSum;
      }
   }
}
/*
 * First merge the histograms from all partitions.
 * Then find the best split point for each terminal
 */
inline void hpMergeHistogramsAndGetBestSplits(
                  int nTerminals,
                  int currentMaxNTerminals,
                  int maxNBins,
                  int nFeatures,
                  int nPartitions,
                  double *histograms,
                  // output variables
                  double *bestSplitFeatures,
                  double *bestSplitValues,
                  double *bestImprovements,
                  double *bestLeftCounts,
                  double *bestLeftSums,
                  double *bestMissingCounts,
                  double *bestMissingSums,
                  double *bestRightCounts,
                  double *bestRightSums)
{
   // allocate the bins for a terminal and a feature
   // missing sum/count will be stored seperately
   double *bins = (double *) malloc(sizeof(double)*3*maxNBins*nPartitions);
   // allocate the indices of bins for each partition
   int *iBins = (int *) malloc(sizeof(int)*nPartitions);
   // allocate a priority queue for current bins
   // current bins of first unprocessed bins for all patitions
   priority_queue<hpBinItem> currentBins;

   // iterate over terminals
   for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
   {
      // initialize the best split for this terminal
      bestSplitFeatures[iTerminal] = -1;
      bestSplitValues[iTerminal] = 0;
      bestImprovements[iTerminal] = 0;
      bestLeftCounts[iTerminal] = 0;
      bestLeftSums[iTerminal] = 0;
      bestMissingCounts[iTerminal] = 0;
      bestMissingSums[iTerminal] = 0;
      bestRightCounts[iTerminal] = 0;
      bestRightSums[iTerminal] = 0;

      // iterate over features
      for (int iFeature = 0; iFeature < nFeatures; iFeature++)
      {
         int nBins = 0;
         double count = 0;
         double sum = 0;
         double currentMissingCount = 0;
         double currentMissingSum = 0;

         hpMergeHistograms(histograms, nPartitions, iFeature, nFeatures,
                     iTerminal, currentMaxNTerminals, maxNBins, iBins, currentBins,
                     bins, &nBins, &count, &sum,
                     &currentMissingCount, &currentMissingSum);

         hpGetBestSplits(bins, nBins, iFeature, iTerminal,
                         count, sum,
                         currentMissingCount, currentMissingSum,
                         bestSplitFeatures, bestSplitValues,
                         bestImprovements,
                         bestLeftCounts, bestLeftSums,
                         bestMissingCounts, bestMissingSums,
                         bestRightCounts, bestRightSums);
      } // end of iterating over features

   } // end of iterating over terminals

   // free memories
   free(iBins);
   free(bins);
}

/*
 * Split the terminals of a tree if it can be improved
 */
inline void  hpSplitNodes(double* bestSplitFeatures,
                          double* bestSplitValues,
                          double* bestImprovements,
                          double* bestLeftCounts,
                          double* bestLeftSums,
                          double* bestMissingCounts,
                          double* bestMissingSums,
                          double* bestRightCounts,
                          double* bestRightSums,
                          // output variables
                          int* nTerminalsP,
                          int* terminalToNode,
                          hpDecisionTree &tree)
{
   int oldNTerminals = *nTerminalsP;
   for (int iTerminal = 0; iTerminal < oldNTerminals; iTerminal++)
   {
      // split the terminal if there is some improvement
      if (bestImprovements[iTerminal] > 0)
      {
         // which node i the terminal
         int iNode = terminalToNode[iTerminal];

         // I use the average of the current terminal as prediction
         // if there is no samples for the new terminal
         double average = (bestLeftSums[iTerminal]+
                           bestRightSums[iTerminal]+
                           bestMissingSums[iTerminal])/
                           (bestLeftCounts[iTerminal]+
                           bestRightCounts[iTerminal]+
                           bestMissingCounts[iTerminal]);

         hpSplitADecisionTreeNode(tree, iNode,
            bestSplitFeatures[iTerminal],
            bestSplitValues[iTerminal],
            bestLeftCounts[iTerminal]?
               bestLeftSums[iTerminal]/bestLeftCounts[iTerminal]:average,
            bestMissingCounts[iTerminal]?
               bestMissingSums[iTerminal]/bestMissingCounts[iTerminal]:average,
            bestRightCounts[iTerminal]?
               bestRightSums[iTerminal]/bestRightCounts[iTerminal]:average);

         // add three terminal nodes, one of which will replace the current split node
         *nTerminalsP += 2;

         // the left child replaces the current terminal
         terminalToNode[iTerminal] = tree.nodes[iNode].leftChild;

         // the missing child is placed at the second last of terminal list
         terminalToNode[*nTerminalsP-2] = tree.nodes[iNode].missingChild;

         // the right child is placed at the last of terminal list
         terminalToNode[*nTerminalsP-1] = tree.nodes[iNode].rightChild;
      }
   }
}

/*
 * Combine histograms and decide the best splits for each terminal
 * Then perform the split
 * RbestSplits has the structure:
 * element 0: number of terminals
 * element 1-maxNTerminals: best split features
 * element (maxNTerminals+1)-2*maxNTerminals: best split values
 */
SEXP hpGrowTree(SEXP RnTerminals, // number of terminals
                SEXP RmaxNBins, // number of bins
                SEXP RnFeatures, // number of features
                SEXP RnPartitions, // number of partitions
                SEXP Rhistograms, // histograms from all partitions
                // input and output variables
                SEXP Rtree, // tree structure
                SEXP RterminalToNode, // terminal to node mapping
                // output variables
                SEXP RbestSplits)// best split for each terminal
{
   int nTerminals = INTEGER(RnTerminals)[0];
   int maxNBins = INTEGER(RmaxNBins)[0];
   int nFeatures = INTEGER(RnFeatures)[0];
   int nPartitions = INTEGER(RnPartitions)[0];
   double *histograms = REAL(Rhistograms);
   int currentMaxNTerminals = length(Rhistograms)/3/nFeatures/(maxNBins+1)/nPartitions;
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   int *terminalToNode = INTEGER(RterminalToNode);
   int maxNTerminals = length(RterminalToNode); 
   double *bestSplits = REAL(RbestSplits);
   bestSplits[0] = nTerminals;
   double *bestSplitFeatures = bestSplits+1;
   double *bestSplitValues = bestSplits+1+maxNTerminals;
   #ifdef DEBUG
   if (3*(maxNBins+1)*currentMaxNTerminals*nFeatures*nPartitions!=length(Rhistograms))
      error("Error: length of Rhistogram does not match.");
   #endif
   // allocate best split statistics for terminals
   double* bestImprovements = (double *) malloc(sizeof(double)*nTerminals);
   double* bestLeftCounts = (double *) malloc(sizeof(double)*nTerminals);
   double* bestLeftSums = (double *) malloc(sizeof(double)*nTerminals);
   double* bestMissingCounts = (double *) malloc(sizeof(double)*nTerminals);
   double* bestMissingSums = (double *) malloc(sizeof(double)*nTerminals);
   double* bestRightCounts = (double *) malloc(sizeof(double)*nTerminals);
   double* bestRightSums = (double *) malloc(sizeof(double)*nTerminals);

   hpMergeHistogramsAndGetBestSplits(nTerminals,
                                     currentMaxNTerminals,
                                     maxNBins,
                                     nFeatures,
                                     nPartitions,
                                     histograms,
                                     bestSplitFeatures,
                                     bestSplitValues,
                                     bestImprovements,
                                     bestLeftCounts,
                                     bestLeftSums,
                                     bestMissingCounts,
                                     bestMissingSums,
                                     bestRightCounts,
                                     bestRightSums);
        
   double totalImprovement = 0;
   for (int iTerminal = 0; iTerminal < nTerminals; iTerminal++)
      totalImprovement += bestImprovements[iTerminal];

   hpSplitNodes(bestSplitFeatures,
                bestSplitValues,
                bestImprovements,
                bestLeftCounts,
                bestLeftSums,
                bestMissingCounts,
                bestMissingSums,
                bestRightCounts,
                bestRightSums,
                INTEGER(RnTerminals),
                terminalToNode,
                tree);

   // free memories
   free(bestRightSums);
   free(bestRightCounts);
   free(bestMissingSums);
   free(bestMissingCounts);
   free(bestLeftSums);
   free(bestLeftCounts);
   free(bestImprovements);

   // return total improvement
   SEXP RtotalImprovement;
   PROTECT(RtotalImprovement = allocVector(REALSXP,1));
   REAL(RtotalImprovement)[0] = totalImprovement;
   UNPROTECT(1);
   return RtotalImprovement;
}

/*
 * split samples on each partition according to the best splits
 * for all previous terminals
 * RbestSplits has the structure:
 * element 0: number of terminals
 * element 1-maxNTerminals: best split features
 * element (maxNTerminals+1)-2*maxNTerminals: best split values
 */
SEXP hpSplitSamplesByBestSplits(
            SEXP RbestSplits, // best splint for each terminal
            SEXP Rx, // feature values of the samples
            // output variables
            SEXP RsampleToTerminal) // sample to terminal mapping
{
   double *bestSplits = REAL(RbestSplits);
   int maxNTerminals = length(RbestSplits)/2;
   int oldNTerminals = (int) bestSplits[0];
   double *bestSplitFeatures = bestSplits+1;
   double *bestSplitValues = bestSplits+1+maxNTerminals;
   double *x = REAL(Rx);
   int nSamples = nrows(Rx);
   double* sampleToTerminal = REAL(RsampleToTerminal);

   // allocate the mapping from the old termial to the indices of new terminals
   // leftChildren always replaces the orignal terminals
   int* missingChildren = (int *) malloc(sizeof(int)*maxNTerminals);
   int* rightChildren = (int *) malloc(sizeof(int)*maxNTerminals);

   // compute the mapping for new terminals
   int nTerminals = oldNTerminals;
   for (int iTerminal = 0; iTerminal < oldNTerminals; iTerminal++)
   {
      if (bestSplitFeatures[iTerminal] != -1)
      {
         nTerminals += 2;
         // missing child is placed at second last of the terminal
         missingChildren[iTerminal] = nTerminals-2;
         // right child is placed at the last of the terminal
         rightChildren[iTerminal] = nTerminals-1;
      }
   }

   // assign samples to the new terminals
   for (int iSample = 0; iSample < nSamples; iSample++)
   {
      int iTerminal = (int) sampleToTerminal[iSample];
      int iFeature = bestSplitFeatures[iTerminal];
      if (iFeature != -1)
      {
         double xValue = x[iFeature*nSamples+iSample];
         if (ISNA(xValue)) // missing value
            sampleToTerminal[iSample] = missingChildren[iTerminal];
         else if (xValue >= bestSplitValues[iTerminal]) // go right
            sampleToTerminal[iSample] = rightChildren[iTerminal];
         // otherwise go left, ie. the replaced terminal
      }
   }

   // free memories
   free(rightChildren);
   free(missingChildren);

   // return nothing
   return R_NilValue;
}
}
