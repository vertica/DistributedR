/*
 * A histogram which can be built using streaming data
 * Author: Hao Peng (penghao888@gmail.com)
 */

#include <cstdlib>

extern "C" {
#include <R.h>
#include <Rinternals.h>
#include "hphistogram.h"

/*
 * Reset the stracutre for a histogram
 */
void hpResetHistogram(hpHistogram *hist)
{
   hist->nSpaces = 0;
   hist->nBins = 0;
   // used bin list (doubly linked)
   hist->bins[0].nextBin = &hist->bins[1];
   hist->bins[0].prevBin = NULL;
   hist->bins[1].nextBin = NULL;
   hist->bins[1].prevBin = &hist->bins[0];
   // free bin list (singly linked)
   hist->nextFreeBin = &hist->bins[2];
   for (int iBin = 2; iBin < hist->maxNBins+3; iBin++)
      hist->bins[iBin].nextBin = &hist->bins[iBin+1];
   hist->bins[hist->maxNBins+3].nextBin = NULL;
}
/*
 * Allocate the structure for a histogram
 */
hpHistogram* hpAllocateHistogram(int maxNBins)
{
   #ifdef DEBUG
   if (maxNBins < 1)
      error("Error: maximum number of bins should be postive\n");
   #endif
   hpHistogram* hist = (hpHistogram*) malloc(sizeof(hpHistogram));
   hist->maxNBins = maxNBins;
   // allow at most maxNBins items in the heap (element 0 is not used)
   hist->spaces = (hpSpace *) malloc(sizeof(hpSpace)*(maxNBins+1));
   // allow at most maxNbins+4 bins in the doubly-linked list
   // 0-element is the head , 1-element is the tail
   // the head and tail are not real bins in use
   hist->bins = (hpBin *) malloc(sizeof(hpBin)*(maxNBins+4));
   hpResetHistogram(hist);

   return hist;
}
/*
 * Free the strcture for a histogram
 */
void hpFreeHistogram(hpHistogram *hist)
{
   free(hist->spaces);
   free(hist->bins);
   free(hist);
}

/*
 * Move an element up the heap if possible
 */
int _hpSpaceHeapUp(hpHistogram *hist, 
              int p) // the space to move
{
   #ifdef DEBUG
   if (p < 1 || p > hist->nSpaces)
      error("Error: wrong element to move up\n");
   #endif
   // q is the parent of node p
   int q = p >> 1;
   // the space to move up
   hpSpace space = hist->spaces[p];
   // loop till the 0'the element
   while(q)
   {
      // if the parent space has a smaller length, stop
      if (space.length > hist->spaces[q].length)
         break;
      // move parent down
      hist->spaces[p] = hist->spaces[q];
      // update the corresponding bins
      hist->spaces[p].leftBin->rightSpace = p;
      hist->spaces[p].rightBin->leftSpace = p;
      p = q;
      q = p >> 1;
   }
   hist->spaces[p] = space;
   // update the corresponding bins
   hist->spaces[p].leftBin->rightSpace = p;
   hist->spaces[p].rightBin->leftSpace = p;
   return p;
}

/*
 * Move an element down the heap if possible
 */
int _hpSpaceHeapDown(hpHistogram *hist, 
                int p) // the space to move
{
   #ifdef DEBUG
   if (p < 1 || p > hist->nSpaces)
      error("Error: wrong element to move down\n");
   #endif
   // q is the child of node p
   int q = p << 1;
   // the space to move down
   hpSpace space = hist->spaces[p];
   // loop till the end of the heap
   while(q <= hist->nSpaces)
   {
      // if right child is smaller, let q be the right child
      if (q < hist->nSpaces &&
          hist->spaces[q+1].length < hist->spaces[q].length)
         q++;
      // if the child space has a larger length, stop
      if (space.length < hist->spaces[q].length)
         break;
      // move child up
      hist->spaces[p] = hist->spaces[q];
      // update the corresponding bins
      hist->spaces[p].leftBin->rightSpace = p;
      hist->spaces[p].rightBin->leftSpace = p;
      p = q;
      q = p << 1;
   }
   hist->spaces[p] = space;
   // update the corresponding bins
   hist->spaces[p].leftBin->rightSpace = p;
   hist->spaces[p].rightBin->leftSpace = p;
   return p;
}

/*
 * Push into heap
 */
int  _hpSpaceHeapPush(hpHistogram *hist,
                 double length,
                 hpBin *leftBin,
                 hpBin *rightBin)
{
   // put the new space at the end of the heap
   hist->nSpaces++;
   hist->spaces[hist->nSpaces].length = length;
   hist->spaces[hist->nSpaces].leftBin = leftBin;
   hist->spaces[hist->nSpaces].rightBin = rightBin;
   // initialize the corresponding bins
   leftBin->rightSpace = hist->nSpaces;
   rightBin->leftSpace = hist->nSpaces;
   // move up the new space
   return _hpSpaceHeapUp(hist, hist->nSpaces);
}

/*
 * Pop from the heap
 */
hpSpace _hpSpaceHeapPop(hpHistogram *hist)
{
   #ifdef DEBUG
   if (hist->nSpaces < 1)
      error("Error: heap empty\n");
   #endif
   hpSpace space = hist->spaces[1];
   space.leftBin->rightSpace = 0;
   space.rightBin->leftSpace = 0;
   // replace the top the heap with the end of heap
   hist->spaces[1] = hist->spaces[hist->nSpaces];
   hist->nSpaces--;
   if (hist->nSpaces)
   {
      // update the corresponding bins
      hist->spaces[1].leftBin->rightSpace = 1;
      hist->spaces[1].rightBin->leftSpace = 1;
      // move down the top of the heap
      _hpSpaceHeapDown(hist, 1);
   }
   return space;
}

/*
 * Remove from middle of the heap 
 */
hpSpace _hpSpaceHeapRemove(hpHistogram *hist, int p)
{
   #ifdef DEBUG
   if (p < 1 || p > hist->nSpaces)
      error("Error: wrong element to remove\n");
   #endif
   hpSpace space = hist->spaces[p];
   space.leftBin->rightSpace = 0;
   space.rightBin->leftSpace = 0;
   // repace the removed space with the end of heap
   hist->spaces[p] = hist->spaces[hist->nSpaces];
   hist->nSpaces--;
   if (hist->nSpaces)
   {
      // update the correspoding bins
      hist->spaces[p].leftBin->rightSpace = p;
      hist->spaces[p].rightBin->leftSpace = p;
      // move the replaced space up first and then down
      p = _hpSpaceHeapUp(hist, p);
      _hpSpaceHeapDown(hist, p);
   }
   return space;
}
/*
 * Update middle of the heap
 */
int _hpSpaceHeapUpdate(hpHistogram *hist, int p)
{
   #ifdef DEBUG
   if (p < 1 || p > hist->nSpaces)
      error("Error: wrong element to update\n");
   #endif
   hist->spaces[p].length = hist->spaces[p].rightBin->center-
                            hist->spaces[p].leftBin->center;
   // move the replaced space up first and then down
   p = _hpSpaceHeapUp(hist, p);
   return _hpSpaceHeapDown(hist, p);
}

/*
 * Get a free bin
 */
hpBin *_hpGetABin(hpHistogram *hist,
                  double center,
                  double count,
                  double response)
{
   hist->nBins++;
   hpBin *bin = hist->nextFreeBin;
   #ifdef DEBUG
   if (bin == NULL)
      error("Error: not enough bins\n");
   #endif
   hist->nextFreeBin = bin->nextBin;
   bin->nextBin = NULL;
   bin->prevBin = NULL;
   bin->center = center;
   bin->count = count;
   bin->response = response;
   bin->leftSpace = 0;
   bin->rightSpace = 0;
   return bin;
}

/*
 * Recylcle an used bin without updating the heap
 */
void _hpRecycleABin(hpHistogram *hist,
                    hpBin *bin)
{
   hist->nBins--;
   bin->nextBin = hist->nextFreeBin;
   hist->nextFreeBin = bin;
}

/*
 * Merge the closest two bins.
 * Do nothing if the number of bins is smaller than 2.
 */
void hpMergeBins(hpHistogram *hist)
{
   if (hist->nBins > 1)
   {
      // get the top of the heap
      hpSpace space = _hpSpaceHeapPop(hist);
      // get the two bins to be merged
      hpBin *leftBin = space.leftBin;
      hpBin *rightBin = space.rightBin;
      // get neighbors of the space (could be 0)
      int leftSpace = leftBin->leftSpace;
      int rightSpace = rightBin->rightSpace;
      // merge two bins into a new bin
      // eqn (11) in "Parallel Boosted Regression Trees for Web Search Ranking" by Stephen Tyree, et al.
      double count = leftBin->count+rightBin->count;
      double center = leftBin->count/count*leftBin->center+
                      rightBin->count/count*rightBin->center;
      double response = leftBin->response+rightBin->response;
      hpBin *bin = _hpGetABin(hist,center,count,response);
      // replaced the old two bins with the new bin
      // remove the old left bin
      leftBin->prevBin->nextBin = bin;
      bin->prevBin = leftBin->prevBin;
      hist->spaces[leftSpace].rightBin = bin;// leftSpace can be 0, but the update is fine
      bin->leftSpace = leftSpace;
      _hpRecycleABin(hist, leftBin);
      // remove the old right bin
      rightBin->nextBin->prevBin = bin;
      bin->nextBin = rightBin->nextBin;
      hist->spaces[rightSpace].leftBin = bin; // rightSpace can be 0, but the update is fine
      bin->rightSpace = rightSpace;
      _hpRecycleABin(hist, rightBin);
      // update the spaces
      if (leftSpace) _hpSpaceHeapUpdate(hist, leftSpace);
      if (rightSpace) _hpSpaceHeapUpdate(hist, rightSpace);
   }
}

/*
 * Insert a bin, assuming that the bins are inserted in order
 * Merge once if the number of bins becomes more than the limit after insertion
 */
void hpInsertBin(hpHistogram *hist, // histogram to be inserted
                 double center, // center of the new bin
                 double count, // count of the new bin
                 double response) //response of the new bin
{
   // merge if the new bin has the same center as the last bin
   if (hist->nBins > 0 && hist->bins[1].prevBin->center == center) 
   {
      hist->bins[1].prevBin->count += count;
      hist->bins[1].prevBin->response += response;
   }
   else // a different center from last bin
   {
      // create a new bin
      hpBin *bin = _hpGetABin(hist, center, count, response);
      // put the bin to the end of the bin lists
      hist->bins[1].prevBin->nextBin = bin;
      bin->prevBin = hist->bins[1].prevBin;
      bin->nextBin = &hist->bins[1];
      hist->bins[1].prevBin = bin;
      // create a new space if # of bins > 1
      if (hist->nBins > 1)
         _hpSpaceHeapPush(hist,bin->center-bin->prevBin->center,bin->prevBin,bin);

      // if # of bins is greater than the maximum # of bins allowed,
      // merge the two nearest bins
      if (hist->nBins > hist->maxNBins)
         hpMergeBins(hist);
   }
}
/*
 * Convert the histogram to an array
 */
void hpSerializeHistogram(hpHistogram *hist, 
                          double *dest) // the array to be written to
{
   hpBin *bin = hist->bins[0].nextBin;
   int iBin = 0;
   while (bin != &hist->bins[1])
   {
      dest[3*iBin] = bin->center;
      dest[3*iBin+1] = bin->count;
      dest[3*iBin+2] = bin->response;
      iBin++;
      bin = bin->nextBin;
   }
}
/*
 * Print the histogram
 */
void hpPrintHistogram(hpHistogram *hist)
{
   hpBin *bin = hist->bins[0].nextBin;
   int iBin = 0;
   while (bin != &hist->bins[1])
   {
      Rprintf("%d: %lf %lf %lf\n", iBin, bin->center, bin->count, bin->response);
      iBin++;
      bin = bin->nextBin;
   }
}
}
