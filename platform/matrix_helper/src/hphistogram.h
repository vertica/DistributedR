/*
 * Author: Hao Peng (penghao888@gmail.com)
 */

#ifndef _hphistogram_h
#define _hphistogram_h

/*
 * A doublely linked list
 */
typedef struct hpBin_s 
{
   struct hpBin_s *nextBin; // pointer to the next bin
   struct hpBin_s *prevBin; // pointer to the last bin
   double center; // center of the bin
   double count; // count of the bin
   double response; // cumulative response of the bin
   int leftSpace; // the space to the left of the bin
   int rightSpace; // the space to the right of the bin
} hpBin;

/*
 * Structure of the heap element to represents the space
 * between each two adjacent bins
 */
typedef struct hpSpace_s
{
   double length; // length of the space
   hpBin *leftBin; // the pointer to the bin to the left of the space
   hpBin *rightBin; // the pointer to the bin to the right of the space
} hpSpace;

/*
 * Structure for a histogram
 * The space and bins are located as:
 * bin_1 space_1 bin_2 space_2 ... bin_b
 */
typedef struct hpHistogram_s 
{
   int maxNBins; // maximum number of bins
   // structure for spaces (heap)
   int nSpaces; // number of spaces between bins
   hpSpace *spaces;

   // structure for bins (doublely linked list)
   int nBins; // number of bins
   hpBin *bins;
   hpBin *nextFreeBin;

} hpHistogram;

/*
 * Reset the stracutre for a histogram
 */
void hpResetHistogram(hpHistogram *hist);

/*
 * Allocate the structure for a histogram
 */
hpHistogram* hpAllocateHistogram(int maxNBins);

/*
 * Free the strcutre for a histogram
 */
void hpFreeHistogram(hpHistogram *hist);

/*
 * Merge the closest two bins.
 * Do nothing if the number of bins is smaller than 2.
 */
void hpMergeBins(hpHistogram *hist);

/*
 * Insert a bin, assuming that the bins are inserted in order
 * Merge once if the number of bins becomes more than the limit after insertion
 */
void hpInsertBin(hpHistogram *hist, // histogram to be inserted
                 double center, // center of the new bin
                 double count, // count of the new bin
                 double response); //response of the new bin

/*
 * Convert the histogram to an array
 */
void hpSerializeHistogram(hpHistogram *hist, 
                          double *dest); // the array to be written to

void hpPrintHistogram(hpHistogram *hist);
#endif
