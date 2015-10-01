/*
* Copyright [2014] Hewlett-Packard Development Company, L.P.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* as published by the Free Software Foundation; either version 2
* of the License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/
#ifndef HPDRF 
#define HPDRF 
#include <R.h>
#include <Rinternals.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<time.h>


struct HPDRFNode;

typedef struct 
{
  double wt;
  double complexity;
  double deviance;
  double* node_counts;
  int n;
  int node_counts_length;

} hpdRFSummaryInfo;

typedef struct 
{
  int* indices;
  double* weights;
  int attempted, completed;
  int leafID;
  int num_obs;
  int depth;
  struct HPDRFNode* parent;

} hpdRFNodeInfo;

typedef struct HPDRFNode
{
  double prediction;
  hpdRFNodeInfo* additional_info;
  struct HPDRFNode* left;
  struct HPDRFNode* right;
  hpdRFSummaryInfo* summary_info;
  double* split_criteria;
  int split_variable, split_criteria_length;
  int treeID;
} hpdRFnode;

typedef struct 
{
  hpdRFnode** trees;
  hpdRFnode** leaf_nodes;
  int* features_cardinality;
  double* features_min;
  double* features_max;
  int* bin_num;
  int* max_nodes;
  int response_cardinality;
  int ntree;
  int features_num;
  int nfeature;
  int nrow;
  int nleaves;
} hpdRFforest;



void formatObservationFeatures(SEXP R_observation_features, 
			       SEXP R_features_min, SEXP R_features_max, 
			       int* R_bin_num, bool scale);


hpdRFnode* createChildNode(hpdRFnode *parent,  
			   bool copy, int* child_node_observations, 
			   double* child_weights, int child_num_obs,
			   int features_num);

void cleanSingleNode(hpdRFnode *node);
SEXP printNode(hpdRFnode *tree, int depth, int max_depth, SEXP classes);
void destroyTree(hpdRFnode *tree);
long long calculateBufferSize(hpdRFnode* tree);
int* serializeTree(hpdRFnode *tree, int* buffer);
int* unserializeTree(hpdRFnode *tree, int* buffer, hpdRFnode**leaf_nodes);
int countSubTree(hpdRFnode *tree, int remaining_depth);
void convertTreeToRandomForest(hpdRFnode* tree, SEXP forest, int* index, 
		  int *features_cardinality, int nrow, int treeID);
void simplifyTree(hpdRFnode *tree);
extern "C"
{
void undoSplits(SEXP R_forest, SEXP R_node_ids);
void printForest(SEXP R_forest, SEXP R_max_depth, SEXP classes);
}


int convertTreetoRpart(hpdRFnode* tree, int* indices,
		       int* n, double* wt,
		       int* var, double* dev, 
		       double* yval, double* complexity, double* improve,
		       double* split_index, int* ncat, 
		       double* node_count, int offset, 
		       int rowID, double parent_cp, int node_index,
		       int* features_cardinality, int response_cardinality,
		       int* csplit_count,
		       int current_depth, int max_depth);


void populateCsplit(hpdRFnode* tree, int* features_cardinality, 
		    int* csplit_count, int* csplit, int nrow);




/*
 This function takes an array from R and converts it to C format by 
 returning a pointer to the actual data values
 @param R_object - R array to convert
 @return - pointer to C array with R data values
 */
template <typename returnval>
returnval RtoCArray(SEXP R_object)
{
  switch(TYPEOF(R_object))
    {
    case INTSXP:
      return (returnval) INTEGER(R_object);
    case REALSXP:
      return (returnval) REAL(R_object);
    default:
      return (returnval) LOGICAL(R_object);
    }
}


#endif
