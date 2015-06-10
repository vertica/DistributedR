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
#include"hpdRF.hpp"
#include<unistd.h>

/*
 This function classifies if a sample should go to left child or right child
 @param observation_feature_value - value of the feature for single observation
 @param split_criteria - the best split information
 @param feature_categorical - flag indicating the feature is categorical
 */
bool updateObservationInNode(int observation_feature_value, SEXP split_criteria,
			     bool feature_categorical)
{
  if(feature_categorical)
    {
      int *left_categories = INTEGER(split_criteria);
      for(int i = 0; i < length(split_criteria); i++)
	{
	  if(observation_feature_value == left_categories[i]-1)
	    return true;
	}
      return(false);
    }
  else
    {
      return observation_feature_value < INTEGER(split_criteria)[0];
    }
  return true;
}

/*
 this function updates all the observations in a single node
 @param R_observations_feature - observations of the feature vectors
 @param R_split_criteria - list of best splits
 @param node_observations - array of indices of observations in the node
 @param node_observations_num - number of observations in the node
 @param left_child_node_observations - observations going to left child
 @param right_child_node_observations - observations going to right child
 @param feature_categorical - flag indicating feature is categorical
 @param bin_num - number of bins for feature variable
 @param node_weights - list of weights for the node
 @param left_child_weights - weights of indices going to left child
 @param right_child_weights - weights of indices going to right child
 */
void updateNode(SEXP R_observations_feature,  
		SEXP R_split_criteria,
		int* node_observations, 
		int node_observations_num, 
		int **left_child_node_observations, 
		int **right_child_node_observations,
		bool feature_categorical, 
		int bin_num,
		double *node_weights,
		double **left_child_weights, 
		double **right_child_weights,
		int* left_child_num_obs,
		int* right_child_num_obs)
{
  int* observations_feature = INTEGER(R_observations_feature);
  int left_child_num = 0,right_child_num = 0;
  int* left_observations, *right_observations;
  double *left_weights, *right_weights;
  int observation_feature;
  double * weights = node_weights;
  *left_child_weights = (double *)malloc(sizeof(double)*node_observations_num);
  *right_child_weights = (double *)malloc(sizeof(double)*node_observations_num);
  *left_child_node_observations = (int *)malloc(sizeof(int)*node_observations_num);
  *right_child_node_observations = (int *)malloc(sizeof(int)*node_observations_num);


  left_observations = *left_child_node_observations;
  right_observations = *right_child_node_observations;
  left_weights = *left_child_weights;
  right_weights = *right_child_weights;
  for(int observation = 0; observation < node_observations_num; observation++)
    {
      int observation_index = node_observations[observation];
      if(observation_index > length(R_observations_feature))
	continue;
      observation_feature = observations_feature[observation_index-1];
      
      if(updateObservationInNode(observation_feature, R_split_criteria, feature_categorical))
	{
	  left_weights[left_child_num] = weights[observation];
	  left_observations[left_child_num++] = node_observations[observation];
	}
      else
	{
	  right_weights[right_child_num] = weights[observation];
	  right_observations[right_child_num++] = node_observations[observation];
	}
    }
  *left_child_num_obs = left_child_num;
  *right_child_num_obs = right_child_num;

}




extern "C" 
{
  /*
   This function updates the forest by splitting the nodes specified
   @param R_observations - observations of feature vectors
   @param R_responses - observations of response variable
   @param R_forest - forest 
   @param R_active_nodes - active nodes to update
   @param R_splits_info - best splits for all nodes 
   @return - count of observations in all leaf nodes
   */
  SEXP updateNodes(SEXP R_observations, SEXP R_responses, SEXP R_forest, SEXP R_active_nodes, SEXP R_splits_info)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);

    int* features_categorical = forest-> features_cardinality;
    int response_categorical = forest->response_cardinality;
    double* features_min = forest->features_min;
    double* features_max = forest->features_max;
    int* bin_num = forest->bin_num;
    int features_num = forest->features_num;
    int leaf_nodes = forest->nleaves -
      length(R_active_nodes) + 
      2*INTEGER(getAttrib(R_splits_info,install("total_completed")))[0];

    hpdRFnode **new_leaves = (hpdRFnode**)malloc(sizeof(hpdRFnode*)*leaf_nodes);

    SEXP R_node_counts;
    PROTECT(R_node_counts = allocVector(INTSXP,leaf_nodes));
    int *node_counts = INTEGER(R_node_counts);

    leaf_nodes = 0;
    int index = 0;
    for(int i  = 0; i < forest->nleaves;i++)
      {
	hpdRFnode* node_curr = forest->leaf_nodes[i];
	int next_active_node = index < length(R_active_nodes) ? 
	  INTEGER(R_active_nodes)[index]-1: -1;
	if(i == next_active_node) 
	  {
	    if(INTEGER(VECTOR_ELT(VECTOR_ELT(R_splits_info,index),0))[0]==1)
	      {
		int active_node = INTEGER(R_active_nodes)[index]-1;
		node_curr = forest->leaf_nodes[active_node];
		int* left_child_node_observations;
		int* right_child_node_observations;
		double* left_child_weights, *right_child_weights;
		int left_child_num_obs, right_child_num_obs;
		int* node_observations = node_curr->additional_info->indices;
		double* node_weights = node_curr->additional_info->weights;
		int node_observations_num = node_curr->additional_info->num_obs;
		int node_split_variable = INTEGER(VECTOR_ELT(VECTOR_ELT(R_splits_info,index),1))[0]-1;
		SEXP node_split_criteria = VECTOR_ELT(VECTOR_ELT(R_splits_info,index),2);
		updateNode(VECTOR_ELT(R_observations,node_split_variable),
			   node_split_criteria, node_observations, node_observations_num,
			   &left_child_node_observations,
			   &right_child_node_observations,
			   features_categorical[node_split_variable] != NA_INTEGER,
			   bin_num[node_split_variable],
			   node_weights,  &left_child_weights, &right_child_weights,
			   &left_child_num_obs, &right_child_num_obs);

		hpdRFnode *node_left_child = createChildNode(node_curr, FALSE,
						  left_child_node_observations, left_child_weights,
						  left_child_num_obs,features_num);
		hpdRFnode *node_right_child = createChildNode(node_curr, FALSE,
						   right_child_node_observations, right_child_weights,
						   right_child_num_obs,features_num);
		node_curr->left = node_left_child;
		node_curr->right = node_right_child;
		
		node_left_child->additional_info->leafID = leaf_nodes+1;
		node_counts[leaf_nodes] = node_left_child->additional_info->num_obs;
		new_leaves[leaf_nodes++] = node_left_child;

		node_right_child->additional_info->leafID = leaf_nodes+1;
		node_counts[leaf_nodes] = node_right_child->additional_info->num_obs;
		new_leaves[leaf_nodes++] = node_right_child;
	      }

	    cleanSingleNode(node_curr);
	    index ++;

	  }
	else if(i != next_active_node)
	  {
	    node_curr->additional_info->leafID = leaf_nodes+1;
	    node_counts[leaf_nodes] = node_curr->additional_info->num_obs;
	    new_leaves[leaf_nodes++]  = node_curr;
	  }
      }

    free(forest->leaf_nodes);
    forest->nleaves = leaf_nodes;
    forest->leaf_nodes = new_leaves;
    SETLENGTH(R_node_counts,leaf_nodes);
    UNPROTECT(1);
    return R_node_counts;
  }

}
