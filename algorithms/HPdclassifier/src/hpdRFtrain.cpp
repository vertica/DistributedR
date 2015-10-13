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

#include "hpdRF.hpp"

extern "C"
{

  SEXP initializeForest(SEXP R_observations, SEXP R_responses, 
			SEXP R_ntree, SEXP bin_max,
			SEXP features_min, SEXP features_max, 
			SEXP features_cardinality, 
			SEXP response_cardinality, 
			SEXP R_features_num, SEXP R_weights, 
			SEXP R_observation_indices, SEXP R_scale,
			SEXP R_max_nodes, SEXP R_tree_ids,
			SEXP R_starting_depth);
  SEXP buildHistograms(SEXP R_observations, SEXP R_responses, 
		       SEXP R_forest,
		       SEXP R_active_nodes, SEXP R_random_features,
		       SEXP histograms);
  SEXP computeSplits(SEXP R_histograms, SEXP R_active_nodes, 
		     SEXP R_features_cardinality, 
		     SEXP R_response_cardinality, 
		     SEXP R_bin_num, SEXP old_splits_info, SEXP R_cp,
		     SEXP R_min_count);
  SEXP applySplits(SEXP R_forest, SEXP R_splits_info, SEXP R_active_nodes,
		   SEXP R_max_depth, SEXP R_summary_info);
  SEXP updateNodes(SEXP R_observations, SEXP R_responses, 
		   SEXP R_forest, SEXP R_active_nodes, SEXP R_splits_info,
		   SEXP R_max_depth);

  SEXP cleanForest(SEXP R_forest, SEXP R_responses);
  SEXP getForestParameters(SEXP R_forest);
  
  /* This is the main function that trains the forests locally
     @param observations - data.frame of observations
     @param responses - data.frame of responses with ncol = 1
     @param ntree - integer number of trees to build
     @param bin_max - an integer number for maximum number of bins to use
     @param features_cardinality - number of classes or NA for each feature
     @param response_cardinality - number of classes or NA for response
     @param features_num - number of random features to choose at each tree node
     @param node_size - minimum amount of observations to consider splitting
     @param weights - the weights of each observation
     @param observation_indices - indices of observations to use
     @param features_min - minimum of all features
     @param features_max - maximum of all features
     @param max_nodes - maximuim number of nodes to use in a given tree
     @param tree_ids - ids of each tree
     @param max_nodes_per_iteration - maximum number of nodes to process 
       in each iteration. This controls the amount of memory allocated
     @param trace - print out trace information
     @param scale - scale features from 0-bin_size again or previously done
     @param max_time - maximum number of seconds to allow training (approximate)
     @param R_cp - complexity parameter 
     @param R_max_depth - max depth of trees
     @param R_min_count - do not split node if child node has < min_cound obs
     @param R_starting_depth - the starting depth of the root tree nodes
     @param R_random_seed - seed to make results reproducible
     @param R_summary_info - if true, provide summary information for each node
   */
  SEXP hpdRF_local(SEXP observations, SEXP responses, SEXP ntree, SEXP bin_max,
		   SEXP features_cardinality, SEXP response_cardinality, 
		   SEXP features_num, SEXP node_size, SEXP weights,
		   SEXP observation_indices, 
		   SEXP features_min, SEXP features_max,
		   SEXP max_nodes, SEXP tree_ids, SEXP max_nodes_per_iteration,
		   SEXP trace, SEXP scale, SEXP max_time, 
		   SEXP R_cp, SEXP R_max_depth, SEXP R_min_count,
		   SEXP R_starting_depth, SEXP R_random_seed, 
		   SEXP R_summary_info)
  {
    printf("Training Function: \n");
    srand(INTEGER(R_random_seed)[0]);
    SEXP R_forest;
    printf("initializing forest\n");
    PROTECT(R_forest=initializeForest(observations, responses, ntree, bin_max,
				      features_min, features_max, 
				      features_cardinality, 
				      response_cardinality,
				      features_num, weights, 
				      observation_indices,
				      scale, max_nodes, tree_ids,
				      R_starting_depth));
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP hist = R_NilValue;
    SEXP splits_info = R_NilValue;
    SEXP active_nodes = R_NilValue;
    PROTECT(active_nodes = allocVector(INTSXP,forest->ntree));
    for(int i = 0; i < forest->ntree; i++)
      INTEGER(active_nodes)[i] = i+1;
    bool max_nodes_reached = true;
    for(int i = 0; i < forest->ntree; i++)
      if(forest->max_nodes[i] > 0)
	max_nodes_reached = false;
    int max_depth = INTEGER(R_max_depth)[0];
    int min_count = INTEGER(R_min_count)[0];

    int i = 0;
    time_t starttime;
    time(&starttime);
    time_t currtime;
    int* features_permutation=(int *)malloc(length(observations)*sizeof(int));

    while(!max_nodes_reached && length(active_nodes))
      {
	i++;
	if(length(active_nodes) > *INTEGER(max_nodes_per_iteration))
	  SETLENGTH(active_nodes,*INTEGER(max_nodes_per_iteration));

	SEXP random_features;

	PROTECT(random_features = allocVector(VECSXP,length(active_nodes)));
	for(int i = 0; i < length(active_nodes); i++)
	  {
	    SEXP features;
	    PROTECT(features = allocVector(INTSXP,forest->features_num));

	    int temp_feature;
	    int nFeatures = length(observations);
	    for(int j = 0; j < nFeatures; j++)
	      features_permutation[j] = j+1;
	    for(int j = nFeatures-1; j > 0; j--)
	      {
		int random_feature = rand()%j;
		temp_feature = features_permutation[j];
		features_permutation[j] = features_permutation[random_feature];
		features_permutation[random_feature] = temp_feature;
	      }
	    
	    for(int j = 0; j < forest->features_num; j++)
	      INTEGER(features)[j] = features_permutation[j];
	    SET_VECTOR_ELT(random_features,i,features);
	    UNPROTECT(1);
	  }


	printf("building histograms\n");
	SEXP temp_hist;
	PROTECT(temp_hist = buildHistograms(observations, responses,
				       R_forest, active_nodes,
				       random_features, hist));
	UNPROTECT_PTR(random_features);
	if(hist != R_NilValue)
	  UNPROTECT_PTR(hist);
	hist = temp_hist;


	SEXP forestparam;
	PROTECT(forestparam = getForestParameters(R_forest));
	
	printf("computing splits\n");
	SEXP temp_splits_info;
	PROTECT(temp_splits_info = computeSplits(hist, active_nodes,
					    VECTOR_ELT(forestparam,0),
					    VECTOR_ELT(forestparam,1),
					    VECTOR_ELT(forestparam,5),
						 splits_info, R_cp,
						 ScalarReal(min_count)));

	UNPROTECT_PTR(forestparam);
	if(splits_info != R_NilValue)
	  UNPROTECT_PTR(splits_info);
	splits_info = temp_splits_info;
	SEXP temp_active_nodes;


	printf("applying splits\n");
	PROTECT(temp_active_nodes = applySplits(R_forest,splits_info,
						active_nodes, 
						R_max_depth,
						R_summary_info));


	UNPROTECT_PTR(active_nodes);
	active_nodes = temp_active_nodes;

	
	printf("updating nodes\n");
	updateNodes(observations, responses, R_forest, 
		    active_nodes, splits_info, R_max_depth);

	UNPROTECT_PTR(active_nodes);
	PROTECT(active_nodes = allocVector(INTSXP,forest->nleaves));
	int num_active_nodes = 0;


	for(int i = 0; i < forest->nleaves; i++)
	  if(!forest->leaf_nodes[i]->additional_info->attempted &&
	     forest->leaf_nodes[i]->additional_info->num_obs > 
	     INTEGER(node_size)[0] &&
	     forest->leaf_nodes[i]->additional_info->depth <= max_depth+1)
	      INTEGER(active_nodes)[num_active_nodes++] = i+1;
	SETLENGTH(active_nodes,num_active_nodes);

	max_nodes_reached = true;
	for(int i = 0; i < forest->ntree; i++)
	  if(forest->max_nodes[i] > 0)
	    max_nodes_reached = false;

	time(&currtime);
	printf("time spent:%d/%d\n",(int)(currtime-starttime),INTEGER(max_time)[0] );
	
	if(INTEGER(max_time)[0] > -1 && 
	   currtime-starttime >= INTEGER(max_time)[0])
	  {
	    if(hist != R_NilValue)
	      UNPROTECT_PTR(hist);
	    if(splits_info != R_NilValue)
	      UNPROTECT_PTR(splits_info);
	    UNPROTECT_PTR(active_nodes);
	    UNPROTECT_PTR(R_forest);
	    free(features_permutation);
	    return R_forest;
	  }
      }
    
    printf("cleaning forest\n");
    cleanForest(R_forest,responses);
    if(hist != R_NilValue)
      UNPROTECT_PTR(hist);
    if(splits_info != R_NilValue)
      UNPROTECT_PTR(splits_info);
    UNPROTECT_PTR(active_nodes);
    UNPROTECT_PTR(R_forest);
    free(features_permutation);
    return R_forest;
  }

}
