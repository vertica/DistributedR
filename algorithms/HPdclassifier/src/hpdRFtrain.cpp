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
			SEXP R_max_nodes, SEXP R_tree_ids);
  SEXP buildHistograms(SEXP R_observations, SEXP R_responses, 
		       SEXP R_forest,
		       SEXP R_active_nodes, SEXP R_random_features,
		       SEXP histograms);
  SEXP computeSplits(SEXP R_histograms, SEXP R_active_nodes, 
		     SEXP R_features_cardinality, 
		     SEXP R_response_cardinality, 
		     SEXP R_bin_num, SEXP old_splits_info, SEXP R_cp);
  SEXP applySplits(SEXP R_forest, SEXP R_splits_info, SEXP R_active_nodes);
  SEXP updateNodes(SEXP R_observations, SEXP R_responses, 
		   SEXP R_forest, SEXP R_active_nodes, SEXP R_splits_info,
		   SEXP R_max_depth);

  SEXP cleanForest(SEXP R_forest, SEXP R_responses);
  SEXP getForestParameters(SEXP R_forest);
  

  SEXP hpdRF_local(SEXP observations, SEXP responses, SEXP ntree, SEXP bin_max,
		   SEXP features_cardinality, SEXP response_cardinality, 
		   SEXP features_num, SEXP node_size, SEXP weights,
		   SEXP observation_indices, 
		   SEXP features_min, SEXP features_max,
		   SEXP max_nodes, SEXP tree_ids, SEXP max_nodes_per_iteration,
		   SEXP trace, SEXP scale, SEXP max_time, 
		   SEXP R_cp, SEXP R_max_depth, SEXP R_min_count,
		   SEXP R_random_seed)
  {
    srand(INTEGER(R_random_seed)[0]);
    SEXP R_forest;
    printf("initializing forest\n");
    PROTECT(R_forest=initializeForest(observations, responses, ntree, bin_max,
				      features_min, features_max, 
				      features_cardinality, 
				      response_cardinality,
				      features_num, weights, 
				      observation_indices,
				      scale, max_nodes, tree_ids));
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
	      int* features_permutation=(int *)malloc(nFeatures*sizeof(int));
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
						 splits_info, R_cp));
	UNPROTECT_PTR(forestparam);
	if(splits_info != R_NilValue)
	  UNPROTECT_PTR(splits_info);
	splits_info = temp_splits_info;
	SEXP temp_active_nodes;
	printf("applying splits\n");
	PROTECT(temp_active_nodes = applySplits(R_forest,splits_info,active_nodes));
	UNPROTECT_PTR(active_nodes);
	active_nodes = temp_active_nodes;
	
	printf("updating nodes\n");
	updateNodes(observations, responses, R_forest, 
		    active_nodes, splits_info, R_max_depth);

	UNPROTECT_PTR(active_nodes);
	PROTECT(active_nodes = allocVector(INTSXP,forest->nleaves));
	int num_active_nodes = 0;

	
	SEXP bad_nodes;
	PROTECT(bad_nodes = allocVector(INTSXP,forest->nleaves));
	for(int i = 0; i < forest->nleaves; i++)
	  if(forest->leaf_nodes[i]->additional_info->num_obs < min_count)
	      INTEGER(bad_nodes)[num_active_nodes++] = i+1;
	SETLENGTH(bad_nodes, num_active_nodes);
	undoSplits(R_forest,bad_nodes);
	UNPROTECT_PTR(bad_nodes);
	
	num_active_nodes = 0;

	for(int i = 0; i < forest->nleaves; i++)
	  if(!forest->leaf_nodes[i]->additional_info->attempted &&
	     forest->leaf_nodes[i]->additional_info->num_obs > 
	     INTEGER(node_size)[0] &&
	     forest->leaf_nodes[i]->additional_info->depth < max_depth)
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
    return R_forest;
  }

}
