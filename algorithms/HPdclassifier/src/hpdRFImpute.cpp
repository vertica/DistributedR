#include"hpdRF.hpp"

hpdRFnode** treeTraverseObservation(hpdRFnode* tree, SEXP observations,
				    int *feature_cardinality, int obs_index,
				    bool na_pass, int* leaf_count, 
				    double** weight);


template <typename obs_type>
void registerTreeIndices(hpdRFnode* tree, obs_type* feature_observations, bool categorical)
{
  bool left;
  hpdRFNodeInfo *leaf;
  int obs_index;
  for(int i = 0; i < tree->additional_info->num_obs; i++)
    {
      obs_index = tree->additional_info->indices[i]-1;
      left = false;
      if(categorical)
	for(int j = 0; j < tree->split_criteria_length; j++)
	  if(tree->split_criteria[j] == feature_observations[obs_index])
	    left = true;
      else
	left = feature_observations[obs_index] < tree->split_criteria[0];
      leaf = left? tree->left->additional_info: tree->right->additional_info;
      leaf->indices[leaf->num_obs++] = obs_index+1;
    }
  cleanSingleNode(tree);
  return;
}

void registerTreeObservations(hpdRFnode* tree, SEXP new_data, 
			 int* feature_cardinality)
{
  if(tree->split_criteria == NULL)
    return;
  bool categorical = feature_cardinality[tree->split_variable-1]==NA_INTEGER;
  SEXP R_feature_observations = VECTOR_ELT(new_data,tree->split_variable-1);
  bool int_data = TYPEOF(VECTOR_ELT(new_data,tree->split_variable-1)) == INTSXP;
  void* feature_observations = RtoCArray<void*>(R_feature_observations);

  tree->left->additional_info = (hpdRFNodeInfo*) malloc(sizeof(hpdRFNodeInfo));
  tree->right->additional_info = (hpdRFNodeInfo*) malloc(sizeof(hpdRFNodeInfo));
  tree->left->additional_info->indices=
    (int*)malloc(sizeof(int)*tree->additional_info->num_obs);
  tree->right->additional_info->indices=
    (int*)malloc(sizeof(int)*tree->additional_info->num_obs);
  tree->left->additional_info->num_obs = 0;
  tree->right->additional_info->num_obs = 0;
  tree->left->additional_info->weights = NULL;
  tree->right->additional_info->weights = NULL;

  if(int_data)
    registerTreeIndices(tree, (int*) feature_observations, categorical);
  else
    registerTreeIndices(tree, (double*) feature_observations, categorical);

  registerTreeObservations(tree->left,new_data,feature_cardinality);
  registerTreeObservations(tree->right,new_data,feature_cardinality);
  return;
}



extern "C"
{

  SEXP registerObservations(SEXP R_forest, SEXP new_data)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int num_obs = length(VECTOR_ELT(new_data,0));

    for(int i = 0; i < forest->ntree; i++)
      {
	forest->trees[i]->additional_info = (hpdRFNodeInfo*)
	  malloc(sizeof(hpdRFNodeInfo));
	forest->trees[i]->additional_info->indices = (int*)
	  malloc(sizeof(int)*num_obs);

	for(int j = 0; j < num_obs; j++)
	  forest->trees[i]->additional_info->indices[j] = j+1;
	forest->trees[i]->additional_info->num_obs = num_obs;
	forest->trees[i]->additional_info->weights = NULL;

	registerTreeObservations(forest->trees[i], 
				 new_data, 
				 forest->features_cardinality);
      }
    return R_NilValue;
  }
  
  SEXP imputeObservations(SEXP R_forest, SEXP registered_data, SEXP new_data)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int temp_leaf_count, leaf_count=0, num_obs = length(VECTOR_ELT(new_data,0));
    hpdRFnode **temp_leaves, **leaves = NULL;
    void **new_feature_observations = 
      (void **) malloc(sizeof(void*)*length(new_data));
    bool* new_int_data = (bool *) malloc(sizeof(bool)*length(new_data));
    void **old_feature_observations = 
      (void **) malloc(sizeof(void*)*length(registered_data));
    bool* old_int_data = (bool *) malloc(sizeof(bool)*length(registered_data));
    double *temp_weights, *weights;
    for(int col = 0; col < length(new_data); col++)
      {
	new_feature_observations[col] = 
	  RtoCArray<void *>(VECTOR_ELT(new_data,col));
	new_int_data[col] = TYPEOF(VECTOR_ELT(new_data,col)) == INTSXP;
      }
    for(int col = 0; col < length(registered_data); col++)
      {
	old_feature_observations[col] = 
	  RtoCArray<void *>(VECTOR_ELT(registered_data,col));
	old_int_data[col] = TYPEOF(VECTOR_ELT(registered_data,col)) == INTSXP;
      }


    for(int obs_index = 0; obs_index < num_obs; obs_index++)
      {
	for(int i = 0; i < forest->ntree; i++)
	  {
	    temp_leaf_count = 0;
	    temp_leaves=
	      treeTraverseObservation(forest->trees[i], 
				      new_data,
				      forest->features_cardinality, 
				      obs_index,
				      true, 
				      &temp_leaf_count, &temp_weights);
	    hpdRFnode** temp = (hpdRFnode**) 
	      malloc(sizeof(hpdRFnode*)*(temp_leaf_count+leaf_count));
	    double* temp1 = (double *) 
	      malloc(sizeof(double)*(temp_leaf_count+leaf_count));

	    double total_tree_weight = 0;
	    for(int j = 0; j < temp_leaf_count; j++)
	      total_tree_weight += temp_leaves[j]->additional_info->num_obs;
	    for(int j = 0; j < temp_leaf_count; j++)
	      temp_weights[j] = temp_leaves[j]->additional_info->num_obs/
		total_tree_weight;

	    if(leaf_count != 0)
	      {
		memcpy(temp,leaves,leaf_count*sizeof(hpdRFnode*));
		memcpy(temp1,weights, leaf_count*sizeof(double));
	      }
	    if(temp_leaf_count != 0)
	      {
		memcpy(temp+leaf_count,temp_leaves,
		       temp_leaf_count*sizeof(hpdRFnode*));
		memcpy(temp1+leaf_count,temp_weights,
		       temp_leaf_count*sizeof(double));

	      }
	    free(temp_leaves);
	    free(leaves);
	    free(weights);
	    free(temp_weights);
	    leaves = temp;
	    weights = temp1;
	    leaf_count += temp_leaf_count;

	  }
	
	for(int i = 0; i < leaf_count; i++)
	  if(isnan(weights[i]))
	    weights[i] = 0;
	

	double sample_id = forest->ntree*((double)rand()/(double)RAND_MAX);
	int i = 0;

	while(i < leaf_count)
	  {
	    if(sample_id >= weights[i])
	      sample_id -= weights[i];
	    else
	      break;
	    i++;
	  }
	if(i < leaf_count && leaves[i]->additional_info->num_obs > 0)
	  {
	    int index = (int) (sample_id*leaves[i]->additional_info->num_obs);
	    index = leaves[i]->additional_info->indices[index]-1;
	    for(int col = 0; col < length(new_data); col++)
	      {
		if(new_int_data[col] && old_int_data[col])
		  {
		    ((int **) new_feature_observations)[col][obs_index] = 
		      ((int **) old_feature_observations)[col][index];
		  }
		if(new_int_data[col] && !old_int_data[col])
		  {
		    ((int **) new_feature_observations)[col][obs_index] = 
		      ((double **) old_feature_observations)[col][index];
		  }
		if(!new_int_data[col] && old_int_data[col])
		  {
		    ((double **) new_feature_observations)[col][obs_index] = 
		      ((int **) old_feature_observations)[col][index];
		  }
		if(!new_int_data[col] && !old_int_data[col])
		  {
		    ((double **) new_feature_observations)[col][obs_index] = 
		      ((double **) old_feature_observations)[col][index];
		  }
	      }
	  }
	free(leaves);
	leaves = NULL;
	leaf_count = 0;
	free(weights);
	weights = NULL;
      }
    return R_NilValue;
  }
}
