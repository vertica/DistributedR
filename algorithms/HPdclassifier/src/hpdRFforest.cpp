#include"hpdRF.hpp"


void destroyForest(SEXP R_forest)
{
  if(R_forest == R_NilValue)
    return;
  hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
  if(forest == NULL)
    return;
  if(forest->trees != NULL)
    {
      for(int i = 0; i < forest->ntree; i++)
	destroyTree(forest->trees[i]);
      free(forest->trees);
      if(forest->leaf_nodes != NULL)
	free(forest->leaf_nodes);
      forest->trees = NULL;
      forest->leaf_nodes = NULL;
    }
  if(forest->features_cardinality != NULL)
    {
      free(forest->features_cardinality);
      forest->features_cardinality = NULL;
    }
  if(forest->features_min != NULL)
    {
      free(forest->features_min);
      forest->features_min = NULL;
    }
  if(forest->features_max != NULL)
    {
      free(forest->features_max);
      forest->features_max = NULL;
    }
  if(forest->bin_num != NULL)
    {
      free(forest->bin_num);
      forest->bin_num = NULL;
    }
  if(forest->max_nodes != NULL)
    {
      free(forest->max_nodes);
      forest->max_nodes = NULL;
    }
  forest = NULL;
}


/*
 This function updates the leaf nodes with predictions 
 @param R_responses - observations of the response variable
 @param R_node - node to update 
 @param response_cardinality - number of classes in response or NA if numerical
 */
template<typename resp_type>
void updateLeafNodeWithPredictions(SEXP R_responses, hpdRFnode *node, 
				   int response_cardinality)
{
  int* indices = node->additional_info->indices;
  int num_obs = node->additional_info->num_obs;
  resp_type* responses = RtoCArray<resp_type*>(VECTOR_ELT(R_responses,0));
  double* weights = node->additional_info->weights;
  if(response_cardinality != NA_INTEGER)
    {
      double* predictions = 
	(double*)malloc(sizeof(double)* response_cardinality);
      memset(predictions,0,sizeof(double)* response_cardinality);
      for(int i = 0; i < num_obs; i++)
	predictions[(int)(responses[indices[i]-1]-1)]+=weights[i];
      int bin=0;
      int max=0;
      for(int i = 0; i < response_cardinality; i++)
	if(predictions[i] > max)
	  {
	    bin = i;
	    max = predictions[i];
	  }
      node->prediction = bin+1;
      free(predictions);
    }
  else
    {
      double L0=0,L1=0;
      for(int i = 0; i < num_obs; i++)
	{
	  L1 += responses[indices[i]-1]*weights[i];
	  L0 += weights[i];
	}
      node->prediction = L1/L0;
    }
}


extern "C" 
{


  /*
   this function initializes the forest 
   @param R_observations - dataframe of observations of feature vectors
   @param R_responses - dataframe of response variable
   @param R_ntree - number of trees to initialize
   @param bin_max - maximum number of bins to use
   @param features_min - the minimum value of features 
   @param features_max - the maximum value of features
   @param features_cardinality - number of classes for each feature
   @param response_cardinality - number of classes for the response
   @param R_features_num - number of features to choose 
   @param R_weights - weights of observations
   @param R_observation_indices - indices of observations in their node
   @param R_scale - flag deciding to scale variables
   */
  SEXP initializeForest(SEXP R_observations, SEXP R_responses, 
			SEXP R_ntree, SEXP bin_max,
			SEXP features_min, SEXP features_max, 
			SEXP features_cardinality, 
			SEXP response_cardinality, 
			SEXP R_features_num, SEXP R_weights, 
			SEXP R_observation_indices, SEXP R_scale,
			SEXP R_max_nodes, SEXP R_tree_ids)
  {
    hpdRFforest *forest = (hpdRFforest *) malloc(sizeof(hpdRFforest));
    SEXP R_forest = PROTECT(R_MakeExternalPtr(forest, R_NilValue, R_NilValue));

    forest->ntree = INTEGER(R_ntree)[0];
    forest->nleaves = forest->ntree;
    forest->trees = (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->ntree));
    forest->leaf_nodes = (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->ntree));
    forest->max_nodes = (int *) malloc(sizeof(int)*(forest->ntree));
    forest->features_num = INTEGER(R_features_num)[0];
    forest->nrow = length(VECTOR_ELT(R_responses,0));
    forest->nfeature = length(R_observations);
    forest->bin_num = (int *) malloc(sizeof(int)*(forest->nfeature)); 
    forest->features_min = (double *) malloc(sizeof(double)*(forest->nfeature));
    forest->features_max = (double *) malloc(sizeof(double)*(forest->nfeature));
    memcpy(forest->features_min, REAL(features_min), sizeof(double)*(forest->nfeature));
    memcpy(forest->features_max, REAL(features_max), sizeof(double)*(forest->nfeature));
    forest->features_cardinality = (int *) malloc(sizeof(int)*(forest->nfeature));
    memcpy(forest->features_cardinality,INTEGER(features_cardinality), 
	   sizeof(int)*(forest->nfeature));
    forest->response_cardinality = INTEGER(response_cardinality)[0];

    for(int i = 0; i < forest->ntree; i ++)
      {
	if(length(R_max_nodes) > 1)
	  forest->max_nodes[i] = INTEGER(R_max_nodes)[i];
	else
	  forest->max_nodes[i] = INTEGER(R_max_nodes)[0];
	SEXP weight = R_weights == R_NilValue ? 
	  R_NilValue: VECTOR_ELT(R_weights,i);
	SEXP observation_indices = R_observation_indices == R_NilValue ? 
	  R_NilValue: VECTOR_ELT(R_observation_indices,i);

	forest->trees[i] = createChildNode(NULL,  
					   TRUE,INTEGER(observation_indices), 
					   REAL(weight), length(weight),
					   forest->features_num);

	forest->leaf_nodes[i] = forest->trees[i];
	hpdRFnode* tree = forest->trees[i];

	if(R_tree_ids == R_NilValue)
	  tree->treeID = i+1;
	else
	  tree->treeID = INTEGER(R_tree_ids)[i];
	tree->additional_info->leafID = i+1;
      }


    for(int i = 0; i < forest->nfeature; i++)
	if(INTEGER(features_cardinality)[i] == NA_INTEGER)
	  forest->bin_num[i] = INTEGER(bin_max)[0];
	else
	  forest->bin_num[i] = INTEGER(features_cardinality)[i];

    formatObservationFeatures(R_observations,features_min, features_max, 
			      forest->bin_num, INTEGER(R_scale)[0] == 1);

    R_RegisterCFinalizerEx(R_forest, destroyForest, TRUE);
    UNPROTECT(1);
    return(R_forest);
  }


  SEXP printForest(SEXP R_forest, SEXP R_max_depth, SEXP classes)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int max_depth = 5;
    if(R_max_depth != R_NilValue)
      max_depth = INTEGER(R_max_depth)[0];
    for(int i = 0; i < forest->ntree; i++)
      {
	printf("Tree %d:\n", i+1);
	printNode(forest->trees[i],1,max_depth, classes);
      }
    for(int i = 0; i < forest->nleaves; i++)
      {
	printf("Leaf %d:\n", i+1);
	printNode(forest->leaf_nodes[i],1, max_depth, classes);
      }
  }

  SEXP getMaxNodes(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP max_nodes;
    PROTECT(max_nodes = allocVector(INTSXP,forest->ntree));
    memcpy(INTEGER(max_nodes),forest->max_nodes,forest->ntree*sizeof(int));
    UNPROTECT(1);
    return max_nodes;
  }
  SEXP getForestParameters(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP output;
    PROTECT(output = allocVector(VECSXP,7));
    SEXP features_cardinality;
    PROTECT(features_cardinality = allocVector(INTSXP,forest->nfeature));
    memcpy(INTEGER(features_cardinality),forest->features_cardinality,
	   forest->nfeature*sizeof(int));
    SET_VECTOR_ELT(output,0,features_cardinality);
    SEXP response_cardinality = ScalarInteger(forest->response_cardinality);
    SET_VECTOR_ELT(output,1,response_cardinality);
    SEXP features_num = ScalarInteger(forest->features_num);
    SET_VECTOR_ELT(output,2,features_num);
    SEXP features_min;
    PROTECT(features_min = allocVector(REALSXP,forest->nfeature));
    memcpy(REAL(features_min),forest->features_min,
	   forest->nfeature*sizeof(double));
    SET_VECTOR_ELT(output,3,features_min);
    SEXP features_max;
    PROTECT(features_max = allocVector(REALSXP,forest->nfeature));
    memcpy(REAL(features_max),forest->features_max,
	   forest->nfeature*sizeof(double));
    SET_VECTOR_ELT(output,4,features_max);
    SEXP bin_num;
    PROTECT(bin_num = allocVector(INTSXP,forest->nfeature));
    memcpy(INTEGER(bin_num),forest->bin_num,
	   forest->nfeature*sizeof(int));
    SET_VECTOR_ELT(output,5,bin_num);
    SEXP ntree = ScalarInteger(forest->ntree);
    SET_VECTOR_ELT(output,6,ntree);
    UNPROTECT(5);
    return output;
  }

  SEXP getAttemptedNodes(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP attempted;
    PROTECT(attempted = allocVector(INTSXP,forest->nleaves));
    for(int i = 0; i < forest->nleaves; i++)
      INTEGER(attempted)[i] = forest->leaf_nodes[i]->additional_info->attempted;
    UNPROTECT(1);
    return attempted;
  }

  SEXP getLeafWeights(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP weights,temp;
    PROTECT(weights = allocVector(VECSXP, forest->nleaves));
    for(int i = 0; i < forest->nleaves; i++)
      {
	PROTECT(temp = allocVector(REALSXP,
				   forest->leaf_nodes[i]->
				   additional_info->num_obs));
	memcpy(REAL(temp),forest->leaf_nodes[i]->additional_info->weights,
	       forest->leaf_nodes[i]->additional_info->num_obs*sizeof(double));
	SET_VECTOR_ELT(weights,i,temp);
	UNPROTECT(1);
      }
    UNPROTECT(1);
    return weights;
  }
  SEXP getLeafIndices(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP indices,temp;
    PROTECT(indices = allocVector(VECSXP, forest->nleaves));
    for(int i = 0; i < forest->nleaves; i++)
      {
	PROTECT(temp = allocVector(INTSXP,
				   forest->leaf_nodes[i]->
				   additional_info->num_obs));
	memcpy(INTEGER(temp),forest->leaf_nodes[i]->additional_info->indices,
	       forest->leaf_nodes[i]->additional_info->num_obs*sizeof(int));
	SET_VECTOR_ELT(indices,i,temp);
	UNPROTECT(1);
      }
    UNPROTECT(1);
    return indices;
  }

  SEXP numLeafNodes(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    return ScalarInteger(forest->nleaves);
  }
  SEXP getTreeIDs(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP treeIDs;
    PROTECT(treeIDs = allocVector(INTSXP, forest->nleaves));
    
    for(int i = 0; i < forest->nleaves; i++)
	INTEGER(treeIDs)[i] = forest->leaf_nodes[i]->treeID;

    UNPROTECT(1);
    return treeIDs;
  }
  /*
   this function cleans the forest by removing the unecessary fields
   @param R_forest - forest to clean
   @param R_responses - dataframe of response variable
   @return - nothing
   */
  SEXP cleanForest(SEXP R_forest, SEXP R_responses)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);

    int* features_categorical = forest->features_cardinality;
    int response_cardinality = forest->response_cardinality;
    double* features_min = forest->features_min;
    double* features_max = forest->features_max;
    int* bin_num = forest->bin_num;
    
    hpdRFnode** leaf_nodes = forest->leaf_nodes;
    for(int i = 0; i < forest->nleaves; i++)
      {
	if(TYPEOF(VECTOR_ELT(R_responses,0)) == INTSXP)
	  updateLeafNodeWithPredictions<int>(R_responses, 
					     forest->leaf_nodes[i],
					     forest->response_cardinality);
	else if(TYPEOF(VECTOR_ELT(R_responses,0)) == REALSXP)
	  updateLeafNodeWithPredictions<double>(R_responses, 
						forest->leaf_nodes[i],
						forest->response_cardinality);
	
	
	cleanSingleNode(forest->leaf_nodes[i]);
      }
    return R_NilValue;
  }


  SEXP serializeForest(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP R_buffer;
    int buffer_size;
    PROTECT(R_buffer = allocVector(VECSXP,forest->ntree+1));
    for(int i =0; i < forest->ntree; i++)
      {
	buffer_size = calculateBufferSize(forest->trees[i]);
	SEXP tree_buffer;
	PROTECT(tree_buffer = allocVector(INTSXP,buffer_size/sizeof(int) + 1));
	SET_VECTOR_ELT(R_buffer,i+1,tree_buffer);
	UNPROTECT(1);
	serializeTree(forest->trees[i], INTEGER(tree_buffer));
      }

    buffer_size = sizeof(int)*(6 + forest->ntree + forest->nfeature*2)
      + sizeof(double)*(forest->nfeature*2);
    SEXP meta_data_buffer;
    PROTECT(meta_data_buffer = 
	    allocVector(INTSXP,buffer_size/sizeof(int) + 1));
    SET_VECTOR_ELT(R_buffer,0,meta_data_buffer);
    UNPROTECT(1);
    int* buffer = INTEGER(VECTOR_ELT(R_buffer,0));
    *(buffer++) = forest->ntree;
    *(buffer++) = forest->response_cardinality;
    *(buffer++) = forest->features_num;
    *(buffer++) = forest->nfeature;
    *(buffer++) = forest->nrow;
    *(buffer++) = forest->nleaves;
    for(int i = 0; i < forest->ntree; i++)
      *(buffer++) = forest->max_nodes[i];

    for(int i = 0; i < forest->nfeature; i++)
      *(buffer++) = forest->bin_num[i];

    for(int i = 0; i < forest->nfeature; i++)
      *(buffer++) = forest->features_cardinality[i];

    double* temp = (double *) buffer;
    for(int i = 0; i < forest->nfeature; i++)
      *(temp++) = forest->features_min[i];
    for(int i = 0; i < forest->nfeature; i++)
      *(temp++) = forest->features_max[i];
    buffer = (int *) temp;
    
    UNPROTECT(1);
    return R_buffer;
  }
  
  SEXP unserializeForest(SEXP R_buffer)
  {
    int* buffer = INTEGER(VECTOR_ELT(R_buffer,0));
    hpdRFforest *forest = (hpdRFforest *) malloc(sizeof(hpdRFforest));
    SEXP R_forest = PROTECT(R_MakeExternalPtr(forest, R_NilValue, R_NilValue));

    forest->ntree = *(buffer++);
    forest->response_cardinality = *(buffer++);
    forest->features_num = *(buffer++);
    forest->nfeature = *(buffer++);
    forest->nrow = *(buffer++);
    forest->nleaves = *(buffer++);

    forest->max_nodes = (int *)malloc(sizeof(int)*forest->ntree);
    for(int i = 0; i < forest->ntree; i++)
      forest->max_nodes[i] = *(buffer++); 

    forest->bin_num = (int *)malloc(sizeof(int)*forest->nfeature);
    for(int i = 0; i < forest->nfeature; i++)
      forest->bin_num[i] = *(buffer++);

    forest->features_cardinality = (int *) malloc(sizeof(int)*forest->nfeature);
    for(int i = 0; i < forest->nfeature; i++)
      forest->features_cardinality[i] = *(buffer++);

    double* temp = (double *) buffer;
    forest->features_min = (double *)malloc(sizeof(double)*forest->nfeature);
    for(int i = 0; i < forest->nfeature; i++)
      forest->features_min[i] = *(temp++);
    forest->features_max = (double *)malloc(sizeof(double)*forest->nfeature);
    for(int i = 0; i < forest->nfeature; i++)
      forest->features_max[i] = *(temp++);
    buffer = (int *) temp;

    forest->trees = 
      (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->ntree));
    forest->leaf_nodes = 
      (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->nleaves));
    for(int i = 0; i < forest->ntree; i++)
      {
	if(VECTOR_ELT(R_buffer,i+1) != R_NilValue)
	  {
	    forest->trees[i] = (hpdRFnode *)malloc(sizeof(hpdRFnode)); 
	    buffer = unserializeTree(forest->trees[i], 
				     INTEGER(VECTOR_ELT(R_buffer,i+1)), 
				     forest->leaf_nodes);
	  }
	else
	  forest->trees[i] = NULL;
      }

    UNPROTECT(1);
    return R_forest;
  }

  SEXP stitchForest(SEXP R_forest, SEXP R_temp_forests, SEXP R_nodes)
  {
     hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
     int partitions = length(R_temp_forests);
     for(int i = 0; i < partitions; i++)
       {
	 hpdRFforest * temp_forest = 
	   (hpdRFforest *) R_ExternalPtrAddr(VECTOR_ELT(R_temp_forests,i));
	 int* nodes = INTEGER(VECTOR_ELT(R_nodes,i));
	 int num_nodes = length(VECTOR_ELT(R_nodes,i));
	 for(int j = 0; j < num_nodes; j++)
	   {
	     memcpy(forest->leaf_nodes[nodes[j]-1], temp_forest->trees[j],
		    sizeof(hpdRFnode));
	     forest->leaf_nodes[nodes[j]-1] = NULL;
	     forest->nleaves--;
	   }
       }
     return R_NilValue;
  }

  SEXP reformatForest(SEXP R_old_forest)
  {
    SEXP R_new_forest;
    SEXP nodestatus;
    SEXP bestvar;
    SEXP treemap;
    SEXP nodepred;
    SEXP xbestsplit;
    SEXP R_features_cardinality;    
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_old_forest);
    int index;
    int ntree = forest->ntree;
    int max_nodes=0;
    SEXP R_ndbigtree;
    PROTECT(R_ndbigtree = allocVector(INTSXP, forest->ntree));
    int* ndbigtree = INTEGER(R_ndbigtree);
    for(int i = 0; i < forest->ntree; i++ )
      {
	ndbigtree[i] = countSubTree(forest->trees[i]);
	if(max_nodes < ndbigtree[i])
	  max_nodes = ndbigtree[i];
      }

    PROTECT(R_new_forest = allocVector(VECSXP,10));
    PROTECT(nodestatus = allocVector(INTSXP,max_nodes*ntree));
    PROTECT(bestvar = allocVector(INTSXP,max_nodes*ntree));
    PROTECT(treemap = allocVector(INTSXP,max_nodes*2*ntree));
    PROTECT(nodepred = allocVector(REALSXP, max_nodes*ntree));
    PROTECT(xbestsplit = allocVector(REALSXP, max_nodes*ntree));
    PROTECT(R_features_cardinality = allocVector(INTSXP, forest->nfeature));
    memset(INTEGER(nodestatus),0,sizeof(int)*max_nodes*ntree);
    memset(INTEGER(bestvar),0,sizeof(int)*max_nodes*ntree);
    memset(INTEGER(treemap),0,sizeof(int)*2*max_nodes*ntree);
    memset(REAL(nodepred),0,sizeof(double)*max_nodes*ntree);
    memset(REAL(xbestsplit),0,sizeof(double)*max_nodes*ntree);

    int maxcat = 1;
    int *features_cardinality = INTEGER(R_features_cardinality);
    for(int i = 0; i < forest->nfeature;i++)
      {
	features_cardinality[i] = forest->features_cardinality[i];
	if(features_cardinality[i] != NA_INTEGER && 
	   features_cardinality[i] > maxcat)
	  maxcat = features_cardinality[i];
      }
    SET_VECTOR_ELT(R_new_forest,0, nodestatus);
    SET_VECTOR_ELT(R_new_forest,1, bestvar);
    SET_VECTOR_ELT(R_new_forest,2, treemap);
    SET_VECTOR_ELT(R_new_forest,3, nodepred);
    SET_VECTOR_ELT(R_new_forest,4, xbestsplit);
    SET_VECTOR_ELT(R_new_forest,5, ScalarInteger(max_nodes));
    SET_VECTOR_ELT(R_new_forest,6, ScalarInteger(ntree));
    SET_VECTOR_ELT(R_new_forest,7, R_features_cardinality);
    SET_VECTOR_ELT(R_new_forest,8, R_ndbigtree);
    SET_VECTOR_ELT(R_new_forest,9, ScalarInteger(maxcat));

    for(int i = 0; i < forest->ntree; i++)
      {
	index = 0;
	reformatTree(forest->trees[i],R_new_forest,&index, 
		     features_cardinality, max_nodes, i);
      }
    UNPROTECT(8);
    return R_new_forest;
  }

  SEXP garbageCollectForest(SEXP forest)
  {
    destroyForest(forest);
    return R_NilValue;
  }

  SEXP mergeCompletedForest(SEXP R_forest1, SEXP R_forest2)
  {
    hpdRFforest* forest1 = (hpdRFforest *) R_ExternalPtrAddr(R_forest1);
    hpdRFforest* forest2 = (hpdRFforest *) R_ExternalPtrAddr(R_forest2);
    hpdRFnode** trees = (hpdRFnode**) malloc(sizeof(hpdRFnode*)*
			       (forest1->ntree + forest2->ntree));
    int* max_nodes = (int *) malloc(sizeof(int)*
				    (forest1->ntree + forest2->ntree));
    memcpy(trees,forest1->trees,sizeof(hpdRFnode*)*(forest1->ntree));
    memcpy(trees+forest1->ntree,forest2->trees,
	   sizeof(hpdRFnode*)*(forest2->ntree));
    memcpy(max_nodes,forest1->max_nodes,sizeof(int)*(forest1->ntree));
    memcpy(max_nodes+forest1->ntree,forest2->max_nodes,
	   sizeof(int)*(forest2->ntree));
    free(forest1->max_nodes);
    free(forest2->max_nodes);
    free(forest1->trees);
    free(forest2->trees);
    
    if(forest2->leaf_nodes)
      free(forest2->leaf_nodes);
    free(forest2->features_cardinality);
    free(forest2->features_min);
    free(forest2->features_max);
    free(forest2->bin_num);
    

    forest1->trees = trees;
    forest1->ntree = forest1->ntree + forest2->ntree;
    forest1->max_nodes = max_nodes;
    forest2->trees = NULL;
    forest2->ntree = 0;
    forest2->max_nodes = NULL;
    forest2->features_cardinality = NULL;
    forest2->features_min = NULL;
    forest2->features_max = NULL;
    forest2->bin_num = NULL;
    forest2->leaf_nodes = NULL;

    return R_NilValue;
  }

  SEXP reformatLocalData(SEXP local_data)
  {
    SEXP observations_local;
    SEXP responses_local; 
    SEXP weights_local;
    SEXP observations_indices_local; 
    PROTECT(observations_local = allocVector(VECSXP,length(local_data)));
    PROTECT(responses_local = allocVector(VECSXP,length(local_data)));
    PROTECT(weights_local = allocVector(VECSXP,length(local_data)));
    PROTECT(observations_indices_local = 
	    allocVector(VECSXP,length(local_data)));

    for(int i = 0; i < length(local_data); i++)
      {
	SET_VECTOR_ELT(observations_local,i,
		       VECTOR_ELT(VECTOR_ELT(local_data,i),0));
	SET_VECTOR_ELT(responses_local,i,
		       VECTOR_ELT(VECTOR_ELT(local_data,i),1));
	SET_VECTOR_ELT(weights_local,i,
		       VECTOR_ELT(VECTOR_ELT(local_data,i),2));
	SET_VECTOR_ELT(observations_indices_local,i,
		       VECTOR_ELT(VECTOR_ELT(local_data,i),3));
      }
    SEXP new_local_data;
    PROTECT(new_local_data = allocVector(VECSXP,4));
    SET_VECTOR_ELT(new_local_data,0,observations_local);
    SET_VECTOR_ELT(new_local_data,1,responses_local);
    SET_VECTOR_ELT(new_local_data,2,weights_local);
    SET_VECTOR_ELT(new_local_data,3,observations_indices_local);
    UNPROTECT(5);
    return new_local_data;
  }

}

