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

/* garbage collect forest
   @param R_forest - forest to garbage collect
 */
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
	if(forest->trees[i] != NULL)
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
      if(node->summary_info)
	{
	  node->summary_info->node_counts_length = response_cardinality;
	  if(!node->summary_info->node_counts)
	    node->summary_info->node_counts = (double *)
	      malloc(sizeof(double)*response_cardinality);
	  memcpy(node->summary_info->node_counts,predictions,
		 sizeof(double)*node->summary_info->node_counts_length);
	}
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
  if(node->summary_info)
    {
      node->summary_info->n = num_obs;
      node->summary_info->wt = 0;
      for(int i = 0; i < num_obs;i++)
	node->summary_info->wt += weights[i];
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
			SEXP R_max_nodes, SEXP R_tree_ids,
			SEXP R_starting_depth)
  {
    hpdRFforest *forest = (hpdRFforest *) malloc(sizeof(hpdRFforest));
    SEXP R_forest = PROTECT(R_MakeExternalPtr(forest, R_NilValue, R_NilValue));
    forest->ntree = INTEGER(R_ntree)[0];
    forest->nleaves = forest->ntree;
    forest->trees = (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->ntree));
    forest->leaf_nodes = 
      (hpdRFnode **) malloc(sizeof(hpdRFnode *)*(forest->ntree));
    forest->max_nodes = (int *) malloc(sizeof(int)*(forest->ntree));
    forest->features_num = INTEGER(R_features_num)[0];
    forest->nrow = length(VECTOR_ELT(R_responses,0));
    forest->nfeature = length(R_observations);
    forest->bin_num = (int *) malloc(sizeof(int)*(forest->nfeature)); 
    forest->features_min = (double *) malloc(sizeof(double)*(forest->nfeature));
    forest->features_max = (double *) malloc(sizeof(double)*(forest->nfeature));
    memcpy(forest->features_min, 
	   REAL(features_min), sizeof(double)*(forest->nfeature));
    memcpy(forest->features_max, 
	   REAL(features_max), sizeof(double)*(forest->nfeature));
    forest->features_cardinality = 
      (int *) malloc(sizeof(int)*(forest->nfeature));
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
	forest->trees[i]->additional_info->depth = INTEGER(R_starting_depth)[i];

	forest->leaf_nodes[i] = forest->trees[i];
	hpdRFnode* tree = forest->trees[i];

	if(R_tree_ids == R_NilValue)
	  tree->treeID = i+1;
	else if(length(R_tree_ids) == forest->ntree)
	  tree->treeID = INTEGER(R_tree_ids)[i];
	else
	  tree->treeID = INTEGER(R_tree_ids)[0];
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

  /*this function prints the forest
    @param R_forest - forest to print
    @param R_max_depth - maximum depth to print to
    @param classes - classes of output variable 
   */
  void printForest(SEXP R_forest, SEXP R_max_depth, SEXP classes)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int max_depth = 5;
    if(R_max_depth != R_NilValue)
      max_depth = INTEGER(R_max_depth)[0];
    for(int i = 0; i < forest->ntree; i++)
      {
	if(forest->trees[i] == NULL)
	  continue;
	printf("Tree %d:\n", i+1);
	printNode(forest->trees[i],1,max_depth, classes);
      }
    /*
    for(int i = 0; i < forest->nleaves; i++)
      {
	printf("Leaf %d:\n", i+1);
	printNode(forest->leaf_nodes[i],1, max_depth, classes);
      }
    */
  }

  /*
    get max nodes value for each tree. returns an array of remaining number of nodes taht can be allocated 
   */
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

    int count = 0;
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
    forest->nleaves = count;
    return R_NilValue;
  }


  SEXP serializeForest(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP R_buffer;
    long long buffer_size;
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
    for(int i = 0; i < forest->ntree; i++)
      *(buffer++) = forest->max_nodes[i];

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

    SEXP header = VECTOR_ELT(R_buffer,0);
    forest->max_nodes = (int *)malloc(sizeof(int)*forest->ntree);
    memset(forest->max_nodes,0,sizeof(int)*forest->ntree);
    for(int i = 0; i < forest->ntree && 
	  buffer-INTEGER(header)< length(header); i++)
      forest->max_nodes[i] = *(buffer++); 

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

  void stitchForest(SEXP R_forest, SEXP R_temp_forests, 
		    SEXP R_forest_nodes, SEXP R_temp_forest_nodes)
  {
     hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
     int partitions = length(R_temp_forests);

     for(int i = 0; i < partitions; i++)
       {
	 hpdRFforest * temp_forest = 
	   (hpdRFforest *) R_ExternalPtrAddr(VECTOR_ELT(R_temp_forests,i));
	 hpdRFnode **leaf_nodes = (hpdRFnode**)
	   malloc(sizeof(hpdRFnode*)*
		  (forest->nleaves+temp_forest->nleaves));  
	 memcpy(leaf_nodes,forest->leaf_nodes,
		sizeof(hpdRFnode*)*(forest->nleaves));
	 memcpy(leaf_nodes+forest->nleaves,temp_forest->leaf_nodes,
		sizeof(hpdRFnode*)*(temp_forest->nleaves));
	 
	 int* nodes = INTEGER(VECTOR_ELT(R_forest_nodes,i));
	 int* tree_nodes = INTEGER(VECTOR_ELT(R_temp_forest_nodes,i));
	 int num_nodes = length(VECTOR_ELT(R_forest_nodes,i));
	 for(int j = 0; j < num_nodes; j++)
	   {
	     cleanSingleNode(leaf_nodes[nodes[j]-1]);
	     memcpy(leaf_nodes[nodes[j]-1], 
		    temp_forest->trees[tree_nodes[j]-1],
		    sizeof(hpdRFnode));
	     temp_forest->trees[tree_nodes[j]-1] = NULL;
	     leaf_nodes[nodes[j]-1] = NULL;
	   }

	 free(forest->leaf_nodes);
	 forest->leaf_nodes = leaf_nodes;
	 forest->nleaves = forest->nleaves + temp_forest->nleaves;

       }
  }
  void removeNullLeaves(SEXP R_forest)
  {
     hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
     int valid = 0;
     for(int i = 0; i < forest->nleaves; i++)
       if(forest->leaf_nodes[i] != NULL)
	 valid++;
     hpdRFnode **leaf_nodes = (hpdRFnode**)malloc(sizeof(hpdRFnode*)*valid); 
     int j = 0;
     for(int i = 0; i < valid; i++)
       {
	 while(forest->leaf_nodes[j] == NULL)
	   j++;
	 leaf_nodes[i] = forest->leaf_nodes[j++];
       }
     free(forest->leaf_nodes);
     forest->leaf_nodes = leaf_nodes;
     forest->nleaves = valid;

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
	ndbigtree[i] = countSubTree(forest->trees[i], 30);
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
	convertTreeToRandomForest(forest->trees[i],R_new_forest,&index, 
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
    hpdRFnode** leaf_nodes = (hpdRFnode**) 
      malloc(sizeof(hpdRFnode*)* (forest1->nleaves + forest2->nleaves));
    if(forest1->leaf_nodes)
      memcpy(leaf_nodes,forest1->leaf_nodes,
	     sizeof(hpdRFnode*)*(forest1->nleaves));
    if(forest2->leaf_nodes)
      memcpy(leaf_nodes+forest1->nleaves,forest2->leaf_nodes,
	     sizeof(hpdRFnode*)*(forest2->nleaves));

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
    forest1->leaf_nodes = leaf_nodes;
    forest2->trees = NULL;
    forest2->ntree = 0;
    forest2->max_nodes = NULL;
    forest2->features_cardinality = NULL;
    forest2->features_min = NULL;
    forest2->features_max = NULL;
    forest2->bin_num = NULL;
    forest2->leaf_nodes = NULL;
    forest1->nleaves += forest2->nleaves;
    forest2->nleaves = 0;
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


  SEXP eliminateTreesFromModel(SEXP R_forest, SEXP R_treeIDs)
  {
    hpdRFforest* forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int* treeIDs = INTEGER(R_treeIDs);
    int* max_nodes = (int*)malloc(sizeof(int)*length(R_treeIDs));
    hpdRFnode** new_trees = (hpdRFnode**)
      malloc(sizeof(hpdRFnode*)*length(R_treeIDs));
    int j = 0;
    for(int i = 0; i < forest->ntree; i++)
      {
	if(treeIDs[j]-1 == i)
	  {
	    max_nodes[j] = forest->max_nodes[i];
	    new_trees[j++] = forest->trees[i];
	  }
	else
	  destroyTree(forest->trees[i]);
      }
    free(forest->trees);
    forest->trees = new_trees;
    free(forest->max_nodes);
    forest->max_nodes = max_nodes;
    forest->ntree = length(R_treeIDs);

    return R_NilValue;
  }
  SEXP getLeafDepths(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP depths;
    PROTECT(depths = allocVector(INTSXP,forest->nleaves));
    for(int i = 0 ; i < forest->nleaves; i++)
      {
	if(forest->leaf_nodes != NULL &&
	   forest->leaf_nodes[i] != NULL && 
	   forest->leaf_nodes[i]->additional_info != NULL)
	  INTEGER(depths)[i] = forest->leaf_nodes[i]->additional_info->depth;
	else
	  INTEGER(depths)[i] = 0;
      }
    UNPROTECT(1);
    return depths;

  }

  SEXP getLeafCounts(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP counts;
    PROTECT(counts = allocVector(INTSXP,forest->nleaves));
    for(int i = 0 ; i < forest->nleaves; i++)
      {
	if(forest->leaf_nodes != NULL &&
	   forest->leaf_nodes[i] != NULL && 
	   forest->leaf_nodes[i]->additional_info != NULL)
	  INTEGER(counts)[i] = forest->leaf_nodes[i]->additional_info->num_obs;
	else
	  INTEGER(counts)[i] = 0;
      }
    UNPROTECT(1);
    return counts;
  }
  SEXP getLeafIDs(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP leaf_ids;
    PROTECT(leaf_ids = allocVector(INTSXP,forest->nleaves));
    for(int i = 0 ; i < forest->nleaves; i++)
      {
	if(forest->leaf_nodes != NULL &&
	   forest->leaf_nodes[i] != NULL && 
	   forest->leaf_nodes[i]->additional_info != NULL)
	  INTEGER(leaf_ids)[i] = 
	    forest->leaf_nodes[i]->additional_info->leafID;
	else
	  INTEGER(leaf_ids)[i] = 0;
      }
    UNPROTECT(1);
    return leaf_ids;
  }
  SEXP getLeafTreeIDs(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP tree_ids;
    PROTECT(tree_ids = allocVector(INTSXP,forest->nleaves));
    for(int i = 0 ; i < forest->nleaves; i++)
      {
	if(forest->leaf_nodes != NULL &&
	   forest->leaf_nodes[i] != NULL)
	  INTEGER(tree_ids)[i] = forest->leaf_nodes[i]->treeID;
	else
	  INTEGER(tree_ids)[i] = 0;
      }
    UNPROTECT(1);
    return tree_ids;

  }

  void undoSplits(SEXP R_forest, SEXP R_node_ids)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int* node_ids = INTEGER(R_node_ids);
    hpdRFnode* parent;

    for(int i = 0; i < length(R_node_ids); i++)
      {
	if(node_ids[i]-1 >= forest->nleaves ||
	   node_ids[i]-1 < 0 ||
	   forest->leaf_nodes[node_ids[i]-1] == NULL)
	  continue;
	parent = forest->leaf_nodes[node_ids[i]-1]->additional_info->parent;
	if(parent == NULL)
	  continue;

	if(parent->left->additional_info)
	  {
	  forest->leaf_nodes[parent->left->additional_info->leafID-1] = NULL;
	  }
	if(parent->right->additional_info)
	  {
	  forest->leaf_nodes[parent->right->additional_info->leafID-1] = NULL;
	  }

	if(parent->left)
	  destroyTree(parent->left);
	if(parent->right)
	  destroyTree(parent->right);

	if(parent->split_criteria)
	  free(parent->split_criteria);
	parent->split_criteria = NULL;
	parent->split_criteria_length = 0;
	parent->split_variable = 0;
	if(parent->additional_info)
	  free(parent->additional_info);
	parent->additional_info = NULL;

	parent->left = NULL;
	parent->right = NULL;

      }

    int nleaves = 0;
    for(int i = 0; i < forest->nleaves; i++)
      if(forest->leaf_nodes[i] != NULL)
	{
	  forest->leaf_nodes[nleaves] = forest->leaf_nodes[i];
	  forest->leaf_nodes[nleaves]->additional_info->leafID = nleaves+1;
	  nleaves++;
	}
    forest->nleaves = nleaves;
  }

  SEXP rpartModel(SEXP R_forest)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    hpdRFnode* tree = forest->trees[0];
    SEXP model;
    PROTECT(model = allocVector(VECSXP, 12));
    int max_depth = 30;

    int numNodes = countSubTree(tree, max_depth);
    int max_ncat = 0;
    for(int i = 0; i < forest-> nfeature; i++)
      if(forest->features_cardinality[i] != NA_INTEGER &&
	 forest->features_cardinality[i] > max_ncat)
	max_ncat = forest->features_cardinality[i];

    SEXP indices, var, dev, yval, complexity, split_index, ncat,
      csplit, n, wt, improve, R_node_counts = R_NilValue;
    PROTECT(n = allocVector(INTSXP, numNodes));
    PROTECT(wt = allocVector(REALSXP, numNodes));
    PROTECT(indices = allocVector(INTSXP, numNodes));
    PROTECT(var = allocVector(INTSXP, numNodes));
    PROTECT(dev = allocVector(REALSXP, numNodes));
    PROTECT(yval = allocVector(REALSXP, numNodes));
    PROTECT(complexity = allocVector(REALSXP, numNodes));
    PROTECT(split_index = allocVector(REALSXP, numNodes));
    PROTECT(ncat = allocVector(INTSXP, numNodes));
    PROTECT(improve = allocVector(REALSXP, numNodes));
    double *node_counts = NULL;
    if(forest->response_cardinality != NA_INTEGER)
      {
	PROTECT(R_node_counts=allocVector(REALSXP,
					  numNodes*forest->response_cardinality));
	node_counts = REAL(R_node_counts);
      }


    int csplit_count = 0;
    convertTreetoRpart(tree, INTEGER(indices), INTEGER(n), 
		       REAL(wt),INTEGER(var), 
		       REAL(dev), REAL(yval), REAL(complexity),
		       REAL(improve),
		       REAL(split_index),INTEGER(ncat),
		       node_counts, numNodes,
		       1, 1, 0, forest->features_cardinality,
		       forest->response_cardinality,
		       &csplit_count, 1, max_depth);

    int nrow = csplit_count;
    csplit_count = 0;
    PROTECT(csplit = allocVector(INTSXP, nrow*max_ncat));
    memset(INTEGER(csplit),0,sizeof(int)*nrow*max_ncat);

    populateCsplit(tree,forest->features_cardinality, &csplit_count, 
		   INTEGER(csplit), nrow);
    setAttrib(csplit,install("nrow"),ScalarInteger(nrow));
    
   
    SET_VECTOR_ELT(model,0,indices);
    SET_VECTOR_ELT(model,1,var);
    SET_VECTOR_ELT(model,2,dev);
    SET_VECTOR_ELT(model,3,yval);
    SET_VECTOR_ELT(model,4,complexity);
    SET_VECTOR_ELT(model,5,n);
    SET_VECTOR_ELT(model,6,wt);

    SET_VECTOR_ELT(model,7,split_index);
    SET_VECTOR_ELT(model,8,ncat);
    SET_VECTOR_ELT(model,9,improve);

    SET_VECTOR_ELT(model,10,csplit);
    SET_VECTOR_ELT(model,11,R_node_counts);
    if(R_node_counts != R_NilValue)
      UNPROTECT(1);

    UNPROTECT(length(model));
    return model;
    
  }

  SEXP gatherForest(SEXP forest_parts)
  {
    int ntree = INTEGER(VECTOR_ELT(VECTOR_ELT(forest_parts,0),0))[0];
    SEXP forest;
    PROTECT(forest = allocVector(VECSXP,ntree+1));
    SET_VECTOR_ELT(forest,0,VECTOR_ELT(VECTOR_ELT(forest_parts,0),0));
    for(int i = 0; i < length(forest_parts); i++)
      {							
	SEXP curr_forest = VECTOR_ELT(forest_parts,i);
	for(int j = 1; j < length(curr_forest); j++)
	  if(VECTOR_ELT(curr_forest,j) != R_NilValue)
	    SET_VECTOR_ELT(forest,ntree--,VECTOR_ELT(curr_forest,j));
      }
    UNPROTECT(1);
    return forest;
  }


  void simplifyForest(SEXP R_forest)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    for(int i = 0; i < forest->ntree; i++)
      simplifyTree(forest->trees[i]);
  }

}

