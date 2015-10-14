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

/*
  This function traverses the tree for a particular set of features
  @param tree - the tree to traverse
  @param observations - the full set of observations of features
  @param feature_cardinality - the number of classes of each feature or NA 
  @param obs_index - the index of which observation to traverse with
  @param na_pass - if na_pass is true go down both subtrees otherwise stop
  @param leaf_count - will be assigned in the function with 
    how many leaf nodes are in the final result
  @parm weight - this variable will be assigned in the function. It determines 
    how many observations worth of weight there is in each leaf returned 
 */
hpdRFnode** treeTraverseObservation(hpdRFnode* tree, SEXP observations,
				    int *feature_cardinality, int obs_index,
				    bool na_pass, int* leaf_count, double** weight)
{
  if(tree->split_criteria == NULL)
    {
      *leaf_count=1;
      *weight = (double* ) malloc(sizeof(double));
      **weight = 1;
      hpdRFnode** leaf = (hpdRFnode**) malloc(sizeof(hpdRFnode*));
      *leaf = tree;
      return leaf;
    }

  int split_var = tree->split_variable-1;
  bool left = false;
  double obs_split_var;
  bool null_feature = false;
  bool right;

  SEXP split_col = VECTOR_ELT(observations,split_var);
  bool col_int = TYPEOF(VECTOR_ELT(observations,split_var)) == INTSXP;
  
  if(col_int)
    null_feature = INTEGER(split_col)[obs_index] == NA_INTEGER;
  else
    null_feature = ISNA(REAL(split_col)[obs_index]);

  if(!na_pass && null_feature)
    {
      *leaf_count=1;
      *weight = (double* ) malloc(sizeof(double));
      **weight = 1;
      hpdRFnode** leaf = (hpdRFnode**) malloc(sizeof(hpdRFnode*));
      *leaf = tree;
      return leaf;
    }
  
  obs_split_var = col_int ? 
    (double) INTEGER(split_col)[obs_index]:REAL(split_col)[obs_index];
  
  if(!null_feature && feature_cardinality[split_var] == NA_INTEGER)
      {
	left = obs_split_var < tree->split_criteria[0];
	right = !left;
      }
    else if(!null_feature)
      {
	for(int i = 0; i < tree->split_criteria_length; i++)
	  if(tree->split_criteria[i] == obs_split_var)
	    left = true;
	right = !left;
      }
    if(null_feature)
      {
	left = true;
	right = true;
      }

    int left_count=0, right_count=0;
    hpdRFnode** left_leaves=NULL, **right_leaves=NULL;
    double* left_weight, *right_weight;

    if(left)
      left_leaves= treeTraverseObservation(tree->left, observations, 
					   feature_cardinality, obs_index, 
					   na_pass,&left_count,&left_weight);
    if(right)
      right_leaves= treeTraverseObservation(tree->right, observations, 
					    feature_cardinality, obs_index, 
					    na_pass,&right_count,&right_weight);
    if(!null_feature)
      {
	if(left)
	  {
	    *leaf_count = left_count;
	    *weight = left_weight;
	    return left_leaves;
	  }
	else
	  {
	    *leaf_count = right_count;
	    *weight = right_weight;
	    return right_leaves;
	  }
      }

    hpdRFnode** leaves = (hpdRFnode**) malloc(sizeof(hpdRFnode*)*
					      (left_count+right_count));
    *weight = (double *) malloc(sizeof(double)*(left_count+right_count));
    if(left_leaves != NULL)
      {
	memcpy(leaves,left_leaves,sizeof(hpdRFnode*)*(left_count));
	memcpy(*weight,left_weight,sizeof(double)*(left_count));
	free(left_leaves);
	free(left_weight);
      }
    if(right_leaves != NULL)
      {
	memcpy(leaves+left_count,right_leaves,
	       sizeof(hpdRFnode*)*(right_count));
	memcpy((*weight) + left_count,right_weight,
	       sizeof(double)*(right_count));
	free(right_leaves);
	free(right_weight);
      }

    *leaf_count = left_count+right_count;
    return leaves;
}

/* This function will predict the tree's output value for a single observation
   @param tree - the tree to predict from
   @param observations - the data.frame of observations
   @param feature_cardinality - the cardinality of feature variables or NA
   @param obs_index - the index of the observation to be predicted
 */
double treePredictObservation(hpdRFnode * tree, SEXP observations, 
		       int *feature_cardinality, int obs_index)
{
  int leaf_count = 0;
  double* weight;
  hpdRFnode** leaves= treeTraverseObservation(tree, 
					      observations,
					      feature_cardinality, 
					      obs_index,
					      false, 
					      &leaf_count, &weight);
  double prediction = leaves[0]->prediction;
  free(leaves);
  free(weight);
  return prediction;

}


extern "C"
{
  /* Wrapper function of treePredictObservation that allows R to call it
     @param R_forest - the forest object containing trees
     @param R_tree_id - index of which tree to use
     @param R_observations - data.frame of observations
     @param R_obs_index - index of which observation to predict
   */
  SEXP specificTreePredictObservation(SEXP R_forest, SEXP R_tree_id, 
				      SEXP R_observations, SEXP R_obs_index)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int obs_index = INTEGER(R_obs_index)[0]-1;
    int tree_id = INTEGER(R_tree_id)[0]-1;
    if(tree_id < 0 || tree_id >= forest->ntree || 
       forest->trees[tree_id] == NULL)
      return ScalarReal(NA_REAL);

    double prediction = treePredictObservation(forest->trees[tree_id], 
					       R_observations, 
					       forest->features_cardinality, 
					       obs_index);
    
    return ScalarReal(prediction);
  }

  /* This function combines predictions accross trees into a single prediction 
     @param R_predictions - predictions accross the trees
     @param R_responses - actual responses to compare accuracy of predictions
     @param R_cutoff - cutoff value for classification trees
     @param R_classes - the classes for classification trees
     @param R_err_count - counting the number of errors in classification trees
     @param class_count - counting the number in each class each tree outputs
     @param R_squared_resid - computing squared error of predictions
     @param R_L0 - calculating how many observations were valid after n trees
     @param R_L1 - calculating the average predictions after n trees
     @param R_L2 - calculating predictions^2 after n trees (used for mse later)
   */
  SEXP cumulativePredictions(SEXP R_predictions, SEXP R_responses, 
			     SEXP R_cutoff, SEXP R_classes, 
			     SEXP R_err_count, SEXP R_class_count,
			     SEXP R_squared_resid,
			     SEXP R_L0, SEXP R_L1, SEXP R_L2)
  {
    SEXP R_final_predictions;
    double* predictions = REAL(R_predictions);
    void * responses;
    bool response_int = true;
    if(TYPEOF(VECTOR_ELT(R_responses,0)) == REALSXP)
      response_int = false;
    if(!response_int)
      responses = REAL(VECTOR_ELT(R_responses,0));
    else 
      responses = INTEGER(VECTOR_ELT(R_responses,0));

    int nrow = nrows(R_predictions);
    int* L0 = INTEGER(R_L0);
    double* L1 = REAL(R_L1);
    double* L2 = REAL(R_L2);
    if(length(R_classes) == 0)
      {
	PROTECT(R_final_predictions = allocVector(REALSXP, ncols(R_predictions)));

	double* final_predictions = REAL(R_final_predictions);
	double* ss_resid = REAL(R_squared_resid);

	double prediction, response;
	for(int obs = 0; obs < ncols(R_predictions); obs++)
	  {
	    double sum = 0, count = 0;
	    response = response_int ? ((int *)responses)[obs] : 
	      ((double *) responses)[obs];
	    final_predictions[obs] = NA_REAL;
	    if(ISNA(response))
	      continue;
	    for(int tree = 0; tree < nrow; tree++)
	      {
		prediction = predictions[nrow*obs + tree];
		if(!ISNA(prediction))
		  {
		    sum += prediction;
		    count ++;
		  }
		if(count > 0)
		  {
		    ss_resid[tree] += (sum/count - response)*
		      (sum/count - response);
		    L0[tree]++;
		    L1[tree]+=response;
		    L2[tree]+=response*response;
		  }
	      }
	    if(count > 0)
	      final_predictions[obs] = sum/count;
	  }
      }
    else
      {
	double* ratios = REAL(R_cutoff);
	PROTECT(R_final_predictions = allocVector(STRSXP, ncols(R_predictions)));
	int* table = (int *)malloc(sizeof(int)*length(R_classes));
	int max_class=-1;
	int prediction, response;
	int* err_count = INTEGER(R_err_count);
	int* class_count = INTEGER(R_class_count);
	int k;
	for(int obs = 0; obs < ncols(R_predictions); obs++)
	  {
	    response = response_int ? ((int *)responses)[obs] : 
	      ((double *) responses)[obs];
	    response--;
	    memset(table,0,sizeof(int)*length(R_classes));
	    bool isvalid = false;
	    for(int tree = 0; tree < nrow; tree++)
	      {
		prediction = (int) predictions[nrow*obs + tree]-1;
		if(prediction >= 0 &&
		   prediction < length(R_classes))
		  {
		    table[prediction] ++;
		    isvalid = true;
		  }
		max_class = -1;
		for( k = 0; k < length(R_classes); k++)
		  if(( max_class != -1 && 
		       table[k]/ratios[k] > table[max_class]/ratios[max_class])||
		     (max_class == -1 &&
		      table[k] > 0))
		    max_class = k;
 
		if(max_class!=response && max_class != -1)
		  err_count[response*nrow+tree] ++;
		if(isvalid)
		  {
		    L0[tree]++;
		    class_count[length(R_classes)*tree + response]++;
		  }
	      }
	    if(max_class >= 0)
	      SET_STRING_ELT(R_final_predictions,obs,
			     STRING_ELT(R_classes,max_class));
	    else
	      SET_STRING_ELT(R_final_predictions,obs,NA_STRING);
	    
	  }
	free(table);
      }
    UNPROTECT(1);
    return R_final_predictions;
  }

  /* combines the predictions of classification votes into a single predictions
     @param R_predictions - array of predictions
     @param R_cutoff - cutoff values for classes of output feature
   */
  SEXP combineVotesClassification(SEXP R_predictions, SEXP R_cutoff)
  {
    int classes_num = length(R_cutoff);
    double* ratios = REAL(R_cutoff);
    double* predictions = REAL(R_predictions); 
    int* table = (int *)malloc(sizeof(int)*classes_num);
    int nrow = nrows(R_predictions);
    int ncol = ncols(R_predictions);
    SEXP R_final_predictions;
    PROTECT(R_final_predictions=allocVector(INTSXP,ncol));
    int* final_predictions = INTEGER(R_final_predictions);
    int max_class, i, j , k;
    int prediction;

    for( i = 0; i < ncol; i++)
      {
	memset(table,0,sizeof(int)*classes_num);
	for( j = 0; j < nrow; j++)
	  {
	    prediction = (int) predictions[nrow*i + j]-1;
	    if(prediction >= 0 &&
	       prediction < classes_num)
	      table[prediction] ++;
	  }
	
	max_class = -1;
	for( k = 0; k < classes_num; k++)
	  if(( max_class != -1 && 
	       table[k]/ratios[k] > table[max_class]/ratios[max_class])||
	     (max_class == -1 &&
	      table[k] > 0))
	    max_class = k;
	if(max_class >= 0)
	  final_predictions[i] = max_class+1;
	else
	  final_predictions[i] = NA_INTEGER;
      }
    free(table);
    UNPROTECT(1);
    return R_final_predictions;
  }
  
  /* Find how much using a given subset of trees affects error rate
     @param - R_votes - array of predictions 
     @param R_responses - the responses to compare to
     @param R_curr_trees - the set of trees to use
     @param R_excluded_trees - the trees to not use
     @param R_response_cardinality - cardinality of response variable or NA
     @param R_cutoff - the cutoff values for classification trees
     @param R_error_count - the error values for using this set of R_curr_trees
   */
  SEXP findAdditionalTreeErrors(SEXP R_votes, SEXP R_responses, 
				SEXP R_curr_trees, SEXP R_excluded_trees,
				SEXP R_response_cardinality,
				SEXP R_cutoff, SEXP R_error_count)
  {
    void * responses;
    bool response_int = true;
    if(TYPEOF(VECTOR_ELT(R_responses,0)) == REALSXP)
      response_int = false;
    if(!response_int)
      responses = REAL(VECTOR_ELT(R_responses,0));
    else 
      responses = INTEGER(VECTOR_ELT(R_responses,0));
    
    int error_count = 0;
    double *votes = REAL(R_votes);
    int ntree = nrows(R_votes);
    int nObs = ncols(R_votes);
    int* curr_trees = INTEGER(R_curr_trees);
    int curr_ntree = length(R_curr_trees);
    int* excluded_trees = INTEGER(R_excluded_trees);
    SEXP R_new_errors;
    PROTECT(R_new_errors = allocVector(REALSXP, ntree - curr_ntree));
    double* new_errors = REAL(R_new_errors);
    memset(new_errors,0,sizeof(double)*length(R_new_errors));
    bool categorical = *INTEGER(R_response_cardinality) != NA_INTEGER;
    int classes_num = *INTEGER(R_response_cardinality);
    double response;
    if(categorical)
      {
	int prediction;
	int* counts = (int*)malloc(sizeof(int)*classes_num);
	double* ratios = REAL(R_cutoff);
	int old_best, new_best=-1;
	bool old_correct, new_correct;
	for(int i = 0; i < nObs; i++)
	  {
	    response = response_int ? ((int *)responses)[i] : 
	      ((double *) responses)[i];
	    response--;

	    memset(counts,0,sizeof(int)*classes_num);
	    for(int j = 0; j < curr_ntree; j++)
	      {
		prediction = votes[curr_trees[j]-1 + ntree*i]-1;
		if(prediction >= 0 &&
		   prediction < classes_num)
		  counts[prediction]++;

	      }
	    old_best = -1;
	    for(int k = 0; k < classes_num; k++)
	      if(( old_best != -1 && 
		   counts[k]/ratios[k] > counts[old_best]/ratios[old_best])||
		 (old_best == -1 &&
		  counts[k] > 0))
		old_best = k;
	    old_correct = old_best == (int)response;
	    error_count += !old_correct;

	    for(int j = 0; j < ntree - curr_ntree; j++)
	      if(!ISNA(votes[excluded_trees[j]-1 + ntree*i]))
		{
		  prediction = votes[excluded_trees[j]-1 + ntree*i]-1;
		  if(( old_best != -1 && 
		       (counts[prediction]+1)/ratios[prediction] > 
		       counts[old_best]/ratios[old_best])|| old_best == -1)
		    new_best = prediction;
		  new_correct = new_best == (int)response;

		  if(new_correct && !old_correct)
		    new_errors[j]--;
		  else if(!new_correct && old_correct)
		    new_errors[j]++;
		}
	  }
	free(counts);
      }
    else
      {	
	double prediction, new_prediction,resid1,resid2;
	int valid_count;
	for(int i = 0; i < nObs; i++)
	  {
	    prediction = 0;
	    new_prediction = 0;
	    response = response_int ? ((int *)responses)[i] : 
	      ((double *) responses)[i];

	    valid_count = 0;
	    for(int j = 0; j < curr_ntree; j++)
	      if(!ISNA(votes[curr_trees[j]-1 + ntree*i]))
		{
		  prediction += votes[curr_trees[j]-1 + ntree*i];
		  valid_count++;
		}

	    for(int j = 0; j < ntree - curr_ntree; j++)
	      if(!ISNA(votes[excluded_trees[j]-1 + ntree*i]))
		{
		  new_prediction = prediction + 
		    votes[excluded_trees[j]-1 + ntree*i];
		  resid1 =  (prediction/valid_count - response)*
		    (prediction/valid_count - response);
		  resid2 =  (new_prediction/(valid_count+1) - response)*
		    (new_prediction/(valid_count+1) - response);
		  if(valid_count > 0)
		    new_errors[j] -= resid1 - resid2;
		  else
		    new_errors[j] = R_NegInf;
		}
	    
	  }
       }
    INTEGER(R_error_count)[0] = error_count;
    UNPROTECT(1);
    return R_new_errors;
  }


  void forestPredictOOB(SEXP R_forest, SEXP R_predictions, SEXP R_observations,
	       SEXP R_indices, SEXP R_tree_ids)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    double *predictions = REAL(R_predictions);
    for(int tree_index = 0; tree_index < length(R_tree_ids); tree_index++)
      {
	int tree_id = INTEGER(R_tree_ids)[tree_index]-1;
	SEXP indices = VECTOR_ELT(R_indices,tree_id);
	for(int i = 0; i < length(indices); i++)
	  {
	    int obs_index = INTEGER(indices)[i]-1;
	    predictions[obs_index*length(R_tree_ids)+tree_index] =
	      treePredictObservation(forest->trees[tree_id], 
				     R_observations, 
				     forest->features_cardinality, 
				     obs_index);
	  }
      }
  }
  void forestPredictObservations(SEXP R_forest, SEXP R_predictions, 
				 SEXP R_observations, SEXP R_tree_ids)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    double *predictions = REAL(R_predictions);
    int num_predictions = length(VECTOR_ELT(R_observations,0));
    for(int tree_index = 0; tree_index < length(R_tree_ids); tree_index++)
      {
	int tree_id = INTEGER(R_tree_ids)[tree_index]-1;
	for(int obs_index = 0; obs_index < num_predictions; obs_index++)
	  {
	    predictions[obs_index*length(R_tree_ids)+tree_index] =
	      treePredictObservation(forest->trees[tree_id], 
				     R_observations, 
				     forest->features_cardinality, 
				     obs_index);
	  }
      }
  }

}
