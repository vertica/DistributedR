  #include"hpdRF.hpp"

double treePredictObservation(hpdRFnode * tree, SEXP observations, 
		       int *feature_cardinality, int obs_index)
{
  
  if(tree->split_criteria == NULL)
      return tree->prediction;
  int split_var = tree->split_variable-1;
  double* split_criteria = tree->split_criteria;
  bool left = false;
  double obs_split_var;
  
    if(TYPEOF(VECTOR_ELT(observations,split_var)) == INTSXP)
      {
	if(INTEGER(VECTOR_ELT(observations,split_var))[obs_index] == NA_INTEGER)
	  return tree->prediction;
	obs_split_var = 
	  (double) INTEGER(VECTOR_ELT(observations,split_var))[obs_index];
      }
    else
      {
	if(ISNA(REAL(VECTOR_ELT(observations,split_var))[obs_index]))
	  return tree->prediction;
	obs_split_var =  REAL(VECTOR_ELT(observations,split_var))[obs_index];
      }
    if(feature_cardinality[split_var] == NA_INTEGER)
      left = obs_split_var < tree->split_criteria[0];
    else
      {
	for(int i = 0; i < tree->split_criteria_length; i++)
	  if(tree->split_criteria[i] == obs_split_var)
	    left = true;
      }
    if(left)
      return treePredictObservation(tree->left, observations, 
				    feature_cardinality, obs_index);
    else
      return treePredictObservation(tree->right, observations, 
				    feature_cardinality, obs_index);
}

extern "C"
{

  SEXP specificTreePredictObservation(SEXP R_forest, SEXP R_tree_id, 
				      SEXP R_observations, SEXP R_obs_index)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    int obs_index = INTEGER(R_obs_index)[0]-1;
    int tree_id = INTEGER(R_tree_id)[0]-1;
    if(forest->trees[tree_id] == NULL)
      return ScalarReal(NA_REAL);
    double prediction = treePredictObservation(forest->trees[tree_id], 
					       R_observations, 
					       forest->features_cardinality, 
					       obs_index);
    
    return ScalarReal(prediction);
  }

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


  SEXP combineVotesClassification(SEXP R_predictions, SEXP R_cutoff, 
				  SEXP R_classes_num)
  {
    int classes_num = INTEGER(R_classes_num)[0];
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

}
