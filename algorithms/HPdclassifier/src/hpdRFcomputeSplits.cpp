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

/*
 This function sorts the bins of the histograms and returns the permutation that sorts the bins of the histograms
 @param hist - histogram to sort by
 @param nbin - number of bins in the histogram
 @return - array of which bin has most weight in the histogram
 */
int* sort(double* hist, int nbin)
{
  if(nbin <= 1)
    {
      int* order = (int *) malloc(sizeof(int));
      order[0] = 0;
      return(order);
    }
  int* left = sort(hist,nbin/2);
  int* right = sort(hist+2*((int)(nbin/2)),nbin - ((int)(nbin/2)));
  int* order = (int *) malloc(sizeof(int)*nbin);
  int left_counter = 0, right_counter = 0;
  for(int counter = 0; counter < nbin; counter++)
    {
      if(left_counter >= ((int)nbin/2))
	{
	  order[counter] = right[right_counter]+((int)(nbin/2));
	  right_counter++;
	  continue;
	}
      if(right_counter >= nbin - ((int)nbin/2))
	{
	  order[counter] = left[left_counter];
	  left_counter++;
	  continue;
	}
      double left_average = hist[2*left[left_counter]+1]/
	hist[2*left[left_counter]];
      double right_average = hist[2*(right[right_counter]+((int)(nbin/2)))+1]/
	hist[2*(right[right_counter]+((int)(nbin/2)))];
      if(left_average < right_average)
	{
	  order[counter] = left[left_counter];
	  left_counter++;
	}
      else
	{
	  order[counter] = right[right_counter]+((int)(nbin/2));
	  right_counter++;
	}
    }
  free(left);
  free(right);
  return(order);
}

/*
 This function computes some basic statistics about all the histograms for this node using the first feature assuming regression tree is being built
 @param hist - the first histogram needed to compute stats
 @param nbin - number of bins in histogram
 @param L0 - pointer to sum of the weights 
 @param L1 - pointer to weighted sum of histogram
 @param default_cost - pointer to the minimum cost without any split
 @retrun - best prediction (not split) of this histogram
 */
double computeRegressionTreeStats(double* hist, int nbin, double* L0, 
				  double* L1, double L2, double* default_cost)
{
  for(int i = 0; i < nbin; i ++)
    {
      *L0 += hist[2*i];
      *L1 += hist[2*i + 1];
    }
  *default_cost = L2 -(*L1)*(*L1)/(*L0);
  return (*L1)/(*L0);
}

/*
 This function computes some basic statistics about all the histograms for this node using the first feature assuming regression tree is being built
 @param hist - the first histogram needed to compute stats
 @param nbin - number of bins in histogram
 @param classes - number of classes in the histogram 
 @param sum - pointer to weighted sum of histogram
 @param default_cost - pointer to the minimum cost without any split
 @return - best prediction (not split) of this histogram
 */
double computeClassificationTreeStats(double* hist, int nbin, 
				      int classes, double* sum, 
				      double* default_cost,
				      double* node_count)
{
  double max_count=0, prediction=0, sq = 0;
  for(int k = 0; k < classes; k++)
    {
      node_count[k] = 0;
      for(int i = 0; i < nbin; i ++)
	node_count[k] += hist[nbin*k+i];

      *sum += node_count[k];
      sq += node_count[k]*node_count[k];
      if(node_count[k] > max_count)
	{
	  prediction = (double) k;
	  max_count = node_count[k];
	}
    }
  *default_cost = 1-sq/(*sum)/(*sum);
  return prediction;
}

/*
 This function computes the best split for a given histogram corresponding to numeric variable and a numeric response
 @param hist - histogram to compute split
 @param nbin - number of bins
 @param cost - cost of the split initialized to cost of no split
 @param L0 - sum of weights in histogram
 @param L1 - weighted sum of responses in histogram
 @param best_split - best split criteria 
 @param best_split_length - what is the length of best_split
 @param complete - pointer indicating if best_split is valid split
 */
void NumericVariableRegressionTreeSplit(double* hist, int nbin, 
					double* cost, double L0, double L1,
					double L2,
					int** best_split,
					int* best_split_length,
					bool* complete, double cp,
					double min_count)

{
  double L0_left = 0, L1_left = 0, L0_right = L0, L1_right = L1; 
  double objective = (1-cp)*(L2-L1*L1/L0), obj_temp;
  int split_val=1;
  for(int i = 0; i < nbin-1; i++)
    {
      L0_left += hist[2*i];
      L1_left += hist[2*i+1];
      L0_right -= hist[2*i];
      L1_right -= hist[2*i+1];
      obj_temp = L2 -L1_left*L1_left/L0_left - L1_right*L1_right/L0_right;
      if(obj_temp < objective)
	if((L0_left >= min_count) && (L0_right >= min_count) && 
	   (L1_left/L0_left != L1_right/L0_right))
	  {
	    objective = obj_temp;
	    split_val = i+1;
	    *complete = true;
	  }
    }
  *cost = objective;
  *best_split = (int *) malloc(sizeof(int));
  **best_split = split_val;
  *best_split_length = 1;
}


/*
 This function computes the best split for a given histogram corresponding to categorical variable and a numeric response
 @param hist - histogram to compute split
 @param nbin - number of bins
 @param cost - cost of the split initialized to cost of no split
 @param L0 - sum of weights in histogram
 @param L1 - weighted sum of responses in histogram
 @param best_split - best split criteria 
 @param best_split_length - what is the length of best_split
 @param complete - pointer indicating if best_split is valid split
 */
void CategoricalVariableRegressionTreeSplit(double* hist, int nbin, 
					    double* cost, double L0, double L1,
					    double L2, 
					    int** best_split,
					    int* best_split_length,
					    bool *complete, double cp,
					    double min_count)
{

  int* order = sort(hist,nbin);
  double L0_left=0,L0_right=L0,L1_left=0,L1_right=L1;
  double objective = (1-cp)*(L2-L1*L1/L0), split_val=0, obj_temp;
  for(int split = 0; split < nbin; split++)
    {
      L0_left += hist[2*order[split]];
      L0_right -= hist[2*order[split]];
      L1_left += hist[2*order[split] + 1];
      L1_right -= hist[2*order[split] + 1];
      obj_temp = L2-L1_left*L1_left/L0_left - L1_right*L1_right/L0_right;
      if(obj_temp < objective)
	if((L0_left >= min_count) && (L0_right >= min_count) && 
	   (L1_left/L0_left != L1_right/L0_right))
	  {
	    objective = obj_temp;
	    split_val = split+1;
	    if((L0_left > 0) && (L0_right > 0) && 
	       (L1_left/L0_left != L1_right/L0_right))
	      *complete = true;
	  }
    }
  
  *cost = objective;
  *best_split = (int *) malloc(sizeof(int)*split_val);  
  for(int i = 0; i < split_val; i++)
    (*best_split)[i] = order[i]+1;
  *best_split_length = split_val;
  free(order);
}


/*
 This function computes the best split for a given histogram corresponding to categorical variable and a numeric response
 @param hist - histogram to compute split
 @param nbin - number of bins
 @param classes - number of classes of feature variable
 @param cost - cost of the split initialized to cost of no split
 @param sum - weighted sum of responses in histogram
 @param best_split - best split criteria 
 @param best_split_length - what is the length of best_split
 @param complete - pointer indicating if best_split is valid split
 */
void NumericVariableClassificationTreeSplit(double* hist, int nbin, 
					    int classes, double* cost, 
					    double sum, int** best_split,
					    int* best_split_length,
					    bool *complete, double cp,
					    double min_count)
{

  double* left_counts = (double* ) malloc(sizeof(double)*classes);
  double* right_counts = (double* ) malloc(sizeof(double)*classes);
  double left_cost, right_cost, split_cost;
  int split_val=0;
  double left_sq,right_sq,left_sum,right_sum;
  *cost = (1-cp)*(*cost);
  for(int k = 0; k < classes; k++)
    {
      left_counts[k] = 0;
      right_counts[k] = 0;
      for(int i = 0; i < nbin; i++)
	right_counts[k] += hist[nbin*k + i];
    }
  for(int i = 0; i <nbin-1; i ++)
    {
      left_sq = 0;
      right_sq = 0;
      left_sum = 0;
      right_sum = 0;

      for(int k = 0; k < classes; k++)
	{
	  left_counts[k] += hist[nbin*k + i];
	  right_counts[k] -= hist[nbin*k + i];
	  left_sum += left_counts[k];
	  right_sum += right_counts[k];
	  left_sq += left_counts[k]*left_counts[k];
	  right_sq += right_counts[k]*right_counts[k];
	}
      left_cost = 1 - left_sq/left_sum/left_sum;
      right_cost = 1 - right_sq/right_sum/right_sum;
      split_cost = left_sum/sum * left_cost + right_sum/sum * right_cost;

      if(split_cost < (*cost) &&
	 left_sum >= min_count &&
	 right_sum >= min_count)
	{
	  *cost = split_cost;
	  split_val = i+1;
	  if((left_sum > .01*sum) && (right_sum > .01*sum))
	    *complete = true;
	  else
	    *complete = false;
	}
    }


  *best_split = (int *) malloc(sizeof(int));
  **best_split = split_val;
  *best_split_length = 1;
  free(left_counts);
  free(right_counts);
}


/*
 This function computes the best split for a given histogram corresponding to categorical variable and a numeric response
 @param hist - histogram to compute split
 @param nbin - number of bins
 @param classes - number of classes of feature variable
 @param cost - cost of the split initialized to cost of no split
 @param sum - weighted sum of responses in histogram
 @param best_split - best split criteria 
 @param best_split_length - what is the length of best_split
 @param complete - pointer indicating if best_split is valid split
 */
void  CategoricalVariableClassificationTreeSplit(double* hist, 
						int classes_var, 
						int classes_response,
						double* cost,
						double sum,
						int** best_split,
						int* best_split_length,
						 bool *complete,
						 double cp, 
						 double min_count)
{
  
  NumericVariableClassificationTreeSplit(hist, classes_var, 
					 classes_response, cost, 
					 sum, best_split, 
					 best_split_length, complete, cp,
					 min_count);
  
  
  int split_location = **best_split;
  
  free(*best_split);
  *best_split = (int* ) malloc(sizeof(int)*split_location);
  *best_split_length = split_location;
  
  for(int i = 0; i < split_location; i++)
    (*best_split)[i] = i+1;
  
}

/*
 this function computes the best split of a given histogram by calling the appropriate helper function
 @param histogram - histogram in R format
 @param bin_num - number of bins in the histogram
 @param class_num - number of classes in the histogram
 @param cost - best cost of the histogram initialized to cost of no split
 @param feature_categorical - flag indicating if feature is categorical
 @param response_categorical - flag indicating if response is categorical
 @param L0 - sum of weights in histogram
 @param L1 - weighted sum of responses in histogram
 @param complete - pointer to flag indicating if there was best split
 @return - best split criteria
 */
int* computeSplit(SEXP histogram, int bin_num, int class_num,
		  double *cost, bool feature_categorical,
		  bool response_categorical,
		  double L0, double L1, bool* complete,
		  int *best_split_length, double cp, double min_count)
{
  int* best_split=NULL;
  if(!response_categorical && !feature_categorical)
    {
      NumericVariableRegressionTreeSplit(REAL(histogram), bin_num, 
					 cost, L0, L1, 
					 REAL(getAttrib(histogram,
							install("L2")))[0],
					 &best_split, best_split_length, complete, cp,
					 min_count);
    }
  if(!response_categorical && feature_categorical)
    {
      CategoricalVariableRegressionTreeSplit(REAL(histogram), bin_num, 
					     cost, L0, L1, 
					     REAL(getAttrib(histogram,
							    install("L2")))[0],
					     &best_split, best_split_length, complete, cp,
					     min_count);
    }
  if(response_categorical && !feature_categorical)
    {
      NumericVariableClassificationTreeSplit(REAL(histogram), bin_num, 
					     class_num, cost, L0, 
					     &best_split, best_split_length, complete, cp,
					     min_count);
      
    }
  if(response_categorical && feature_categorical)
    {
      CategoricalVariableClassificationTreeSplit(REAL(histogram), bin_num, 
						 class_num, cost, L0, 
						 &best_split, best_split_length, complete, cp,
						 min_count);
      
    }
  return best_split;
}


extern "C"
{
  /*
   this function loops over the histograms and computes the best splits for each histogram and collects the results
   @param R_histograms - list of list of histograms
   @param R_bin_num - array of number of bins for each feature
   @param R_response_cardinality - number of classes for the response variable
   @param R_features_cardinality - number of classes for the features variable
   */
  SEXP computeSplits(SEXP R_histograms, SEXP R_active_nodes, 
		     SEXP R_features_cardinality, 
		     SEXP R_response_cardinality, 
		     SEXP R_bin_num, SEXP old_splits_info,
		     SEXP R_cp, SEXP R_min_count)
  {

    int* features_categorical = INTEGER(R_features_cardinality);
    int response_cardinality = INTEGER(R_response_cardinality)[0];
    bool response_categorical = response_cardinality != NA_INTEGER;
    int* bin_num = INTEGER(R_bin_num);
    SEXP splits_info;
    double cp = REAL(R_cp)[0];
    double min_count = REAL(R_min_count)[0];

    PROTECT(splits_info = allocVector(VECSXP,length(R_active_nodes)));
    for(int i = 0; i < length(old_splits_info) && i <length(splits_info); i++)
      {
	SEXP split_info = VECTOR_ELT(old_splits_info,i);
	SET_VECTOR_ELT(splits_info,i,split_info);
	*INTEGER(VECTOR_ELT(split_info,0)) = 0;
	SET_VECTOR_ELT(split_info,1,R_NilValue);
	SET_VECTOR_ELT(split_info,2,R_NilValue);
	*REAL(VECTOR_ELT(split_info,3)) = DBL_MAX;
	*REAL(VECTOR_ELT(split_info,4)) = 0;
	*REAL(VECTOR_ELT(split_info,5)) = DBL_MAX;
	*REAL(VECTOR_ELT(split_info,6)) = 0;
	*INTEGER(VECTOR_ELT(split_info,7)) = 0;
	SET_VECTOR_ELT(split_info,8,R_NilValue);

      }
    for(int i = length(old_splits_info); i < length(R_active_nodes); i++)
      {
	SEXP split_info;
	PROTECT(split_info = allocVector(VECSXP,9));
	SET_VECTOR_ELT(splits_info,i,split_info);
	SET_VECTOR_ELT(split_info,0,ScalarInteger(0));
	SET_VECTOR_ELT(split_info,1,R_NilValue);
	SET_VECTOR_ELT(split_info,2,R_NilValue);
	SET_VECTOR_ELT(split_info,3,ScalarReal(DBL_MAX));
	SET_VECTOR_ELT(split_info,4,ScalarReal(0));
	SET_VECTOR_ELT(split_info,5,ScalarReal(DBL_MAX));
	SET_VECTOR_ELT(split_info,6,ScalarReal(0));
	SET_VECTOR_ELT(split_info,7,ScalarInteger(0));
	SET_VECTOR_ELT(split_info,8,R_NilValue);
	UNPROTECT(1);
      }
    
    int total_completed=0;
    for(int i = 0; i < length(R_active_nodes); i++)
      {
	SEXP split_info=VECTOR_ELT(splits_info,i);
	SEXP split_criteria=NULL;
	SEXP node_histograms = VECTOR_ELT(R_histograms,i);

	double L0 = 0, L1 = 0, L2 =REAL(getAttrib(VECTOR_ELT(node_histograms,0),
						   install("L2")))[0];
	double prediction;
 	double default_cost;
	int featureIndex = INTEGER(getAttrib(VECTOR_ELT(node_histograms,0),
					     install("feature")))[0]-1;
	SEXP node_count = R_NilValue;

	if(!response_categorical)
	  prediction = computeRegressionTreeStats(REAL(VECTOR_ELT(node_histograms,0)), 
						  bin_num[featureIndex],
						  &L0, &L1, L2, 
						  &default_cost);
	else
	  {
	    PROTECT(node_count = allocVector(REALSXP,response_cardinality));
	    prediction = computeClassificationTreeStats(REAL(VECTOR_ELT(node_histograms,0)),
							bin_num[featureIndex], 
							response_cardinality, 
							&L0,
							&default_cost,
							REAL(node_count));
	    prediction = prediction + 1;
	    SET_VECTOR_ELT(split_info,8,node_count);
	    UNPROTECT(1);
 
	  }
	*REAL(VECTOR_ELT(split_info,4)) = prediction;
	int best_featureIndex = -1;
	int best_split_length = 0;
	int* best_split = NULL;
	*REAL(VECTOR_ELT(split_info,3)) = default_cost;
	*REAL(VECTOR_ELT(split_info,5)) = default_cost;
	*REAL(VECTOR_ELT(split_info,6)) = L0;
	*INTEGER(VECTOR_ELT(split_info,7)) = 
	  INTEGER(getAttrib(node_histograms,install("n")))[0];
	for(int feature = 0; feature < length(node_histograms); feature++)
	  {
	    SEXP histogram = VECTOR_ELT(node_histograms,feature);
	    featureIndex = INTEGER(getAttrib(histogram,install("feature")))[0]-1;
	    bool complete = false;
	    double hist_cost = default_cost;
	    int curr_split_length;
	    int* curr_split = computeSplit(histogram, 
					   bin_num[featureIndex], 
					   response_cardinality,
					   &hist_cost, 
					   features_categorical[featureIndex] 
					   != NA_INTEGER,
					   response_categorical, L0, L1, 
					   &complete, &curr_split_length,
					   cp, min_count);
	    if((complete && hist_cost < REAL(VECTOR_ELT(split_info,3))[0]) ||
	       (complete && hist_cost == REAL(VECTOR_ELT(split_info,3))[0] && 
		featureIndex > best_featureIndex &&
		hist_cost < (1-cp)*default_cost))
	      {
		best_featureIndex = featureIndex;
		*INTEGER(VECTOR_ELT(split_info,0)) = 1;
		if(best_split != NULL)
		  free(best_split);
		best_split = curr_split;
		best_split_length = curr_split_length;
		SET_VECTOR_ELT(split_info,1,getAttrib(histogram,install("feature")));
		*REAL(VECTOR_ELT(split_info,3)) = hist_cost;
	      }
	    else
	      free(curr_split);

	  }
	if(best_split != NULL)
	  {
	    PROTECT(split_criteria=allocVector(INTSXP,best_split_length));
	    memcpy(INTEGER(split_criteria),best_split,best_split_length*sizeof(int));
	    SET_VECTOR_ELT(split_info,2,split_criteria);
	    free(best_split);
	    UNPROTECT(1);
	  }
	total_completed += INTEGER(VECTOR_ELT(split_info,0))[0];
      }
    UNPROTECT(1);
    setAttrib(splits_info,install("total_completed"),ScalarInteger(total_completed));
    return splits_info;
  }

  /*
   this function uses the ouptut of computeSplits and applies them to the forest
   @param R_forest - the forest 
   @param R_splits_info - list of best splits 
   @param R_active_nodes - which nodes to update in the forest
   @param R_max_depth - what is the maximum depth allowed for registering split
   */
  SEXP applySplits(SEXP R_forest, SEXP R_splits_info, SEXP R_active_nodes, 
		   SEXP R_max_depth, SEXP R_summary_info)
  {
    hpdRFforest *forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    double min, max;
    int nbin, split_variable, *split_criteria;
    SEXP new_active_nodes;
    PROTECT(new_active_nodes = allocVector(INTSXP,length(R_active_nodes)));
    int num_new_active_nodes=0;
    int* max_nodes = forest->max_nodes;
    int max_depth = INTEGER(R_max_depth)[0];
    bool summary_info = LOGICAL(R_summary_info)[0];
    for(int i = 0; i < length(R_active_nodes) && i < length(R_splits_info); i++)
      {
	int active_node = INTEGER(R_active_nodes)[i]-1;
	hpdRFnode *node_curr = forest->leaf_nodes[active_node];
	node_curr->additional_info->attempted = 1;
	node_curr->additional_info->completed = 
	  INTEGER(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),0))[0];
	int tree_id = node_curr->treeID-1;
	node_curr->prediction = 
	  *REAL(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),4));
	if(summary_info)
	  {
	    if(!node_curr->summary_info)
	      node_curr->summary_info = (hpdRFSummaryInfo*) 
		malloc(sizeof(hpdRFSummaryInfo));

	    if(forest->response_cardinality != NA_INTEGER)
	      {
		node_curr->summary_info->node_counts_length = 
		  forest->response_cardinality;
		node_curr->summary_info->node_counts = (double *)
		  malloc(sizeof(double)* forest->response_cardinality);
		memcpy(node_curr->summary_info->node_counts,
		       REAL(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),8)),
		       sizeof(double)*forest->response_cardinality);
	      }
	    else
	      {
		node_curr->summary_info->node_counts_length = 0;
		node_curr->summary_info->node_counts = NULL;
	      }
	    node_curr->summary_info->complexity = 
	      REAL(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),3))[0];
	    node_curr->summary_info->deviance = 
	      REAL(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),5))[0];
	    if(node_curr->summary_info->deviance != 0)
	    node_curr->summary_info->complexity = 
	      (node_curr->summary_info->deviance-
	       node_curr->summary_info->complexity)/
	      node_curr->summary_info->deviance;
	    else
	      node_curr->summary_info->complexity = 0;
	    node_curr->summary_info->n = 
	      INTEGER(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),7))[0];
	    node_curr->summary_info->wt = 
	      REAL(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),6))[0];
	  }

	SEXP R_split_criteria = VECTOR_ELT(VECTOR_ELT(R_splits_info,i),2);
	if(R_split_criteria != R_NilValue && 
	   node_curr->additional_info->depth <= max_depth-1)
	  {
	    node_curr->split_variable = 
	      INTEGER(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),1))[0];
	    node_curr->split_criteria_length = 
	      length(VECTOR_ELT(VECTOR_ELT(R_splits_info,i),2));
	    node_curr->split_criteria = 
	      (double *) malloc(sizeof(double)*
				node_curr->split_criteria_length);
	    split_variable = node_curr->split_variable-1;


	    min = forest->features_min[split_variable];
	    max = forest->features_max[split_variable];
	    nbin = forest->bin_num[split_variable];
	    
	    split_criteria = INTEGER(R_split_criteria);
	    if(forest->features_cardinality[split_variable] == NA_INTEGER)
	      {
		for(int j = 0; j < node_curr->split_criteria_length; j++)
		  node_curr->split_criteria[j] = 
		    (double) (split_criteria[j]-0.5)*
		    (max - min)/ (nbin-1) + min;
	      }
	    else 
	      {
		for(int j = 0; j < node_curr->split_criteria_length; j++)
		  node_curr->split_criteria[j] = (double) split_criteria[j];
	      }
	  }
	SET_VECTOR_ELT(R_splits_info,num_new_active_nodes,
		       VECTOR_ELT(R_splits_info,i));
	if(node_curr->additional_info->depth <= max_depth-1)
	  INTEGER(new_active_nodes)[num_new_active_nodes++] = active_node+1;
	if(tree_id >= 0 && tree_id < forest->ntree && max_nodes[tree_id] > 0)
	  max_nodes[tree_id] --;
      }
   SETLENGTH(R_splits_info,num_new_active_nodes);
   SETLENGTH(new_active_nodes,num_new_active_nodes);
    UNPROTECT(1);
    return new_active_nodes;
  } 

}
