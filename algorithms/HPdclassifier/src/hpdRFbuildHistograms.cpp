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
 This function builds a single histogram 
 @param R_observations_feature - dataframe of observations of feature vectors
 @param R_responses - dataframe of observations of response variables
 @param observations_weights - weights of each of the observations in the node
 @param node_observations - array of which rows in R_observations_feature
    belong to the node
 @param bin_num - number of bins to use in the histogram
 @param class_num - the number of classes the feature has if categorical
 @feature_categorical - if the feature is categorical or not
 @response_categorical - if the response is categorical
 @histogram - the histogram that has to be filled in
 */
template <typename resp_type>
void buildHistogram(SEXP R_observations_feature, SEXP R_responses,
		    double* observations_weights, 
		    int* node_observations, int node_observations_num, 
		    int bin_num, int class_num,
		    bool feature_categorical, bool response_categorical,
		    SEXP histogram)
{
  int* observations_feature = INTEGER(R_observations_feature);
  resp_type* responses = RtoCArray<resp_type*>(VECTOR_ELT(R_responses,0));
  register int bin;
  register double weight;
  double* hist = REAL(histogram);
  register resp_type response;
  if(response_categorical)
    {
      for(int  observation = 0; observation < node_observations_num; observation++)
	{
	  bin = observations_feature[node_observations[observation]-1];
	  if(0 > bin || bin >= bin_num)
	    continue;
	  weight = observations_weights[observation];
	  response = responses[node_observations[observation]-1]-1;
	  if(response < 0 || response > class_num)
	    continue;
	  hist[bin_num*((int) response)+bin] += weight;
	}
    }
  else
    {
      for(int  observation = 0; observation < node_observations_num; observation++)
	{
	  bin = observations_feature[node_observations[observation]-1] << 1;
	  weight = observations_weights[observation];
	  response = responses[node_observations[observation]-1];
	  hist[bin] += weight;
	  hist[bin+1] += weight*response;
	}
    }  
}

extern "C"
{

  /*
    This function loops through the histograms and calls buildHistogram
    @param R_observations_feature - dataframe of observations of feature vectors
    @param R_responses - dataframe of observations of response variables
    @param R_forest - dlist forest keeping track of all the node assignments etc
    @param R_active_nodes - array of which nodes to build histogram for
   */

  SEXP buildHistograms(SEXP R_observations, SEXP R_responses, 
		       SEXP R_forest,
		       SEXP R_active_nodes, SEXP R_random_features,
		       SEXP histograms)
  {
    hpdRFforest * forest = (hpdRFforest *) R_ExternalPtrAddr(R_forest);
    SEXP R_response_cardinality = getAttrib(R_forest,install("response_cardinality"));
    SEXP R_features_cardinality = getAttrib(R_forest,install("features_cardinality"));
    SEXP R_bin_num = getAttrib(R_forest,install("bin_num"));
    SEXP R_nodes = getAttrib(R_forest,install("leaf_nodes"));


    int* active_nodes = INTEGER(R_active_nodes);
    int* bin_num = forest->bin_num;
    int class_num = forest->response_cardinality == NA_INTEGER ? 
      2: forest->response_cardinality;
    int* features_categorical = forest->features_cardinality;
    bool response_categorical = forest->response_cardinality != NA_INTEGER;
    int single_hist_size = 0;
    for(int i = 0; i < forest->nfeature; i++)
      if(single_hist_size < bin_num[i])
	single_hist_size = bin_num[i];
    single_hist_size *= class_num;

    int* node_observations;
    int node_observations_num;
    double* weight_observations; 
    int* node_features;
    bool realloc_histogram = false;

    hpdRFnode *node_curr;
    if(histograms == R_NilValue ||
       length(histograms) < length(R_active_nodes))
      {
	SEXP temp_histograms = histograms;
	PROTECT(histograms = allocVector(VECSXP, length(R_active_nodes)));
	for(int i = 0; i < length(temp_histograms); i++)
	  SET_VECTOR_ELT(histograms,i,VECTOR_ELT(temp_histograms,i));
	for(int i = length(temp_histograms); i < length(histograms); i++)
	  {
	    SEXP node_histogram;
	    PROTECT(node_histogram=allocVector(VECSXP,forest->features_num));
	    SET_VECTOR_ELT(histograms,i,node_histogram);
	    for(int j = 0; j < forest->features_num; j++)
	      {
		SEXP single_hist;
		PROTECT(single_hist = allocVector(REALSXP,single_hist_size));
		SET_VECTOR_ELT(node_histogram,j,single_hist);
		setAttrib(single_hist,install("feature"),ScalarInteger(-1));
		UNPROTECT(1);
	      }
	    UNPROTECT(1);
	  }
	realloc_histogram = true;
      }


    for(int i = 0; i < length(R_active_nodes); i++)
      {
	int active_node = active_nodes[i]-1;
	SEXP random_node_features = VECTOR_ELT(R_random_features,i);
	node_features = INTEGER(random_node_features);

	node_curr = forest->leaf_nodes[active_node];
	weight_observations = node_curr->additional_info->weights;
	node_observations = node_curr->additional_info->indices;
	node_observations_num = node_curr->additional_info->num_obs;

	SEXP node_histograms;
	node_histograms = VECTOR_ELT(histograms,i);

	for(int feature = 0; feature < length(random_node_features); feature++)
	  {
	    int featureIndex = node_features[feature]-1;
	    SEXP histogram_curr;  
	    int histogram_size = class_num*bin_num[featureIndex] ;

	    histogram_curr = VECTOR_ELT(node_histograms,feature);

	    memset(REAL(histogram_curr),0,sizeof(double)*histogram_size);
#define buildSingleHistogram(x) buildHistogram<x>(VECTOR_ELT(R_observations,featureIndex), \
						  R_responses,weight_observations, \
						  node_observations, node_observations_num, \
						  bin_num[featureIndex], class_num, \
						  features_categorical[featureIndex] != NA_INTEGER, \
						  response_categorical, histogram_curr)

	    if(TYPEOF(VECTOR_ELT(R_responses,0)) == INTSXP)
	      buildSingleHistogram(int);
	    else if(TYPEOF(VECTOR_ELT(R_responses,0)) == REALSXP)
	      buildSingleHistogram(double);
	    SET_VECTOR_ELT(node_histograms,feature,histogram_curr);
	    SEXP featureAttrib = getAttrib(histogram_curr,install("feature"));
	    *INTEGER(featureAttrib) = featureIndex+1;
	  }
      }
    if(realloc_histogram)
      UNPROTECT(1);
    return histograms;
  }


}
