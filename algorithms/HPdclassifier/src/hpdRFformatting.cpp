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
 this function formats a single feature by converting all observations to bins
 @param  R_observations_feature - vector of feature observations
 @param feature_min - minimum value of all observations of this feature
 @param feature_max - maximum value of all observations of this feature
 @param bin_num - number of bins for this feature
 @param scale - flag indicating whether to scale this feature
 @return - the new feature observations converted to bins
 */
template<typename obs_type>
SEXP formatObservationFeature(SEXP R_observations_feature, 
			      double feature_min, double feature_max, 
			      int bin_num, bool scale)
{
  SEXP R_new_observations_feature;
  PROTECT(R_new_observations_feature=allocVector(INTSXP,length(R_observations_feature)));
  obs_type* observations_feature = RtoCArray<obs_type*>(R_observations_feature);
  int* new_observations_feature = INTEGER(R_new_observations_feature);
  for(int i = 0; i < length(R_observations_feature); i++)
    {
      if(feature_max <= feature_min)
	  new_observations_feature[i] = 0;
      else 
	{
	  if(scale)
	    {
	      new_observations_feature[i] = (observations_feature[i]-feature_min)*bin_num/
		(feature_max - feature_min);
	      if(new_observations_feature[i] >= bin_num)
		new_observations_feature[i] = bin_num-1;
	    }
	  else
	    new_observations_feature[i] = observations_feature[i];
	}
    }
  UNPROTECT(1);
  return R_new_observations_feature;
}

/*
 this function loops over the different features and converts them to bins
 @param R_observation_features - observations of feature variables
 @param R_features_min - min value of each feature
 @param R_features_max - max value of each feature
 @param R_bin_num - number of bins for each feature
 @param scale - flag indicating whether to scale this feature
 */
void formatObservationFeatures(SEXP R_observation_features, 
			       SEXP R_features_min, SEXP R_features_max, 
			       int* bin_num, bool scale)
{
  SEXP observation_feature;
  double *features_min = REAL(R_features_min);
  double *features_max = REAL(R_features_max);
  for(int i = 0; i < length(R_observation_features); i++)
    {
      observation_feature = VECTOR_ELT(R_observation_features,i);
#define formatSingleObservationFeature(x) formatObservationFeature<x>(observation_feature, \
								      features_min[i],features_max[i],bin_num[i], scale)
      if(TYPEOF(observation_feature) == INTSXP)
	SET_VECTOR_ELT(R_observation_features,i,formatSingleObservationFeature(int));
      else if (TYPEOF(observation_feature) == REALSXP)
	SET_VECTOR_ELT(R_observation_features,i,formatSingleObservationFeature(double));
    }
}
