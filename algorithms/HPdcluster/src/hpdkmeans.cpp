/********************************************************************
 * Copyright (C) [2013] Hewlett-Packard Development Company, L.P.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.  You should have received a
 * copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 59 Temple Place,
 * Suite 330, Boston, MA 02111-1307 USA
 ********************************************************************/

/********************************************************************
 *  Functions related to the hpdkmeans algorithm
 ********************************************************************/

#include <Rcpp.h>
#include <Rinternals.h>
#include <cmath>    // std::sqrt

using namespace std;

/*
**  Calculating K-means clustering based on Lloyd algorithm
**  Rx: the matrix of samples
**  Rnorm: the matrix of norms
**  Rcen: the matrix of centers
**  Rcl: the vector of cluster labels
**  Rnc: the number of points in each cluster
*/

RcppExport SEXP hpdkmeans_Lloyd(SEXP Rx, SEXP Rnorm, SEXP Rcen, SEXP Rcl, SEXP Rnc)
{
    BEGIN_RCPP
    int n, k, p, cencol;
    int i, j, c, it, inew = 0;
    double best, dd, tmp;
    double *x, *norm, *cen;
    int *cl, *nc;
    double approxDistance = 0;

    int * dimx = INTEGER(Rf_getAttrib(Rx, R_DimSymbol));
    n = dimx[0]; // number of samples
    p = dimx[1]; // number of predictors
    int *dimc = INTEGER(Rf_getAttrib(Rcen, R_DimSymbol));
    k = dimc[0]; // number of centers
    cencol = dimc[1];
/*
    int * dimnorm = INTEGER(Rf_getAttrib(Rnorm, R_DimSymbol));
    if(dimnorm[0] != n)
        Rf_error("Wrong number of norms");
    if(dimnorm[1] != 1)
        Rf_error("There must be only one norm per sample");
*/
    Rx = Rf_coerceVector(Rx, REALSXP);
    Rnorm = Rf_coerceVector(Rnorm, REALSXP);
    Rcen = Rf_coerceVector(Rcen, REALSXP);
    Rcl = Rf_coerceVector(Rcl, INTSXP);
    Rnc = Rf_coerceVector(Rnc, INTSXP);

    x = REAL(Rx);
    norm = REAL(Rnorm);
    cen = REAL(Rcen);
    cl = INTEGER(Rcl);
    nc = INTEGER(Rnc);

    if(Rf_length(Rnorm) != n)
        Rf_error("Wrong number of norms");
    x = REAL(Rx);
    if( p != cencol )
        Rf_error("Wrong dimention of matrices");
    if( Rf_length(Rcl) != n )
        Rf_error("wrong dimention of cl");
    if( Rf_length(Rnc) != k )
        Rf_error("wrong dimention of nc");

    // calculating the norm of centers
    Rcpp::DoubleVector centerNorm(k);
    for(i = 0; i < k; i++) // iterating on centers
        for(j = 0; j < p; j++) // iterating on features
            centerNorm[i] += cen[i+k*j] * cen[i+k*j];
    for(i = 0; i < k; i++) // iterating on centers
        centerNorm[i] = std::sqrt(centerNorm[i]);

    // clear all the cluster labels
    for(i = 0; i < n; i++) cl[i] = -1;
    for(i = 0; i < n; i++) { // iterating on samples
        // find nearest centre for each point 
        best = R_PosInf;
        for(j = 0; j < k; j++) { // iterating on centers
            approxDistance = norm[i] - centerNorm[j];
            approxDistance *= approxDistance;
            if(best < approxDistance) continue;
	        dd = 0.0;
	        for(c = 0; c < p; c++) { // iterating on features
	            tmp = x[i+n*c] - cen[j+k*c];
	            dd += tmp * tmp;
	        }
	        if(dd < best) {
	            best = dd;
	            inew = j+1;
	        }
        }
        if(cl[i] != inew) {
               cl[i] = inew;
        }
    }
    // update each centre
    for(j = 0; j < k*p; j++) cen[j] = 0.0;
    for(j = 0; j < k; j++) nc[j] = 0;
    for(i = 0; i < n; i++) {
        it = cl[i] - 1; nc[it]++;
        for(c = 0; c < p; c++) cen[it+c*k] += x[i+c*n];
    }
    for(j = 0; j < k*p; j++) cen[j] /= nc[j % k];

    return R_NilValue;
    END_RCPP
}

/*
** Calculates the norm of input vectors (used in hpdkmeans)
**  Rx: the matrix of samples
**  Rnorm: the matrix of norms
*/
RcppExport SEXP calculate_norm(SEXP Rx, SEXP Rnorm) {
    BEGIN_RCPP
    int n, p;
    int i,j;
    double *x, *norm;

    int * dimx = INTEGER(Rf_getAttrib(Rx, R_DimSymbol));
    n = dimx[0]; // number of samples
    p = dimx[1]; // number of predictors
/*
    int * dimnorm = INTEGER(Rf_getAttrib(Rnorm, R_DimSymbol));
    if(dimnorm[0] != n)
        Rf_error("Wrong number of norms");
    if(dimnorm[1] != 1)
        Rf_error("There must be only one norm per sample");
*/
    Rx = Rf_coerceVector(Rx, REALSXP);
    Rnorm = Rf_coerceVector(Rnorm, REALSXP);
    if(Rf_length(Rnorm) != n)
        Rf_error("Wrong number of norms");
    x = REAL(Rx);
    norm = REAL(Rnorm);

    // calcilating the norm of samples
    for(i = 0; i < n; i++) // iterating on samples
        for(j = 0; j < p; j++) // iterating on features
            norm[i] += x[i+n*j] * x[i+n*j];
    for(i = 0; i < n; i++) // iterating on samples
        norm[i] = std::sqrt(norm[i]);
        
    return Rnorm;
    END_RCPP
}

/*
** Calculates wss (used in hpdkmeans)
**  Rx: the matrix of samples
**  Rcen: the matrix of centers
**  Rcl: the vector of cluster labels
*/
RcppExport SEXP calculate_wss(SEXP Rx, SEXP Rcen, SEXP Rcl) {
    BEGIN_RCPP
    int n, k, p, cencol;
    int i, j, label;
    double *x, *cen;
    int *cl;
    double dd, temp;

    int * dimx = INTEGER(Rf_getAttrib(Rx, R_DimSymbol));
    n = dimx[0]; // number of samples
    p = dimx[1]; // number of predictors
    int *dimc = INTEGER(Rf_getAttrib(Rcen, R_DimSymbol));
    k = dimc[0]; // number of centers
    cencol = dimc[1];

    Rx = Rf_coerceVector(Rx, REALSXP);
    Rcen = Rf_coerceVector(Rcen, REALSXP);
    Rcl = Rf_coerceVector(Rcl, INTSXP);

    x = REAL(Rx);
    cen = REAL(Rcen);
    cl = INTEGER(Rcl);

    if( p != cencol )
        Rf_error("Wrong dimention of matrices");
    if( Rf_length(Rcl) != n )
        Rf_error("wrong dimention of cluster labels");

    // the vector of wss
    Rcpp::DoubleVector dwss(k);
    for(i = 0; i < k; i++)
        dwss[i] = 0;

    for(i = 0; i < n; i++) { // iterating on samples
        dd = 0;
        label = cl[i] - 1;
        for(j = 0; j < p; j++) { // iterating on features
            temp = x[i+n*j] - cen[label+k*j];
            dd += temp * temp;
        }
        dwss[label] += dd;
    }

    return dwss;
    END_RCPP
}


