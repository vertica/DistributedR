// -*- mode: C++; c-indent-level: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

// we only include RcppArmadillo.h which pulls Rcpp.h in for us
#include "RcppArmadillo.h"
#include <cmath>

// via the depends attribute we tell Rcpp to create hooks for
// RcppArmadillo so that the build process will know what to do
//
// [[Rcpp::depends(RcppArmadillo)]]

// simple example of creating two matrices and
// returning the result of an operatioon on them
//
// via the exports attribute we tell Rcpp to make this function
// available from R
//
// [[Rcpp::export]]
arma::mat rcpparma_hello_world() {
    arma::mat m1 = arma::eye<arma::mat>(3, 3);
    arma::mat m2 = arma::eye<arma::mat>(3, 3);
	                     
    return m1 + 3 * (m1 + m2);
}


// another simple example: outer product of a vector, 
// returning a matrix
//
// [[Rcpp::export]]
arma::mat rcpparma_outerproduct(const arma::colvec & x) {
    arma::mat m = x * x.t();
    return m;
}

// and the inner product returns a scalar
//
// [[Rcpp::export]]
double rcpparma_innerproduct(const arma::colvec & x) {
    double v = arma::as_scalar(x.t() * x);
    return v;
}


// and we can use Rcpp::List to return both at the same time
//
// [[Rcpp::export]]
Rcpp::List rcpparma_bothproducts(const arma::colvec & x) {
    arma::mat op = x * x.t();
    double    ip = arma::as_scalar(x.t() * x);
    return Rcpp::List::create(Rcpp::Named("outer")=op,
                              Rcpp::Named("inner")=ip);
}

/*
// [[Rcpp::export]]
arma::mat computeFactors(arma::mat& ls, const arma::mat& fs, const arma::sp_mat& sp_rs, double lambda, int numFactors, bool row_wise) {
	int ncol = ls.n_cols;
	int i, num_rated;

    arma::mat lambda_I = lambda * arma::eye<arma::mat>(numFactors, numFactors);
	arma::vec ratings;
	arma::uvec idx_rated;
	arma::mat Y_i;
	arma::vec r_i;
	arma::vec x_i;
	arma::mat rs;
	double lmbd;
	arma::mat YYt;
	arma::mat Yr;

	for(i = 0; i < ncol; i++) {
		rs = sp_rs.col(i);
		ratings = arma::conv_to<arma::vec>::from(rs);
		idx_rated = find(ratings);
		num_rated = idx_rated.n_elem;

		if(num_rated != 0) {
			Y_i = fs.cols(idx_rated);
			r_i = ratings.rows(idx_rated);
			x_i = arma::solve(Y_i * Y_i.t() + num_rated * lambda_I, Y_i * r_i);
		} else {
			x_i = arma::zeros<arma::vec>(numFactors);
		}

		ls.col(i) = x_i;
	}

	return ls;
}
*/

// [[Rcpp::export]]
arma::mat computeFactors(arma::mat& ls, const arma::mat& fs, const arma::sp_mat& sp_rs, double lambda, int numFactors) {
	int ncol = ls.n_cols;
	int i, num_rated;

    //arma::mat lambda_I = lambda * arma::eye<arma::mat>(numFactors, numFactors);
	arma::vec ratings;
	arma::uvec idx_rated;
	arma::mat Y_i;
	arma::vec r_i;
	arma::vec x_i;
	arma::mat rs;
	double lmbd;
	arma::mat YYt(numFactors, numFactors);
	arma::mat Yr(numFactors, 1);
	arma::vec y;

	for(i = 0; i < ncol; i++) {
		rs = sp_rs.col(i);
		//ratings = arma::conv_to<arma::vec>::from(rs);
		idx_rated = find(rs);
		num_rated = idx_rated.n_elem;

		if(num_rated != 0) {
			/*
			Y_i = fs.cols(idx_rated);
			r_i = ratings.rows(idx_rated);
			YYt = Y_i * Y_i.t();
			Yr = Y_i * r_i;
			*/

			YYt.fill(0);
			Yr.fill(0);

			for(int j = 0; j < num_rated; j++) {
				y = fs.col(idx_rated(j));
				YYt += y * y.t();
				Yr += y * rs(idx_rated(j));
			}

			lmbd = num_rated * lambda;
			for(int j = 0; j < numFactors; j++) {
				YYt(j, j) += lmbd;
			}
			x_i = arma::solve(YYt, Yr);

		} else {
			x_i = arma::zeros<arma::vec>(numFactors);
		}

		ls.col(i) = x_i;
	}

	return ls;
}

// [[Rcpp::export]]
arma::mat computeFactors_new(arma::mat& ls, const arma::mat& fs, const arma::sp_mat& sp_rs, double lambda, int numFactors, const arma::vec& partInfo, int fsSplit) {
	int ncol = ls.n_cols;
	int i, j, num_rated;

    //arma::mat lambda_I = lambda * arma::eye<arma::mat>(numFactors, numFactors);
	arma::vec ratings;
	arma::uvec idx_rated;
	arma::uvec orig_idx_rated;
	arma::mat Y_i;
	arma::vec r_i;
	arma::vec x_i;
	arma::mat rs;
	double lmbd;
	arma::mat YYt(numFactors, numFactors);
	arma::mat Yr(numFactors, 1);
	arma::vec y;
	int idx_in_part;
	int orig_part;
	int new_part;
	int idx;

	for(i = 0; i < ncol; i++) {
		rs = sp_rs.col(i);
		//ratings = arma::conv_to<arma::vec>::from(rs);
		idx_rated = find(rs);
		num_rated = idx_rated.n_elem;

		if(num_rated != 0) {
			/*
			Y_i = fs.cols(idx_rated);
			r_i = ratings.rows(idx_rated);
			YYt = Y_i * Y_i.t();
			Yr = Y_i * r_i;
			*/

			orig_idx_rated = idx_rated;

			for(j = 0; j < num_rated; j++) {
				idx = int(idx_rated(j));
				idx_in_part = idx % fsSplit;
				orig_part = (idx - 1) / fsSplit + 1;
				new_part = arma::conv_to<int>::from(find(partInfo == orig_part)) + 1;
				idx_rated(j) = idx - (orig_part - new_part) * fsSplit;
			}

			YYt.fill(0);
			Yr.fill(0);

			for(j = 0; j < num_rated; j++) {
				y = fs.col(idx_rated(j));
				YYt += y * y.t();
				Yr += y * rs(orig_idx_rated(j));
			}

			lmbd = num_rated * lambda;
			for(j = 0; j < numFactors; j++) {
				YYt(j, j) += lmbd;
			}
			x_i = arma::solve(YYt, Yr);

		} else {
			x_i = arma::zeros<arma::vec>(numFactors);
		}

		ls.col(i) = x_i;
	}

	return ls;
}

// [[Rcpp::export]]
double countNonzeros(arma::sp_mat& R) {
	int total = 0;
	int i;
	arma::vec ratings;
	arma::uvec idx_rated;
	arma::mat r;

	for(i = 0; i < R.n_cols; i++) {
		r = R.col(i);
		ratings = arma::conv_to<arma::vec>::from(r);
		idx_rated = find(ratings);
		total += idx_rated.n_elem;
	}

	return double(total) / i;
}

// [[Rcpp::export]]
arma::vec gatherPartitionInfo(arma::mat& us, arma::sp_mat& sp_rs, int np) {
	int numItems = sp_rs.n_rows;
	int nnz, i;
	arma::mat rs;
	arma::vec ratings;
	arma::uvec idx_rated;
	arma::vec ret(np);
	int splitSize = ceil(numItems/ double(np));
	int idx;

	for(i = 0; i < us.n_cols; i++) {
		rs = sp_rs.col(i);
		ratings = arma::conv_to<arma::vec>::from(rs);
		idx_rated = find(ratings);
		nnz = idx_rated.n_elem;

		if(nnz != 0) {
			for(int j=0; j < nnz; j++) {
				idx = idx_rated(j) / splitSize;
				ret(idx) = 1;
			}
		}
	}

	return ret;
}
