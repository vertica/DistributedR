generate_df <- function(n){
	a = c(1e5 + runif(n), -1e5 + runif(n))
	b = c(rep(1, n), rep(-1, n))
    data.frame(x=a, y=b)
}
