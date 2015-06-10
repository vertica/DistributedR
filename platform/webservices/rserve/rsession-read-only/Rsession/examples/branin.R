branin <- function(x) {
	x1 <- x[1]*15-5   
	x2 <- x[2]*15     
	(x2 - 5/(4*pi^2)*(x1^2) + 5/pi*x1 - 6)^2 + 10*(1 - 1/(8*pi))*cos(x1) + 10
}

cat("z = ",branin(c( 0.456 , 0.789 )),"\n");
