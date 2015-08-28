\name{deploy.hpdegbm}
\alias{deploy.hpdegbm}
\title{
Verify the hpdegbm Model 
}
\description{
This function verifies that the formatting of the input model matches that of distributed gbm so that predict.hpdegbm can be used.
}
\usage{
deploy.hpdegbm <- function(model)

}
\arguments{
  \item{model}{
an object of class hpdegbm, such as that created by the function hpdegbm.
}
}

\value{
An object of class gbm.
}
\author{
    HP Vertica Analytics Team
}
