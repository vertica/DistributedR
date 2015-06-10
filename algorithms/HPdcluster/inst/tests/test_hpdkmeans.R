library(HPdcluster)

iris2 <- iris[,-5]
X <- as.darray(data.matrix(iris2))
centers <- matrix(c(5.901613,5.006000,6.850000,2.748387,3.428000, 3.073684,4.393548,1.462000,5.742105,1.433871,0.246000,2.071053),3,4)
dimnames(centers) <- list(1L:3L, colnames(iris2))

rkm <- kmeans(iris2, centers, algorithm="Lloyd")

########## General Tests for input validation ##########
context("Checking the input validation in hpdkmeans")

test_that("the inputes are validated", {
    expect_error(hpdkmeans(iris2, 3))
    expect_error(hpdkmeans(X, -3))
    expect_error(hpdkmeans(X, 3, iter.max = 0))
    expect_error(hpdkmeans(X, 3, nstart = 0))
    expect_error(hpdkmeans(X, 3, na_action = "pass"))
    expect_error(hpdkmeans(X, c(1,1)))
})

########## Evaluate results ##########
context("Checking the results of hpdkmeans")

test_that("the results are correct in incomplete mode", {
    set.seed(10)
    hkm <- hpdkmeans(X, centers=3)

    expect_is(hkm$centers, "matrix")
    expect_equivalent(dim(hkm$centers), c(3L,ncol(X)))
    expect_true(is.na(hkm$totss))
    expect_true(is.na(hkm$withinss))
    expect_true(is.na(hkm$tot.withinss))
    expect_true(is.na(hkm$betweenss))
    expect_equivalent(hkm$size, c(50, 61, 39))
    expect_true(is.numeric(hkm$iter))

    newdata <- matrix(c(5,4,3,5,7,1,0,8),2,4)
    expect_equivalent(hpdapply(newdata,hkm$centers), matrix(c(3,2),ncol=1))
})

test_that("the results are correct in complete mode", {
    hkm <- hpdkmeans(X, centers=centers, completeModel=TRUE)

    expect_equivalent(hkm$centers, rkm$centers)
    expect_equivalent(hkm$totss, rkm$totss)
    expect_equivalent(hkm$withinss, rkm$withinss)
    expect_equivalent(hkm$tot.withinss, rkm$tot.withinss)
    expect_equivalent(hkm$betweenss, rkm$betweenss)
    expect_equivalent(length(hkm$size), nrow(centers))
    expect_equivalent(sum(hkm$size), nrow(X))
    expect_equivalent(hkm$iter, rkm$iter)

    expect_false(is.null(hkm$cluster))
    depkm <- deploy.hpdkmeans(hkm)
    expect_true(is.null(depkm$cluster))

    newdata <- matrix(c(5,4,3,5,7,1,0,8),2,4)
    expect_equivalent(hpdapply(newdata,depkm$centers), matrix(c(3,1),ncol=1))
})

