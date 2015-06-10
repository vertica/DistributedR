library(HPdgraph)

graph <- matrix(0, 6,6)
graph[2,1] <- 1L;graph[2,3] <- 1L;graph[3,1] <- 1L;graph[3,2] <- 1L;
graph[3,4] <- 1L;graph[4,5] <- 1L;graph[4,6] <- 1L;graph[5,4] <- 1L; 
graph[5,6] <- 1L;graph[6,4] <- 1L

wrong_dgraph <- as.darray(graph)
dgraph <- as.darray(graph, c(6,3))

graph[6,5] <- NA
dgraph_na <- as.darray(graph, c(6,3))

wrong_personalized1 <- rep(1,nrow(graph))
wrong_personalized2 <- clone(dgraph, nrow=2, data=1/6)
wrong_personalized3 <- darray(c(1,6), c(1,2), data=1/6)
correct_personalized <- darray(c(1,6), c(1,3), data=1/6)

wrong_weights1 <- darray(c(6,3),c(2,3), data=1)
wrong_weights2 <- darray(c(1,6), c(1,3), data=1)
wrong_weights3 <- darray(c(6,6), c(3,6), data=1)
correct_weights <- darray(c(6,6),c(6,3), data=1)

########## General Tests for input validation ##########
context("Checking the input validation in hpdpagerank")

test_that("the inputes are validated", {
    expect_error(hpdpagerank(graph))
    expect_error(hpdpagerank(wrong_dgraph))
    expect_error(hpdpagerank(dgraph, niter = 0))
    expect_error(hpdpagerank(dgraph, eps = 0))
    expect_error(hpdpagerank(dgraph, damping = 2))
    expect_error(hpdpagerank(dgraph, personalized = wrong_personalized1))
    expect_error(hpdpagerank(dgraph, personalized = wrong_personalized2))
    expect_error(hpdpagerank(dgraph, personalized = wrong_personalized3))
    expect_error(hpdpagerank(dgraph, weights = wrong_weights1))
    expect_error(hpdpagerank(dgraph, weights = wrong_weights2))
    expect_error(hpdpagerank(dgraph, weights = wrong_weights3))
    expect_error(hpdpagerank(dgraph, na_action = "what?"))
    expect_error(hpdpagerank(dgraph_na, na_action = "pass"))
    expect_error(hpdpagerank(dgraph_na, na_action = "fail"))
})

########## Evaluate results ##########
context("Checking the results of hpdpagerank for a dense graph")

test_that("the returns result is correct despite available NA", {
    expect_warning(pg <- hpdpagerank(dgraph_na, na_action = "exclude"))

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(getpartition(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(hpdwhich.max(pg), 4)
})

test_that("the returns result is correct with available personalized darray", {
    pg <- hpdpagerank(dgraph, personalized = correct_personalized)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(getpartition(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(hpdwhich.max(pg), 4)
})

test_that("the returns result is correct with available weights darray", {
    pg <- hpdpagerank(dgraph, weights = correct_weights)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(getpartition(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(hpdwhich.max(pg), 4)
})

context("Checking the results of hpdpagerank for a sparse graph")

    Sdgraph <- darray(c(6,6), c(6,2), sparse=TRUE)
    foreach(i, 1:npartitions(Sdgraph), progress=FALSE, function(sg=splits(Sdgraph,i), i=i) {
        if(i == 1) {
            sg[2,1] <- 1L; sg[3,1] <- 1L; sg[3,2] <- 1L;
        } else if (i == 2) {
            sg[2,1] <- 1L; sg[3,2] <- 1L;  sg[5,2] <- 1L; 
            sg[6,2] <- 1L
        } else {
            sg[4,1] <- 1L; sg[4,2] <- 1L; sg[5,2] <- 1L; 
        }

        update(sg)
    })

test_that("the returns result is correct", {

    pg <- hpdpagerank(Sdgraph)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(getpartition(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(hpdwhich.max(pg), 4)
})

test_that("the returns result is correct with weights", {
    expect_error(hpdpagerank(Sdgraph, weights = correct_weights))

    Sweights <- clone(Sdgraph)
    pg <- hpdpagerank(Sdgraph, weights = Sweights)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(getpartition(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(hpdwhich.max(pg), 4)
})


