writeLines("
library(mvtnorm)

simulate_mvp <- function(X, beta, cov_matrix) {
  n_samples <- nrow(X)
  n_features <- ncol(X)
  n_traits <- ncol(beta)
  
  if (nrow(beta) != n_features || nrow(cov_matrix) != n_traits || ncol(cov_matrix) != n_traits) {
    stop('Dimensions of beta or cov_matrix are inconsistent with input data.')
  }
  
  latent_variables <- X %*% beta + rmvnorm(n_samples, sigma = cov_matrix)
  
  Y <- ifelse(latent_variables > 0, 1, 0)
  
  return(Y)
}

set.seed(123)
n_samples <- 100
n_features <- 3
n_traits <- 2

X <- matrix(rnorm(n_samples * n_features), nrow = n_samples, ncol = n_features)
beta <- matrix(rnorm(n_features * n_traits), nrow = n_features, ncol = n_traits)
cov_matrix <- matrix(c(1, 0.5, 0.5, 1), nrow = n_traits, ncol = n_traits)

Y <- simulate_mvp(X, beta, cov_matrix)

print(Y)
", con = "ethiopia-wheat-preference-mvp.r")
