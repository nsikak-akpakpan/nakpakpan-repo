random_utility_framework <- function(X, beta, noise_scale = 1.0) {
  # Parameters:
  # X: Matrix of independent variables (n_samples x n_features)
  # beta: Matrix of coefficients for choices (n_features x n_choices)
  # noise_scale: Scale parameter for random noise added to utility
  
  # Validate dimensions
  n_samples <- nrow(X)
  n_features <- ncol(X)
  n_choices <- ncol(beta)
  
  if (nrow(beta) != n_features) {
    stop("Dimensions of X and beta are inconsistent.")
  }
  
  # Calculate deterministic utility
  deterministic_utility <- X %*% beta
  
  # Add random noise to utility
  random_noise <- matrix(rnorm(n_samples * n_choices, mean = 0, sd = noise_scale), 
                         nrow = n_samples, ncol = n_choices)
  total_utility <- deterministic_utility + random_noise
  
  # Choose the option with the highest utility for each sample
  choices <- apply(total_utility, 1, which.max)
  
  return(choices)
}

# Example usage
set.seed(123)
n_samples <- 100
n_features <- 3
n_choices <- 4

# Simulated data
X <- matrix(rnorm(n_samples * n_features), nrow = n_samples, ncol = n_features)
beta <- matrix(rnorm(n_features * n_choices), nrow = n_features, ncol = n_choices)
noise_scale <- 0.5

# Simulate choices
choices <- random_utility_framework(X, beta, noise_scale)

print("Chosen options:")
print(choices)
