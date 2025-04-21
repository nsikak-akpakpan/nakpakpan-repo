import numpy as np
from scipy.stats import multivariate_normal

def simulate_multivariate_probit(X, beta, cov_matrix):
    """
    Simulates a Multivariate Probit Model.
    
    Parameters:
    X : ndarray
        Independent variables (n_samples x n_features).
    beta : ndarray
        Coefficients for each dependent variable (n_features x n_traits).
    cov_matrix : ndarray
        Covariance matrix for the error terms (n_traits x n_traits).
        
    Returns:
    Y : ndarray
        Binary outcomes (n_samples x n_traits).
    """
    # Check dimensions
    n_samples, n_features = X.shape
    n_traits = beta.shape[1]
    
    if cov_matrix.shape != (n_traits, n_traits):
        raise ValueError("Covariance matrix dimensions do not match number of traits.")
    
    # Calculate latent variables
    latent_variables = X @ beta + multivariate_normal.rvs(mean=np.zeros(n_traits),
                                                          cov=cov_matrix,
                                                          size=n_samples)
    
    # Convert latent variables to binary outcomes
    Y = (latent_variables > 0).astype(int)
    
    return Y

# Example usage
# Simulate data
n_samples = 100
n_features = 3
n_traits = 2

X = np.random.randn(n_samples, n_features)  # Random independent variables
beta = np.random.randn(n_features, n_traits)  # Random coefficients
cov_matrix = np.array([[1, 0.5], [0.5, 1]])  # Example covariance matrix

# Simulate Multivariate Probit outcomes
Y = simulate_multivariate_probit(X, beta, cov_matrix)

print("Simulated binary outcomes:\n", Y)
