import numpy as np

def random_utility_framework(X, beta, noise_scale=1.0):
    """
    Simulates the Random Utility Framework for decision-making.
    
    Parameters:
    X : ndarray
        Independent variables (n_samples x n_features).
    beta : ndarray
        Coefficients for each choice (n_features x n_choices).
    noise_scale : float
        Scale parameter for random noise added to utility.
        
    Returns:
    choices : ndarray
        Indices of the chosen options (n_samples x 1).
    """
    # Validate dimensions
    n_samples, n_features = X.shape
    n_choices = beta.shape[1]
    
    if beta.shape[0] != n_features:
        raise ValueError("Dimensions of X and beta are inconsistent.")
    
    # Calculate deterministic utility for each choice
    deterministic_utility = X @ beta
    
    # Add random noise to utility
    random_noise = np.random.normal(loc=0.0, scale=noise_scale, size=(n_samples, n_choices))
    total_utility = deterministic_utility + random_noise
    
    # Choose the option with the highest utility for each sample
    choices = np.argmax(total_utility, axis=1)
    
    return choices

# Example usage
# Simulate data
n_samples = 100
n_features = 3
n_choices = 4

X = np.random.randn(n_samples, n_features)  # Random independent variables
beta = np.random.randn(n_features, n_choices)  # Random coefficients for choices
noise_scale = 0.5  # Adjust the noise scale for randomness in utility

# Simulate choices
choices = random_utility_framework(X, beta, noise_scale)

print("Chosen options:\n", choices)
