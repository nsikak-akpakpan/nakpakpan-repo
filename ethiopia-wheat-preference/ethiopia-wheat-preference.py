'''
The **Multivariate Probit Model (MVP)** is a statistical tool used to analyze multiple correlated binary outcomes simultaneously. Here's a breakdown of its key features and applications:

1. **Purpose**: The MVP model is ideal for situations where decisions or preferences are interdependent. For example, in the context of wheat trait preferences, it helps identify correlations and trade-offs among traits like high yield, disease resistance, and good taste.

2. **Structure**: 
   - Each binary outcome is modeled as a latent variable influenced by explanatory variables and unobserved characteristics.
   - The latent variables follow a multivariate normal distribution, allowing for correlations between outcomes.

3. **Estimation**:
   - The model uses maximum likelihood estimation techniques, often with simulation-based methods like the GHK simulator for higher-dimensional cases.
   - It accounts for interdependencies among traits, making it suitable for studies involving multiple simultaneous decisions.

4. **Applications**:
   - Widely used in econometrics and agricultural studies to understand preferences, adoption of technologies, or decision-making processes.
   - In the study you referenced, the MVP model was employed to explore gendered wheat trait preferences and their correlations.


The **Multivariate Probit Model (MVP)** is based on a random utility framework. Here's the mathematical formulation:

1. **Latent Variable Representation**:
   $$Y^*_{ik} = X_i \beta_k + \mu_{ik}$$
   - \(Y^*_{ik}\): Latent variable representing the utility derived by respondent \(i\) for trait \(k\).
   - \(X_i\): Explanatory variables for respondent \(i\).
   - \(\beta_k\): Coefficients for trait \(k\).
   - \(\mu_{ik}\): Error term.

2. **Binary Outcome**:
   $$Y_{ik} = 
   \begin{cases} 
   1 & \text{if } Y^*_{ik} > 0 \\
   0 & \text{otherwise}
   \end{cases}$$
   - \(Y_{ik}\): Observed binary outcome for trait \(k\).

3. **Error Term Distribution**:
   The error terms \((\mu_{ik})\) follow a multivariate normal distribution:
   $$\mu \sim MVN(0, \Omega)$$
   - \(\Omega\): Covariance matrix with correlations between traits.

4. **Covariance Matrix**:
   $$\Omega = 
   \begin{bmatrix}
   1 & \rho_{T1T2} & \rho_{T1T3} & \dots \\
   \rho_{T2T1} & 1 & \rho_{T2T3} & \dots \\
   \dots & \dots & \dots & \dots
   \end{bmatrix}$$
   - \(\rho_{T1T2}\): Correlation between traits \(T1\) and \(T2\).

This model allows for simultaneous analysis of preferences for multiple traits, capturing interdependencies and correlations among them. Let me know if you'd like further clarification or examples!
'''

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
