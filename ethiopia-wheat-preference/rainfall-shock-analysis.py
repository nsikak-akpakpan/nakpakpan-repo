import numpy as np

def calculate_rainfall_shock(rainfall_data, long_term_mean, long_term_std):
    """
    Calculates normalized deviations (Rainfall Shocks) from long-term mean rainfall.
    
    Parameters:
    rainfall_data : ndarray
        Seasonal rainfall values (1D array for a single season or 2D array for multiple seasons).
    long_term_mean : float
        Long-term mean rainfall (calculated from historical data).
    long_term_std : float
        Long-term standard deviation of rainfall (calculated from historical data).
        
    Returns:
    rainfall_shocks : ndarray
        Normalized deviations (Rainfall Shocks) as a 1D or 2D array.
    """
    # Validate input
    if long_term_std <= 0:
        raise ValueError("Standard deviation must be greater than zero.")
    
    # Calculate normalized deviations (Rainfall Shocks)
    rainfall_shocks = (rainfall_data - long_term_mean) / long_term_std
    
    return rainfall_shocks

# Example usage
# Simulated rainfall data for a season
rainfall_data = np.array([100, 120, 90, 130, 110])  # Example data in millimeters

# Long-term mean and standard deviation
long_term_mean = 105.0  # Example mean in millimeters
long_term_std = 15.0    # Example standard deviation in millimeters

# Calculate Rainfall Shocks
rainfall_shocks = calculate_rainfall_shock(rainfall_data, long_term_mean, long_term_std)

print("Rainfall shocks:", rainfall_shocks)
