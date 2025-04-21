calculate_rainfall_shock <- function(rainfall_data, long_term_mean, long_term_std) {
  # Parameters:
  # rainfall_data: Vector of seasonal rainfall values
  # long_term_mean: Long-term mean rainfall (scalar)
  # long_term_std: Long-term standard deviation of rainfall (scalar)
  
  # Validate inputs
  if (long_term_std <= 0) {
    stop("Standard deviation must be greater than zero.")
  }
  
  # Calculate Rainfall Shocks (normalized deviations)
  rainfall_shocks <- (rainfall_data - long_term_mean) / long_term_std
  
  return(rainfall_shocks)
}

# Example usage
set.seed(123)
rainfall_data <- c(100, 120, 90, 130, 110) # Example rainfall data in millimeters
long_term_mean <- 105.0                   # Example long-term mean in millimeters
long_term_std <- 15.0                     # Example long-term standard deviation in millimeters

rainfall_shocks <- calculate_rainfall_shock(rainfall_data, long_term_mean, long_term_std)
print("Rainfall shocks:")
print(rainfall_shocks)
