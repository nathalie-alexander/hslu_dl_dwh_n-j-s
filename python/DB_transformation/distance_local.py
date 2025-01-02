import pandas as pd
from geopy.distance import geodesic

df = pd.read_csv('temp_vehicle_demographics.csv')
df['timestamp'] = pd.to_datetime(df['rounded_timestamp'])

# Sort data by vehicle_id and timestamp
df.sort_values(by=['vehicle_id', 'timestamp'], inplace=True)

# Initialize the distance column with 0
df['distance'] = 0.0

# Compute distances for consecutive rows within each vehicle
for vehicle_id, group in df.groupby('vehicle_id'):
    group = group.sort_values(by='timestamp')
    previous_coords = None  # To store the previous coordinates
    
    for index, row in group.iterrows():
        current_coords = (row['latitude'], row['longitude'])
        if previous_coords is not None:
            # Calculate distance from the previous row's coordinates
            df.loc[index, 'distance'] = geodesic(previous_coords, current_coords).meters
        # Update previous coordinates
        previous_coords = current_coords

# Define the distance thresholds
thresholds = [10, 100, 500, 1000, 10000, 100000]

# Calculate counts for each range
counts = {}
for i in range(len(thresholds)):
    if i == 0:
        # For the first threshold, count values less than the first threshold
        counts[f'< {thresholds[i]} m'] = (df['distance'] < thresholds[i]).sum()
    else:
        # For subsequent thresholds, count values >= lower bound and < upper bound
        lower_bound = thresholds[i - 1]
        upper_bound = thresholds[i]
        counts[f'>= {lower_bound} m and < {upper_bound} m'] = (
            (df['distance'] >= lower_bound) & (df['distance'] < upper_bound)
        ).sum()

# Count values greater than the last threshold
counts[f'>= {thresholds[-1]} m'] = (df['distance'] >= thresholds[-1]).sum()

# Convert the result to a DataFrame for better presentation
counts_df = pd.DataFrame(list(counts.items()), columns=['Range', 'Count'])
print(counts_df)

df.to_csv('temp_vehicle_demographics_with_distance.csv', index=False)