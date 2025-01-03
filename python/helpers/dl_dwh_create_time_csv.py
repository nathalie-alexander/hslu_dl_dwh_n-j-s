from datetime import datetime, timedelta
import pandas as pd

# define timeslot
start_date = datetime(2024, 10, 20)
end_date = datetime(2024, 11, 30)

# split time in hours
date_range = pd.date_range(start=start_date, end=end_date, freq="H")

# create dataframe
data = {
    "time_id": range(1, len(date_range) + 1),
    "timestamp": date_range,
    "date": date_range.date,
    "day": date_range.day,
    "month": date_range.month,
    "year": date_range.year,
    "weekday": date_range.weekday,  # 0=Montag, 6=Sonntag
    "week_of_year": date_range.isocalendar().week,
    "is_weekend": date_range.weekday.isin([5, 6]),  # Samstag=5, Sonntag=6
}

df = pd.DataFrame(data)

# create and save file
file_path = "path/to/directory/time_clean_3_hour_interval.csv"
df.to_csv(file_path, index=False)
