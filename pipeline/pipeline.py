import sys
print("arguments", sys.argv)

month = int(sys.argv[1])


import pandas as pd
df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_month_{month}.parquet")

print(f"Running pipeline for month {month}")