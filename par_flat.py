import pandas as pd

parquet_file = 'userdata1.parquet'
df = pd.read_parquet(parquet_file)

#print(df.head(5))

csv_output = 'userdata1.csv'
df.to_csv(csv_output,index=False)

print("Conversion completed successfully.")