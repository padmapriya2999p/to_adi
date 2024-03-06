import pandas as pd
import fastavro

avro_file = 'userdata1.avro'
csv_file = 'output.csv'

# Read Avro file and convert it to a list of dictionaries
with open(avro_file, 'rb') as f:
    avro_reader = fastavro.reader(f)
    records = [record for record in avro_reader]

# Convert list of dictionaries to a pandas DataFrame
df = pd.DataFrame(records)
print(df.head(5))

# Write DataFrame to CSV file
df.to_csv(csv_file, index=False)

print("Conversion completed successfully.")
