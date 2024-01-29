# postgres load

#importing required packages
import json
import psycopg2
import pandas as pd 
import time

#reading json file
df_json=pd.read_json('config.json',typ='series')
#print(df_json['postgres_host'])

#to connect with postgres
conn=psycopg2.connect(
    database=df_json['postgres_database'],
    user=df_json['postgres_user'],
    password=df_json['postgres_password'],
    host=df_json['postgres_host'],
    port=df_json['postgres_port']
)

conn.autocommit = True
cursor = conn.cursor()

#Import .csv file
#df_csv = pd.read_csv('td_pg_input.csv')

start_time = time.time()

sql = ''' select * from public.demo_phrm '''
cursor.execute(sql)
results = cursor.fetchall()
print("Output:",results)

results =pd.DataFrame(results)

results.to_csv('test_output.csv', index=False, header=False)



#perform COPY test and print result
sql_to_load ='''copy public.demo_phrm from 'E:\\Programming\\vscode_python\\to_adi\\td_pg_input.csv' delimiter ',' csv '''
cursor.execute(sql_to_load)

print("Done")

cursor.close()
print("COPY duration: {} seconds".format(time.time() - start_time))



#close connection
conn.close()