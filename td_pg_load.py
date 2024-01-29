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
'''cursor.execute(sql)
results = cursor.fetchall()
print("Output:",results)'''

results =pd.read_sql(sql,conn)

results.to_csv('test_output.csv', index=False, header=False)






