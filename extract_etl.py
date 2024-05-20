# %%
import requests
import json
import pandas as pd 
from datetime import datetime
from importlib import reload 
import gzip 
import connection


connection = reload(connection) 




# %% [markdown]
# CREATE CONNECTIONS TO AWS S3, AWS REDSHIFT, LOCAL POSTGRES DB

# %%
# create s3 object
s3 = connection.s3_bucket

# create Redshift connection and cursor
cur_redshift = connection.cur_redshift
conn_redshift = connection.conn_redshift


#create local postgres connection and cursor
cur_pg = connection.cur_pg
conn_pg = connection.conn_pg



# %%
# open exchange data, url is found in config.py


response = requests.get(connection.api_url)         

# %%
# Assign the API response to the variable api_data
api_data = response.text
# api_data

# %% [markdown]
# MAKE TRANSFORMATIONS TO THE API DATA: DATE COLUMN CREATION, RATE_DATE VARIABLE TO APPEND TO CSV FILE NAME

# %%
if response.status_code == 200:  
    api_data = f''' {api_data} '''
    api_data
    api_data = json.loads(api_data)
    # api_data
    df = pd.DataFrame(api_data)

    # extract base and timestamp column data
    base = api_data['base']
    timestamp = api_data['timestamp']

    # Get date and time from timestamp 
    date = datetime.fromtimestamp(timestamp)
    # format date to YYYYMMDD to be used as a column in the dataframe
    year = date.strftime('%Y%m%d')
    # format time to HMS
    time = date.strftime('%H%M%S')
    # this will be part of the csv filename to ensure uniqueness of filename 
    rate_date = year + '_'+ time

    df = pd.DataFrame(api_data['rates'].items(), columns=['Currency', 'Rate'])

    # Add base, timestamp, and date columns
    df['Base'] = base
    df['Timestamp'] = timestamp 
    df['Date'] = year
    print("API Data Successfully Extracted")
else:
    ("Failed to retrieve data. Status code:", response.status_code)



# %%
# File name to be used for saving csv, exchange_rate + date and time from the timestamp of the dataframe
csv_file_path = f'./datasets/exchange_rate_{rate_date}.csv' 
print(csv_file_path)
df.to_csv(csv_file_path, index=False)

# %%
# creating a filename variable for compressed file to be uploaded to S3
file_name = csv_file_path + '.gz'
print(file_name)

# %%
# Compress CSV to zip format and assign the compressed filename to compressed_csv

df.to_csv(file_name, index=False, compression='gzip')

# location for local storage
compressed_csv_local = file_name

# formatting the location to be stored as datasets/exchange_rate_date_time.csv.gz for s3 bucket
compressed_csv_s3 = file_name[2:]

print(compressed_csv_s3)




# %% [markdown]
# DATA UPLOAD TO AWS S3 BUCKET 

# %%
print("*****S3 DATA UPLOAD")
with open(compressed_csv_local, 'rb') as file:
    # Upload the file object to S3 bucket
    s3.upload_fileobj(file, 'emil-coinbase-bucket', compressed_csv_s3)

# %%
# Check if the file has been successfully uploaded
if s3.head_object(Bucket='emil-coinbase-bucket', Key=compressed_csv_s3):
    print("CSV file uploaded to S3 bucket successfully")
else:
    print("Upload not successful")

# %% [markdown]
# CREATE LOCAL DATABASE TABLE, AND INSERT DATA INTO DATABASE

# %%
print(f"Creating table in Local Database on Server: {connection.pg_host}")
table_name = 'exchange_rates'
try:
    
    rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        currency VARCHAR(3),
                        rate NUMERIC(16, 8),
                        base VARCHAR(3),
                        timestamp VARCHAR(16),
                        date VARCHAR(8)
                    )'''
    connection.cur_pg.execute(rates_table)
    connection.conn_pg.commit()
    print("Table Creation Successful")
except conn_pg.Error as e:
    connection.conn_pg.rollback()
    print("Error occurred:", e)




# %%
# INSERT DATA TO LOCAL DB
print("Data Upload Local DB")
try:
    # size of each batch
    batch_size = 200

    # Insert DataFrame into database in batches
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        values = [tuple(row) for row in batch_df.values]

        placeholders = ','.join(['%s'] * len(df.columns))
        try:
            insert_query = f'''INSERT INTO {table_name} (currency, rate, base, timestamp, date) 
                   VALUES ({placeholders})'''
                   

            connection.cur_pg.executemany(insert_query, values)
            connection.conn_pg.commit()

            print(insert_query)
            print('Data Successfully Inserted into Local Postgres DB')
        except Exception as e:
            # Rollback the transaction
            connection.conn_pg.rollback()


except Exception as e:
    print("An error occurred during batch data insertion:", e)
    connection.conn_pg.rollback()  # Rollback the transaction in case of an error


# %% [markdown]
# REDSHIFT TABLE CREATION AND FILE COPY FROM S3 BUCKET

# %%
    # create table REDSHIFT
print("Create Table Redshift")
table_name = 'exchange_rates'
try:
    
 rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        currency VARCHAR(3),
                        rate NUMERIC(16, 8),
                        base VARCHAR(3),
                        timestamp VARCHAR(16),
                        date VARCHAR(8)
                    )'''
    
  
 connection.cur_redshift.execute(rates_table)
 connection.conn_redshift.commit()
 print(f"Redshift Table {table_name} created successfully.")

except Exception as e:
 connection.conn_redshift.rollback()
 print(f"Error creating table: {e}")

# %%
# upload data to Redshift form s3 bucket


aws_access_key_id = connection.aws_access_key_id
aws_secret_access_key = connection.aws_secret_access_key


try:
    insert_from_s3 = f"""COPY {table_name}
    FROM 's3://emil-coinbase-bucket/{compressed_csv_s3}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    IGNOREHEADER 1
    FORMAT AS csv
    gzip
    DELIMITER ','; """
    
    connection.cur_redshift.execute(insert_from_s3)
    connection.conn_redshift.commit()
    print(f"Copy to Redshift: {table_name} was successful")

except Exception as e:
    connection.conn_redshift.rollback()
    print(f"An error occurred: {str(e)}")

finally:
    # Close the Redshift connection
    connection.conn_redshift.close()






