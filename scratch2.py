# %%
import requests
import json
import pandas as pd 
from importlib import reload 
import gzip 
import connection


connection = reload(connection) 


# %%
# create connection object postgres db 
local_con =connection.local_pgcon
local_cur = connection.local_pgcur

print(local_con) 
print("\n")
print(local_cur) 


# %%



# %%
# create s3 object
s3 = connection.s3_bucket

# create Redshift connection and cursor
cur_redshift = connection.redshift_cur
conn_redshift = connection.redshift_con
# print(cur_redshift)
print(cur_redshift)

#create local postgres connection and cursor
cur_pg = connection.local_pgcur
conn_pg = connection.local_pgcon



# %%
# delete this block
# conn_redshift = connection.conn_redshift
# cur_redshift = connection.cur_redshift
# print(cur_redshift)

# %%
url = 'https://api.coinbase.com/v2/currencies'
response = requests.get(url)         

# %%
# api_data = response.text

# %%
# Check if the request was successful (status code 200)
if response.status_code == 200:

    api_data = response.text
   
    list_data = json.loads(api_data)
    
    currency_data = list_data.get('data', [])

    # Read the JSON data into a Pandas DataFrame
    df = pd.DataFrame(currency_data)
    
    # Specify the file path where you want to save the CSV file
    csv_file_path = 'scratch_csv.csv'
    
    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)
    
    print("CSV file saved successfully:", csv_file_path)
else:
    print("Failed to retrieve data. Status code:", response.status_code)

# %%
# creating a filename variable for compressed file to be uploaded to S3
file_name = csv_file_path + '.gz'
print(file_name)

# %%
df.describe()

# %%
# df to zip

df.to_csv(file_name, index=False, compression='gzip')

compressed_csv = file_name


# %%
with open(compressed_csv, 'rb') as file:
    # Upload the file object to S3 bucket
    s3.upload_fileobj(file, 'emil-coinbase-bucket', compressed_csv)

# %%
# Check if the file has been successfully uploaded
if s3.head_object(Bucket='emil-coinbase-bucket', Key=compressed_csv):
    print("CSV file uploaded to S3 bucket successfully")
else:
    print("Upload not successful")

# %%
    # create table for local db
table_name = 'currency_rates'
rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (ID VARCHAR(3) PRIMARY KEY, Name VARCHAR(65), min_size numeric(10, 8) ) '''
connection.cur_pg.execute(rates_table)
connection.conn_pg.commit()



# %%
# INSERT DATA TO LOCAL DB
try:
    # size of each batch
    batch_size = 1000

    # Insert DataFrame into database in batches
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        values = [tuple(row) for row in batch_df.values]

        placeholders = ','.join(['%s'] * len(df.columns))
        try:
            insert_query = f"INSERT INTO currency_rates (ID, Name, min_size) VALUES ({placeholders})"
            connection.cur_pg.executemany(insert_query, values)
            connection.conn_pg.commit()

            print(insert_query)
        except Exception as e:
            # Rollback the transaction
            connection.conn_pg.rollback()

            # If the insert fails due to a unique constraint violation, perform an update instead
            if 'duplicate key' in str(e):
                for value in values:
                    update_query = "UPDATE currency_rates SET Name = %s, min_size = %s WHERE ID = %s"
                    connection.cur_pg.execute(update_query, (value[1], value[2], value[0]))
                    connection.conn_pg.commit()  # Commit each update
            else:
                raise e

except Exception as e:
    print("An error occurred during batch data insertion:", e)
    connection.conn_pg.rollback()  # Rollback the transaction in case of an error



# %%
connection.conn_pg.rollback() 

# %%
# Upload data to local postgres db 
    # create table
table_name = 'currency_rates'
rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (ID VARCHAR(3), Name VARCHAR(65), min_size numeric(10, 8) ) '''
cur_pg.execute(rates_table)
conn_pg.commit()
    # copy csv


# insert_from_local = f"""COPY currency_rates
# FROM '{csv_file_path}'
# DELIMITER ','
# CSV
# HEADER;
#  """

insert_from_local= f"""copy currency_rates FROM '{csv_file_path}' CSV HEADER DELIMITER ','; """


# insert_from_local = f"""COPY currency_rates
# FROM '{csv_file_path}'
# IGNOREHEADER 1
# FORMAT AS csv
# DELIMITER ','; """

cur_pg.execute(insert_from_local)
conn_pg.commit()

conn_pg.close()

# %%
#  delete this block
connection.cur_redshift.execute('ROLLBACK')

# %%
    # create table REDSHIFT


try:
    table_name = 'currency_rates'
    rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        ID VARCHAR(3) PRIMARY KEY, 
                        Name VARCHAR(65), 
                        min_size numeric(10, 8)
                    )'''
    
  
    connection.cur_redshift.execute(rates_table)
    connection.conn_redshift.commit()
    print("Table created successfully.")

except Exception as e:
    connection.conn_redshift.rollback()
    print(f"Error creating table: {e}")

finally:
    connection.cur_redshift.close()


# %%
# upload data to Redshift
    # create table
table_name = 'currency_rates'
rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (ID VARCHAR(3) PRIMARY KEY, Name VARCHAR(65), min_size numeric(10, 8) ) '''
connection.cur_redshift.execute(rates_table)
connection.conn_redshift.commit()
    # copy csv


try:
    # Define the size of each batch
    batch_size = 1000

    # Insert DataFrame into Redshift database in batches
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        values = [tuple(row) for row in batch_df.values]

        placeholders = ','.join(['%s'] * len(df.columns))
        # # column_names = ','.join(df.columns)
        # column_names = ', '.join([f"{re.sub('[^a-zA-Z]+', '', col.replace(' ', '_'))}" for col in df.columns])
        insert_query = f"INSERT INTO currency_rates (ID, Name, min_size) VALUES ({placeholders})"
        print(insert_query)
        connection.cur_redshift.executemany(insert_query, values)
        connection.conn_redshift.commit()

except Exception as e:
    print("An error occurred during batch data insertion:", e)
    connection.conn_redshift.rollback()  # Rollback the transaction in case of an error

aws_access_key_id = connection.aws_access_key_id
aws_secret_access_key = connection.aws_secret_access_key

# insert_from_s3 = f"""COPY currency_rates
# FROM 's3://emil-coinbase-bucket/{csv_file_path}'
# CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
# IGNOREHEADER 1
# FORMAT AS csv
# DELIMITER ','; """

# insert_from_s3 = f"""COPY currency_rates
# FROM './{csv_file_path}'
# CREDENTIALS 'admin;Hello123'
# IGNOREHEADER 1
# FORMAT AS csv
# DELIMITER ','; """

# connection.cur_redshift.execute(insert_from_s3)
# connection.conn_redshift.commit()

# conn_redshift.close()








# %%


# %%
conn_redshift = connection.conn_redshift
cur_redshift = connection.cur_redshift
print(conn_redshift)

# %%
cur_redshift.execute('ROLLBACK')

# %%


)

# %%



