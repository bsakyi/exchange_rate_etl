import psycopg2
import boto3
from importlib import reload

import config 
config = reload(config)





# function for connecting to pg and pg based databases
def postgresdb_connection(host, database, user, port, password):
    try:

        conn = psycopg2.connect(host=host, database=database, user=user, port=port, password=password)
        if conn.closed == 0:
            print(f"Connection to Postgres Server: {host} is successful on Port: {port}. Database is {database}")
        else:
            print(f"Cannot establish connection to {host}")

        cur = conn.cursor 
        return cur, conn 
    except psycopg2.Error as e:
        print(f"Cannot establish connection to {host}: {e}")
        return None, None

  

#Redshift Connection params
dbname = config.redshift_dbname
host =config.redshift_host
port = config.redshift_port
user = config.redshift_user
password = config.redshift_password

#Connect to redshift
conn_redshift = psycopg2.connect(dbname=dbname, host=host, port=port, user=user,  password=password )
cur_redshift = conn_redshift.cursor()
print(conn_redshift)

# Establish connection to redshift.
redshift_cur, redshift_con = postgresdb_connection(host, dbname, user, port, password)



#Aws access keys and s3 bucket object
aws_access_key_id = config.aws_access_key_id
aws_secret_access_key= config.aws_secret_access_key
region_name=config.region_name


s3_bucket = boto3.client('s3',
                  aws_access_key_id = aws_access_key_id,
                  aws_secret_access_key= aws_secret_access_key,
                  region_name=region_name)
# print(s3_bucket)



# local postgres conection params
pg_dbname = config.pg_dbname 
pg_host = config.pg_host
pg_port = config.pg_port
pg_user = config.pg_user
pg_password = config.pg_password 

# #Connect to local postgres
conn_pg = psycopg2.connect(dbname=pg_dbname, host=pg_host, port=pg_port, user=pg_user,  password=pg_password )
cur_pg = conn_pg.cursor()
# print(conn_pg)



# establish connection local postgres
local_pgcur, local_pgcon = postgresdb_connection( pg_host, pg_dbname, pg_user, pg_port, pg_password)



def main():
    print("REDSHIFT")
    print(redshift_cur, redshift_con)
    print("LOCAL")
    print(local_pgcur, local_pgcon)
    print("Hello")

if __name__ == "__main__":
    main()