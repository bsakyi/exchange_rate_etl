# BAKGROUND
This  data for this project comes from openexchangerates.org in the form api calls.
It is a data of exchange rates of major currencies in the world. The dataset has the American USD as the base currency by which all the other currencies are measured.
Some of the data are current, some are historical, as far as the year 2004


# Transformations
The json data returned contains the base currency, that is USD, and the rate at which it is been traded against all the other currencies.

The timestamp returned in the request is converted to a unix datetime format which will be later used as part of the filename. The filename is in the format 'exchange_rate_yyyymmdd_hms'

A column is created with the name 'Date' which contains the date from the timestamp in the format 'YYYY-MM-DD'
The date together with the timestamp will be put together to create a unique column and a primary key.

Due to the fact that the data is that of exchange rates, which seldom changes within a day, when there script is run more than once within same day,  the rate values are updated, eventhough they will likely be the same.


# Persisted Files (datasets directory)
The ETL Pipeline extracts daily exchange rates, transforms it into a csv format, compresses it into gzip and persists in both the infrastructure where the code is run from(local infastructure) and also AWS S3 bucket.
In the local infrastructure, the persisted csv and zip files are in the directory called datasets.
In the S3 they are in {your-bucket-name}/datasets directory. The subdirectory datasets is to make the persisted dataset more organized, and the file naming used is to make it easy to identify which year month and day that dataset belongs to.


# Files and Directories
    Directories
        datasets - 
            it contains the persisted files, both csv and zipped, each filename ends with digits which are the year and time the dataset belongs to in the format YYYYMMDD_HMS

        images -
            it contains screenshots of the csv, s3 bucket, code editor, terminal, and redshift query editor
    
    Files
        Background.MD(this file) - 
            it contains a write up on the datasets, files, directories, and transformations.

        config_example.py -
            it contains the configuration parameters that other users will use, if the main config file, config.py, were to be excluded from the repo.
        
        config.py -
            it contains the config parameters for connecting to the various infrastructure and services

        connection.py -
            it contains the connection objects, the connection parameters are referenced from the config.py

        extract_etl.py -
            this is the file that orchestrate the pipeline process
        
        extract.ipynb
            this is a jupyter notebook copy of the extract_etl.py script

        Makefile -
            this file helps in calling either one or all the script with the make command

        Readme.MD -
            A description of the process and how to setup and use this repo

        requirements.txt -
            A text based file, specifying the python packages required to run this program

        s3.tf - 
            A simple terraform script that creates a bucket












