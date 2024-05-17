# ETL Pipeline README

This repository contains an ETL (Extract, Transform, Load) pipeline designed to retrieve data from the Coinbase API, process it, and load it into both a Redshift database and a local PostgreSQL database. Below is an overview of the files and their functionalities:

## Files:

### 1. `config.py`

This file contains the connection parameters for various databases and services used in the ETL pipeline, including Redshift, S3, and local PostgreSQL.

### 2. `etl_process.py`

This Python script orchestrates the ETL process. It connects to both the Redshift and local PostgreSQL databases, retrieves data from the Coinbase API, processes it, and uploads it to the databases.

### 3. `connection.py`

This file establishes database connections and contains functions for connecting to both Redshift and local PostgreSQL databases.

### 4. `README.md`

This README file provides an overview of the ETL pipeline, its components, and how to set up and run the pipeline.

## Setup:

To set up the ETL pipeline, follow these steps:

1. Ensure you have Python installed on your system.

2. Install the required dependencies by running:

   ```bash
   pip install -r requirements.txt