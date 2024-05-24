# ETL Pipeline README

This repository contains an ETL (Extract, Transform, Load) pipeline designed to retrieve data from the OpenExchangeRates API, process it, and load it into both a Redshift database and a local PostgreSQL database. Below is an overview of the files and their functionalities:

## Files:

### 1. `config.py`

This file contains the connection parameters for various databases and services used in the ETL pipeline, including Redshift, S3, and local PostgreSQL. Change the parameters to what applies in your local and cloud environment. The Cloud Provider used is Amazon AWS.

### 2. `extract_etl.py`

This Python script orchestrates the ETL process. It connects to both the Redshift and local PostgreSQL databases, retrieves data from the openexchangerates API, processes it, and uploads it to the databases. The locally persisted files are in the datasets directory.
The files persisted in S3 bucket are in {the-name-of-your-bucket}/datasets

### 3. `connection.py`

This file establishes database connections and contains functions for connecting to both Redshift and local PostgreSQL databases.

### 4. `README.md`

This README file provides an overview of the ETL pipeline, its components, and how to set up and run the pipeline.

## Setup:

To set up the ETL pipeline, follow these steps:

1. Ensure you have Python installed on your system.

2. Install the required dependencies by running:

   ```bash
   [pip install -r requirements.txt]
3. Test your connection parameters, run [make connection] in your terminal

4. To orchestrate the ETL process, run [make] in your terminal

### Contributors <br>

Benjamin Sakyi - bsakyi@trestleacademyghana.org -Project Lead <br>
Nana Kwame Opoku - nopoku@trestleacademyghana.org <br>
Kenneth Boateng - boateng@trestleacademyghana.org <br>
Frank Agbenyo Fugah - fugah@trestleacademyghana.org <br>
Carl Bebli - cbebli@trestleacademyghana.org <br>
Richard Owusu - rich@trestleacademyghana.org <br>
Delbert Pecker - pecker@trestleacademyghana.org
