# Makefile for connection.py and extract_etl.py

# Define variables
PYTHON = python3

# Default target
all: connection extract_etl

# Target to run connection.py
connection:
	$(PYTHON) connection.py

# Target to run extract_etl.py
extract_etl:
	$(PYTHON) extract_etl.py

