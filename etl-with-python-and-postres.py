# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### ETL PROCESS WITH PYTHON AND POSTGRES
# MAGIC
# MAGIC This notebook shows how to perform ETL processes using Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Connection information
# MAGIC First define some variables to programmatically create these connections.
# MAGIC

# COMMAND ----------

# Import necessary libraries
import sys
import logging
import psycopg2

class Database:
    def __init__(self):
        self.host = "<++++++++++++++++++>.rds.amazonaws.com" # endpoint from AWS
        self.username = "postgres"
        self.password = "<++++++++++++++++++>"
        self.port = "5432"
        self.dbname = "shelter"
        self.conn = None
        logging.basicConfig(level=logging.INFO)

    def open_connection(self):
        """Connect to a postgres database."""
        try:
            if(self.conn is None):
                self.conn = psycopg2.connect(host=self.host,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port,
                                             dbname=self.dbname)
        except psycopg2.DatabaseError as e:
            logging.error(e)
            sys.exit()
        finally:
            logging.info('Connection success.')

post1 = Database()
post1.open_connection() 

# COMMAND ----------

# MAGIC %md #####  Extract 

# COMMAND ----------

# Import library
import pandas as pd

# Read the dataset:
def extract_data(file):
    data = pd.read_csv("/dbfs/mnt/shelter/petrol_consumption.csv")
    return data

data = extract_data("/dbfs/mnt/shelter/petrol_consumption.csv")
print(data.head(5))

# COMMAND ----------

# MAGIC %md #####  Transform

# COMMAND ----------

# Import library
import pandas as pd

def transform_data(data):
    # Remove unnecessary columns
    data.drop(data.iloc[:, 2:44], inplace=True, axis=1)
    # Remove missing values:
    data_clean = data.dropna()   
    # Round consumption values to the nearest Integer
    data_column_name = data_clean.columns[-1]
    consumption_data = round(data_clean[data_column_name], 2)

    new_consumption_data = {
        data_column_name : consumption_data
    }

    new_consumption_df = pd.DataFrame(new_consumption_data)
    data_cleaned = data_clean.drop([data_column_name], axis = 1)

    data_new = data_cleaned.join(new_consumption_df)
    return data_new

# COMMAND ----------

# MAGIC %md #####  Load 

# COMMAND ----------

# Import libraries
import psycopg2
import argparse

def load_data(file_path):
    connection = psycopg2.connect(database="shelter",
        host="<++++++++++++++++++>.rds.amazonaws.com",
        user="postgres",
        password="<++++++++++++++++++>",
        port="5432")

    cursor = connection.cursor()
    print("Loading data...")
    data = extract_data(file_path)

    print("Transforming data...")
    data_transform = transform_data(data)
    column_name = data_transform.columns[-1]

    # Create table
    query_create_table = f"CREATE TABLE IF NOT EXISTS {column_name}(\
        id SERIAL PRIMARY KEY,\
        continent VARCHAR(55) NOT NULL,\
        country VARCHAR(55) NOT NULL,\
        {column_name} numeric\
    );"

    cursor.execute(query_create_table)

    # Start loading data
    print('Loading data to the database ...')
    for index, row in data_transform.iterrows():
        query_insert = f"INSERT INTO {column_name} (continent, country, {column_name}) VALUES ('{row[0]}', \
            '{row[1]}', {row[2]})" 
        
        cursor.execute(query_insert)
    connection.commit()
    cursor.close()
    connection.close()

    print("ETL completed ...\n")
    return "All processes completed"

if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser()  
    # Adding argument (optional)
    parser.add_argument("-f", "--file", help = "file path to dataset") 
    # Read arguments from the CLI
    args = parser.parse_args()
    load_data(args.file)
