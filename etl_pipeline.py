import pymysql
import pandas as pd
import numpy as np
import pymongo
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MySQL Connection
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DB = os.getenv("MYSQL_DB", "employee_db")

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = "employee_data"
MONGO_COLLECTION = "transformed_employees"

def extract_from_mysql():
    """Extract data from MySQL database."""
    connection = pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB
    )
    query = "SELECT * FROM employees"
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def transform_data(df):
    """Transform data using Pandas and NumPy."""
    df["salary"] = df["salary"] * 1.1  # 10% Salary Hike
    df["age_category"] = np.where(df["age"] > 30, "Senior", "Junior")
    return df

def load_into_mongodb(df):
    """Load transformed data into MongoDB."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    records = df.to_dict(orient="records")
    collection.insert_many(records)
    client.close()

if __name__ == "__main__":
    print("Extracting data from MySQL...")
    data = extract_from_mysql()
    print(data.head())

    print("Transforming data...")
    transformed_data = transform_data(data)
    print(transformed_data.head())

    print("Loading data into MongoDB...")
    load_into_mongodb(transformed_data)
    print("Data pipeline completed successfully!")
