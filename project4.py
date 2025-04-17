"""
Server Log Data Extraction and User History Database Update

Author: GOKULARAJA R
"""

#importing necessary modules
import re
from datetime import datetime, timezone
from pymongo import MongoClient
import credentials
import mysql.connector
from mysql.connector import Error
import pandas as pd
from ETL_USER_LOG import log  # Importing custom log function

LOG_FILE = 'mbox.txt'

email_pattern = re.compile(r'From\s+([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)')
date_pattern = re.compile(r'Date:\s+([A-Za-z]{3},\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+[-+]\d{4})')

def extract_log_data():
    log("Starting log data extraction.")
    with open(LOG_FILE, 'r') as file:
        log_content = file.read()

    emails = email_pattern.findall(log_content)
    dates = date_pattern.findall(log_content)

    structured_data = []
    for email, date_str in zip(emails, dates):
        date_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
        utc_date_obj = date_obj.astimezone(timezone.utc)
        formatted_date = utc_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        structured_data.append({'email': email, 'date': formatted_date})

    log(f"Extracted {len(structured_data)} records from log file.")
    return structured_data

# loading into MongoDB
def load_to_mongodb(data):
    try:
        Mongo_Uri = credentials.MONGO_URI
        client = MongoClient(Mongo_Uri)
        db = client['MINI_PROJECT4']
        collection = db['user_history']
        log("Connected to MongoDB.")

        inserted_count = 0
        for record in data:
            if not collection.find_one({"email": record["email"], "date": record["date"]}):
                collection.insert_one(record)
                inserted_count += 1

        log(f"Inserted {inserted_count} new records into MongoDB.")
    except Exception as e:
        log(f"MongoDB insertion error: {e}")
    finally:
        client.close()
        log("MongoDB connection closed.")

# fetching from mongo db
def fetch_data_from_mongodb():
    try:
        Mongo_Uri = credentials.MONGO_URI
        client = MongoClient(Mongo_Uri)
        db = client['MINI_PROJECT4']
        collection = db['user_history']
        documents = collection.find({}, {'_id': 0})
        data = list(documents)
        log(f"Fetched {len(data)} records from MongoDB.")
        return data
    except Exception as e:
        log(f"MongoDB fetch error: {e}")
        return []
    finally:
        client.close()
        log("MongoDB connection closed after fetch.")

# loading into mysql & queries
def insert_data_into_mysql(data):
    try:
        c = credentials.mysql_credentials
        connection = mysql.connector.connect(**c)

        if connection.is_connected():
            cursor = connection.cursor()
            log("Connected to MySQL.")

            create_table_query = """
            CREATE TABLE IF NOT EXISTS user_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                date DATETIME NOT NULL,
                UNIQUE(email, date)
            )
            """
            cursor.execute(create_table_query)

            insert_query = """
            INSERT IGNORE INTO user_history (email, date)
            VALUES (%s, %s)
            """
            records_to_insert = [(record['email'], record['date']) for record in data]
            cursor.executemany(insert_query, records_to_insert)
            connection.commit()
            log(f"Inserted {cursor.rowcount} new records into MySQL.")

            queries = {
                "Distinct emails in the user history":
                    "SELECT DISTINCT email FROM user_history;",
                "Count of emails received per day":
                    "SELECT DATE(date) AS email_date, COUNT(*) AS email_count "
                    "FROM user_history GROUP BY DATE(date) ORDER BY email_date;",
                "First and last email dates for each email address":
                    "SELECT email, MIN(date) AS first_email_date, MAX(date) AS last_email_date "
                    "FROM user_history GROUP BY email ORDER BY email;",
                "Count of total emails from each domain":
                    "SELECT SUBSTRING_INDEX(email, '@', -1) AS domain, COUNT(*) AS email_count "
                    "FROM user_history GROUP BY domain ORDER BY email_count DESC;"
            }

            for title, query in queries.items():
                log(f"Running query: {title}")
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                print(f"\n--- {title} ---")
                print(df.to_string(index=False))

            log("All MySQL queries executed successfully.")
    except Error as e:
        log(f"MySQL error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            log("MySQL connection closed.")

#Main function
if __name__ == "__main__":
    log("ETL process started.")
    raw_data = extract_log_data()
    load_to_mongodb(raw_data)

    final_data = fetch_data_from_mongodb()
    if final_data:
        insert_data_into_mysql(final_data)
    else:
        log("No data available to insert into MySQL.")
    log("ETL process completed.")
    log("#########################################################################################")
