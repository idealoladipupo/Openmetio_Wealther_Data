# Function to connect to database
import sqlite3


def connect_to_database(database_path):
    try:
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        return connection
    except sqlite3.Error as e:
        print(f"Error connecting to the database: {e}")
        return None


def close_connection(connection):
    if connection:
        connection.close()
