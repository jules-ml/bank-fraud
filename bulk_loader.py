import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection configuration - UPDATED WITH YOUR CREDENTIALS
config = {
    'host': 'localhost',
    'database': 'fraud_detection',
    'user': 'root',
    'password': 'Library1',
    'port': 3306
}

# Folder containing CSV files
folder = '/Users/LibraryMac/Downloads/PythonProject'

# CSV file to table mapping - ALL your CSV files
csv_to_table = {
    'party_table.csv': 'party',
    'customer_data.csv': 'customer_data',
    'merchant_data.csv': 'merchant_data',
    'customer_account_data.csv': 'customer_account_data',
    'card_data.csv': 'card_data',
    'account_data.csv': 'account_data',
    'transaction_data_ready.csv': 'transaction_ready',
    'transaction_data_cleaned.csv': 'transaction_data_cleaned',
    'transaction_data.csv': 'transaction_data',
    'device_data.csv': 'device_data',
    'location_data.csv': 'location_data',
    'login_instance_data.csv': 'login_instance_data',
    'analyst_data.csv': 'analyst_data',
    'fraud_alert_data.csv': 'fraud_alert_data',
    'investigation_data.csv': 'investigation_data'
}

def load_csv_to_mysql(csv_file, table_name, connection):
    """Load a CSV file into a MySQL table"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        print(f"Loading {csv_file} into table {table_name}...")
        print(f"Found {len(df)} rows in {csv_file}")
        
        # Get cursor
        cursor = connection.cursor()
        
        # Clear existing data with foreign key handling
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute(f"DELETE FROM {table_name}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # Prepare INSERT statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]
        
        # Execute bulk insert
        cursor.executemany(insert_query, data)
        connection.commit()
        
        print(f"Successfully loaded {len(data)} rows into {table_name}")
        cursor.close()
        
    except Error as e:
        print(f"Error loading {csv_file} into {table_name}: {e}")
    except Exception as e:
        print(f"General error loading {csv_file}: {e}")

def main():
    connection = None
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            print("Connected to MySQL database")
            
            # Load each CSV file
            for csv_file, table_name in csv_to_table.items():
                csv_path = os.path.join(folder, csv_file)
                
                if os.path.exists(csv_path):
                    load_csv_to_mysql(csv_path, table_name, connection)
                else:
                    print(f"Warning: {csv_path} not found, skipping...")
            
            print("\nBulk loading completed!")
            
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()