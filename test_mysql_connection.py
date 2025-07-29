import mysql.connector
from mysql.connector import Error

# 🔧 Replace with your actual connection values
config = {
    "host": "127.0.0.1",         # or "localhost"
    "user": "root",              # your MySQL username
    "password": "Library1", # your MySQL password
    "database": "fraud_detection" # your database name
}

try:
    print("Attempting connection...")
    connection = mysql.connector.connect(**config)

    if connection.is_connected():
        print("✅ Successfully connected to MySQL database!")
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        print("📋 Available tables:")
        for table in tables:
            print("-", table[0])
        cursor.close()
    else:
        print("⚠️ Connection failed (not connected)")

except Error as e:
    print(f"❌ Connection error: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("🔌 Connection closed.")
