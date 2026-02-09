"""
Clear all data from MySQL and MongoDB
"""
import mysql.connector
from pymongo import MongoClient

# Clear MySQL
try:
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='devil',
        database='streaming_db'
    )
    cursor = mysql_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS logs")
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
    print("✓ MySQL table 'logs' dropped")
except Exception as e:
    print(f"✗ MySQL error: {e}")

# Clear MongoDB
try:
    mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=3000)
    db = mongo_client['streaming_db']
    result = db['logs'].delete_many({})
    print(f"✓ MongoDB collection 'logs' cleared ({result.deleted_count} documents deleted)")
    mongo_client.close()
except Exception as e:
    print(f"✗ MongoDB error: {e}")

print("\n✓ Databases cleared successfully")
