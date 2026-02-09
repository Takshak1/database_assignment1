"""
storage_manager.py - Handles routing and storage to SQL/MongoDB
Implements Phase 4: Commit & Routing with bi-temporal timestamps
"""
import mysql.connector
from pymongo import MongoClient
from datetime import datetime
import json

# Database Configurations
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'devil',
    'database': 'streaming_db'
}

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'streaming_db',
    'collection': 'logs'
}


class StorageManager:
    """Manages hybrid storage across SQL and MongoDB"""
    
    def __init__(self):
        self.mysql_conn = None
        self.mysql_cursor = None
        self.mongo_client = None
        self.mongo_collection = None
        self.sql_schema_created = False
        self.metadata = {}  # Store complete metadata for schema creation
        
    def connect(self):
        """Establish connections to both backends"""
        # Connect to MySQL
        try:
            self.mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            self.mysql_cursor = self.mysql_conn.cursor()
            print("✓ MySQL connected")
        except Exception as e:
            print(f"✗ MySQL connection failed: {e}")
            return False
        
        # Connect to MongoDB
        try:
            uri = f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
            self.mongo_client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            self.mongo_client.server_info()  # Test connection
            db = self.mongo_client[MONGO_CONFIG['database']]
            self.mongo_collection = db[MONGO_CONFIG['collection']]
            print("✓ MongoDB connected")
        except Exception as e:
            print(f"✗ MongoDB connection failed: {e}")
            return False
        
        return True
    
    def initialize_schema(self, metadata):
        """Initialize SQL schema from complete metadata before processing records"""
        self.metadata = metadata
        if metadata:
            all_sql_fields = [f for f, d in metadata.items() if d == 'sql']
            if all_sql_fields:
                self.create_sql_schema(all_sql_fields)
    
    def create_sql_schema(self, sql_fields):
        """Dynamically create SQL table based on classified fields"""
        if self.sql_schema_created:
            return
        
        # Drop existing table to recreate with full schema
        try:
            self.mysql_cursor.execute("DROP TABLE IF EXISTS logs")
            self.mysql_conn.commit()
        except Exception as e:
            print(f"⚠ Could not drop existing table: {e}")
        
        # Build column definitions
        columns = ["id BIGINT AUTO_INCREMENT PRIMARY KEY"]
        
        # Add username (required in both backends)
        columns.append("username VARCHAR(255) NOT NULL")
        
        # Add other SQL fields (excluding special columns)
        for field in sql_fields:
            if field in ['username', 'timestamp', 't_stamp', 'sys_ingested_at']:
                continue
            columns.append(f"{field} TEXT")
        
        # Bi-temporal timestamps
        columns.append("t_stamp VARCHAR(50)")  # Client timestamp
        columns.append("sys_ingested_at DATETIME NOT NULL")  # Server timestamp
        
        # Create table
        create_query = f"""
        CREATE TABLE IF NOT EXISTS logs (
            {', '.join(columns)},
            INDEX idx_username (username),
            INDEX idx_timestamps (t_stamp, sys_ingested_at)
        )
        """
        
        try:
            self.mysql_cursor.execute(create_query)
            self.mysql_conn.commit()
            self.sql_schema_created = True
            print(f"✓ SQL schema created with {len(columns)} columns")
        except Exception as e:
            print(f"✗ SQL schema creation failed: {e}")
    
    def store_record(self, record, decisions):
        """
        Route and store record to appropriate backends
        
        Args:
            record: normalized record dict
            decisions: dict mapping field -> 'sql' or 'mongo'
        
        Returns:
            (sql_id, mongo_id) tuple
        """
        # Extract timestamps
        t_stamp = record.get('timestamp', datetime.now().isoformat())
        sys_ingested_at = datetime.now()
        
        # Extract username (required in both backends)
        username = record.get('username', 'unknown')
        
        # Split record based on decisions
        sql_data = {'username': username}
        mongo_data = {'username': username}
        
        for field, value in record.items():
            if field == 'timestamp':
                continue
            
            decision = decisions.get(field, 'mongo')
            
            if decision == 'sql':
                # Only store non-nested in SQL
                if not isinstance(value, (dict, list)):
                    sql_data[field] = value
                else:
                    mongo_data[field] = value
            else:
                mongo_data[field] = value
        
        # Add bi-temporal timestamps to both
        sql_data['t_stamp'] = t_stamp
        sql_data['sys_ingested_at'] = sys_ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        mongo_data['t_stamp'] = t_stamp
        mongo_data['sys_ingested_at'] = sys_ingested_at
        
        # Insert into SQL
        sql_id = self._insert_sql(sql_data)
        
        # Insert into MongoDB
        mongo_id = self._insert_mongo(mongo_data)
        
        return sql_id, mongo_id
    
    def _insert_sql(self, data):
        """Insert record into MySQL"""
        try:
            columns = list(data.keys())
            values = [data[col] for col in columns]
            
            placeholders = ', '.join(['%s'] * len(values))
            column_names = ', '.join(columns)
            
            query = f"INSERT INTO logs ({column_names}) VALUES ({placeholders})"
            self.mysql_cursor.execute(query, values)
            self.mysql_conn.commit()
            
            return self.mysql_cursor.lastrowid
        except Exception as e:
            print(f"✗ SQL insert error: {e}")
            return None
    
    def _insert_mongo(self, data):
        """Insert document into MongoDB"""
        try:
            result = self.mongo_collection.insert_one(data)
            return str(result.inserted_id)
        except Exception as e:
            print(f"✗ MongoDB insert error: {e}")
            return None
    
    def get_stats(self):
        """Get record counts from both backends"""
        sql_count = 0
        mongo_count = 0
        
        try:
            self.mysql_cursor.execute("SELECT COUNT(*) FROM logs")
            sql_count = self.mysql_cursor.fetchone()[0]
        except:
            pass
        
        try:
            mongo_count = self.mongo_collection.count_documents({})
        except:
            pass
        
        return {'sql': sql_count, 'mongo': mongo_count}
    
    def close(self):
        """Close all connections"""
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()
            print("✓ MySQL closed")
        if self.mongo_client:
            self.mongo_client.close()
            print("✓ MongoDB closed")
