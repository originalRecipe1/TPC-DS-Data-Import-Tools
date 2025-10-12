#!/usr/bin/env python3
"""
import_postgres_sequential.py - Import TPC-DS chunked data into PostgreSQL sequentially

This script imports TPC-DS data chunks into PostgreSQL one by one (sequentially) for benchmarking
against the parallel version.

Usage:
    python import_postgres_sequential.py --chunks 8

Dependencies:
    pip install psycopg2-binary
"""

import argparse
import psycopg2
from pathlib import Path
import time
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# TPC-DS tables from instructions.txt
TPCDS_TABLES = [
    'customer_address',
    'customer_demographics', 
    'income_band',
    'household_demographics',
    'date_dim',
    'customer',
    'call_center',
    'item',
    'promotion',
    'time_dim',
    'store',
    'store_sales',
    'reason',
    'store_returns',
    'web_site',
    'ship_mode',
    'warehouse',
    'web_page',
    'web_sales',
    'web_returns',
    'catalog_page',
    'catalog_returns',
    'catalog_sales',
    'inventory',
    'dbgen_version'
]

class PostgresSequentialImporter:
    def __init__(self, host='localhost', port=5439, database='db1', 
                 user='postgres', password='123456'):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        
    def get_connection(self):
        """Get a PostgreSQL connection"""
        return psycopg2.connect(**self.connection_params)
    
    def create_tables(self) -> bool:
        """Create TPC-DS tables if they don't exist"""
        logger.info("Creating TPC-DS tables if they don't exist...")
        
        # Read the schema file and convert CREATE TABLE to CREATE TABLE IF NOT EXISTS
        schema_file = Path(__file__).parent / "schema" / "tpcds.sql"
        if not schema_file.exists():
            logger.warning("Schema file not found. Tables must be created manually.")
            return True
            
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Replace 'create table' with 'CREATE TABLE IF NOT EXISTS'
            schema_sql = schema_sql.replace('create table', 'CREATE TABLE IF NOT EXISTS')
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Execute the schema
                    cursor.execute(schema_sql)
                    conn.commit()
                    
            logger.info("✓ Tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to create tables: {e}")
            return False
    
    def import_chunk_file(self, table_name: str, container_path: str, chunk_id: Optional[str] = None) -> bool:
        """Import a single chunk file into PostgreSQL using container path"""
        chunk_label = f" (chunk {chunk_id})" if chunk_id else ""
        logger.info(f"Starting import: {table_name}{chunk_label} from {container_path}")
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Use COPY command as shown in instructions.txt with container path
                    copy_sql = f"COPY {table_name} FROM %s DELIMITER '|' NULL ''"
                    cursor.execute(copy_sql, (container_path,))
                    
                    conn.commit()
                    
            logger.info(f"✓ Completed: {table_name}{chunk_label}")
            return True
            
        except Exception as e:
            # Check if it's a file not found error (expected for distributed chunks)
            if "No such file" in str(e) or "cannot be opened" in str(e):
                logger.debug(f"⚬ Skipped: {table_name}{chunk_label} - file not in this chunk")
                return True  # This is expected, not a failure
            else:
                logger.error(f"✗ Failed: {table_name}{chunk_label} - {e}")
                return False
    
    def import_chunked_data_sequential(self, num_chunks: int) -> None:
        """Import all chunks sequentially (one by one)"""
        logger.info(f"Starting SEQUENTIAL import with {num_chunks} chunks")
        
        tasks = []
        
        # Create import tasks for each table and chunk using container paths
        for chunk in range(1, num_chunks + 1):
            for table_name in TPCDS_TABLES:
                # Use actual TPC-DS chunk naming: table_chunknum_totalchunks.dat
                container_path = f"/data/chunk_{chunk}/{table_name}_{chunk}_{num_chunks}.dat"
                tasks.append((table_name, container_path, str(chunk)))
        
        logger.info(f"Found {len(tasks)} files to import sequentially")
        
        # Execute imports sequentially (one at a time)
        success_count = 0
        start_time = time.time()
        
        for i, (table_name, container_path, chunk_id) in enumerate(tasks, 1):
            logger.info(f"Processing {i}/{len(tasks)}: {table_name} chunk {chunk_id}")
            success = self.import_chunk_file(table_name, container_path, chunk_id)
            if success:
                success_count += 1
        
        elapsed_time = time.time() - start_time
        logger.info(f"Sequential import completed: {success_count}/{len(tasks)} files successful in {elapsed_time:.1f}s")
    
    def import_combined_data_sequential(self) -> None:
        """Import combined (non-chunked) data files sequentially"""
        logger.info(f"Starting SEQUENTIAL import from combined data")
        
        tasks = []
        
        # Create import tasks for each table using container paths
        for table_name in TPCDS_TABLES:
            container_path = f"/data/{table_name}.dat"
            tasks.append((table_name, container_path, None))
        
        logger.info(f"Found {len(tasks)} files to import sequentially")
        
        # Execute imports sequentially
        success_count = 0
        start_time = time.time()
        
        for i, (table_name, container_path, chunk_id) in enumerate(tasks, 1):
            logger.info(f"Processing {i}/{len(tasks)}: {table_name}")
            success = self.import_chunk_file(table_name, container_path, chunk_id)
            if success:
                success_count += 1
        
        elapsed_time = time.time() - start_time
        logger.info(f"Sequential import completed: {success_count}/{len(tasks)} files successful in {elapsed_time:.1f}s")
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    logger.info(f"Connected to PostgreSQL: {version}")
                    return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

def main():
    overall_start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Import TPC-DS data into PostgreSQL SEQUENTIALLY (for benchmarking)")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5439, help="PostgreSQL port")
    parser.add_argument("--database", default="db1", help="Database name")
    parser.add_argument("--user", default="postgres", help="Username")
    parser.add_argument("--password", default="123456", help="Password")
    
    # Data source options
    data_group = parser.add_mutually_exclusive_group(required=True)
    data_group.add_argument("--chunks", type=int, help="Number of chunks to import")
    data_group.add_argument("--combined-data", action="store_true", help="Import combined data files")
    
    parser.add_argument("--test-connection", action="store_true", help="Test connection and exit")
    
    args = parser.parse_args()
    
    # Create importer
    importer = PostgresSequentialImporter(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    # Test connection
    if args.test_connection:
        success = importer.test_connection()
        exit(0 if success else 1)
    
    if not importer.test_connection():
        logger.error("Cannot connect to database. Exiting.")
        exit(1)
    
    # Create tables first
    if not importer.create_tables():
        logger.error("Failed to create tables. Exiting.")
        exit(1)
    
    # Import data SEQUENTIALLY
    if args.chunks:
        importer.import_chunked_data_sequential(args.chunks)
    else:
        importer.import_combined_data_sequential()
    
    # Print total execution time
    total_time = time.time() - overall_start_time
    logger.info(f"🏁 SEQUENTIAL Total execution time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")

if __name__ == "__main__":
    main()