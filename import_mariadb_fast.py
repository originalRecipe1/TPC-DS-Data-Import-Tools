#!/usr/bin/env python3
"""
import_mariadb_fast.py - Fast bulk import for TPC-DS data into MariaDB

Optimized version using MariaDB-specific performance best practices:
- Disables autocommit and foreign key checks
- Uses MariaDB-specific optimizations
- One connection per table for true parallelism
- Optimized for MariaDB bulk loading performance

Usage:
    python import_mariadb_fast.py --chunks 8

Dependencies:
    pip install mysql-connector-python
"""

import argparse
import mysql.connector
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# TPC-DS tables from instructions.txt
TPCDS_TABLES = [
    'customer_address', 'customer_demographics', 'income_band', 'household_demographics',
    'date_dim', 'customer', 'call_center', 'item', 'promotion', 'time_dim',
    'store', 'store_sales', 'reason', 'store_returns', 'web_site', 'ship_mode',
    'warehouse', 'web_page', 'web_sales', 'web_returns', 'catalog_page',
    'catalog_returns', 'catalog_sales', 'inventory', 'dbgen_version'
]

class MariaDBFastImporter:
    def __init__(self, host='localhost', port=3309, database='db1', 
                 user='root', password='root'):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
            'allow_local_infile': True,
            'autocommit': False,  # Disable autocommit for bulk operations
            'use_pure': False     # Use C extension for better performance
        }
        
    def get_connection(self):
        """Get an optimized MariaDB connection for bulk loading"""
        conn = mysql.connector.connect(**self.connection_params)
        cursor = conn.cursor()
        
        # MariaDB-specific optimization queries
        optimization_queries = [
            "SET SESSION autocommit = 0",
            "SET SESSION unique_checks = 0", 
            "SET SESSION foreign_key_checks = 0",
            "SET SESSION sql_log_bin = 0",
            "SET SESSION sql_mode = 'ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
            "SET SESSION innodb_flush_log_at_trx_commit = 0",
            "SET SESSION sync_binlog = 0",
            "SET SESSION query_cache_type = 0",  # Disable query cache during bulk load
            "SET SESSION innodb_lock_wait_timeout = 900",  # Further increase lock wait timeout
            "SET SESSION lock_wait_timeout = 900",  # MariaDB-specific lock timeout
            "SET SESSION innodb_rollback_on_timeout = 1",  # Rollback on timeout
        ]
        
        for query in optimization_queries:
            try:
                cursor.execute(query)
            except mysql.connector.Error as e:
                # Some settings might not be available, continue anyway
                logger.debug(f"Could not set optimization: {query} - {e}")
        
        cursor.close()
        return conn
    
    def create_tables(self) -> bool:
        """Create TPC-DS tables if they don't exist"""
        logger.info("Creating TPC-DS tables if they don't exist...")
        
        schema_file = Path(__file__).parent / "schema" / "tpcds.sql"
        if not schema_file.exists():
            logger.warning("Schema file not found. Tables must be created manually.")
            return True
            
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Replace 'create table' with 'CREATE TABLE IF NOT EXISTS'
            schema_sql = schema_sql.replace('create table', 'CREATE TABLE IF NOT EXISTS')
            
            # Split into individual statements and filter out comments/empty lines
            statements = []
            current_statement = ""
            
            for line in schema_sql.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
                    
                current_statement += line + " "
                
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Execute each CREATE TABLE statement individually
                for statement in statements:
                    if statement and 'CREATE TABLE' in statement.upper():
                        cursor.execute(statement)
                        
                conn.commit()
                cursor.close()
                    
            logger.info("✓ Tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to create tables: {e}")
            return False
    
    def import_chunk_file(self, table_name: str, container_path: str, chunk_id: Optional[str] = None, max_retries: int = 5) -> bool:
        """Import a single chunk file into MariaDB using optimized bulk loading with retry logic"""
        chunk_label = f" (chunk {chunk_id})" if chunk_id else ""
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    import random
                    wait_time = random.uniform(2, 6) * attempt
                    time.sleep(wait_time)
                    logger.info(f"Retrying import: {table_name}{chunk_label} (attempt {attempt + 1}/{max_retries})")
                else:
                    logger.info(f"Starting import: {table_name}{chunk_label} from {container_path}")
                
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # MariaDB-specific optimizations for this table
                    try:
                        # Check if table uses Aria (MariaDB's default) or InnoDB
                        cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
                        result = cursor.fetchone()
                        if result and result[1] == 'Aria':
                            # Aria-specific optimizations
                            cursor.execute(f"ALTER TABLE {table_name} DISABLE KEYS")
                    except Exception as e:
                        logger.debug(f"Could not optimize table {table_name}: {e}")
                    
                    # Use LOAD DATA INFILE with MariaDB optimizations
                    load_sql = f"""
                    LOAD DATA INFILE %s 
                    INTO TABLE {table_name} 
                    FIELDS TERMINATED BY '|' 
                    LINES TERMINATED BY '\\n'
                    """
                    
                    cursor.execute(load_sql, (container_path,))
                    
                    # Re-enable indexes for Aria tables
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS")
                    except:
                        pass  # Not all storage engines support this
                    
                    conn.commit()
                    cursor.close()
                        
                logger.info(f"✓ Completed: {table_name}{chunk_label}")
                return True
                
            except Exception as e:
                # Check if it's a file not found error (expected for distributed chunks)
                if "No such file" in str(e) or "cannot be opened" in str(e) or "doesn't exist" in str(e):
                    logger.debug(f"⚬ Skipped: {table_name}{chunk_label} - file not in this chunk")
                    return True  # This is expected, not a failure
                # Check if it's a lock timeout error - retry
                elif "Lock wait timeout exceeded" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠ Lock timeout for {table_name}{chunk_label}, retrying...")
                    continue
                else:
                    logger.error(f"✗ Failed: {table_name}{chunk_label} - {e}")
                    return False
        
        return False
    
    def import_chunked_data(self, num_chunks: int, max_workers: int = 3) -> None:
        """Import all chunks in parallel using container paths"""
        logger.info(f"Starting OPTIMIZED MariaDB parallel import with {num_chunks} chunks")
        
        tasks = []
        
        # Create import tasks for each table and chunk using container paths
        for chunk in range(1, num_chunks + 1):
            for table_name in TPCDS_TABLES:
                # Use actual TPC-DS chunk naming: table_chunknum_totalchunks.dat
                container_path = f"/data/chunk_{chunk}/{table_name}_{chunk}_{num_chunks}.dat"
                tasks.append((table_name, container_path, str(chunk)))
        
        logger.info(f"Found {len(tasks)} files to import with MariaDB optimizations")
        
        # Execute imports in parallel
        success_count = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.import_chunk_file, table_name, container_path, chunk_id): 
                (table_name, chunk_id)
                for table_name, container_path, chunk_id in tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                table_name, chunk_id = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                except Exception as e:
                    logger.error(f"Task failed for {table_name} chunk {chunk_id}: {e}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"MariaDB optimized import completed: {success_count}/{len(tasks)} files successful in {elapsed_time:.1f}s")
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"Connected to MariaDB: {version}")
                return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

def main():
    overall_start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Fast import TPC-DS data into MariaDB")
    parser.add_argument("--host", default="localhost", help="MariaDB host")
    parser.add_argument("--port", type=int, default=3309, help="MariaDB port")
    parser.add_argument("--database", default="db1", help="Database name")
    parser.add_argument("--user", default="root", help="Username")
    parser.add_argument("--password", default="root", help="Password")
    
    # Data source options
    data_group = parser.add_mutually_exclusive_group(required=True)
    data_group.add_argument("--chunks", type=int, help="Number of chunks to import")
    data_group.add_argument("--combined-data", action="store_true", help="Import combined data files")
    
    parser.add_argument("--max-workers", type=int, default=3, help="Maximum parallel workers (MariaDB optimized for less contention)")
    parser.add_argument("--test-connection", action="store_true", help="Test connection and exit")
    
    args = parser.parse_args()
    
    # Create importer
    importer = MariaDBFastImporter(
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
    
    # Import data with optimizations
    if args.chunks:
        importer.import_chunked_data(args.chunks, args.max_workers)
    
    # Print total execution time
    total_time = time.time() - overall_start_time
    logger.info(f"🏁 OPTIMIZED MariaDB Total execution time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")

if __name__ == "__main__":
    main()