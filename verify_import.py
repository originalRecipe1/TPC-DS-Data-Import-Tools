#!/usr/bin/env python3
"""
verify_import.py - Verify TPC-DS data import by showing table row counts and sizes

This script connects to a database and displays all tables with their row counts and sizes.
Useful for verifying data import completion and comparing across databases.

Usage:
    python verify_import.py --type postgres --host localhost --port 5439 --database db1 --user postgres --password 123456
    python verify_import.py --type mysql --host localhost --port 3308 --database db1 --user mysql --password 123456
    python verify_import.py --type mariadb --host localhost --port 3309 --database db1 --user mariadb --password 123456

Dependencies:
    pip install psycopg2-binary mysql-connector-python
"""

import argparse
import sys
from typing import List, Tuple


def get_postgres_connection(host: str, port: int, database: str, user: str, password: str):
    """Get PostgreSQL connection"""
    try:
        import psycopg2
        return psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    except ImportError:
        print("Error: psycopg2-binary not installed. Run: pip install psycopg2-binary")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)


def get_mysql_connection(host: str, port: int, database: str, user: str, password: str):
    """Get MySQL/MariaDB connection"""
    try:
        import mysql.connector
        return mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    except ImportError:
        print("Error: mysql-connector-python not installed. Run: pip install mysql-connector-python")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to MySQL/MariaDB: {e}")
        sys.exit(1)


def get_postgres_table_counts(conn) -> List[Tuple[str, int, float]]:
    """Get all table names, row counts, and sizes from PostgreSQL"""
    with conn.cursor() as cursor:
        # Get all table names
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]

        # Get count and size for each table
        results = []
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            # Get total table size (table + indexes + TOAST)
            cursor.execute(f"SELECT pg_total_relation_size('{table}')")
            size_bytes = cursor.fetchone()[0]

            results.append((table, count, size_bytes))

        return results


def get_mysql_table_counts(conn) -> List[Tuple[str, int, float]]:
    """Get all table names, row counts, and sizes from MySQL/MariaDB"""
    with conn.cursor() as cursor:
        # Get table names and sizes from information_schema
        cursor.execute("""
            SELECT
                table_name,
                data_length + index_length as total_size
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        table_info = {row[0]: row[1] for row in cursor.fetchall()}

        # Get count for each table
        results = []
        for table, size_bytes in table_info.items():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results.append((table, count, size_bytes or 0))

        return results


def format_table(data: List[Tuple[str, int, float]], db_type: str, database: str) -> str:
    """Format data as a nice ASCII table"""
    if not data:
        return "No tables found in database."

    # Calculate column widths
    max_table_width = max(len(row[0]) for row in data)
    max_table_width = max(max_table_width, len("Table Name"))
    max_count_width = max(len(f"{row[1]:,}") for row in data)
    max_count_width = max(max_count_width, len("Row Count"))

    # Size column width (format: "1.23 GB")
    size_width = 10

    # Build table
    lines = []

    # Header
    header = f"{'Table Name':<{max_table_width}} | {'Row Count':>{max_count_width}} | {'Size (GB)':>{size_width}}"
    separator = f"{'-' * max_table_width}-+-{'-' * max_count_width}-+-{'-' * size_width}"

    lines.append("")
    lines.append(f"Database: {database} ({db_type.upper()})")
    lines.append("=" * len(header))
    lines.append(header)
    lines.append(separator)

    # Data rows
    total_rows = 0
    total_size = 0
    for table_name, row_count, size_bytes in data:
        size_gb = size_bytes / (1024 ** 3)  # Convert bytes to GB
        lines.append(f"{table_name:<{max_table_width}} | {row_count:>{max_count_width},} | {size_gb:>{size_width}.3f}")
        total_rows += row_count
        total_size += size_bytes

    # Footer
    total_size_gb = total_size / (1024 ** 3)
    lines.append(separator)
    lines.append(f"{'TOTAL':<{max_table_width}} | {total_rows:>{max_count_width},} | {total_size_gb:>{size_width}.3f}")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Verify database import by showing table row counts and sizes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify_import.py --type postgres --host localhost --port 5439 --database db1 --user postgres --password 123456
  python verify_import.py --type mysql --host localhost --port 3308 --database db1 --user mysql --password 123456
  python verify_import.py --type mariadb --host localhost --port 3309 --database db1 --user mariadb --password 123456
        """
    )

    parser.add_argument("--type", required=True, choices=["postgres", "mysql", "mariadb"],
                       help="Database type")
    parser.add_argument("--host", required=True, help="Database host")
    parser.add_argument("--port", type=int, required=True, help="Database port")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")

    args = parser.parse_args()

    # Connect to database
    print(f"Connecting to {args.type.upper()} at {args.host}:{args.port}...")

    if args.type == "postgres":
        conn = get_postgres_connection(args.host, args.port, args.database, args.user, args.password)
        table_counts = get_postgres_table_counts(conn)
    else:  # mysql or mariadb
        conn = get_mysql_connection(args.host, args.port, args.database, args.user, args.password)
        table_counts = get_mysql_table_counts(conn)

    conn.close()

    # Display results
    print(format_table(table_counts, args.type, args.database))


if __name__ == "__main__":
    main()
