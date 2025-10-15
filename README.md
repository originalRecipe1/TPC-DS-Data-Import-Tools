# TPC-DS Data Generator and Parallel Importer

This project provides tools for generating TPC-DS benchmark data and importing it into multiple database systems using parallel processing.

## Quick Start

1. **Build the TPC-DS generator image:**

   ```bash
   docker build -t tpcds-test .
   ```

2. **Generate chunked data:**

   ```bash
   # Generate scale-1 data in 4 chunks
   docker run -it --rm \
     -v tpcds-data:/app/data/tables \
     -v "$(pwd)/schema:/app/data/schema" \
     -v "$(pwd)/queries:/app/data/queries" \
     -v "$(pwd)/build_chunked.sh:/app/build_chunked.sh" \
     tpcds-test /app/build_chunked.sh 1 4
   ```

3. **Start the database services:**

   ```bash
   docker compose up -d
   ```

4. **Install Python dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

5. **Import data in parallel:**

   ```bash
   # PostgreSQL (sequential)
   python import_postgres_sequential.py --chunks 4

   # MySQL (fast parallel)
   python import_mysql_fast.py --chunks 4

   # MariaDB (fast parallel)
   python import_mariadb_fast.py --chunks 4
   ```

## Data Generation

### Standard Generation (Single Files)

```bash
docker run -it --rm \
  -v tpcds-data:/app/data/tables \
  -v "$(pwd)/schema:/app/data/schema" \
  -v "$(pwd)/queries:/app/data/queries" \
  tpcds-test ./build.sh 1
```

### Chunked Generation (Parallel Processing)

```bash
# Using shell script
./build_chunked.sh [SCALE] [CHUNKS]
./build_chunked.sh 10 8

# Using Python script (more control)
python generate_chunks.py --scale 10 --chunks 8 --max-workers 4
python generate_chunks.py --scale 1 --chunks 4 --max-rows-per-part 1000000 --combine-chunks
```

### Chunk Management

```bash
# Combine chunks into single files
cd /app/data/tables && ./combine_chunks.sh

# Or specify when generating
python generate_chunks.py --scale 1 --chunks 4 --combine-chunks
```

## Database Services

The `compose.yml` file provides three database services:

| Database   | Port | User     | Password | Database |
| ---------- | ---- | -------- | -------- | -------- |
| PostgreSQL | 5439 | postgres | 123456   | db1      |
| MySQL      | 3308 | mysql    | 123456   | db1      |
| MariaDB    | 3309 | mariadb  | 123456   | db1      |

### Start/Stop Services

```bash
# Start all databases
docker compose up -d

# Start specific database
docker compose up -d postgres

# Stop all
docker compose down

# View logs
docker compose logs postgres
```

## Data Import

### Test Database Connections

```bash
# Full connection tests
python import_postgres_sequential.py --test-connection
python import_mysql_fast.py --test-connection
python import_mariadb_fast.py --test-connection
```

### Import Chunked Data (Recommended)

```bash
# PostgreSQL - import 4 chunks sequentially
python import_postgres_sequential.py --chunks 4

# MySQL - import chunks (use half of available CPU cores for workers)
python import_mysql_fast.py --chunks 4 --max-workers 4

# MariaDB - import chunks (use half of available CPU cores for workers)
python import_mariadb_fast.py --chunks 4 --max-workers 4
```

### Import Combined Data

```bash
# If you have single combined files instead of chunks
python import_postgres_sequential.py --combined-data
python import_mysql_fast.py --combined-data
python import_mariadb_fast.py --combined-data
```

### Custom Connection Parameters

```bash
# Custom host/port/credentials
python import_postgres_sequential.py \
  --host myserver.com \
  --port 5432 \
  --database mydb \
  --user myuser \
  --password mypass \
  --chunks 4
```

## Query Processing

### Distributed Query Generation

```bash
# Generate federated queries for distributed databases
python distribute.py --input-dir queries --mapping config.json --output-dir postgres_distributed_queries

# Load queries into H2 database
python insert_queries.py
```

### Available Query Sets

- `queries/` - Standard TPC-DS queries (99 queries)
- `postgres_distributed_queries/` - PostgreSQL distributed variants
- `neteeza/` - Netezza-specific variants
- `working_queries/` - Modified queries with single-line variants

## Performance Optimization

### Chunked Data Benefits

- **Parallel Import**: Each chunk imports concurrently
- **Memory Efficiency**: Smaller files reduce memory usage
- **Fault Tolerance**: Failed chunks can be retried individually
- **Scalability**: Distribute chunks across multiple systems

### Recommended Settings

- **Workers**: Use half of your available CPU cores for `--max-workers`
- **Scale Factor 1**: 2-4 chunks
- **Scale Factor 10**: 4-8 chunks
- **Scale Factor 100+**: 8-16 chunks

### Large Scale Generation

```bash
# Generate large scale data with row splitting
python generate_chunks.py \
  --scale 100 \
  --chunks 16 \
  --max-workers 8 \
  --max-rows-per-part 5000000
```

## Project Structure

```
├── build.sh                       # Standard TPC-DS generation
├── build_chunked.sh              # Chunked generation (shell)
├── generate_chunks.py            # Chunked generation (Python)
├── import_postgres_sequential.py # PostgreSQL sequential importer
├── import_mysql_fast.py          # MySQL fast parallel importer
├── import_mariadb_fast.py        # MariaDB fast parallel importer
├── distribute.py                 # Federated query generator
├── compose.yml                   # Multi-database Docker setup
├── schema/                       # TPC-DS table definitions
│   ├── tpcds.sql
│   ├── tpcds_ri.sql
│   └── tpcds_source.sql
├── queries/                      # Standard TPC-DS queries (1-99)
├── compose files/                # Database-specific compose files
│   ├── dremio-compose/
│   ├── spark-compose/
│   ├── trino-compose/
│   └── presto-compose/
└── data/tables/                  # Generated data location
    ├── chunk_1/
    ├── chunk_2/
    └── ...
```

## Troubleshooting

### Common Issues

1. **"Permission denied" on scripts:**

   ```bash
   chmod +x build_chunked.sh generate_chunks.py import_*.py
   ```
2. **Database connection refused:**

   ```bash
   # Check if containers are running
   docker compose ps

   # Restart services
   docker compose restart postgres mysql mariadb
   ```
3. **"File not found" during import:**

   ```bash
   # Verify data was generated
   ls -la data/tables/chunk_1/

   # Check volume mounting
   docker volume ls | grep tpcds
   ```
4. **Memory issues with large scales:**

   - Use more chunks: `--chunks 16`
   - Reduce workers: `--max-workers 4`
   - Split large tables: `--max-rows-per-part 1000000`

### Performance Monitoring

```bash
# Monitor import progress
tail -f import.log

# Check database performance
docker stats postgres_ds mysql_ds mariadb_ds

# Monitor disk usage
df -h
du -sh data/tables/
```

## Dependencies

- **Docker & Docker Compose** - For database services and TPC-DS tools
- **Python 3.7+** - For import and generation scripts
- **psycopg2-binary** - PostgreSQL connectivity
- **mysql-connector-python** - MySQL/MariaDB connectivity
- **sqlglot** - SQL parsing for distributed queries (optional)

## Scale Factor Guidelines

| Scale Factor | Data Size | Recommended Chunks | Import Time*      |
| ------------ | --------- | ------------------ | ----------------- |
| 1            | ~1GB      | 2-4                | 2-10 min         |
| 10           | ~10GB     | 4-8                | 30 min - 40 mins |
| 100          | ~100GB    | 8-16               | 5-10 hours       |
| 1000         | ~1TB      | 16-32              | ~ 1 week          |

*Approximate times vary by hardware and database configuration. Consider that there are no foreign key checks nor constraints since that would raise the import time dramatically. Numbers are partially made up, but it shouldn't be too far off
