#!/usr/bin/env bash

# Usage:
#   ./build_chunked.sh [SCALE] [CHUNKS]
# Example:
#   ./build_chunked.sh 10 4
#
# SCALE defaults to 1, CHUNKS defaults to 2

TEMPLATE_DIR="../query_templates"
DIALECT="netezza"

SCALE="${1:-1}"
CHUNKS="${2:-2}"

mkdir -p "/app/data/schema"
mkdir -p "/app/data/tables"
mkdir -p "/app/data/queries"

# Generate queries (same as original)
for i in $(seq 1 99); do
  template_file="query${i}.tpl"

  if [ ! -f "${TEMPLATE_DIR}/${template_file}" ]; then
    echo "Skipping: ${template_file} does not exist."
    continue
  fi

  ./dsqgen \
    -directory "${TEMPLATE_DIR}" \
    -template "${template_file}" \
    -dialect "${DIALECT}" \
    -scale "${SCALE}" \
    -output_dir "/app/data/queries"

  if [ -f "/app/data/queries/query_0.sql" ]; then
    mv "/app/data/queries/query_0.sql" "/app/data/queries/query${i}.sql"
    echo "Generated: query${i}.sql"
  fi
done

echo "Done generating individual query files."

# Generate data in chunks
echo "Generating TPC-DS data in ${CHUNKS} chunks at scale ${SCALE}..."

for chunk in $(seq 1 "${CHUNKS}"); do
  echo "Generating chunk ${chunk} of ${CHUNKS}..."
  
  # Create subdirectory for this chunk
  mkdir -p "/app/data/tables/chunk_${chunk}"
  
  ./dsdgen \
    -scale "${SCALE}" \
    -delimiter '|' \
    -terminate N \
    -dir "/app/data/tables/chunk_${chunk}" \
    -parallel "${CHUNKS}" \
    -child "${chunk}"
    
  echo "Completed chunk ${chunk}"
done

echo "Done generating chunked workload data"

# Copy schema files
cp tpcds_source.sql /app/data/schema/
cp tpcds.sql /app/data/schema/
cp tpcds_ri.sql /app/data/schema/

echo "Schema files copied to /app/data/schema"

# Create a script to combine chunks if needed
cat > /app/data/tables/combine_chunks.sh << 'EOF'
#!/bin/bash
# Script to combine all chunks into single files

echo "Combining chunks..."
for table_file in chunk_1/*.dat; do
    table_name=$(basename "$table_file")
    echo "Combining $table_name..."
    cat chunk_*/"$table_name" > "$table_name"
done
echo "All chunks combined."
EOF

chmod +x /app/data/tables/combine_chunks.sh

echo "Created combine_chunks.sh script in /app/data/tables/"
echo ""
echo "Chunks are in separate directories: /app/data/tables/chunk_1, chunk_2, etc."
echo "To combine all chunks: cd /app/data/tables && ./combine_chunks.sh"