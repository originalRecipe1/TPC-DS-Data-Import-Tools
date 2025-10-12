#!/usr/bin/env bash

# Usage:
#   ./generate.sh [SCALE]
# Example:
#   ./generate.sh 10
#
# SCALE defaults to 1

TEMPLATE_DIR="../query_templates"
DIALECT="netezza"

SCALE="${1:-1}"

mkdir -p "/app/data/schema"
mkdir -p "/app/data/tables"
mkdir -p "/app/data/queries"

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

./dsdgen -scale "${SCALE}" -delimiter '|' -terminate N -dir /app/data/tables

echo "Done generating workload data"

cp tpcds_source.sql /app/data/schema/
cp tpcds.sql /app/data/schema/
cp tpcds_ri.sql /app/data/schema/

echo "Schema files copied to /app/data/schema"

