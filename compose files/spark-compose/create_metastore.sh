#!/bin/bash

TMP_DIR=$(mktemp -d)
HIVE_VERSION="4.0.1"
HIVE_ARCHIVE="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_ARCHIVE}"
HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
HADOOP_ARCHIVE="hadoop-3.3.6.tar.gz"

# Cleanup function to ensure no traces are left
cleanup() {
    echo "Cleaning up temporary files..."
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

# Step 1: Download Hive
echo "Downloading Apache Hive ${HIVE_VERSION}..."
wget -q -P "${TMP_DIR}" "${HIVE_URL}"
if [ $? -ne 0 ]; then
    echo "Error: Failed to download Hive."
    exit 1
fi

# Step 2: Extract Hive
echo "Extracting Hive archive..."
tar -xzf "${TMP_DIR}/${HIVE_ARCHIVE}" -C "${TMP_DIR}"
HIVE_HOME="${TMP_DIR}/apache-hive-${HIVE_VERSION}-bin"

# Step 3: Download Hadoop for HADOOP_HOME
echo "Downloading Hadoop..."
wget -q -P "${TMP_DIR}" "${HADOOP_URL}"
if [ $? -ne 0 ]; then
    echo "Error: Failed to download Hadoop."
    exit 1
fi

# Step 4: Extract Hadoop
echo "Extracting Hadoop archive..."
tar -xzf "${TMP_DIR}/${HADOOP_ARCHIVE}" -C "${TMP_DIR}"
HADOOP_HOME="${TMP_DIR}/hadoop-3.3.6"

# Step 5: Set environment variables
echo "Setting up environment..."
export PATH="${HIVE_HOME}/bin:$PATH"
export HADOOP_HOME="${HADOOP_HOME}"
export PATH="${HADOOP_HOME}/bin:$PATH"

# Step 6: Verify beeline
echo "Checking beeline availability..."
if ! command -v beeline &> /dev/null; then
    echo "Error: beeline is not available."
    exit 1
fi

CONFIG_FILE="db_config.json"

HIVE_SERVER="localhost"
HIVE_PORT="10000"
HIVE_CMD="beeline -u jdbc:hive2://${HIVE_SERVER}:${HIVE_PORT}"

if ! command -v jq &> /dev/null; then
  echo "Error: jq is required but not installed."
  exit 1
fi

echo "Reading configuration from ${CONFIG_FILE}..."
CONFIG=$(jq -c '.[]' "$CONFIG_FILE")

echo "$CONFIG" | while read -r DB_CONFIG; do
  DB_NAME=$(echo "$DB_CONFIG" | jq -r '.name')
  DB_URL=$(echo "$DB_CONFIG" | jq -r '.url')
  TABLES=$(echo "$DB_CONFIG" | jq -c '.tables[]')
  DB_USERNAME=$(echo "$DB_CONFIG" | jq -r '.username')
  DB_PASSWORD=$(echo "$DB_CONFIG" | jq -r '.password')

  echo "Processing database: $DB_NAME"

  echo "Creating Hive database for $DB_NAME..."
  $HIVE_CMD -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};"

  echo "Processing tables for $DB_NAME..."
  echo "$TABLES" | while read -r TABLE; do
    TABLE_NAME=$(echo "$TABLE" | tr -d '"')
    echo "Processing table: $TABLE_NAME"

    TABLE_CREATE_QUERY="
    CREATE TABLE IF NOT EXISTS ${DB_NAME}.${TABLE_NAME}
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      url '${DB_URL}',
      dbtable '${TABLE_NAME}',
      user '${DB_USERNAME}',
      password '${DB_PASSWORD}'
    );
    "
    echo "Executing: $TABLE_CREATE_QUERY"
    $HIVE_CMD -e "$TABLE_CREATE_QUERY"
  done

done

beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"

echo "All databases and tables processed."
