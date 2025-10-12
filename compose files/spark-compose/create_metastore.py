import json
from pyhive import hive

CONFIG_FILE = "db_config.json"
HIVE_SERVER = "localhost"
HIVE_PORT = 10000

with open(CONFIG_FILE) as f:
    config = json.load(f)

conn = hive.Connection(host=HIVE_SERVER, port=HIVE_PORT)
cursor = conn.cursor()

for db_conf in config:
    db_name = db_conf["name"]
    db_url = db_conf["url"]
    username = db_conf["username"]
    password = db_conf["password"]
    tables = db_conf["tables"]

    # Create the database (if needed)
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    # Create tables using Spark's JDBC source syntax
    for table_name in tables:
        query = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              url '{db_url}',
              dbtable '{table_name}',
              user '{username}',
              password '{password}'
            )
        """
        print(f"Creating table {db_name}.{table_name} via Spark JDBC.")
        cursor.execute(query)

cursor.close()
conn.close()
