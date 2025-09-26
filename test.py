import yaml
import os
import snowflake.connector
from dotenv import load_dotenv

from typing import (
    Any,
    Optional
)

load_dotenv()

conn = snowflake.connector.connect(
            user=os.getenv(key='user'),
            password=os.getenv(key='password'),
            account=os.getenv(key='account'),
            warehouse=os.getenv(key='warehouse'),
            database=os.getenv(key='database'),
            schema=os.getenv(key='schema'),
            role=os.getenv(key='role')
        )

cursor = conn.cursor()

with open("snowflake_schemas.yaml", "r") as f:
    schema: Any = yaml.safe_load(f)

for table, columns in schema.items():
    cols: list[str] = [f"{col} {dtype}" for col, dtype in columns.items()]
    sql: str = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)});"
    cursor.execute(sql)
