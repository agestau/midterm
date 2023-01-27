import os

from mysql import connector

db_connection = connector.connect(
    host=os.environ.get("DB_HOST"),
    username=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_DATABASE")
)
