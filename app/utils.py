# app/utils.py
import os

def read_sql_file(filename: str) -> str:
    sql_path = os.path.join("app", "queries", filename)
    with open(sql_path, "r") as f:
        return f.read()
