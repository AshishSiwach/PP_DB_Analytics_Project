import os
import re
import pyodbc

# ---- Update these if needed ----
SERVER = r"localhost\SQLEXPRESS"              # e.g. r"localhost\SQLEXPRESS"
DATABASE = "master"                # connect to master first; script creates FinancePolyglotDB
TRUSTED_CONNECTION = "yes"         # Windows auth
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"  # or "ODBC Driver 18 for SQL Server"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_DIR = os.path.join(BASE_DIR, "sql")

SQL_FILES = [
    os.path.join(SQL_DIR, "01_staging.sql")
]

def split_sql_batches(sql_text: str):
    """
    Split SQL Server script on lines that contain only 'GO' (case-insensitive).
    """
    # Normalize line endings
    sql_text = sql_text.replace("\r\n", "\n")
    batches = re.split(r"(?im)^\s*GO\s*$", sql_text)
    return [b.strip() for b in batches if b.strip()]

def run_sql_file(cursor, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    batches = split_sql_batches(sql_text)
    for i, batch in enumerate(batches, start=1):
        cursor.execute(batch)

def main():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection={TRUSTED_CONNECTION};"
    )

    print("Connecting to SQL Server...")
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        for fpath in SQL_FILES:
            print(f"Running: {fpath}")
            run_sql_file(cursor, fpath)

    print("✅ SQL-1 completed: staging tables created.")

if __name__ == "__main__":
    main()
