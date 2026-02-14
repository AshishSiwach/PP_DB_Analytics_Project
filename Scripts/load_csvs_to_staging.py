"""
load_staging.py
---------------
ETL script for the FinancePolyglotDB project.

Responsibility: Load raw CSV data into SQL Server staging tables
(schema: `stg`).

Workflow:
    1. Validate that all required CSV files exist on disk.
    2. Open a single transactional connection to SQL Server.
    3. For each staging table (in dependency order):
        a. Truncate the existing staging data.
        b. Read the corresponding CSV with all columns as strings.
        c. Validate that the CSV contains every expected column.
        d. Bulk-insert rows via fast_executemany in configurable batches.
        e. Commit and print a row-count confirmation.

Usage:
    python load_staging.py

    Ensure the CONFIG block at the top of this file matches your environment
    before running.

Dependencies:
    - pandas      (CSV I/O)
    - pyodbc      (SQL Server connectivity via ODBC)
    - ODBC Driver 17 or 18 for SQL Server must be installed on the host.
"""

import os
import pandas as pd
import pyodbc

# ─────────────────────────────────────────────
# CONFIG: update these values to match your env
# ─────────────────────────────────────────────

# SQL Server host.  Use r"HOST\INSTANCE" for a named instance,
# e.g. r"localhost\SQLEXPRESS".
SERVER = r"localhost\SQLEXPRESS"

# Target database that contains the `stg` schema.
DATABASE = "FinancePolyglotDB"

# Use Windows Authentication.
# if using SQL Server Authentication instead.
TRUSTED_CONNECTION = "yes"

# Must match the driver version installed on this machine.
# Run `odbcinst -q -d` (Linux) or check ODBC Data Sources (Windows) to verify.
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"  # or 18

# Absolute path to the folder that contains all source CSV files.
# Defaults to a `data/` sub-folder relative to the current working directory.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ─────────────────────────────────────────────────────────────────────────────
# TABLE_TO_FILE
# Maps each staging table (schema.table) to its source CSV filename.
#
# IMPORTANT versioning notes:
#   - transaction_types_v3.csv  → v3 adds the "Transfer" type required by v2
#                                  transactions; do NOT substitute an older file.
#   - loans_v2.csv              → v2 adds RepaymentAccountID; v1 is incompatible
#                                  with the current stg.loans DDL.
#   - transactions_v2.csv       → v2 adds LoanID column for loan repayments.
# ─────────────────────────────────────────────────────────────────────────────
TABLE_TO_FILE = {
    "stg.customer_types":   "customer_types.csv",
    "stg.account_types":    "account_types.csv",
    "stg.account_statuses": "account_statuses.csv",
    "stg.loan_statuses":    "loan_statuses.csv",
    "stg.transaction_types": "transaction_types_v3.csv",  # IMPORTANT: must be v3
    "stg.addresses":        "addresses.csv",
    "stg.branches":         "branches.csv",
    "stg.customers":        "customers.csv",
    "stg.accounts":         "accounts.csv",
    "stg.loans":            "loans_v2.csv",               # IMPORTANT: must be v2
    "stg.transactions":     "transactions_v2.csv",        # IMPORTANT: must be v2
}

# ─────────────────────────────────────────────────────────────────────────────
# EXPECTED_COLS
# Defines the exact column set (and insert order) for each staging table.
# These must match the column definitions in 01_staging.sql.
#
# Purpose:
#   - Acts as a contract between the CSV files and the DDL.
#   - Any CSV column NOT listed here is silently ignored.
#   - Any column listed here but MISSING from the CSV raises a ValueError.
# ─────────────────────────────────────────────────────────────────────────────
EXPECTED_COLS = {
    "stg.customer_types":   ["CustomerTypeID", "TypeName"],
    "stg.account_types":    ["AccountTypeID", "TypeName"],
    "stg.account_statuses": ["AccountStatusID", "StatusName"],
    "stg.loan_statuses":    ["LoanStatusID", "StatusName"],
    "stg.transaction_types": ["TransactionTypeID", "TypeName"],
    "stg.addresses":  ["AddressID", "Street", "City", "Country"],
    "stg.branches":   ["BranchID", "BranchName", "AddressID"],
    "stg.customers":  ["CustomerID", "FirstName", "LastName", "DateOfBirth",
                       "AddressID", "CustomerTypeID"],
    "stg.accounts":   ["AccountID", "CustomerID", "AccountTypeID",
                       "AccountStatusID", "Balance", "OpeningDate"],
    "stg.loans":      ["LoanID", "DisbursementAccountID", "RepaymentAccountID",
                       "LoanStatusID", "PrincipalAmount", "InterestRate",
                       "StartDate", "EstimatedEndDate"],
    "stg.transactions": ["TransactionID", "AccountOriginID", "AccountDestinationID",
                         "TransactionTypeID", "LoanID", "Amount",
                         "TransactionDate", "BranchID", "Description"],
}


# ─────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────

def connect() -> pyodbc.Connection:
    """
    Open and return a pyodbc connection to the configured SQL Server database.

    The connection string is assembled from the module-level CONFIG constants.
    TrustServerCertificate=yes is set to avoid certificate validation errors
    on self-signed / local dev instances.

    Returns:
        pyodbc.Connection: An open (but not yet in a transaction) connection.

    Raises:
        pyodbc.Error: If the driver is missing, server is unreachable, or
                      authentication fails.
    """
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection={TRUSTED_CONNECTION};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def truncate_table(cursor: pyodbc.Cursor, table_name: str) -> None:
    """
    Remove all rows from a staging table without logging individual row deletes.

    TRUNCATE is used instead of DELETE for performance; it resets the table to
    an empty state in a single operation and is safe here because staging tables
    have no dependent foreign-key relationships.

    Args:
        cursor:     An active pyodbc cursor (connection must be open).
        table_name: Fully-qualified table name, e.g. ``"stg.customers"``.
    """
    cursor.execute(f"TRUNCATE TABLE {table_name};")


def load_csv(path: str) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame with all columns treated as strings.

    Keeping every column as ``dtype=str`` prevents pandas from silently
    mis-casting values before they reach SQL Server — e.g., interpreting
    "00123" as the integer 123, or converting date strings to Timestamps.
    SQL Server will perform the appropriate type coercions during the load
    into the strongly-typed staging tables.

    Empty strings are preserved as ``""`` at this stage; they are converted
    to ``None`` (SQL NULL) later by :func:`normalize_empty_to_none`.

    Args:
        path: Absolute or relative path to the CSV file.

    Returns:
        pd.DataFrame: Raw data frame with all values as Python ``str``.

    Raises:
        FileNotFoundError: If the file does not exist at ``path``.
        pd.errors.ParserError: If the CSV is malformed.
    """
    # keep_default_na=False prevents pandas from treating strings like "NA",
    # "NULL", "N/A", etc. as NaN — those values should reach SQL Server as-is.
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def normalize_empty_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace every empty string in a DataFrame with ``None``.

    pyodbc maps Python ``None`` to SQL ``NULL``.  Without this step, empty
    cells read from a CSV would arrive in SQL Server as the string ``''``
    rather than ``NULL``, violating NOT NULL constraints or polluting
    nullable columns with empty strings.

    Args:
        df: DataFrame (typically produced by :func:`load_csv`) that may
            contain empty-string cells.

    Returns:
        pd.DataFrame: New DataFrame with all ``""`` values replaced by ``None``.
    """
    return df.replace({"": None})


def insert_dataframe(
    cursor: pyodbc.Cursor,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
    batch_size: int = 5000,
) -> None:
    """
    Bulk-insert a DataFrame into a SQL Server staging table using batched
    ``executemany`` calls.

    Column validation:
        - Columns listed in ``columns`` but absent from ``df`` raise a
          ``ValueError`` immediately — this prevents a silent partial load.
        - Columns present in ``df`` but absent from ``columns`` are silently
          dropped; they are not part of the staging schema contract.

    Performance notes:
        - ``cursor.fast_executemany = True`` switches pyodbc from a
          row-by-row ODBC call to a TVP (table-valued parameter) batch,
          which is significantly faster for large datasets.
        - Rows are sent in chunks of ``batch_size`` to limit per-call
          memory usage and allow partial progress if the cursor is reused.

    Args:
        cursor:     Active pyodbc cursor on the target database.
        table_name: Fully-qualified destination table, e.g. ``"stg.loans"``.
        df:         Source data frame; all values should already be strings
                    or ``None`` (see :func:`load_csv` and
                    :func:`normalize_empty_to_none`).
        columns:    Ordered list of column names that defines both the INSERT
                    column list and the column selection from ``df``.
        batch_size: Number of rows per ``executemany`` call.  Reduce if you
                    hit driver memory limits; increase for faster throughput
                    on large files (default: 5 000).

    Raises:
        ValueError:   If any column in ``columns`` is missing from ``df``.
        pyodbc.Error: On SQL Server constraint violations or connection issues.
    """
    # Build a parameterised INSERT statement using the expected column list.
    # Square brackets around column names handle reserved words or spaces.
    col_list = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    # ── Column validation ──────────────────────────────────────────────────
    missing = [c for c in columns if c not in df.columns]
    extra   = [c for c in df.columns if c not in columns]

    if missing:
        # Hard fail: the CSV is missing data the staging table requires.
        raise ValueError(f"{table_name}: CSV missing expected columns: {missing}")

    if extra:
        # Soft warning (logged implicitly by dropping the columns).
        # Extra columns might be leftover artefacts from upstream processes;
        # they are not harmful but should be reviewed if unexpected.
        df = df[columns]

    # Ensure the DataFrame column order matches the INSERT statement exactly.
    df = df[columns]

    # Convert empty strings to None so SQL Server receives NULL.
    df = normalize_empty_to_none(df)

    # Convert to a plain list of tuples — the format executemany expects.
    rows = [tuple(x) for x in df.itertuples(index=False, name=None)]

    # Enable the fast bulk-insert path (TVP / array binding).
    cursor.fast_executemany = True

    # Insert in batches to bound per-call memory consumption.
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i : i + batch_size])


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    """
    Orchestrate the full staging-table load for pipeline step SQL-1.5.

    Steps:
        1. Pre-flight check — confirm every mapped CSV file exists on disk
           before opening a database connection, to fail fast on missing files.
        2. Connect to SQL Server with autocommit disabled so each table load
           is individually committed (allows partial progress inspection while
           still rolling back on unhandled errors).
        3. Iterate over tables in dependency order (lookup / reference tables
           first, transactional tables last) so that FK constraints in
           downstream pipeline steps are satisfied immediately after this
           script finishes.
        4. For each table: truncate → load CSV → insert → commit → log count.

    Load order rationale:
        Lookup tables (customer_types, account_types, …) have no dependencies
        and must exist before entity tables (customers, accounts) reference
        them.  Transactional tables (loans, transactions) come last because
        they reference both entity and lookup tables.

    Error handling:
        Any exception during the load loop triggers a rollback of the current
        (uncommitted) table before re-raising.  Already-committed tables are
        NOT rolled back — staging tables can be safely re-truncated on a
        subsequent run.

    Raises:
        FileNotFoundError: If a required CSV is absent from DATA_DIR.
        ValueError:         If a CSV is missing columns required by the schema.
        pyodbc.Error:       On any SQL Server connectivity or DML error.
    """
    # ── Pre-flight: verify all source files are present ───────────────────
    for table, fname in TABLE_TO_FILE.items():
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Missing file for {table}: {fpath}")

    print("Connecting...")
    conn = connect()
    conn.autocommit = False  # Explicit transaction control; commit per table.
    cur = conn.cursor()

    try:
        # Tables are processed in this explicit order to respect logical
        # dependencies (parent tables before child tables).
        load_order = [
            "stg.customer_types",    # lookup: no dependencies
            "stg.account_types",     # lookup: no dependencies
            "stg.account_statuses",  # lookup: no dependencies
            "stg.loan_statuses",     # lookup: no dependencies
            "stg.transaction_types", # lookup: no dependencies
            "stg.addresses",         # shared by both branches and customers
            "stg.branches",          # depends on stg.addresses
            "stg.customers",         # depends on stg.addresses, stg.customer_types
            "stg.accounts",          # depends on stg.customers, lookup tables
            "stg.loans",             # depends on stg.accounts, stg.loan_statuses
            "stg.transactions",      # depends on stg.accounts, stg.loans, stg.branches
        ]

        for table in load_order:
            fname = TABLE_TO_FILE[table]
            fpath = os.path.join(DATA_DIR, fname)
            cols  = EXPECTED_COLS[table]

            print(f"\n--- Loading {table} from {fname}")
            df = load_csv(fpath)

            # Strip leading/trailing whitespace from header names to guard
            # against invisible formatting differences in CSV editors.
            df.columns = [c.strip() for c in df.columns]

            # Truncate first so re-runs are idempotent (no duplicate rows).
            truncate_table(cur, table)
            insert_dataframe(cur, table, df, cols)

            # Commit after each table so a failure on a later table does not
            # require re-loading earlier ones.
            conn.commit()

            # Confirm row count as a quick sanity check.
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            print(f"✅ Loaded {count:,} rows into {table}")

        print("\n🎉 all staging tables loaded.")

    except Exception:
        # Roll back any rows inserted in the current (uncommitted) table
        # to leave the database in a clean state for the failed table.
        conn.rollback()
        raise  # Re-raise so the caller / CI system sees the original error.

    finally:
        # Always release the cursor and connection, even on error.
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()