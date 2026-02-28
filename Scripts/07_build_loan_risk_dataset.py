# ============================================================
# INT-2: Build enriched loan views (analytics.loan_risk_view)
# Combines:
#   - SQL Server OLTP loans (truth)
#   - MongoDB risk_events (behavioural intelligence)
# Output:
#   - analytics.loan_risk_view (1 row per loan, Power BI friendly)
# ============================================================

import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import math

import numpy as np
import pandas as pd
import pyodbc
from pymongo import MongoClient
from dotenv import load_dotenv


# ============================================================
# Config
# ============================================================
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER", r"localhost\SQLEXPRESS")
SQL_DB = os.getenv("SQL_DB", "FinancePolyglotDB")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "finance_pp")

RISK_WINDOW_DAYS = int(os.getenv("RISK_WINDOW_DAYS", "30"))

# IMPORTANT:
# Set this to match your SQL column definition for analytics.loan_risk_view.max_score_impact_30d
# If your table uses DECIMAL(10,6), keep 6. If DECIMAL(10,2), set 2.
MAX_SCORE_IMPACT_SCALE = int(os.getenv("MAX_SCORE_IMPACT_SCALE", "6"))


# ============================================================
# Connections
# ============================================================
def sql_connect() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"Trusted_Connection={SQL_TRUSTED_CONNECTION};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def mongo_connect():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


# ============================================================
# Helpers: safe conversions for SQL Server
# ============================================================
def to_utc_naive(dt: object):
    """
    SQL Server via pyodbc often prefers timezone-naive datetime.
    We store UTC-naive to represent UTC timestamps consistently.
    """
    if dt is None:
        return None
    if isinstance(dt, pd.Timestamp):
        if pd.isna(dt):
            return None
        dt = dt.to_pydatetime()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            # treat as already UTC-naive
            return dt
        # convert aware -> UTC naive
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return None


def to_decimal_or_none(x, scale: int):
    """
    Converts values to Decimal with fixed scale.
    - NaN/inf/invalid -> None (NULL)
    - Quantizes to avoid 'scale greater than precision' errors
    """
    if x is None:
        return None

    # pandas NaN / numpy NaN
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None

    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None

    try:
        d = Decimal(s)
        q = Decimal("1." + "0" * scale)  # e.g., 1.000000
        return d.quantize(q, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None

from datetime import datetime

SQL_DATETIME_MIN = datetime(1753, 1, 1)
SQL_DATETIME_MAX = datetime(9999, 12, 31, 23, 59, 59)

def safe_sql_datetime(x):
    try:
        ts = pd.to_datetime(x, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        dt = ts.to_pydatetime()
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        if dt < SQL_DATETIME_MIN or dt > SQL_DATETIME_MAX:
            return None
        return dt
    except Exception:
        return None
# ============================================================
# SQL: ensure schema/table
# ============================================================
def ensure_analytics_objects(conn: pyodbc.Connection) -> None:
    """
    Creates analytics schema + analytics.loan_risk_view table if missing.
    Adjust DECIMAL precision/scale here if you want.
    """
    ddl = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
        EXEC('CREATE SCHEMA analytics');
    """

    # NOTE: adjust DECIMAL precision/scale if required
    create_table = f"""
    IF OBJECT_ID('analytics.loan_risk_view', 'U') IS NULL
    BEGIN
        CREATE TABLE analytics.loan_risk_view (
            loan_id INT NOT NULL,
            customer_id INT NULL,
            disbursement_account_id INT NULL,
            repayment_account_id INT NULL,
            loan_status_id INT NULL,
            loan_status_name VARCHAR(100) NULL,
            principal_amount DECIMAL(18, 2) NULL,
            interest_rate DECIMAL(10, 6) NULL,
            start_date DATETIME NULL,
            estimated_end_date DATETIME NULL,

            risk_events_30d INT NOT NULL,
            high_severity_events_30d INT NOT NULL,
            medium_severity_events_30d INT NOT NULL,
            distinct_event_types_30d INT NOT NULL,

            max_score_impact_30d DECIMAL(18, {MAX_SCORE_IMPACT_SCALE}) NULL,
            last_risk_event_ts_30d DATETIME NULL,
            high_risk_flag_30d BIT NOT NULL,

            risk_events_all INT NOT NULL,
            last_risk_event_ts_all DATETIME NULL,

            as_of_utc DATETIME NOT NULL
        );
    END
    """

    cur = conn.cursor()
    cur.execute(ddl)
    cur.execute(create_table)
    conn.commit()


# ============================================================
# SQL: load loans base (truth)
# ============================================================
def load_sql_loans(conn: pyodbc.Connection) -> pd.DataFrame:
    """
    Pull loan truth from OLTP.
    We map customer_id using repayment account -> accounts -> customer_id.
    """
    q = """
    SELECT
        l.LoanID AS loan_id,
        a.CustomerID AS customer_id,
        l.DisbursementAccountID AS disbursement_account_id,
        l.RepaymentAccountID AS repayment_account_id,
        l.LoanStatusID AS loan_status_id,
        ls.StatusName AS loan_status_name,
        l.PrincipalAmount AS principal_amount,
        l.InterestRate AS interest_rate,
        l.StartDate AS start_date,
        l.EstimatedEndDate AS estimated_end_date
    FROM dbo.loans l
    JOIN dbo.accounts a ON l.RepaymentAccountID = a.AccountID
    JOIN dbo.loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
    """
    df = pd.read_sql(q, conn)
    return df


# ============================================================
# Mongo: load risk events
# ============================================================
def load_mongo_risk_events(db) -> pd.DataFrame:
    """
    Reads risk_events collection built in PP-3.
    We only fetch fields needed for aggregation.
    """
    projection = {
        "_id": 0,
        "customer_id": 1,
        "loan_id": 1,
        "account_id": 1,
        "transaction_id": 1,
        "event_type": 1,
        "severity": 1,
        "score_impact": 1,
        "observed_at": 1
    }
    docs = list(db.risk_events.find({}, projection))
    if not docs:
        return pd.DataFrame(columns=list(projection.keys()))

    df = pd.DataFrame(docs)

    # normalize observed_at
    if "observed_at" in df.columns:
        df["observed_at_dt"] = pd.to_datetime(df["observed_at"], errors="coerce", utc=True)
    else:
        df["observed_at_dt"] = pd.NaT

    # numeric score impact
    if "score_impact" in df.columns:
        df["score_impact_num"] = pd.to_numeric(df["score_impact"], errors="coerce")
    else:
        df["score_impact_num"] = np.nan

    # normalize severity
    if "severity" in df.columns:
        df["severity_norm"] = df["severity"].astype(str).str.upper()
    else:
        df["severity_norm"] = ""

    return df


# ============================================================
# Risk aggregation
# ============================================================
def aggregate_risk_by(df_events: pd.DataFrame, key_col: str, window_days: int) -> pd.DataFrame:
    """
    Aggregates risk_events into BI-friendly features at a given grain (loan_id or customer_id).
    Produces both 30-day and all-time metrics.
    """
    if df_events is None or df_events.empty:
        # return empty frame with expected columns
        cols = [
            key_col,
            "risk_events_30d", "high_severity_events_30d", "medium_severity_events_30d", "distinct_event_types_30d",
            "max_score_impact_30d", "last_risk_event_ts_30d", "high_risk_flag_30d",
            "risk_events_all", "last_risk_event_ts_all",
        ]
        return pd.DataFrame(columns=cols)

    df = df_events.copy()
    df = df[df[key_col].notna()].copy()

    # all-time
    all_grp = df.groupby(key_col, dropna=False).agg(
        risk_events_all=("event_type", "count"),
        last_risk_event_ts_all=("observed_at_dt", "max")
    ).reset_index()

    # 30d
    now_utc = datetime.now(timezone.utc)
    cutoff = pd.Timestamp(now_utc - timedelta(days=window_days))
    df_30 = df[df["observed_at_dt"] >= cutoff].copy()

    if df_30.empty:
        # still return all-time with 30d as zeros
        all_grp["risk_events_30d"] = 0
        all_grp["high_severity_events_30d"] = 0
        all_grp["medium_severity_events_30d"] = 0
        all_grp["distinct_event_types_30d"] = 0
        all_grp["max_score_impact_30d"] = np.nan
        all_grp["last_risk_event_ts_30d"] = pd.NaT
        all_grp["high_risk_flag_30d"] = 0
        return all_grp

    def count_sev(series, sev):
        return (series == sev).sum()

    grp_30 = df_30.groupby(key_col, dropna=False).agg(
        risk_events_30d=("event_type", "count"),
        high_severity_events_30d=("severity_norm", lambda s: count_sev(s, "HIGH")),
        medium_severity_events_30d=("severity_norm", lambda s: count_sev(s, "MEDIUM")),
        distinct_event_types_30d=("event_type", pd.Series.nunique),
        max_score_impact_30d=("score_impact_num", "max"),
        last_risk_event_ts_30d=("observed_at_dt", "max")
    ).reset_index()

    grp_30["high_risk_flag_30d"] = (grp_30["high_severity_events_30d"] > 0).astype(int)

    # merge all-time + 30d
    out = all_grp.merge(grp_30, on=key_col, how="left")

    # fill 30d defaults
    for c in ["risk_events_30d", "high_severity_events_30d", "medium_severity_events_30d", "distinct_event_types_30d", "high_risk_flag_30d"]:
        out[c] = out[c].fillna(0).astype(int)

    return out


# ============================================================
# Compose final loan-level view with loan-first + customer fallback
# ============================================================
def build_loan_risk_view(df_loans: pd.DataFrame, df_events: pd.DataFrame) -> pd.DataFrame:
    """
    Produces one row per loan.
    Enrichment logic:
      - Prefer loan-linked risk (risk_events.loan_id)
      - Fallback to customer-level risk (risk_events.customer_id)
    """
    df_final = df_loans.copy()

    # 1) loan-level risk (only events where loan_id exists)
    df_events_loan = df_events[df_events.get("loan_id").notna()].copy() if not df_events.empty else df_events
    df_risk_loan = aggregate_risk_by(df_events_loan, key_col="loan_id", window_days=RISK_WINDOW_DAYS)

    # 2) customer-level risk (fallback context)
    df_risk_cust = aggregate_risk_by(df_events, key_col="customer_id", window_days=RISK_WINDOW_DAYS)

    # join loan-level
    df_final = df_final.merge(df_risk_loan, on="loan_id", how="left")

    # join customer-level (suffix for coalesce)
    df_final = df_final.merge(df_risk_cust, on="customer_id", how="left", suffixes=("", "_cust"))

    # coalesce: prefer loan-level, fallback to customer-level
    risk_cols = [
        "risk_events_30d", "high_severity_events_30d", "medium_severity_events_30d", "distinct_event_types_30d",
        "max_score_impact_30d", "last_risk_event_ts_30d", "high_risk_flag_30d",
        "risk_events_all", "last_risk_event_ts_all"
    ]
    for c in risk_cols:
        cust_c = f"{c}_cust"
        if cust_c in df_final.columns:
            df_final[c] = df_final[c].where(df_final[c].notna(), df_final[cust_c])

    # drop fallback columns
    df_final = df_final.drop(columns=[c for c in df_final.columns if c.endswith("_cust")], errors="ignore")

    # enforce defaults for counts/flags
    for c in ["risk_events_30d", "high_severity_events_30d", "medium_severity_events_30d", "distinct_event_types_30d", "risk_events_all"]:
        df_final[c] = pd.to_numeric(df_final.get(c), errors="coerce").fillna(0).astype(int)

    df_final["high_risk_flag_30d"] = pd.to_numeric(df_final.get("high_risk_flag_30d"), errors="coerce").fillna(0).astype(int)

    # timestamps
    for c in ["last_risk_event_ts_30d", "last_risk_event_ts_all", "start_date", "estimated_end_date"]:
        if c in df_final.columns:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce", utc=True)

    # IMPORTANT FIX: decimal column for SQL Server
    # Convert max_score_impact_30d to Decimal/None with correct scale
    df_final["max_score_impact_30d"] = df_final["max_score_impact_30d"].apply(
        lambda v: to_decimal_or_none(v, scale=MAX_SCORE_IMPACT_SCALE)
    )

    df_final["as_of_utc"] = datetime.now(timezone.utc)

    return df_final


# ============================================================
# Write to SQL (with safe placeholder generation)
# ============================================================
def get_sql_column_specs(conn: pyodbc.Connection, schema: str, table: str) -> dict:
    """Return {column_name: {data_type, numeric_precision, numeric_scale}} from INFORMATION_SCHEMA."""
    q = """
    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    cur = conn.cursor()
    rows = cur.execute(q, (schema, table)).fetchall()
    specs = {}
    for r in rows:
        specs[str(r.COLUMN_NAME)] = {
            "data_type": str(r.DATA_TYPE).lower(),
            "precision": int(r.NUMERIC_PRECISION) if r.NUMERIC_PRECISION is not None else None,
            "scale": int(r.NUMERIC_SCALE) if r.NUMERIC_SCALE is not None else None,
        }
    return specs


def to_decimal_quantized(value, scale: int | None):
    """Convert value to Decimal and quantize to given scale (e.g., scale=2 -> 0.01).
    Returns None if value is NaN/inf/None.
    """
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return None
    if pd.isna(value):
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if scale is None:
        return d
    q = Decimal("1").scaleb(-scale)  # 10**(-scale)
    return d.quantize(q, rounding=ROUND_HALF_UP)


def write_loan_risk_view(conn: pyodbc.Connection, df_final: pd.DataFrame) -> None:
    """Truncate + insert analytics.loan_risk_view with schema-aware DECIMAL handling.
    Fixes 'Converting decimal loses precision' by quantizing to the actual column scale.
    Also prints a helpful diagnostic if a specific row still fails.
    """
    schema, table = "analytics", "loan_risk_view"
    specs = get_sql_column_specs(conn, schema, table)

    cols = [
        "loan_id", "customer_id",
        "disbursement_account_id", "repayment_account_id",
        "loan_status_id", "loan_status_name",
        "principal_amount", "interest_rate", "start_date", "estimated_end_date",
        "risk_events_30d", "high_severity_events_30d", "medium_severity_events_30d", "distinct_event_types_30d",
        "max_score_impact_30d", "last_risk_event_ts_30d", "high_risk_flag_30d",
        "risk_events_all", "last_risk_event_ts_all",
        "as_of_utc"
    ]

    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.{table};")
    conn.commit()

    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"""\
    INSERT INTO {schema}.{table} ({", ".join(cols)})
    VALUES ({placeholders});
    """

    def convert(col: str, v):
        dt = specs.get(col, {}).get("data_type", "")
        scale = specs.get(col, {}).get("scale")
        # DECIMAL/NUMERIC: always send Decimal quantized to scale
        if dt in {"decimal", "numeric"}:
            return to_decimal_quantized(v, scale)
        # INT-ish
        if dt in {"int", "bigint", "smallint", "tinyint"}:
            return int(v) if pd.notna(v) else None
        # DATETIME-ish
        if dt in {"datetime", "datetime2", "smalldatetime", "date", "time"}:
            return safe_sql_datetime(v)
        # strings
        if dt in {"nvarchar", "varchar", "nchar", "char", "text", "ntext"}:
            return None if pd.isna(v) else str(v)
        # fallback: best effort
        if pd.isna(v):
            return None
        return v

    rows = []
    for _, r in df_final.iterrows():
        row = tuple(convert(c, r.get(c)) for c in cols)
        rows.append(row)

    # sanity check
    if rows and insert_sql.count("?") != len(rows[0]):
        raise ValueError(f"Placeholder mismatch: insert has {insert_sql.count('?')} params, row has {len(rows[0])} values.")

    # Bulk insert with diagnostics
    cur.fast_executemany = True
    try:
        cur.executemany(insert_sql, rows)
        conn.commit()
    except pyodbc.ProgrammingError as e:
        # fallback: find first bad row and print it
        conn.rollback()
        cur.fast_executemany = False
        for i, row in enumerate(rows):
            try:
                cur.execute(insert_sql, row)
            except pyodbc.Error as e2:
                # Try to extract loan_id for easier debugging
                loan_id = row[0]
                print("\n❌ Insert failed for a specific row (showing key fields):")
                print({"row_index": i, "loan_id": loan_id})
                # show all decimal columns in this row
                dec_cols = [c for c in cols if specs.get(c, {}).get("data_type") in {"decimal", "numeric"}]
                dec_preview = {c: row[cols.index(c)] for c in dec_cols}
                print("Decimal fields:", dec_preview)
                print("Original error:", e2)
                raise
        # if none found, re-raise original
        raise


# ============================================================
# Main
# ============================================================
def main():
    sql_conn = sql_connect()
    mongo_db = mongo_connect()

    try:
        ensure_analytics_objects(sql_conn)

        df_loans = load_sql_loans(sql_conn)
        df_events = load_mongo_risk_events(mongo_db)

        df_final = build_loan_risk_view(df_loans, df_events)

        write_loan_risk_view(sql_conn, df_final)

        print(f"✅ Wrote {len(df_final)} rows into analytics.loan_risk_view (as_of_utc={df_final['as_of_utc'].iloc[0]})")

    finally:
        sql_conn.close()


if __name__ == "__main__":
    main()