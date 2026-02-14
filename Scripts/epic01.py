"""
EPIC 1 (DA-1 to DA-4): Exploratory Data Analysis / Profiling Code

This module performs comprehensive data profiling on a banking dataset including:
- Data loading with column name standardization (PascalCase -> snake_case)
- Table-level profiling (shape, dtypes, missingness, duplicates)
- Primary key uniqueness validation
- Foreign key referential integrity checks
- Date parsing validation
- Numeric field coercion validation
- Automated remediation checklist generation

Updated: To match uploaded CSV column names (PascalCase -> snake_case)
"""

import os
import re
import numpy as np
import pandas as pd

# ========== Configuration ==========

DATA_DIR = "../data"

# Mapping of logical table names to physical CSV filenames
FILES = {
    "customers": "customers.csv",
    "customer_types": "customer_types.csv",
    "addresses": "addresses.csv",
    "branches": "branches.csv",
    "accounts": "accounts.csv",
    "account_types": "account_types.csv",
    "account_statuses": "account_statuses.csv",
    "loans": "loans.csv",
    "loan_statuses": "loan_statuses.csv",
    "transactions": "transactions.csv",
    "transaction_types": "transaction_types.csv",
}

# ========== Helper Functions ==========

def to_snake(name: str) -> str:
    """
    Convert PascalCase/camelCase column names to snake_case.
    
    This function performs a three-step transformation:
    1. Insert underscore before capitalized words (e.g., "UserName" -> "User_Name")
    2. Handle lowercase-to-uppercase transitions (e.g., "userID" -> "user_ID")
    3. Replace non-alphanumeric characters with underscores and clean up
    
    Args:
        name: The column name to convert (e.g., "AccountOriginID", "customerTypeId")
    
    Returns:
        Lowercase snake_case version of the name (e.g., "account_origin_id", "customer_type_id")
    
    Examples:
        >>> to_snake("AccountOriginID")
        'account_origin_id'
        >>> to_snake("CustomerTypeId")
        'customer_type_id'
        >>> to_snake("First-Name")
        'first_name'
    """
    # Step 1: Insert underscore before capitalized words (e.g., "HTTPResponse" -> "HTTP_Response")
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    
    # Step 2: Handle transitions from lowercase/digit to uppercase (e.g., "userID" -> "user_ID")
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    
    # Step 3: Replace any non-alphanumeric sequences with single underscore, trim edge underscores
    s3 = re.sub(r"[^0-9a-zA-Z]+", "_", s2).strip("_")
    
    # Convert to lowercase for final snake_case format
    return s3.lower()


def read_csv_safely(path: str) -> pd.DataFrame:
    """
    Read CSV file and standardize all column names to snake_case.
    
    Uses low_memory=False to ensure consistent dtype inference across chunks.
    All column names are converted from PascalCase/camelCase to snake_case
    to ensure consistency across the codebase.
    
    Args:
        path: Full path to the CSV file
    
    Returns:
        DataFrame with snake_case column names
    """
    df = pd.read_csv(path, low_memory=False)
    df.columns = [to_snake(c) for c in df.columns]
    return df


def guess_id_columns(df: pd.DataFrame):
    """
    Identify columns that likely represent identifiers.
    
    Detects columns ending with '_id' or 'id' which typically indicate
    primary keys, foreign keys, or other identifier fields.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        List of column names that appear to be ID fields
    
    Examples:
        For a DataFrame with columns ['customer_id', 'name', 'account_id'],
        returns ['customer_id', 'account_id']
    """
    return [c for c in df.columns if c.endswith("_id") or c.endswith("id")]


def missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate summary of missing values by column.
    
    Calculates both absolute count and percentage of missing values
    for each column, sorted by missing count in descending order.
    Only returns columns that have at least one missing value.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        DataFrame with columns:
            - missing_count: Number of missing values
            - missing_pct: Percentage of missing values (rounded to 2 decimals)
        Sorted by missing_count descending, filtered to columns with missingness
    """
    miss = df.isna().sum()
    out = pd.DataFrame({
        "missing_count": miss,
        "missing_pct": (miss / len(df) * 100).round(2) if len(df) else 0
    }).sort_values("missing_count", ascending=False)
    
    # Return only columns with missing values
    return out[out["missing_count"] > 0]


def duplicate_key_summary(df: pd.DataFrame, key: str) -> dict:
    """
    Analyze uniqueness and validity of a candidate primary key column.
    
    Checks for:
    - Column existence
    - Total unique values (including NULLs)
    - Number of NULL values
    - Number of duplicate rows based on this key
    
    Args:
        df: DataFrame to analyze
        key: Column name to check as potential primary key
    
    Returns:
        Dictionary containing:
            - key: The column name checked
            - exists: Whether the column exists in the DataFrame
            - rows: Total row count
            - nunique_including_null: Count of unique values (NULLs counted as one group)
            - nulls: Count of NULL values
            - duplicate_rows_on_key: Count of rows with duplicate key values
    """
    if key not in df.columns:
        return {"key": key, "exists": False}
    
    total = len(df)
    nunique = df[key].nunique(dropna=False)  # Include NULLs in unique count
    dup_rows = int(df.duplicated(subset=[key]).sum())
    nulls = int(df[key].isna().sum())
    
    return {
        "key": key,
        "exists": True,
        "rows": total,
        "nunique_including_null": int(nunique),
        "nulls": nulls,
        "duplicate_rows_on_key": dup_rows,
    }


def normalize_possible_numeric(s: pd.Series) -> pd.Series:
    """
    Attempt to coerce a series to numeric by handling common formatting issues.
    
    Handles:
    - Comma separators in numbers (e.g., "1,000,000")
    - Leading/trailing whitespace
    - String representations of NULL ('nan', 'none', '')
    
    Args:
        s: Pandas Series that may contain numeric data as strings
    
    Returns:
        Series coerced to numeric type, with unparseable values as NaN
    
    Examples:
        Input: ["1,000", "2,500.50", "invalid", ""]
        Output: [1000.0, 2500.50, NaN, NaN]
    """
    if s.dtype == "O":  # Object dtype (typically strings)
        # Remove commas and strip whitespace
        cleaned = s.astype(str).str.replace(",", "", regex=False).str.strip()
        # Replace common NULL representations
        cleaned = cleaned.replace({"nan": np.nan, "none": np.nan, "": np.nan})
        return pd.to_numeric(cleaned, errors="coerce")
    
    # Already numeric or non-object type
    return pd.to_numeric(s, errors="coerce")


def try_parse_dates(s: pd.Series) -> pd.Series:
    """
    Attempt to parse a series as datetime values.
    
    Uses pandas' flexible datetime parsing with automatic format inference.
    Unparseable values are returned as NaT (Not a Time).
    
    Args:
        s: Pandas Series that may contain date/datetime data
    
    Returns:
        Series of datetime64 type, with unparseable values as NaT
    
    Examples:
        Input: ["2024-01-01", "01/15/2024", "invalid", ""]
        Output: [Timestamp('2024-01-01'), Timestamp('2024-01-15'), NaT, NaT]
    """
    return pd.to_datetime(s, errors="coerce", infer_datetime_format=True)


# ========== Data Loading ==========

print("=" * 80)
print("LOADING DATASETS")
print("=" * 80)

dfs = {}  # Dictionary to store all loaded DataFrames

# Load each CSV file
for name, fname in FILES.items():
    fpath = os.path.join(DATA_DIR, fname)
    if not os.path.exists(fpath):
        print(f"[WARN] Missing file: {fpath}")
        continue
    dfs[name] = read_csv_safely(fpath)

print("Loaded tables:", list(dfs.keys()))
for k, df in dfs.items():
    print(f"{k:18s} -> shape={df.shape}")

# ========== Schema Configuration ==========

# Primary Key definitions for each table
PK = {
    "customers": "customer_id",
    "customer_types": "customer_type_id",
    "addresses": "address_id",
    "branches": "branch_id",
    "accounts": "account_id",
    "account_types": "account_type_id",
    "account_statuses": "account_status_id",
    "loans": "loan_id",
    "loan_statuses": "loan_status_id",
    "transactions": "transaction_id",
    "transaction_types": "transaction_type_id",
}

# Foreign Key definitions: child_table -> [(child_col, parent_table, parent_pk), ...]
# Each tuple represents: (FK column in child, parent table name, PK column in parent)
FK = {
    "customers": [
        ("customer_type_id", "customer_types", "customer_type_id"),
        ("address_id", "addresses", "address_id"),
    ],
    "branches": [
        ("address_id", "addresses", "address_id"),
    ],
    "accounts": [
        ("customer_id", "customers", "customer_id"),
        ("account_type_id", "account_types", "account_type_id"),
        ("account_status_id", "account_statuses", "account_status_id"),
        # NOTE: accounts.csv does NOT contain branch_id in the source dataset
    ],
    "loans": [
        ("account_id", "accounts", "account_id"),
        ("loan_status_id", "loan_statuses", "loan_status_id"),
    ],
    "transactions": [
        # Transactions have two FK references to accounts (origin and destination)
        ("account_origin_id", "accounts", "account_id"),
        ("account_destination_id", "accounts", "account_id"),
        ("transaction_type_id", "transaction_types", "transaction_type_id"),
        ("branch_id", "branches", "branch_id"),
    ],
}

# ========== Table-Level Profiling ==========

print("\n" + "=" * 80)
print("TABLE-LEVEL PROFILING")
print("=" * 80)

table_profiles = []

for tname, df in dfs.items():
    prof = {
        "table": tname,
        "rows": df.shape[0],
        "cols": df.shape[1],
        "object_cols": int((df.dtypes == "object").sum()),  # String/mixed columns
        "total_missing": int(df.isna().sum().sum()),
        "pct_missing": round((df.isna().sum().sum() / df.size) * 100, 2) if df.size else 0,
        "duplicate_rows_full": int(df.duplicated().sum()),  # Completely duplicate rows
    }
    
    # Detect and list ID columns
    id_cols = guess_id_columns(df)
    prof["id_cols_detected"] = ", ".join(id_cols[:12]) + (" ..." if len(id_cols) > 12 else "")
    
    table_profiles.append(prof)

table_profiles_df = pd.DataFrame(table_profiles).sort_values("table")
print("\n=== Table Profiles (overview) ===")
print(table_profiles_df.to_string(index=False))

# ========== Column-Level Missingness Analysis ==========

print("\n" + "=" * 80)
print("COLUMN-LEVEL MISSINGNESS ANALYSIS")
print("=" * 80)

for tname, df in dfs.items():
    miss = missing_summary(df)
    if miss.empty:
        continue
    print(f"\n[{tname}]")
    print(miss.head(10).to_string())

# ========== Primary Key Uniqueness Checks ==========

print("\n" + "=" * 80)
print("PRIMARY KEY UNIQUENESS VALIDATION")
print("=" * 80)

pk_checks = []

for t, pk in PK.items():
    if t not in dfs:
        continue
    res = duplicate_key_summary(dfs[t], pk)
    pk_checks.append(res)
    print(res)

pk_checks_df = pd.DataFrame(pk_checks)
print("\nPK checks table:")
print(pk_checks_df.to_string(index=False))

# ========== Foreign Key Referential Integrity Checks ==========

def fk_integrity_check(child_df: pd.DataFrame, child_col: str,
                       parent_df: pd.DataFrame, parent_pk: str) -> dict:
    """
    Check referential integrity between child foreign key and parent primary key.
    
    Identifies "orphan" records - non-NULL foreign key values in the child table
    that don't exist in the parent table's primary key column. This indicates
    data quality issues where relationships are broken.
    
    Args:
        child_df: DataFrame containing the foreign key column
        child_col: Name of the foreign key column in child table
        parent_df: DataFrame containing the primary key column
        parent_pk: Name of the primary key column in parent table
    
    Returns:
        Dictionary containing:
            - child_col: FK column name
            - parent_pk: PK column name
            - child_nonnull_unique: Count of unique non-NULL FK values
            - orphan_unique: Count of FK values with no matching PK
            - orphan_sample: Sample of orphan values (up to 10)
    
    Example:
        If transactions.account_origin_id contains values [1, 2, 3, NULL]
        but accounts.account_id only contains [1, 2],
        then value 3 is an orphan.
    """
    # Validate columns exist
    if child_col not in child_df.columns or parent_pk not in parent_df.columns:
        return {"child_col": child_col, "parent_pk": parent_pk, "check": "missing_column"}
    
    # Get unique non-NULL values from child FK column
    child_vals = child_df[child_col].dropna().unique()
    
    # Get set of unique non-NULL values from parent PK column
    parent_vals = set(parent_df[parent_pk].dropna().unique())
    
    # Find orphan values (exist in child but not in parent)
    orphans = [v for v in child_vals if v not in parent_vals]
    
    return {
        "child_col": child_col,
        "parent_pk": parent_pk,
        "child_nonnull_unique": int(len(child_vals)),
        "orphan_unique": int(len(orphans)),
        "orphan_sample": orphans[:10],  # Sample for investigation
    }


print("\n" + "=" * 80)
print("FOREIGN KEY REFERENTIAL INTEGRITY VALIDATION")
print("=" * 80)

fk_results = []

# Iterate through all defined foreign key relationships
for child_table, rules in FK.items():
    if child_table not in dfs:
        continue
    
    # Check each FK relationship for this child table
    for child_col, parent_table, parent_pk in rules:
        if parent_table not in dfs:
            continue
        
        r = fk_integrity_check(dfs[child_table], child_col, dfs[parent_table], parent_pk)
        r.update({"child_table": child_table, "parent_table": parent_table})
        fk_results.append(r)
        print(r)

fk_results_df = pd.DataFrame(fk_results)
print("\nFK integrity summary:")
print(fk_results_df.to_string(index=False))

# ========== Date Parsing Validation ==========

# Column name patterns that likely indicate date/datetime fields
LIKELY_DATE_COLS = ["date", "dt", "datetime", "timestamp", "dob", "birth", "created", "opening", "start", "end"]


def is_likely_date_col(col: str) -> bool:
    """
    Heuristic to identify columns that likely contain date/datetime values.
    
    Args:
        col: Column name to check
    
    Returns:
        True if column name contains common date-related keywords
    """
    c = col.lower()
    return any(token in c for token in LIKELY_DATE_COLS)


print("\n" + "=" * 80)
print("DATE PARSING VALIDATION")
print("=" * 80)

date_parse_report = []

# Check each column that appears to contain dates
for tname, df in dfs.items():
    for col in df.columns:
        if not is_likely_date_col(col):
            continue
        
        s = df[col]
        parsed = try_parse_dates(s)
        
        nonnull = int(s.notna().sum())  # Original non-NULL count
        parsed_nonnull = int(parsed.notna().sum())  # Successfully parsed count
        success = round((parsed_nonnull / nonnull) * 100, 2) if nonnull else None
        
        date_parse_report.append({
            "table": tname,
            "column": col,
            "nonnull_values": nonnull,
            "parsed_as_datetime": parsed_nonnull,
            "parse_success_pct": success,
        })

date_parse_df = pd.DataFrame(date_parse_report).sort_values(["table", "column"])
print("\n=== Date parse report (likely date columns) ===")
print("No likely date columns detected by heuristic." if date_parse_df.empty else date_parse_df.to_string(index=False))

# ========== Numeric Coercion Validation ==========

# Column name patterns that likely indicate numeric fields
LIKELY_NUM_COLS = ["amount", "balance", "principal", "rate", "interest", "fee", "limit", "income", "payment"]


def is_likely_numeric_col(col: str) -> bool:
    """
    Heuristic to identify columns that likely contain numeric values.
    
    Args:
        col: Column name to check
    
    Returns:
        True if column name contains common numeric field keywords
    """
    c = col.lower()
    return any(token in c for token in LIKELY_NUM_COLS)


print("\n" + "=" * 80)
print("NUMERIC COERCION VALIDATION")
print("=" * 80)

num_coerce_report = []

# Check each column that appears to contain numeric data
for tname, df in dfs.items():
    for col in df.columns:
        if not is_likely_numeric_col(col):
            continue
        
        s = df[col]
        coerced = normalize_possible_numeric(s)
        
        nonnull = int(s.notna().sum())  # Original non-NULL count
        numeric = int(coerced.notna().sum())  # Successfully coerced count
        success = round((numeric / nonnull) * 100, 2) if nonnull else None
        
        num_coerce_report.append({
            "table": tname,
            "column": col,
            "nonnull_values": nonnull,
            "coerced_numeric": numeric,
            "coerce_success_pct": success,
            "min": float(coerced.min()) if numeric else None,
            "max": float(coerced.max()) if numeric else None,
        })

num_coerce_df = pd.DataFrame(num_coerce_report).sort_values(["table", "column"])
print("\n=== Numeric coercion report (likely numeric columns) ===")
print("No likely numeric columns detected by heuristic." if num_coerce_df.empty else num_coerce_df.to_string(index=False))

# ========== Export Profiling Reports ==========

print("\n" + "=" * 80)
print("EXPORTING PROFILING REPORTS")
print("=" * 80)

EXPORT_DIR = os.path.join(DATA_DIR, "epic1_profiling_outputs")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Export all profiling reports as CSV
table_profiles_df.to_csv(os.path.join(EXPORT_DIR, "table_profiles_overview.csv"), index=False)
pk_checks_df.to_csv(os.path.join(EXPORT_DIR, "pk_checks.csv"), index=False)
fk_results_df.to_csv(os.path.join(EXPORT_DIR, "fk_integrity_checks.csv"), index=False)
date_parse_df.to_csv(os.path.join(EXPORT_DIR, "date_parse_report.csv"), index=False)
num_coerce_df.to_csv(os.path.join(EXPORT_DIR, "numeric_coercion_report.csv"), index=False)

print(f"\nSaved profiling outputs to: {EXPORT_DIR}")

# ========== DA-4 Remediation Checklist Generation ==========

print("\n" + "=" * 80)
print("GENERATING DATA QUALITY REMEDIATION CHECKLIST (DA-4)")
print("=" * 80)

remediation_actions = []

# Issue 1: Check for duplicate primary keys
for _, row in pk_checks_df.iterrows():
    if row.get("exists") and row.get("duplicate_rows_on_key", 0) > 0:
        remediation_actions.append({
            "issue_type": "duplicate_pk",
            "table": next((t for t, pk in PK.items() if pk == row["key"]), None),
            "key": row["key"],
            "action": "Deduplicate in staging using ROW_NUMBER() over PK; keep first, log others.",
            "severity": "high"
        })

# Issue 2: Check for foreign key orphans (broken referential integrity)
for _, row in fk_results_df.iterrows():
    if row.get("orphan_unique", 0) > 0:
        remediation_actions.append({
            "issue_type": "fk_orphans",
            "table": row["child_table"],
            "key": row["child_col"],
            "action": "Validate FK in staging; quarantine orphan rows or correct source mapping.",
            "severity": "high"
        })

# Issue 3: Check for date parsing failures (< 95% success rate)
if not date_parse_df.empty:
    bad_dates = date_parse_df[(date_parse_df["parse_success_pct"].notna()) & (date_parse_df["parse_success_pct"] < 95)]
    for _, row in bad_dates.iterrows():
        remediation_actions.append({
            "issue_type": "date_format_inconsistency",
            "table": row["table"],
            "key": row["column"],
            "action": "Standardize date parsing in staging; store as DATE/DATETIME in final; log unparseable rows.",
            "severity": "medium"
        })

# Issue 4: Check for numeric coercion failures (< 95% success rate)
if not num_coerce_df.empty:
    bad_nums = num_coerce_df[(num_coerce_df["coerce_success_pct"].notna()) & (num_coerce_df["coerce_success_pct"] < 95)]
    for _, row in bad_nums.iterrows():
        remediation_actions.append({
            "issue_type": "numeric_format_inconsistency",
            "table": row["table"],
            "key": row["column"],
            "action": "Coerce numeric fields in staging (strip commas/currency); enforce DECIMAL in final; log failures.",
            "severity": "medium"
        })

# Export remediation checklist
remediation_df = pd.DataFrame(remediation_actions)
remediation_path = os.path.join(EXPORT_DIR, "da4_remediation_checklist.csv")
remediation_df.to_csv(remediation_path, index=False)

print(f"Saved DA-4 remediation checklist to: {remediation_path}")
print(f"\nTotal issues identified: {len(remediation_actions)}")
print(f"  - High severity: {len([x for x in remediation_actions if x['severity'] == 'high'])}")
print(f"  - Medium severity: {len([x for x in remediation_actions if x['severity'] == 'medium'])}")

print("\n" + "=" * 80)
print("PROFILING COMPLETE")
print("=" * 80)