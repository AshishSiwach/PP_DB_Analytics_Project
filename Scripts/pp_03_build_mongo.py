import os
from datetime import datetime, timezone, timedelta
import math
from collections import defaultdict
import random

import pyodbc
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv


# -----------------------------
# Config
# -----------------------------
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER", r"localhost\SQLEXPRESS")
SQL_DB = os.getenv("SQL_DB", "FinancePolyglotDB")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "finance_pp")

PIPELINE_VERSION = "pp_v1"

RECENT_ACTIVITY_CAP = 100
RISK_SCORE_IMPACT_THRESHOLD = 0.05
SEVERITIES_TO_KEEP = {"MEDIUM", "HIGH"}

# Used to make dates realistic when source dates are NULL/1900
SYNTHETIC_DATE_WINDOW_DAYS = int(os.getenv("SYNTHETIC_DATE_WINDOW_DAYS", "120"))
MIN_REALISTIC_DATE = datetime(2000, 1, 1)  # anything older treated as placeholder


# -----------------------------
# SQL helpers
# -----------------------------
def sql_connect():
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"Trusted_Connection={SQL_TRUSTED_CONNECTION};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def fetchall_dict(cursor, query: str, params=None):
    cursor.execute(query, params or [])
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchall()
    out = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def normalize_dt(dt):
    """
    Ensures a usable datetime in UTC.
    If dt is None or clearly placeholder (e.g. 1900), generate a synthetic recent timestamp.
    """
    now = datetime.now(timezone.utc)

    if dt is None:
        delta_minutes = random.randint(0, SYNTHETIC_DATE_WINDOW_DAYS * 24 * 60)
        return now - timedelta(minutes=delta_minutes)

    # pyodbc gives naive datetime, treat as UTC for project purposes
    if isinstance(dt, datetime):
        if dt < MIN_REALISTIC_DATE:
            delta_minutes = random.randint(0, SYNTHETIC_DATE_WINDOW_DAYS * 24 * 60)
            return now - timedelta(minutes=delta_minutes)
        return dt.replace(tzinfo=timezone.utc)

    # fallback
    delta_minutes = random.randint(0, SYNTHETIC_DATE_WINDOW_DAYS * 24 * 60)
    return now - timedelta(minutes=delta_minutes)


# -----------------------------
# Mongo helpers
# -----------------------------
def mongo_connect():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return client, db


def ensure_indexes(db):
    db.customer_profiles.create_index("customer_id", unique=True)
    db.customer_profiles.create_index("accounts.account_id")
    db.customer_profiles.create_index([("risk_profile.risk_band", 1), ("risk_profile.risk_score", -1)])

    db.risk_events.create_index([("customer_id", 1), ("observed_at", -1)])
    db.risk_events.create_index([("severity", 1), ("observed_at", -1)])
    db.risk_events.create_index([("event_type", 1), ("observed_at", -1)])
    db.risk_events.create_index("transaction_id")

    print("✅ MongoDB indexes ensured.")


# -----------------------------
# Transform: build customer_profiles
# -----------------------------
def build_customer_profiles(cur):
    customer_types = {
        r["CustomerTypeID"]: r["TypeName"]
        for r in fetchall_dict(cur, "SELECT CustomerTypeID, TypeName FROM dbo.customer_types;")
    }
    account_types = {
        r["AccountTypeID"]: r["TypeName"]
        for r in fetchall_dict(cur, "SELECT AccountTypeID, TypeName FROM dbo.account_types;")
    }
    account_statuses = {
        r["AccountStatusID"]: r["StatusName"]
        for r in fetchall_dict(cur, "SELECT AccountStatusID, StatusName FROM dbo.account_statuses;")
    }
    loan_statuses = {
        r["LoanStatusID"]: r["StatusName"]
        for r in fetchall_dict(cur, "SELECT LoanStatusID, StatusName FROM dbo.loan_statuses;")
    }
    txn_types = {
        r["TransactionTypeID"]: r["TypeName"]
        for r in fetchall_dict(cur, "SELECT TransactionTypeID, TypeName FROM dbo.transaction_types;")
    }

    addr_rows = fetchall_dict(cur, "SELECT AddressID, Street, City, Country FROM dbo.addresses;")
    addresses = {r["AddressID"]: r for r in addr_rows}

    customers = fetchall_dict(cur, """
        SELECT CustomerID, FirstName, LastName, DateOfBirth, AddressID, CustomerTypeID
        FROM dbo.customers;
    """)

    accounts_rows = fetchall_dict(cur, """
        SELECT AccountID, CustomerID, AccountTypeID, AccountStatusID, Balance, OpeningDate
        FROM dbo.accounts;
    """)
    accounts_by_customer = defaultdict(list)
    for a in accounts_rows:
        accounts_by_customer[a["CustomerID"]].append(a)

    loans_rows = fetchall_dict(cur, """
        SELECT LoanID, DisbursementAccountID, RepaymentAccountID, LoanStatusID,
               PrincipalAmount, InterestRate, StartDate, EstimatedEndDate
        FROM dbo.loans;
    """)

    account_to_customer = {a["AccountID"]: a["CustomerID"] for a in accounts_rows}

    loans_by_customer = defaultdict(list)
    for l in loans_rows:
        cust_id = account_to_customer.get(l["RepaymentAccountID"]) or account_to_customer.get(l["DisbursementAccountID"])
        if cust_id is not None:
            loans_by_customer[cust_id].append(l)

    # Use LEFT JOINs so we DON'T drop transactions if an account lookup fails.
    txn_rows = fetchall_dict(cur, """
        SELECT
            t.TransactionID,
            t.AccountOriginID,
            t.AccountDestinationID,
            t.TransactionTypeID,
            t.LoanID,
            t.Amount,
            t.TransactionDate,
            t.BranchID,
            t.Description,
            COALESCE(a1.CustomerID, a2.CustomerID) AS CustomerID
        FROM dbo.transactions t
        LEFT JOIN dbo.accounts a1 ON t.AccountOriginID = a1.AccountID
        LEFT JOIN dbo.accounts a2 ON t.AccountDestinationID = a2.AccountID;
    """)

    # Drop transactions where we STILL cannot derive a customer
    txn_rows = [t for t in txn_rows if t.get("CustomerID") is not None]

    # Normalize transaction dates for downstream 30-day logic
    for t in txn_rows:
        t["TransactionDate"] = normalize_dt(t.get("TransactionDate"))

    txns_by_customer = defaultdict(list)
    for t in txn_rows:
        txns_by_customer[t["CustomerID"]].append(t)

    as_of_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    docs = []
    for c in customers:
        cust_id = c["CustomerID"]

        profile = {
            "first_name": c.get("FirstName"),
            "last_name": c.get("LastName"),
            "date_of_birth": c.get("DateOfBirth").isoformat() if c.get("DateOfBirth") else None,
            "customer_type": {
                "id": c.get("CustomerTypeID"),
                "name": customer_types.get(c.get("CustomerTypeID"))
            }
        }

        addr = addresses.get(c.get("AddressID"))
        address_doc = None
        if addr:
            address_doc = {
                "address_id": addr["AddressID"],
                "street": addr.get("Street"),
                "city": addr.get("City"),
                "country": addr.get("Country")
            }

        acc_docs = []
        for a in accounts_by_customer.get(cust_id, []):
            acc_docs.append({
                "account_id": a["AccountID"],
                "type": {"id": a["AccountTypeID"], "name": account_types.get(a["AccountTypeID"])},
                "status": {"id": a["AccountStatusID"], "name": account_statuses.get(a["AccountStatusID"])},
                "balance": float(a["Balance"]) if a.get("Balance") is not None else None,
                "opening_date": a["OpeningDate"].isoformat() if a.get("OpeningDate") else None
            })

        cust_loans = loans_by_customer.get(cust_id, [])
        loan_docs = []
        active_count = 0
        total_principal = 0.0
        rates = []

        for l in cust_loans:
            status_name = loan_statuses.get(l["LoanStatusID"])
            if status_name and status_name.lower() == "active":
                active_count += 1

            principal = float(l["PrincipalAmount"]) if l.get("PrincipalAmount") is not None else None
            rate = float(l["InterestRate"]) if l.get("InterestRate") is not None else None

            if principal is not None:
                total_principal += principal
            if rate is not None:
                rates.append(rate)

            loan_docs.append({
                "loan_id": l["LoanID"],
                "status": {"id": l["LoanStatusID"], "name": status_name},
                "principal_amount": principal,
                "interest_rate": rate,
                "start_date": l["StartDate"].isoformat() if l.get("StartDate") else None,
                "estimated_end_date": l["EstimatedEndDate"].isoformat() if l.get("EstimatedEndDate") else None,
                "disbursement_account_id": l["DisbursementAccountID"],
                "repayment_account_id": l["RepaymentAccountID"]
            })

        loans_summary = {
            "loan_count": len(cust_loans),
            "active_loan_count": active_count,
            "total_principal": round(total_principal, 2),
            "avg_interest_rate": round(sum(rates) / len(rates), 6) if rates else None,
            "loans": loan_docs
        }

        cust_txns = txns_by_customer.get(cust_id, [])
        cust_txns.sort(key=lambda x: x["TransactionDate"], reverse=True)
        cust_txns = cust_txns[:RECENT_ACTIVITY_CAP]

        recent_activity = []
        for t in cust_txns:
            recent_activity.append({
                "transaction_id": t["TransactionID"],
                "ts": t["TransactionDate"].isoformat(),
                "type": {"id": t["TransactionTypeID"], "name": txn_types.get(t["TransactionTypeID"])},
                "amount": float(t["Amount"]) if t.get("Amount") is not None else None,
                "origin_account_id": t.get("AccountOriginID"),
                "destination_account_id": t.get("AccountDestinationID"),
                "branch_id": t.get("BranchID"),
                "loan_id": t.get("LoanID"),
                "description": t.get("Description")
            })

        now = datetime.now(timezone.utc)
        last_24h = now - timedelta(hours=24)

        txn_24h = sum(1 for t in txns_by_customer.get(cust_id, []) if t["TransactionDate"] >= last_24h)

        base = 0.20
        velocity_component = min(txn_24h / 50.0, 0.50)
        loan_component = 0.10 if active_count > 0 else 0.0
        risk_score = max(0.0, min(1.0, base + velocity_component + loan_component))

        if risk_score < 0.35:
            band = "LOW"
        elif risk_score < 0.65:
            band = "MEDIUM"
        else:
            band = "HIGH"

        last_event_ts = recent_activity[0]["ts"] if recent_activity else None

        risk_profile = {
            "risk_score": round(risk_score, 4),
            "risk_band": band,
            "last_risk_event_ts": last_event_ts,
            "signals": {
                "txn_velocity_24h": txn_24h,
                "loan_active": (active_count > 0),
            }
        }

        doc = {
            "_id": f"cust_{cust_id}",
            "customer_id": cust_id,
            "profile": profile,
            "address": address_doc,
            "accounts": acc_docs,
            "loans_summary": loans_summary,
            "recent_activity": recent_activity,
            "risk_profile": risk_profile,
            "as_of": as_of_iso,
            "source": {"sql_db": SQL_DB, "pipeline_version": PIPELINE_VERSION}
        }
        docs.append(doc)

    return docs, txn_rows, txn_types


# -----------------------------
# Transform: derive risk_events (flagged only)
# -----------------------------
def _dt_is_bad(dt: datetime) -> bool:
    """Return True for missing/placeholder timestamps that break time-window logic."""
    if dt is None:
        return True
    # Common placeholder values in synthetic/dirty datasets
    if dt.year <= 1901:
        return True
    return False


def _repair_transaction_timestamps(txn_rows, days_back: int = 60, jitter_minutes: int = 30):
    """
    Reconstruct a usable TransactionDate timeline **per customer** when upstream timestamps are NULL/1900.

    Why:
      - Velocity rules rely on timestamps to compute baselines + rolling windows.
      - If most TransactionDate values are NULL or 1900-01-01, everything collapses into one hour
        and MEDIUM velocity events will never trigger.

    Strategy (minimal & deterministic):
      - Keep per-customer ordering stable (TransactionDate if valid else TransactionID).
      - Spread transactions across the last `days_back` days with a small jitter.
      - Does NOT change amounts/counts/customers; only repairs the broken time field used for windowing.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    by_customer = defaultdict(list)
    for t in txn_rows:
        by_customer[t.get("CustomerID")].append(t)

    for cust_id, txns in by_customer.items():
        # Stable order: valid timestamp first; otherwise TransactionID.
        def _sort_key(x):
            dt = x.get("TransactionDate")
            if isinstance(dt, datetime) and not _dt_is_bad(dt):
                return (0, dt)
            return (1, int(x.get("TransactionID") or 0))

        txns.sort(key=_sort_key)

        n = max(1, len(txns))
        # Spread roughly evenly; keep spacing >= 30 minutes to allow velocity windows to exist.
        total_minutes = days_back * 24 * 60
        step = max(30, total_minutes // n)

        base = now - timedelta(days=days_back)
        for i, t in enumerate(txns):
            # jitter in [-jitter_minutes, +jitter_minutes]
            j = random.randint(-jitter_minutes, jitter_minutes) if jitter_minutes else 0
            t["TransactionDate"] = base + timedelta(minutes=i * step + j)

    return txn_rows


def profile_transactions(txn_rows) -> dict:
    """Lightweight data profiling for TransactionDate + Amount used by risk rules."""
    dates = [t.get("TransactionDate") for t in txn_rows]
    bad = [d for d in dates if not isinstance(d, datetime) or _dt_is_bad(d)]
    good = [d for d in dates if isinstance(d, datetime) and not _dt_is_bad(d)]

    amounts = []
    for t in txn_rows:
        a = t.get("Amount")
        try:
            if a is not None:
                amounts.append(float(a))
        except Exception:
            pass

    profile = {
        "rows": len(txn_rows),
        "transaction_date_good": len(good),
        "transaction_date_bad": len(bad),
        "transaction_date_bad_pct": round((len(bad) / max(1, len(txn_rows))) * 100, 2),
        "transaction_date_min": min(good).isoformat() if good else None,
        "transaction_date_max": max(good).isoformat() if good else None,
        "amount_min": min(amounts) if amounts else None,
        "amount_p50": (sorted(amounts)[len(amounts)//2] if amounts else None),
        "amount_max": max(amounts) if amounts else None,
    }
    return profile


def derive_risk_events(txn_rows, txn_types):
    """
    Produce flagged risk events from transactions.

    Rules (adaptive, data-driven):
      1) TXN_VELOCITY_SPIKE (MEDIUM)
         - rolling 2-hour window count compared against customer baseline
           threshold_2h = max(min_floor, ceil(multiplier * baseline_per_hour * 2))

      2) HIGH_VALUE_TXN (HIGH)
         - per-customer P95 threshold (fallback to global P95 if customer has few txns)

    Guardrails:
      - If TransactionDate is broken (NULL/1900), either fail-fast or auto-repair (env controlled).
    """
    # --------------------------
    # Data readiness gate
    # --------------------------
    prof = profile_transactions(txn_rows)
    print("[DQ] transactions profile:", prof)

    bad_pct = prof["transaction_date_bad_pct"]
    fix_bad_dates = os.getenv("FIX_BAD_TRANSACTION_DATES", "1").strip() in {"1", "true", "True", "yes", "YES"}
    fail_on_bad_dates = os.getenv("FAIL_ON_BAD_TRANSACTION_DATES", "0").strip() in {"1", "true", "True", "yes", "YES"}

    if bad_pct >= 50.0:
        msg = f"[DQ] TransactionDate appears invalid for {bad_pct}% rows (NULL/<=1901)."
        if fail_on_bad_dates and not fix_bad_dates:
            raise ValueError(msg + " Set FIX_BAD_TRANSACTION_DATES=1 to auto-repair for window-based rules.")
        if fix_bad_dates:
            print(msg + " Auto-repairing timestamps (per-customer) to enable velocity windows...")
            txn_rows = _repair_transaction_timestamps(
                txn_rows,
                days_back=int(os.getenv("REPAIR_DATES_DAYS_BACK", "60")),
                jitter_minutes=int(os.getenv("REPAIR_DATES_JITTER_MINUTES", "30")),
            )

    # --------------------------
    # Config for rules
    # --------------------------
    window_hours = int(os.getenv("VELOCITY_WINDOW_HOURS", "2"))
    min_floor = int(os.getenv("VELOCITY_MIN_FLOOR", "4"))
    multiplier = float(os.getenv("VELOCITY_MULTIPLIER", "3.0"))
    min_txns_for_customer_p95 = int(os.getenv("MIN_TXNS_FOR_CUSTOMER_P95", "20"))

    score_medium = float(os.getenv("SCORE_IMPACT_MEDIUM", "0.08"))
    score_high = float(os.getenv("SCORE_IMPACT_HIGH", "0.12"))

    # Global P95 for fallback
    amounts = [float(t["Amount"]) for t in txn_rows if t.get("Amount") is not None]
    global_p95 = None
    if amounts:
        s = sorted(amounts)
        idx = int(round(0.95 * (len(s) - 1)))
        global_p95 = s[idx]

    # Group by customer
    by_customer = defaultdict(list)
    for t in txn_rows:
        by_customer[t["CustomerID"]].append(t)

    events = []
    as_of_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for cust_id, txns in by_customer.items():
        # Sort by timestamp (now repaired if needed)
        txns.sort(key=lambda x: x["TransactionDate"])

        # --------------------------
        # Baseline: median txns/hour (simple, robust)
        # --------------------------
        if len(txns) >= 2:
            start = txns[0]["TransactionDate"]
            end = txns[-1]["TransactionDate"]
            hours = max(1.0, (end - start).total_seconds() / 3600.0)
            baseline_per_hour = len(txns) / hours
        else:
            baseline_per_hour = 0.0

        threshold_2h = max(min_floor, int(math.ceil(multiplier * baseline_per_hour * window_hours)))

        # --------------------------
        # Rolling window velocity spike
        # --------------------------
        window = []
        for t in txns:
            ts = t["TransactionDate"]
            window.append((ts, t))
            cutoff = ts - timedelta(hours=window_hours)
            while window and window[0][0] < cutoff:
                window.pop(0)

            if len(window) >= threshold_2h:
                events.append({
                    "_id": f"re_{t['TransactionID']}_VEL",
                    "event_id": f"re_{t['TransactionID']}_VEL",
                    "customer_id": cust_id,
                    "account_id": t.get("AccountOriginID"),
                    "transaction_id": t["TransactionID"],
                    "loan_id": t.get("LoanID"),
                    "event_type": "TXN_VELOCITY_SPIKE",
                    "severity": "MEDIUM",
                    # round to 3dp to avoid DECIMAL precision issues downstream
                    "score_impact": round(score_medium, 3),
                    "observed_at": ts.replace(tzinfo=timezone.utc).isoformat(),
                    "features": {
                        "window_hours": window_hours,
                        "txn_count_last_window": len(window),
                        "threshold_window": threshold_2h,
                        "baseline_per_hour": round(baseline_per_hour, 6),
                        "amount": float(t["Amount"]) if t.get("Amount") is not None else None,
                        "transaction_type": txn_types.get(t.get("TransactionTypeID")),
                    },
                    "as_of_utc": as_of_iso,
                })

        # --------------------------
        # High value txn: per-customer P95 (fallback global)
        # --------------------------
        cust_amounts = [float(x["Amount"]) for x in txns if x.get("Amount") is not None]
        cust_p95 = None
        if len(cust_amounts) >= min_txns_for_customer_p95:
            s = sorted(cust_amounts)
            idx = int(round(0.95 * (len(s) - 1)))
            cust_p95 = s[idx]
        else:
            cust_p95 = global_p95

        if cust_p95 is not None and cust_p95 > 0:
            for t in txns:
                amt = t.get("Amount")
                if amt is None:
                    continue
                amt_f = float(amt)
                if amt_f >= cust_p95:
                    ts = t["TransactionDate"]
                    events.append({
                        "_id": f"re_{t['TransactionID']}_HIV",
                        "event_id": f"re_{t['TransactionID']}_HIV",
                        "customer_id": cust_id,
                        "account_id": t.get("AccountOriginID"),
                        "transaction_id": t["TransactionID"],
                        "loan_id": t.get("LoanID"),
                        "event_type": "HIGH_VALUE_TXN",
                        "severity": "HIGH",
                        "score_impact": round(score_high, 3),
                        "observed_at": ts.replace(tzinfo=timezone.utc).isoformat(),
                        "features": {
                            "amount": amt_f,
                            "threshold_p95": cust_p95,
                            "threshold_source": "customer_p95" if len(cust_amounts) >= min_txns_for_customer_p95 else "global_p95",
                            "transaction_type": txn_types.get(t.get("TransactionTypeID")),
                        },
                        "as_of_utc": as_of_iso,
                    })

    return events

def upsert_customer_profiles(db, docs):
    ops = [
        UpdateOne({"customer_id": d["customer_id"]}, {"$set": d}, upsert=True)
        for d in docs
    ]
    if not ops:
        print("No customer profile docs to upsert.")
        return
    res = db.customer_profiles.bulk_write(ops, ordered=False)
    print(f"✅ customer_profiles upserted. matched={res.matched_count}, upserted={len(res.upserted_ids) if res.upserted_ids else 0}")


def insert_risk_events(db, events):
    if not events:
        print("❌ No risk events generated (nothing flagged).")
        return
    ops = [UpdateOne({"_id": e["_id"]}, {"$setOnInsert": e}, upsert=True) for e in events]
    try:
        res = db.risk_events.bulk_write(ops, ordered=False)
        print(f"✅ risk_events written. upserted={len(res.upserted_ids) if res.upserted_ids else 0}, matched={res.matched_count}")
    except BulkWriteError as bwe:
        print("Bulk write error:", bwe.details)


# -----------------------------
# Main
# -----------------------------
def main():
    sql_conn = sql_connect()
    cur = sql_conn.cursor()
    mongo_client, db = mongo_connect()

    print("Resetting MongoDB collections...")
    db.customer_profiles.delete_many({})
    db.risk_events.delete_many({})

    try:
        ensure_indexes(db)

        profiles, txn_rows, txn_types = build_customer_profiles(cur)
        print(f"DEBUG: extracted txn_rows={len(txn_rows)}")

        events = derive_risk_events(txn_rows, txn_types)
        print(f"DEBUG: derived risk_events={len(events)}")

        upsert_customer_profiles(db, profiles)
        insert_risk_events(db, events)

        print(f"Mongo counts: customer_profiles={db.customer_profiles.count_documents({})}, risk_events={db.risk_events.count_documents({})}")

    finally:
        cur.close()
        sql_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    main()