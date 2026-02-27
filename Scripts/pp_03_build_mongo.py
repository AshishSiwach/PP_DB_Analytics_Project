import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import random
import statistics
import bisect

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
def derive_risk_events(txn_rows, txn_types):
    amounts = [float(t["Amount"]) for t in txn_rows if t.get("Amount") is not None]
    amounts_sorted = sorted(amounts)
    p95 = None
    if amounts_sorted:
        idx = int(round(0.95 * (len(amounts_sorted) - 1)))
        p95 = amounts_sorted[idx]

    by_customer = defaultdict(list)
    for t in txn_rows:
        by_customer[t["CustomerID"]].append(t)

    events = []
    as_of_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for cust_id, txns in by_customer.items():
        txns.sort(key=lambda x: x["TransactionDate"])

        # Adaptive velocity: flag when activity in a 2-hour window is unusually high for this customer
        # Baseline = median transactions per hour for this customer (data-driven, per-customer)
        hourly_counts = defaultdict(int)
        for t0 in txns:
            hour_bucket = t0["TransactionDate"].replace(minute=0, second=0, microsecond=0)
            hourly_counts[hour_bucket] += 1
        baseline_per_hour = statistics.median(hourly_counts.values()) if hourly_counts else 0
        # Expected in 2h ~= baseline_per_hour * 2; flag when > 3x expected, with a small floor to avoid noise
        adaptive_threshold = max(4, int((baseline_per_hour * 2) * 3 + 0.9999))

        window = []
        for t in txns:
            ts = t["TransactionDate"]
            window.append((ts, t))
            cutoff = ts - timedelta(hours=2)
            while window and window[0][0] < cutoff:
                window.pop(0)
            if len(window) >= adaptive_threshold:
                events.append({
                    "_id": f"re_{t['TransactionID']}_VEL",
                    "event_id": f"re_{t['TransactionID']}_VEL",
                    "customer_id": cust_id,
                    "account_id": t.get("AccountOriginID"),
                    "transaction_id": t["TransactionID"],
                    "loan_id": t.get("LoanID"),
                    "event_type": "TXN_VELOCITY_SPIKE",
                    "severity": "MEDIUM",
                    "score_impact": None,
                    "observed_at": ts.isoformat(),
                    "features": {
                        "txn_count_last_2h": len(window),
                        "baseline_txn_per_hour_median": float(baseline_per_hour),
                        "adaptive_threshold_last_2h": int(adaptive_threshold),
                        "raw_velocity_ratio": float(len(window) / adaptive_threshold if adaptive_threshold else 0.0),
                        "amount": float(t["Amount"]) if t.get("Amount") is not None else None,
                        "transaction_type": txn_types.get(t["TransactionTypeID"])
                    },
                    "rule": {
                        "rule_id": "R_TXN_002_ADAPT",
                        "description": "Transaction velocity spike vs per-customer baseline (2h window)"
                    },
                    "as_of": as_of_iso,
                    "source": {"derived_from": "dbo.transactions", "pipeline_version": PIPELINE_VERSION}
                })
# High value: per-customer 95th percentile (fallback to global P95 if a customer has too few transactions)
        cust_amounts = [float(t0["Amount"]) for t0 in txns if t0.get("Amount") is not None]
        cust_amounts_sorted = sorted(cust_amounts)
        cust_p95 = None
        cust_p95_scope = None

        MIN_TXNS_FOR_CUST_P95 = 20  # keep this small; synthetic data can be sparse per customer
        if len(cust_amounts_sorted) >= MIN_TXNS_FOR_CUST_P95:
            idx_c = int(round(0.95 * (len(cust_amounts_sorted) - 1)))
            cust_p95 = cust_amounts_sorted[idx_c]
            cust_p95_scope = "customer"
        elif p95 is not None:
            cust_p95 = p95
            cust_p95_scope = "global_fallback"

        if cust_p95 is not None:
            for t in txns:
                amt = float(t["Amount"]) if t.get("Amount") is not None else None
                if amt is not None and amt >= cust_p95:
                    ts = t["TransactionDate"]
                    events.append({
                        "_id": f"re_{t['TransactionID']}_HV",
                        "event_id": f"re_{t['TransactionID']}_HV",
                        "customer_id": cust_id,
                        "account_id": t.get("AccountOriginID"),
                        "transaction_id": t["TransactionID"],
                        "loan_id": t.get("LoanID"),
                        "event_type": "HIGH_VALUE_TXN",
                        "severity": "HIGH",
                        "score_impact": 0.12,
                        "observed_at": ts.isoformat(),
                        "features": {
                            "amount": amt,
                            "threshold_p95": float(cust_p95),
                            "threshold_scope": cust_p95_scope,
                            "raw_amount_ratio": float(amt / cust_p95) if cust_p95 else None,
                            "customer_txn_amount_n": int(len(cust_amounts_sorted)),
                            "transaction_type": txn_types.get(t["TransactionTypeID"])
                        },
                        "rule": {"rule_id": "R_TXN_001_ADAPT", "description": "Transaction amount above 95th percentile (per-customer, with fallback)"},
                        "as_of": as_of_iso,
                        "source": {"derived_from": "dbo.transactions", "pipeline_version": PIPELINE_VERSION}
                    })

    # ------------------------------------------------------------
    # Data-calibrated scoring (unsupervised)
    # ------------------------------------------------------------
    # We do not have ground-truth labels (fraud/default) in this prototype.
    # Instead of hard-coding fixed impacts (e.g., 0.08/0.12), we calibrate
    # impacts using the empirical distribution of "how extreme" each event is.
    #
    # - TXN_VELOCITY_SPIKE: raw = txn_count_last_2h / adaptive_threshold_last_2h  (>=1 means threshold crossed)
    # - HIGH_VALUE_TXN:     raw = amount / threshold_p95  (>=1 means threshold crossed)
    #
    # We map raw extremeness to a bounded score_impact via an empirical CDF per event_type.
    # More extreme events get higher impact, preserving rank ordering for analytics.
    def _ecdf_score(sorted_vals, x):
        if not sorted_vals:
            return 0.0
        # proportion of values <= x
        return bisect.bisect_right(sorted_vals, x) / float(len(sorted_vals))

    # collect raw values per event_type
    raw_by_type = defaultdict(list)
    for e in events:
        et = e.get("event_type")
        feats = e.get("features", {}) or {}
        raw = None
        if et == "TXN_VELOCITY_SPIKE":
            raw = feats.get("raw_velocity_ratio")
        elif et == "HIGH_VALUE_TXN":
            raw = feats.get("raw_amount_ratio")
        if raw is not None:
            raw_by_type[et].append(float(raw))

    sorted_raw = {k: sorted(v) for k, v in raw_by_type.items()}

    # Map to bounded ranges by severity band (tunable but now *data-calibrated* within band)
    # MEDIUM: 0.04–0.10   HIGH: 0.08–0.20
    BAND = {
        "MEDIUM": (0.04, 0.10),
        "HIGH": (0.08, 0.20),
    }

    for e in events:
        et = e.get("event_type")
        sev = e.get("severity")
        lo, hi = BAND.get(sev, (0.05, 0.15))
        feats = e.get("features", {}) or {}
        if et == "TXN_VELOCITY_SPIKE":
            raw = feats.get("raw_velocity_ratio")
        elif et == "HIGH_VALUE_TXN":
            raw = feats.get("raw_amount_ratio")
        else:
            raw = None

        if raw is None or et not in sorted_raw:
            # fallback to mid-band if we cannot compute raw
            e["score_impact"] = round((lo + hi) / 2.0, 6)
            continue

        q = _ecdf_score(sorted_raw[et], float(raw))  # 0..1
        e["score_impact"] = round(lo + (hi - lo) * q, 6)



    filtered = []
    for e in events:
        if (e["severity"] in SEVERITIES_TO_KEEP) or (e.get("score_impact", 0) >= RISK_SCORE_IMPACT_THRESHOLD):
            filtered.append(e)

    return filtered


# -----------------------------
# Load to Mongo
# -----------------------------
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