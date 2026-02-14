/* ==========================================================
   SQL-2: Implement Deduplication Logic (Staging Layer)
   Goal: Remove duplicate primary keys using window functions
   Target: SQL Server 2016+
   Schema: stg
   ========================================================== */

USE FinancePolyglotDB;
GO

/* --------------------------
   (Optional) Quick duplicate counts BEFORE
   -------------------------- */
-- Uncomment if you want a quick check
-- SELECT 'stg.customers' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT CustomerID) AS distinct_pk FROM stg.customers;
-- SELECT 'stg.accounts'  AS tbl, COUNT(*) AS rows, COUNT(DISTINCT AccountID)  AS distinct_pk FROM stg.accounts;
-- SELECT 'stg.loans'     AS tbl, COUNT(*) AS rows, COUNT(DISTINCT LoanID)     AS distinct_pk FROM stg.loans;
-- SELECT 'stg.transactions' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT TransactionID) AS distinct_pk FROM stg.transactions;


BEGIN TRY
    BEGIN TRAN;

    /* ==========================================================
       1) CORE TABLES
       ========================================================== */

    /* ---- stg.customers (PK: CustomerID) ----
       Keep rule: deterministic stable order (no strong "latest" column in raw)
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CustomerID
                   ORDER BY CustomerID
               ) AS rn
        FROM stg.customers
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ---- stg.accounts (PK: AccountID) ----
       Keep rule: latest OpeningDate if parseable, else keep deterministic order
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY AccountID
                   ORDER BY TRY_CONVERT(date, OpeningDate) DESC, AccountID
               ) AS rn
        FROM stg.accounts
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ---- stg.loans (PK: LoanID) ----
       Keep rule: latest StartDate if parseable, else deterministic
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY LoanID
                   ORDER BY TRY_CONVERT(date, StartDate) DESC, LoanID
               ) AS rn
        FROM stg.loans
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ---- stg.transactions (PK: TransactionID) ----
       Keep rule: latest TransactionDate if parseable, else deterministic
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY TransactionID
                   ORDER BY TRY_CONVERT(datetime, TransactionDate) DESC, TransactionID
               ) AS rn
        FROM stg.transactions
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ==========================================================
       2) LOOKUP / REFERENCE TABLES
       (Keep deterministic; optionally prefer non-null names)
       ========================================================== */

    /* ---- stg.customer_types (PK: CustomerTypeID) ---- */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CustomerTypeID
                   ORDER BY CASE WHEN TypeName IS NULL OR LTRIM(RTRIM(TypeName)) = '' THEN 1 ELSE 0 END,
                            CustomerTypeID
               ) AS rn
        FROM stg.customer_types
    )
    DELETE FROM cte
    WHERE rn > 1;

    /* ---- stg.account_types (PK: AccountTypeID) ---- */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY AccountTypeID
                   ORDER BY CASE WHEN TypeName IS NULL OR LTRIM(RTRIM(TypeName)) = '' THEN 1 ELSE 0 END,
                            AccountTypeID
               ) AS rn
        FROM stg.account_types
    )
    DELETE FROM cte
    WHERE rn > 1;

    /* ---- stg.account_statuses (PK: AccountStatusID) ---- */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY AccountStatusID
                   ORDER BY CASE WHEN StatusName IS NULL OR LTRIM(RTRIM(StatusName)) = '' THEN 1 ELSE 0 END,
                            AccountStatusID
               ) AS rn
        FROM stg.account_statuses
    )
    DELETE FROM cte
    WHERE rn > 1;

    /* ---- stg.loan_statuses (PK: LoanStatusID) ---- */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY LoanStatusID
                   ORDER BY CASE WHEN StatusName IS NULL OR LTRIM(RTRIM(StatusName)) = '' THEN 1 ELSE 0 END,
                            LoanStatusID
               ) AS rn
        FROM stg.loan_statuses
    )
    DELETE FROM cte
    WHERE rn > 1;

    /* ---- stg.transaction_types (PK: TransactionTypeID) ---- */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY TransactionTypeID
                   ORDER BY CASE WHEN TypeName IS NULL OR LTRIM(RTRIM(TypeName)) = '' THEN 1 ELSE 0 END,
                            TransactionTypeID
               ) AS rn
        FROM stg.transaction_types
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ==========================================================
       3) SUPPORTING TABLES
       ========================================================== */

    /* ---- stg.addresses (PK: AddressID) ----
       Keep rule: prefer more complete address (non-null street/city/country)
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY AddressID
                   ORDER BY
                       (CASE WHEN Street  IS NULL OR LTRIM(RTRIM(Street))  = '' THEN 0 ELSE 1 END +
                        CASE WHEN City    IS NULL OR LTRIM(RTRIM(City))    = '' THEN 0 ELSE 1 END +
                        CASE WHEN Country IS NULL OR LTRIM(RTRIM(Country)) = '' THEN 0 ELSE 1 END) DESC,
                       AddressID
               ) AS rn
        FROM stg.addresses
    )
    DELETE FROM cte
    WHERE rn > 1;


    /* ---- stg.branches (PK: BranchID) ----
       Keep rule: prefer non-null branch name
    */
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY BranchID
                   ORDER BY CASE WHEN BranchName IS NULL OR LTRIM(RTRIM(BranchName)) = '' THEN 1 ELSE 0 END,
                            BranchID
               ) AS rn
        FROM stg.branches
    )
    DELETE FROM cte
    WHERE rn > 1;


    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;

    -- Bubble up the error with details
    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrLine INT = ERROR_LINE();
    DECLARE @ErrProc NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), 'N/A');

    RAISERROR('SQL-2 Dedup failed. Proc: %s | Line: %d | Msg: %s', 16, 1, @ErrProc, @ErrLine, @ErrMsg);
END CATCH;
GO


/* --------------------------
   (Optional) Quick duplicate counts AFTER
   -------------------------- */
-- SELECT 'stg.customers' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT CustomerID) AS distinct_pk FROM stg.customers;
-- SELECT 'stg.accounts'  AS tbl, COUNT(*) AS rows, COUNT(DISTINCT AccountID)  AS distinct_pk FROM stg.accounts;
-- SELECT 'stg.loans'     AS tbl, COUNT(*) AS rows, COUNT(DISTINCT LoanID)     AS distinct_pk FROM stg.loans;
-- SELECT 'stg.transactions' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT TransactionID) AS distinct_pk FROM stg.transactions;
