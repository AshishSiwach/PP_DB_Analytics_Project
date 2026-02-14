/* ==========================================================
   SQL-5 (Tier 1): Performance Indexing (HIGH-VOLUME ONLY)
   Goal : Index ONLY dbo.transactions for OLTP + Power BI query patterns
   Why  : transactions is the largest table; other tables are small today.
   DB   : FinancePolyglotDB
   ========================================================== */

USE FinancePolyglotDB;
GO

BEGIN TRY
    BEGIN TRAN;

    /* ==========================================================
       1) Drop ONLY transactions indexes (idempotent)
       ========================================================== */
    DROP INDEX IF EXISTS IX_transactions_TransactionDate       ON dbo.transactions;
    DROP INDEX IF EXISTS IX_transactions_OriginAccount_Date    ON dbo.transactions;
    DROP INDEX IF EXISTS IX_transactions_DestinationAccount_Date ON dbo.transactions;
    DROP INDEX IF EXISTS IX_transactions_BranchID_Date         ON dbo.transactions;
    DROP INDEX IF EXISTS IX_transactions_Type_Date             ON dbo.transactions;
    DROP INDEX IF EXISTS IX_transactions_LoanID                ON dbo.transactions;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('SQL-5 Tier 1 (drop) failed: %s', 16, 1, @ErrMsg);
END CATCH;
GO


BEGIN TRY
    BEGIN TRAN;

    /* ==========================================================
       2) Create ONLY transactions indexes (Tier 1)
       Recommended set (practical + not excessive):
         - Date filtering (time windows / trends)
         - Origin account + date (account statement)
         - Destination account + date (incoming transfers)
         - Type + date (category analysis)
         - Branch + date (branch activity)
         - LoanID (optional drilldowns; keep because LoanID exists)
       ========================================================== */

    /* A) Global time filtering (Power BI + "last N days") */
    CREATE NONCLUSTERED INDEX IX_transactions_TransactionDate
    ON dbo.transactions (TransactionDate)
    INCLUDE (Amount, TransactionTypeID, AccountOriginID, AccountDestinationID, BranchID, LoanID);

    /* B) Account activity feed / statements (origin) */
    CREATE NONCLUSTERED INDEX IX_transactions_OriginAccount_Date
    ON dbo.transactions (AccountOriginID, TransactionDate)
    INCLUDE (Amount, TransactionTypeID, AccountDestinationID, BranchID, LoanID);

    /* C) Incoming activity (destination) */
    CREATE NONCLUSTERED INDEX IX_transactions_DestinationAccount_Date
    ON dbo.transactions (AccountDestinationID, TransactionDate)
    INCLUDE (Amount, TransactionTypeID, AccountOriginID, BranchID, LoanID);

    /* D) Category analysis over time (withdrawal vs transfer trends) */
    CREATE NONCLUSTERED INDEX IX_transactions_Type_Date
    ON dbo.transactions (TransactionTypeID, TransactionDate)
    INCLUDE (Amount, AccountOriginID, AccountDestinationID, BranchID, LoanID);

    /* E) Branch activity over time */
    CREATE NONCLUSTERED INDEX IX_transactions_BranchID_Date
    ON dbo.transactions (BranchID, TransactionDate)
    INCLUDE (Amount, TransactionTypeID, AccountOriginID, AccountDestinationID, LoanID);

    /* F) Loan drilldowns (optional, but cheap and useful if LoanID is used) */
    CREATE NONCLUSTERED INDEX IX_transactions_LoanID
    ON dbo.transactions (LoanID)
    INCLUDE (TransactionDate, Amount, TransactionTypeID, AccountOriginID, AccountDestinationID, BranchID);

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('SQL-5 Tier 1 (create) failed: %s', 16, 1, @ErrMsg);
END CATCH;
GO


/* ==========================================================
   Tier 2 (DOCUMENT ONLY): Candidate indexes for growth
   Do NOT run these unless data grows or query plans demand it.
   Keep this section in docs/architecture.md or README.

   -- Accounts (if accounts grows / frequent joins from customers):
   -- CREATE INDEX IX_accounts_CustomerID ON dbo.accounts(CustomerID);

   -- Loans (if large loan portfolio / frequent filtering):
   -- CREATE INDEX IX_loans_Status ON dbo.loans(LoanStatusID);

   -- Customers (if heavy segmentation filters):
   -- CREATE INDEX IX_customers_CustomerTypeID ON dbo.customers(CustomerTypeID);

   ========================================================== */

-- Optional: check created indexes
-- EXEC sp_helpindex 'dbo.transactions';
