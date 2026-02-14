/* ==========================================================
   SQL-3: Handle Missing & Invalid Values (Staging Validation)
   Story: SQL-3 Handle missing & invalid values
   Goal : Apply null-handling + validation rules so staging data
          becomes constraint-safe for later PK/FK enforcement.
   DB   : FinancePolyglotDB
   Schema: stg
   Notes:
     - This script cleans IN PLACE in staging.
     - We avoid heavy deletion; we NULL/standardize where sensible.
     - We DO delete rows that cannot exist relationally (e.g., txn with no origin).
   ========================================================== */

USE FinancePolyglotDB;
GO

BEGIN TRY
    BEGIN TRAN;

    /* ==========================================================
       0) Standardize empty strings -> NULL (common after CSV load)
       ========================================================== */

    -- Customers
    UPDATE stg.customers
    SET FirstName = NULL
    WHERE LTRIM(RTRIM(ISNULL(FirstName, ''))) = '';

    UPDATE stg.customers
    SET LastName = NULL
    WHERE LTRIM(RTRIM(ISNULL(LastName, ''))) = '';

    UPDATE stg.customers
    SET DateOfBirth = NULL
    WHERE LTRIM(RTRIM(ISNULL(DateOfBirth, ''))) = '';

    -- Accounts
    UPDATE stg.accounts
    SET Balance = NULL
    WHERE LTRIM(RTRIM(ISNULL(Balance, ''))) = '';

    UPDATE stg.accounts
    SET OpeningDate = NULL
    WHERE LTRIM(RTRIM(ISNULL(OpeningDate, ''))) = '';

    -- Loans
    UPDATE stg.loans
    SET PrincipalAmount = NULL
    WHERE LTRIM(RTRIM(ISNULL(PrincipalAmount, ''))) = '';

    UPDATE stg.loans
    SET InterestRate = NULL
    WHERE LTRIM(RTRIM(ISNULL(InterestRate, ''))) = '';

    UPDATE stg.loans
    SET StartDate = NULL
    WHERE LTRIM(RTRIM(ISNULL(StartDate, ''))) = '';

    UPDATE stg.loans
    SET EstimatedEndDate = NULL
    WHERE LTRIM(RTRIM(ISNULL(EstimatedEndDate, ''))) = '';

    -- Transactions
    UPDATE stg.transactions
    SET Amount = NULL
    WHERE LTRIM(RTRIM(ISNULL(Amount, ''))) = '';

    UPDATE stg.transactions
    SET TransactionDate = NULL
    WHERE LTRIM(RTRIM(ISNULL(TransactionDate, ''))) = '';

    UPDATE stg.transactions
    SET Description = NULL
    WHERE LTRIM(RTRIM(ISNULL(Description, ''))) = '';


    /* ==========================================================
       1) Numeric validation (set invalid numeric strings -> NULL)
          We keep as VARCHAR in staging, but ensure convertibility.
       ========================================================== */

    -- Accounts.Balance -> DECIMAL(18,2) candidate
    UPDATE stg.accounts
    SET Balance = NULL
    WHERE Balance IS NOT NULL
      AND TRY_CONVERT(DECIMAL(18,2), Balance) IS NULL;

    -- Loans.PrincipalAmount -> DECIMAL(18,2) candidate
    UPDATE stg.loans
    SET PrincipalAmount = NULL
    WHERE PrincipalAmount IS NOT NULL
      AND TRY_CONVERT(DECIMAL(18,2), PrincipalAmount) IS NULL;

    -- Loans.InterestRate -> DECIMAL(10,4) candidate
    UPDATE stg.loans
    SET InterestRate = NULL
    WHERE InterestRate IS NOT NULL
      AND TRY_CONVERT(DECIMAL(10,4), InterestRate) IS NULL;

    -- Transactions.Amount -> DECIMAL(18,2) candidate
    UPDATE stg.transactions
    SET Amount = NULL
    WHERE Amount IS NOT NULL
      AND TRY_CONVERT(DECIMAL(18,2), Amount) IS NULL;


    /* ==========================================================
       2) Business sanity rules (NULL out impossible values)
          Keep rules conservative (avoid deleting unless necessary).
       ========================================================== */

    -- Negative balances may exist in real life (overdraft), so we DON'T null them.
    -- But negative transaction amounts typically indicate sign conventions;
    -- for simplicity in this project, treat negatives as invalid.
    UPDATE stg.transactions
    SET Amount = NULL
    WHERE Amount IS NOT NULL
      AND TRY_CONVERT(DECIMAL(18,2), Amount) < 0;

    -- Loan principal must be > 0
    UPDATE stg.loans
    SET PrincipalAmount = NULL
    WHERE PrincipalAmount IS NOT NULL
      AND TRY_CONVERT(DECIMAL(18,2), PrincipalAmount) <= 0;

    -- Interest rate must be >= 0 (upper bound varies; we won't enforce max here)
    UPDATE stg.loans
    SET InterestRate = NULL
    WHERE InterestRate IS NOT NULL
      AND TRY_CONVERT(DECIMAL(10,4), InterestRate) < 0;


    /* ==========================================================
       3) Date validation (set invalid date strings -> NULL)
          Ensures later CAST/CONVERT into typed OLTP tables succeeds.
       ========================================================== */

    -- Customers.DateOfBirth -> DATE (if present)
    UPDATE stg.customers
    SET DateOfBirth = NULL
    WHERE DateOfBirth IS NOT NULL
      AND TRY_CONVERT(DATE, DateOfBirth) IS NULL;

    -- Accounts.OpeningDate -> DATE (if present)
    UPDATE stg.accounts
    SET OpeningDate = NULL
    WHERE OpeningDate IS NOT NULL
      AND TRY_CONVERT(DATE, OpeningDate) IS NULL;

    -- Loans.StartDate -> DATE (if present)
    UPDATE stg.loans
    SET StartDate = NULL
    WHERE StartDate IS NOT NULL
      AND TRY_CONVERT(DATE, StartDate) IS NULL;

    -- Loans.EstimatedEndDate -> DATE (if present)
    UPDATE stg.loans
    SET EstimatedEndDate = NULL
    WHERE EstimatedEndDate IS NOT NULL
      AND TRY_CONVERT(DATE, EstimatedEndDate) IS NULL;

    -- Transactions.TransactionDate -> DATETIME (if present)
    UPDATE stg.transactions
    SET TransactionDate = NULL
    WHERE TransactionDate IS NOT NULL
      AND TRY_CONVERT(DATETIME, TransactionDate) IS NULL;


    /* ==========================================================
       4) Foreign key (FK) validation
          Any FK pointing to a non-existing parent is set to NULL.
          This prevents FK creation failures later.
       ========================================================== */

    /* ---- Accounts -> Customers ---- */
    UPDATE a
    SET a.CustomerID = NULL
    FROM stg.accounts a
    LEFT JOIN stg.customers c
           ON a.CustomerID = c.CustomerID
    WHERE a.CustomerID IS NOT NULL
      AND c.CustomerID IS NULL;

    /* ---- Customers -> Addresses (optional) ---- */
    UPDATE c
    SET c.AddressID = NULL
    FROM stg.customers c
    LEFT JOIN stg.addresses a
           ON c.AddressID = a.AddressID
    WHERE c.AddressID IS NOT NULL
      AND a.AddressID IS NULL;

    /* ---- Customers -> CustomerTypes (mandatory later) ---- */
    UPDATE c
    SET c.CustomerTypeID = NULL
    FROM stg.customers c
    LEFT JOIN stg.customer_types ct
           ON c.CustomerTypeID = ct.CustomerTypeID
    WHERE c.CustomerTypeID IS NOT NULL
      AND ct.CustomerTypeID IS NULL;

    /* ---- Accounts -> AccountTypes / AccountStatuses (mandatory later) ---- */
    UPDATE a
    SET a.AccountTypeID = NULL
    FROM stg.accounts a
    LEFT JOIN stg.account_types at
           ON a.AccountTypeID = at.AccountTypeID
    WHERE a.AccountTypeID IS NOT NULL
      AND at.AccountTypeID IS NULL;

    UPDATE a
    SET a.AccountStatusID = NULL
    FROM stg.accounts a
    LEFT JOIN stg.account_statuses s
           ON a.AccountStatusID = s.AccountStatusID
    WHERE a.AccountStatusID IS NOT NULL
      AND s.AccountStatusID IS NULL;

    /* ---- Loans -> Accounts (role-based) ---- */
    UPDATE l
    SET l.DisbursementAccountID = NULL
    FROM stg.loans l
    LEFT JOIN stg.accounts a
           ON l.DisbursementAccountID = a.AccountID
    WHERE l.DisbursementAccountID IS NOT NULL
      AND a.AccountID IS NULL;

    UPDATE l
    SET l.RepaymentAccountID = NULL
    FROM stg.loans l
    LEFT JOIN stg.accounts a
           ON l.RepaymentAccountID = a.AccountID
    WHERE l.RepaymentAccountID IS NOT NULL
      AND a.AccountID IS NULL;

    /* ---- Loans -> LoanStatuses (mandatory later) ---- */
    UPDATE l
    SET l.LoanStatusID = NULL
    FROM stg.loans l
    LEFT JOIN stg.loan_statuses ls
           ON l.LoanStatusID = ls.LoanStatusID
    WHERE l.LoanStatusID IS NOT NULL
      AND ls.LoanStatusID IS NULL;

    /* ---- Transactions -> Accounts ---- */
    UPDATE t
    SET t.AccountOriginID = NULL
    FROM stg.transactions t
    LEFT JOIN stg.accounts a
           ON t.AccountOriginID = a.AccountID
    WHERE t.AccountOriginID IS NOT NULL
      AND a.AccountID IS NULL;

    UPDATE t
    SET t.AccountDestinationID = NULL
    FROM stg.transactions t
    LEFT JOIN stg.accounts a
           ON t.AccountDestinationID = a.AccountID
    WHERE t.AccountDestinationID IS NOT NULL
      AND a.AccountID IS NULL;

    /* ---- Transactions -> TransactionTypes (mandatory later) ---- */
    UPDATE t
    SET t.TransactionTypeID = NULL
    FROM stg.transactions t
    LEFT JOIN stg.transaction_types tt
           ON t.TransactionTypeID = tt.TransactionTypeID
    WHERE t.TransactionTypeID IS NOT NULL
      AND tt.TransactionTypeID IS NULL;

    /* ---- Transactions -> Branches (mandatory later) ---- */
    UPDATE t
    SET t.BranchID = NULL
    FROM stg.transactions t
    LEFT JOIN stg.branches b
           ON t.BranchID = b.BranchID
    WHERE t.BranchID IS NOT NULL
      AND b.BranchID IS NULL;

    /* ---- Branches -> Addresses (optional) ---- */
    UPDATE b
    SET b.AddressID = NULL
    FROM stg.branches b
    LEFT JOIN stg.addresses a
           ON b.AddressID = a.AddressID
    WHERE b.AddressID IS NOT NULL
      AND a.AddressID IS NULL;

    /* ---- Transactions -> Loans (optional) ---- */
    UPDATE t
    SET t.LoanID = NULL
    FROM stg.transactions t
    LEFT JOIN stg.loans l
           ON t.LoanID = l.LoanID
    WHERE t.LoanID IS NOT NULL
      AND l.LoanID IS NULL;


    /* ==========================================================
       5) Minimal deletions (rows that cannot exist relationally)
          These are required to avoid failures later.
       ========================================================== */

    -- Accounts must have a valid CustomerID for final OLTP
    DELETE FROM stg.accounts
    WHERE CustomerID IS NULL;

    -- Loans: at least ONE of the role-based account IDs must exist
    DELETE FROM stg.loans
    WHERE DisbursementAccountID IS NULL
      AND RepaymentAccountID IS NULL;

    -- Transactions must have an origin account (core event requirement)
    DELETE FROM stg.transactions
    WHERE AccountOriginID IS NULL;

    -- Transactions must have a transaction type for OLTP
    DELETE FROM stg.transactions
    WHERE TransactionTypeID IS NULL;

    -- Transactions must have a branch for OLTP
    DELETE FROM stg.transactions
    WHERE BranchID IS NULL;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;

    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrLine INT = ERROR_LINE();
    DECLARE @ErrProc NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), 'N/A');

    RAISERROR('SQL-3 Validation/Clean failed. Proc: %s | Line: %d | Msg: %s',
              16, 1, @ErrProc, @ErrLine, @ErrMsg);
END CATCH;
GO
