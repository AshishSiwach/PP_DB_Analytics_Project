/* ==========================================================
   SQL-4: Create Final OLTP Schema with Constraints
   Goal: Enforce relational integrity using PK/FK constraints
   Source: Clean staging tables (stg.*)
   Target: Production tables (dbo.*)
   ========================================================== */

USE FinancePolyglotDB;
GO

BEGIN TRY
    BEGIN TRAN;

    /* ==========================================================
       1) DROP EXISTING TABLES (dependency order)
       ========================================================== */

    DROP TABLE IF EXISTS dbo.transactions;
    DROP TABLE IF EXISTS dbo.loans;
    DROP TABLE IF EXISTS dbo.accounts;
    DROP TABLE IF EXISTS dbo.customers;
    DROP TABLE IF EXISTS dbo.branches;

    DROP TABLE IF EXISTS dbo.account_statuses;
    DROP TABLE IF EXISTS dbo.account_types;
    DROP TABLE IF EXISTS dbo.customer_types;
    DROP TABLE IF EXISTS dbo.loan_statuses;
    DROP TABLE IF EXISTS dbo.transaction_types;
    DROP TABLE IF EXISTS dbo.addresses;


    /* ==========================================================
       2) CREATE REFERENCE TABLES
       ========================================================== */

    CREATE TABLE dbo.customer_types(
        CustomerTypeID INT PRIMARY KEY,
        TypeName VARCHAR(100) NOT NULL
    );

    CREATE TABLE dbo.account_types(
        AccountTypeID INT PRIMARY KEY,
        TypeName VARCHAR(100) NOT NULL
    );

    CREATE TABLE dbo.account_statuses(
        AccountStatusID INT PRIMARY KEY,
        StatusName VARCHAR(100) NOT NULL
    );

    CREATE TABLE dbo.loan_statuses(
        LoanStatusID INT PRIMARY KEY,
        StatusName VARCHAR(100) NOT NULL
    );

    CREATE TABLE dbo.transaction_types(
        TransactionTypeID INT PRIMARY KEY,
        TypeName VARCHAR(100) NOT NULL
    );

    CREATE TABLE dbo.addresses(
        AddressID INT PRIMARY KEY,
        Street VARCHAR(200),
        City VARCHAR(100),
        Country VARCHAR(100)
    );

    CREATE TABLE dbo.branches(
        BranchID INT PRIMARY KEY,
        BranchName VARCHAR(150) NOT NULL,
        AddressID INT NULL,
        FOREIGN KEY (AddressID) REFERENCES dbo.addresses(AddressID)
    );


    /* ==========================================================
       3) CREATE CORE TABLES
       ========================================================== */

    CREATE TABLE dbo.customers(
        CustomerID INT PRIMARY KEY,
        FirstName VARCHAR(100),
        LastName VARCHAR(100),
        DateOfBirth DATE NULL,
        AddressID INT NULL,
        CustomerTypeID INT NOT NULL,
        FOREIGN KEY (AddressID) REFERENCES dbo.addresses(AddressID),
        FOREIGN KEY (CustomerTypeID) REFERENCES dbo.customer_types(CustomerTypeID)
    );

    CREATE TABLE dbo.accounts(
        AccountID INT PRIMARY KEY,
        CustomerID INT NOT NULL,
        AccountTypeID INT NOT NULL,
        AccountStatusID INT NOT NULL,
        Balance DECIMAL(18,2) NULL,
        OpeningDate DATE NULL,
        FOREIGN KEY (CustomerID) REFERENCES dbo.customers(CustomerID),
        FOREIGN KEY (AccountTypeID) REFERENCES dbo.account_types(AccountTypeID),
        FOREIGN KEY (AccountStatusID) REFERENCES dbo.account_statuses(AccountStatusID)
    );

    CREATE TABLE dbo.loans(
        LoanID INT PRIMARY KEY,
        DisbursementAccountID INT NOT NULL,
        RepaymentAccountID INT NOT NULL,
        LoanStatusID INT NOT NULL,
        PrincipalAmount DECIMAL(18,2) NULL,
        InterestRate DECIMAL(10,4) NULL,
        StartDate DATE NULL,
        EstimatedEndDate DATE NULL,
        FOREIGN KEY (DisbursementAccountID) REFERENCES dbo.accounts(AccountID),
        FOREIGN KEY (RepaymentAccountID) REFERENCES dbo.accounts(AccountID),
        FOREIGN KEY (LoanStatusID) REFERENCES dbo.loan_statuses(LoanStatusID)
    );

    CREATE TABLE dbo.transactions(
        TransactionID INT PRIMARY KEY,
        AccountOriginID INT NOT NULL,
        AccountDestinationID INT NULL,
        TransactionTypeID INT NOT NULL,
        LoanID INT NULL,
        Amount DECIMAL(18,2) NULL,
        TransactionDate DATETIME NULL,
        BranchID INT NOT NULL,
        Description VARCHAR(500),
        FOREIGN KEY (AccountOriginID) REFERENCES dbo.accounts(AccountID),
        FOREIGN KEY (AccountDestinationID) REFERENCES dbo.accounts(AccountID),
        FOREIGN KEY (TransactionTypeID) REFERENCES dbo.transaction_types(TransactionTypeID),
        FOREIGN KEY (LoanID) REFERENCES dbo.loans(LoanID),
        FOREIGN KEY (BranchID) REFERENCES dbo.branches(BranchID)
    );


    /* ==========================================================
       4) LOAD DATA FROM STAGING
       ========================================================== */

    INSERT INTO dbo.customer_types SELECT * FROM stg.customer_types;
    INSERT INTO dbo.account_types SELECT * FROM stg.account_types;
    INSERT INTO dbo.account_statuses SELECT * FROM stg.account_statuses;
    INSERT INTO dbo.loan_statuses SELECT * FROM stg.loan_statuses;
    INSERT INTO dbo.transaction_types SELECT * FROM stg.transaction_types;
    INSERT INTO dbo.addresses SELECT * FROM stg.addresses;
    INSERT INTO dbo.branches SELECT * FROM stg.branches;

    INSERT INTO dbo.customers
    SELECT CustomerID, FirstName, LastName,
           TRY_CONVERT(DATE, DateOfBirth),
           AddressID, CustomerTypeID
    FROM stg.customers;

    INSERT INTO dbo.accounts
    SELECT AccountID, CustomerID, AccountTypeID, AccountStatusID,
           TRY_CONVERT(DECIMAL(18,2), Balance),
           TRY_CONVERT(DATE, OpeningDate)
    FROM stg.accounts;

    INSERT INTO dbo.loans
    SELECT LoanID, DisbursementAccountID, RepaymentAccountID, LoanStatusID,
           TRY_CONVERT(DECIMAL(18,2), PrincipalAmount),
           TRY_CONVERT(DECIMAL(10,4), InterestRate),
           TRY_CONVERT(DATE, StartDate),
           TRY_CONVERT(DATE, EstimatedEndDate)
    FROM stg.loans;

    INSERT INTO dbo.transactions
    SELECT TransactionID, AccountOriginID, AccountDestinationID,
           TransactionTypeID, LoanID,
           TRY_CONVERT(DECIMAL(18,2), Amount),
           TRY_CONVERT(DATETIME, TransactionDate),
           BranchID, Description
    FROM stg.transactions;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;

    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('SQL-4 Failed: %s', 16, 1, @ErrMsg);
END CATCH;
GO
