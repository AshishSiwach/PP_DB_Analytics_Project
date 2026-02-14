/* =========================
   SQL-1: Setup + Staging Layer (SQL Server 2016+)
   ========================= */

-- Create DB (optional)
IF DB_ID('FinancePolyglotDB') IS NULL
BEGIN
    CREATE DATABASE FinancePolyglotDB;
END
GO

USE FinancePolyglotDB;
GO

-- Create schema for staging
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

/* =========================
   Drop + Create STAGING TABLES
   (no PK/FK constraints here)
   ========================= */

-- Lookups
DROP TABLE IF EXISTS stg.customer_types;
CREATE TABLE stg.customer_types (
    CustomerTypeID      INT,
    TypeName            VARCHAR(100)
);

DROP TABLE IF EXISTS stg.account_types;
CREATE TABLE stg.account_types (
    AccountTypeID       INT,
    TypeName            VARCHAR(100)
);

DROP TABLE IF EXISTS stg.account_statuses;
CREATE TABLE stg.account_statuses (
    AccountStatusID     INT,
    StatusName          VARCHAR(100)
);

DROP TABLE IF EXISTS stg.loan_statuses;
CREATE TABLE stg.loan_statuses (
    LoanStatusID        INT,
    StatusName          VARCHAR(100)
);

DROP TABLE IF EXISTS stg.transaction_types;
CREATE TABLE stg.transaction_types (
    TransactionTypeID   INT,
    TypeName            VARCHAR(100)
);

-- Supporting
DROP TABLE IF EXISTS stg.addresses;
CREATE TABLE stg.addresses (
    AddressID           INT,
    Street              VARCHAR(200),
    City                VARCHAR(100),
    Country             VARCHAR(100)
);

DROP TABLE IF EXISTS stg.branches;
CREATE TABLE stg.branches (
    BranchID            INT,
    BranchName          VARCHAR(150),
    AddressID           INT
);

-- Core
DROP TABLE IF EXISTS stg.customers;
CREATE TABLE stg.customers (
    CustomerID          INT,
    FirstName           VARCHAR(100),
    LastName            VARCHAR(100),
    DateOfBirth         VARCHAR(50),
    AddressID           INT,
    CustomerTypeID      INT
);

DROP TABLE IF EXISTS stg.accounts;
CREATE TABLE stg.accounts (
    AccountID           INT,
    CustomerID          INT,
    AccountTypeID       INT,
    AccountStatusID     INT,
    Balance             VARCHAR(50),
    OpeningDate         VARCHAR(50)
);

-- loans_v2.csv: LoanID, DisbursementAccountID, RepaymentAccountID, LoanStatusID, PrincipalAmount, InterestRate, StartDate, EstimatedEndDate
DROP TABLE IF EXISTS stg.loans;
CREATE TABLE stg.loans (
    LoanID                  INT,
    DisbursementAccountID    INT,
    RepaymentAccountID       INT,
    LoanStatusID             INT,
    PrincipalAmount          VARCHAR(50),
    InterestRate             VARCHAR(50),
    StartDate                VARCHAR(50),
    EstimatedEndDate         VARCHAR(50)
);

-- transactions_v2.csv: includes LoanID nullable
DROP TABLE IF EXISTS stg.transactions;
CREATE TABLE stg.transactions (
    TransactionID            INT,
    AccountOriginID          INT,
    AccountDestinationID     INT,
    TransactionTypeID        INT,
    LoanID                   INT,
    Amount                   VARCHAR(50),
    TransactionDate          VARCHAR(50),
    BranchID                 INT,
    Description              VARCHAR(500)
);
GO
