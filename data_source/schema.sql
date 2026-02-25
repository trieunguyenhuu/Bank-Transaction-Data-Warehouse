/* ==========================================================================
   PROJECT: BANKING DATA WAREHOUSE
   FILE: schema.sql
   DESC: Script tạo cấu trúc cơ sở dữ liệu (Tables & Relationships)
   AUTHOR: Nguyen Huu Trieu
   ========================================================================== */

USE DW_Bank;
GO

-- ==========================================================================
-- PHẦN 1: TẠO CẤU TRÚC NGUỒN (SOURCE / OLTP)
-- Các bảng này sẽ chứa dữ liệu thô (được nạp từ các file .sql rời)
-- ==========================================================================

-- 1.1. Bảng Khách hàng
IF OBJECT_ID('tbl_Customers', 'U') IS NOT NULL DROP TABLE tbl_Customers;
CREATE TABLE tbl_Customers (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    BirthDate DATE,
    Gender NVARCHAR(50),
    City NVARCHAR(50),
    Country NVARCHAR(50)
);

-- 1.2. Bảng Tài khoản
IF OBJECT_ID('tbl_Accounts', 'U') IS NOT NULL DROP TABLE tbl_Accounts;
CREATE TABLE tbl_Accounts (
    AccountID INT PRIMARY KEY,
    CustomerID INT,
    AccountType NVARCHAR(20),
    OpenDate DATETIME,
    FOREIGN KEY (CustomerID) REFERENCES tbl_Customers(CustomerID)
);

-- 1.3. Bảng Giao dịch (Bao gồm cả POS và P2P)
IF OBJECT_ID('tbl_Transactions', 'U') IS NOT NULL DROP TABLE tbl_Transactions;
CREATE TABLE tbl_Transactions (
    TransactionID BIGINT PRIMARY KEY,
    AccountID INT,
    MerchantName NVARCHAR(100),       -- Có giá trị với giao dịch POS
    Amount DECIMAL(18, 2),
    TransactionTimestamp DATETIME,
    TransactionCountry NVARCHAR(100),
    BeneficiaryName NVARCHAR(100),    -- Có giá trị với giao dịch P2P
    TransactionDescription NVARCHAR(255),
    FOREIGN KEY (AccountID) REFERENCES tbl_Accounts(AccountID)
);

PRINT '--- Tao cau truc bang NGUON (OLTP) thanh cong! ---';


-- ==========================================================================
-- PHẦN 2: TẠO CẤU TRÚC KHO DỮ LIỆU (TARGET / OLAP)
-- Mô hình Star Schema
-- ==========================================================================

-- Xóa bảng Fact trước (vì có khóa ngoại)
IF OBJECT_ID('Fact_Spending', 'U') IS NOT NULL DROP TABLE Fact_Spending;

-- Xóa các bảng Dim
IF OBJECT_ID('Dim_Date', 'U') IS NOT NULL DROP TABLE Dim_Date;
IF OBJECT_ID('Dim_Customer', 'U') IS NOT NULL DROP TABLE Dim_Customer;
IF OBJECT_ID('Dim_Account', 'U') IS NOT NULL DROP TABLE Dim_Account;
IF OBJECT_ID('Dim_Merchant', 'U') IS NOT NULL DROP TABLE Dim_Merchant;
IF OBJECT_ID('Dim_Location', 'U') IS NOT NULL DROP TABLE Dim_Location;

-- 2.1. Các Bảng Dimension
CREATE TABLE Dim_Date (
    Date_Key INT PRIMARY KEY,
    Full_Date DATE,
    Day_Of_Week INT,
    Day_Name NVARCHAR(20),
    Is_Weekend BIT,
    Month INT,
    Month_Name NVARCHAR(20),
    Quarter INT,
    Year INT,
    Hour_Of_Day INT
);

CREATE TABLE Dim_Customer (
    Customer_Key INT PRIMARY KEY IDENTITY(1,1),
    CustomerID_Source INT,
    CustomerName NVARCHAR(101),
    Age_Group NVARCHAR(20),
    Gender NVARCHAR(50),
    City NVARCHAR(50),
    Country NVARCHAR(50),
    BirthDate DATE -- Dùng cho Data Mining
);

CREATE TABLE Dim_Account (
    Account_Key INT PRIMARY KEY IDENTITY(1,1),
    AccountID_Source INT,
    Account_Type NVARCHAR(20),
    Account_Open_Date DATETIME -- Dùng DATETIME để tính Tenure chính xác
);

CREATE TABLE Dim_Merchant (
    Merchant_Key INT PRIMARY KEY IDENTITY(1,1),
    MerchantName_Source NVARCHAR(100),
    Category NVARCHAR(50)
);

CREATE TABLE Dim_Location (
    Location_Key INT PRIMARY KEY IDENTITY(1,1),
    Transaction_Country NVARCHAR(100),
    Transaction_Region NVARCHAR(50)
);

-- 2.2. Bảng Fact
CREATE TABLE Fact_Spending (
    Transaction_Key BIGINT PRIMARY KEY IDENTITY(1,1),
    
    -- Khóa ngoại
    Date_Key INT,
    Customer_Key INT,
    Account_Key INT,
    Merchant_Key INT,
    Location_Key INT,
    
    -- Measures
    Amount_Spent DECIMAL(18, 2),
    Transaction_Count INT,

    -- Ràng buộc
    CONSTRAINT FK_Fact_Date FOREIGN KEY (Date_Key) REFERENCES Dim_Date(Date_Key),
    CONSTRAINT FK_Fact_Customer FOREIGN KEY (Customer_Key) REFERENCES Dim_Customer(Customer_Key),
    CONSTRAINT FK_Fact_Account FOREIGN KEY (Account_Key) REFERENCES Dim_Account(Account_Key),
    CONSTRAINT FK_Fact_Merchant FOREIGN KEY (Merchant_Key) REFERENCES Dim_Merchant(Merchant_Key),
    CONSTRAINT FK_Fact_Location FOREIGN KEY (Location_Key) REFERENCES Dim_Location(Location_Key)
);

PRINT '--- Tao cau truc KHO DU LIEU (Star Schema) thanh cong! ---';