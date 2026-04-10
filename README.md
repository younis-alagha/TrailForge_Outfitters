# TrailForge Outfitters — Retail Intelligence Data Warehouse

## Overview

This project builds a complete retail data warehouse for **TrailForge Outfitters** using SQL Server.  
The goal is to transform raw CRM and ERP CSV data into structured, analytics-ready tables that support reporting, dashboarding, and business decision-making.

The solution follows a Medallion Architecture (Bronze → Silver → Gold) and is designed to support Power BI dashboards focused on revenue drivers, profitability, product performance, and customer value.

---

# Architecture

The warehouse follows a layered architecture:

Source → Bronze → Silver → Gold → Power BI

Each layer has a specific responsibility:

- Bronze → raw data ingestion  
- Silver → data cleaning and standardization  
- Gold → star schema for analytics  
- Power BI → dashboards and insights  

This structure separates ingestion, transformation, and reporting logic.

---

# Data Sources

The TrailForge Outfitters warehouse integrates six datasets.

## CRM Data
- Customers  
- Products  
- Sales  

## ERP Data
- Customer demographics  
- Location  
- Product categories  

All datasets are provided as CSV files and loaded into SQL Server.

---

# Database Setup

The SQL Server database is organized into three schemas:

bronze → raw data  
silver → cleaned data  
gold → analytics-ready model  

This structure keeps ingestion, transformation, and reporting separated.

---

# Bronze Layer (Raw Data)

The Bronze layer stores raw source data exactly as received.

Characteristics:

- Tables match source files exactly  
- Data loaded using BULK INSERT  
- No transformations applied  
- Tables truncated before each load  

This layer acts as a raw landing zone and preserves source data integrity.

Tables include:

- bronze.crm_cust_info  
- bronze.crm_prd_info  
- bronze.crm_sales_details  
- bronze.erp_cust_az12  
- bronze.erp_loc_a101  
- bronze.erp_px_cat_g1v2  

---

# Silver Layer (Data Cleaning)

The Silver layer transforms raw data into clean and standardized tables.

Cleaning logic includes:

- Removing duplicate records  
- Handling missing values  
- Fixing inconsistent formats  
- Standardizing text values  
- Applying business rules  
- Validating relationships  

This layer prepares data for analytics and reporting.

Tables include:

- silver.crm_cust_info  
- silver.crm_prd_info  
- silver.crm_sales_details  
- silver.erp_cust_az12  
- silver.erp_loc_a101  
- silver.erp_px_cat_g1v2  

---

# Gold Layer (Analytics)

The Gold layer organizes data into a dimensional model designed for reporting.

Star Schema:

Fact Table  
- fact_sales  

Dimension Tables  
- dim_customers  
- dim_products  
- dim_date  

This model allows analysis by:

- product  
- customer  
- category  
- location  
- date  

The Gold layer is built using views for flexibility and performance.

---

# ETL Process

The ETL pipeline runs in three stages:

Step 1 — Load Bronze  
Raw CSV files are loaded into Bronze tables using BULK INSERT.

Step 2 — Transform Silver  
Data is cleaned, standardized, and validated.

Step 3 — Build Gold  
Dimension and fact views are created for analytics.

Stored procedures manage the pipeline execution.

---

# Data Validation

Data quality checks are included throughout the pipeline:

- Row count validation  
- Duplicate detection  
- Missing value checks  
- Data consistency checks  
- Relationship validation  
- Standardized category validation  

These checks ensure reliable reporting.

---

# Data Model

The dimensional model is designed as a star schema.

fact_sales  
Contains transaction-level sales data

dim_products  
Product attributes and category hierarchy

dim_customers  
Customer demographics and location

dim_date  
Calendar dimension for time-based analysis

This structure supports fast Power BI queries.

---

# Analytics Capabilities

The model enables analysis such as:

Sales Trends  
- revenue over time  
- growth analysis  
- seasonality  

Product Performance  
- top products  
- low margin products  
- revenue vs profit  

Category Performance  
- category contribution  
- profitability by category  
- product mix analysis  

Customer Value  
- repeat customers  
- lifetime value  
- average order value  

---

# Power BI Dashboard

The Gold layer connects directly to Power BI.

Dashboard Pages:

Overview  
- total sales  
- profit  
- margin  
- trend  

Category Performance  
- revenue by category  
- profit by category  
- contribution  

Product Performance  
- top products  
- bottom products  
- margin comparison  

Customer Value  
- top customers  
- repeat customers  
- order frequency  

---

# Final Output

At the end of the pipeline:

- Data is cleaned and standardized  
- Star schema is built  
- Analytics views are created  
- Power BI connects to Gold layer  
- Dashboard provides business insights  

The TrailForge Outfitters Retail Intelligence Dashboard helps answer:

- What drives revenue  
- What drives profit  
- Which products perform best  
- Which categories underperform  
- Who are the most valuable customers  
- Are sales growing or declining  
