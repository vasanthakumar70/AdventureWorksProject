# AdventureWorks Project

## Overview
Welcome to the AdventureWorks Data Engineering Project! This repository showcases a comprehensive data engineering solution that utilizes Azure Data Factory, Azure Data Lake Storage, and Azure Databricks to transform and analyze sales data from the AdventureWorks database. The project employs a medallion architecture (Bronze, Silver, Gold) to ensure data quality and enhance analytics capabilities.

## Project Description
The main goal of this project is to provide insightful analytics on sales data, focusing on several key performance indicators (KPIs) that drive business decisions. The data has been sourced from an on-premises SQL Server, transferred to Azure Data Lake Storage using a self-hosted runtime in Azure Data Factory, and transformed in Azure Databricks.

## Database
The project utilizes the AdventureWorks database, which includes the following tables:

- **CreditCard**
- **Customer**
- **SalesOrderDetail**
- **SalesOrderHeader**
- **SalesPerson**
- **SalesTerritory**
- **SpecialOfferProduct**
- **Store**

## Key Features
The project addresses the following analytical questions:

- **Year-Over-Year Sales Comparison:**  
  Total sales per month and how they compare year-over-year.

- **Sales Percentage Change:**  
  Calculation of sales percentage compared with the previous year.

- **High-Value Customers:**  
  Identification of customers who consistently make high-value purchases and contribute significantly to revenue.

- **Customer Purchase Timeline:**  
  Analysis of the first and last purchase of each customer, including the number of days in between.

- **Order Fulfillment Analysis:**  
  Assessment of average order fulfillment time and its variation across different products or regions.

- **Credit Card Usage Trends:**  
  Examination of how the usage of different credit card types varies across different customer segments and purchase amounts.

- **Special Offers Effectiveness:**  
  Evaluation of the impact of special offers on sales volume and revenue, including an analysis of which offers are most effective, and insight into how special offers influence customer retention and repeat purchases.

- **Customer Orders Post-Special Offer:**  
  Tracking customer orders after their first special offer purchase.

## Data Architecture
The project utilizes a medallion architecture:

- **Bronze Layer:**  
  Raw data ingested from SQL Server into Azure Data Lake Storage.

- **Silver Layer:**  
  Cleansed and transformed data, including deduplication and handling null values.

- **Gold Layer:**  
  Curated data that has undergone advanced transformations and analytics, ready for reporting and insights.

## Technologies Used
- **Data Source:** SQL Server (on-premises)
- **Data Transfer:** Azure Data Factory (self-hosted runtime)
- **Data Storage:** Azure Data Lake Storage
- **Data Transformation:** Azure Databricks
- **Architecture:** Medallion Architecture (Bronze, Silver, Gold)
