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

You can download the AdventureWorks database [here](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms).

## Data Model Diagram
Below is the data model diagram used in this project:

![Data Model Diagram](https://github.com/vasanthakumar70/AdventureWorksProject/blob/fc8e8a8a10aa391edfbc92ba5c103691459942d8/Data%20Model%20Diagram.svg)


## Technologies Used
- **Data Source:** SQL Server (on-premises)
- **Data Transfer:** Azure Data Factory (self-hosted runtime)
- **Data Storage:** Azure Data Lake Storage
- **Data Transformation:** Azure Databricks
- **Architecture:** Medallion Architecture (Bronze, Silver, Gold)

  
### Process Flow:  
![Process Flow]( https://github.com/vasanthakumar70/AdventureWorksProject/blob/bc705a6ecc5d23ad0babb3430130d9685773d29d/process%20Flow.png)

### Datafactory

![Azure data factory](https://github.com/vasanthakumar70/AdventureWorksProject/blob/edc3a64db1ed4b2c9380052ed049d2fee27e748d/Data%20factory.png)



## Key Features
The project addresses the following analytical questions:

- **Year-Over-Year Sales Comparison:**  
- **Sales Percentage Change:**  
- **High-Value Customers:**  
- **Customer Purchase Timeline:**  
- **Order Fulfillment Analysis:**  
- **Credit Card Usage Trends:**  
- **Special Offers Effectiveness:**  
- **Customer Orders Post-Special Offer:**  



## Additional Resources
- For designing flow diagrams, check out: [Diagrams.net](https://app.diagrams.net/)
- For creating database model diagrams, visit: [DBDiagram.io](https://dbdiagram.io/)


