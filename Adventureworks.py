# Databricks notebook source
# MAGIC %md
# MAGIC Mount ADLS

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "",
  "fs.azure.account.oauth2.client.secret": "",
  "fs.azure.account.oauth2.client.endpoint": ""
}

dbutils.fs.mount(
  source = "abfss://adventureworks@practiceblobstorage.dfs.core.windows.net",
  mount_point = "/mnt/adventureworks",
  extra_configs = configs
)


# COMMAND ----------

# MAGIC %md
# MAGIC Import funtction and types

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format,col,to_date,month,year,concat,substring,lit,sum,round,count,countDistinct,lag,lead,coalesce,datediff,min,max,avg,ceil
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC list of tables

# COMMAND ----------

tablenames=[]
for i in dbutils.fs.ls("dbfs:/mnt/adventureworks/Bronze/"):
    tablenames.append(i.name.split('/')[-1])
tablenames


# COMMAND ----------

# MAGIC %md
# MAGIC table headers

# COMMAND ----------


sales_tables_columns = {
    "SalesOrderHeader": [
        "SalesOrderID", "RevisionNumber", "OrderDate", "DueDate", "ShipDate", "Status", "OnlineOrderFlag", 
        "SalesOrderNumber", "PurchaseOrderNumber", "AccountNumber", "CustomerID", "SalesPersonID", 
        "TerritoryID", "BillToAddressID", "ShipToAddressID", "ShipMethodID", "CreditCardID", 
        "CreditCardApprovalCode", "CurrencyRateID", "SubTotal", "TaxAmt", "Freight", "TotalDue", 
        "Comment", "rowguid", "ModifiedDate"
    ],
    "SalesOrderDetail": [
        "SalesOrderID", "SalesOrderDetailID", "CarrierTrackingNumber", "OrderQty", "ProductID", 
        "SpecialOfferID", "UnitPrice", "UnitPriceDiscount", "LineTotal", "rowguid", "ModifiedDate"
    ],
    "Customer": [
        "CustomerID", "PersonID", "StoreID", "TerritoryID", "AccountNumber", "rowguid", "ModifiedDate"
    ],
    "SalesPerson": [
        "SalesPersonID", "TerritoryID", "SalesQuota", "Bonus", "CommissionPct", "SalesYTD", 
        "SalesLastYear", "rowguid", "ModifiedDate"
    ],
    "SpecialOfferProduct": [
        "SpecialOfferID", "ProductID", "rowguid", "ModifiedDate"
    ],
    "Store": [
        "BusinessEntityID", "Name", "SalesPersonID", "Demographics", "rowguid", "ModifiedDate"
    ],
    "SalesTerritory": [
        "TerritoryID", "Name", "CountryRegionCode", "Group", "SalesYTD", "SalesLastYear", 
        "CostYTD", "CostLastYear", "rowguid", "ModifiedDate"
    ],
    "CreditCard": [
        "CreditCardID", "CardType", "CardNumber", "ExpMonth", "ExpYear", "ModifiedDate"
    ]
}

    

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/adventureworks/"

# COMMAND ----------

# MAGIC %md
# MAGIC Create dataframe

# COMMAND ----------

for table in tablenames:
    columns=sales_tables_columns[table]
    df=spark.read.format("csv").\
        option("header","false").\
            option("inferSchema","true").\
                load("dbfs:/mnt/adventureworks/Bronze/"+table)

    for i, col in enumerate(columns):
        if "Date" in col or "date" in col:
            df = df.withColumnRenamed(df.columns[i], col) \
               .withColumn(col, date_format(from_utc_timestamp(col, "UTC").cast(TimestampType()), "yyyy-MM-dd"))\
                   .withColumn(col,to_date(col,"yyyy-MM-dd"))
               
        else:
        # Just rename the column
            df = df.withColumnRenamed(df.columns[i], col)

    
    locals()[f"{table}_df"]=df

# COMMAND ----------

# MAGIC %md
# MAGIC data validation 
# MAGIC remove dublicates,null values

# COMMAND ----------

Customer_df=Customer_df.dropna(subset=["customerid"])
SalesPerson_df=SalesPerson_df.dropna(subset=["SalesPersonID"])\
    .dropDuplicates(["salesPersonid"])
SalesOrderHeader_df=SalesOrderHeader_df.\
    dropna(subset=["orderdate"])\
        .withColumn("Ordermonth",month("orderdate"))\
            .withColumn("orderYear",year("orderdate"))
SalesOrderDetail_df = SalesOrderDetail_df.filter((col("OrderQty") != 0) | (col("UnitPrice") > 0))



# COMMAND ----------

# MAGIC %md
# MAGIC mask card number

# COMMAND ----------

CreditCard_df=CreditCard_df.withColumn(
    "maskedcardnumber",
    concat(lit("xxxx-xxxx-xxxx-"),substring(col("cardnumber"),-4,4))
    ).drop(col("cardnumber"))

# COMMAND ----------

# MAGIC %md
# MAGIC save transformed file in silver layer

# COMMAND ----------

for i in tablenames:
    df_name=f'{i}_df'
    df=globals()[df_name]
    df.coalesce(1).\
        write.mode('overwrite')\
            .format('parquet')\
                .option('header','true')\
                    .save(f'dbfs:/mnt/adventureworks/Silver/{i}')


# COMMAND ----------

for i in dbutils.fs.ls('dbfs:/mnt/adventureworks/Silver/'):
    filename=i.path.split('/')[-2]
    df=spark.read.format('parquet').load(f'dbfs:/mnt/adventureworks/Silver/{filename}/')
    locals()[f's_{filename}_df']=df

# COMMAND ----------

# MAGIC %md
# MAGIC total sales per month  how do they compare year-over-year

# COMMAND ----------

salesjoined=s_SalesOrderHeader_df.join(
    s_SalesOrderDetail_df,
    s_SalesOrderDetail_df["salesorderid"]==s_SalesOrderHeader_df["salesorderid"],
    "inner"
).drop(s_SalesOrderDetail_df["salesorderid"])
salestotal=salesjoined.withColumn("orderMonth",month("orderdate"))\
    .withColumn("orderyear",year("orderdate"))\
        .withColumn("discountprice",col("unitprice")*col("unitpricediscount"))\
            .groupBy(["orderyear","ordermonth"]).agg(round(sum(col("orderqty")*(col("unitprice")-col("discountprice"))),2).alias("totalSales"))\
                .orderBy(col("orderyear"),col("ordermonth"))
salestotal.show()
        

# COMMAND ----------

# MAGIC %md
# MAGIC sales percentage compared with previous year

# COMMAND ----------

salestotal.alias("c").join(
    salestotal.alias("p"),
    (col("c.ordermonth")==col("p.ordermonth")) & (col("c.orderyear")==col("p.orderyear")+1)
    ,"left" 
)\
    .withColumn("percentageIncrease",round((col("c.totalsales")-col("p.totalsales"))*100/col("p.totalsales"),2))\
        .select([
            col("c.orderyear"),
            col("c.ordermonth"),
            col("c.totalsales").alias("current month sales"),
            col("p.totalsales").alias("previous year sale"),
            col("percentageIncrease")
        ])\
            .orderBy(col("c.orderyear"),col("c.ordermonth")).\
                show()

# COMMAND ----------

# MAGIC %md
# MAGIC High-Value Customers:
# MAGIC Customers who consistently make high-value purchases and contribute significantly to revenue.

# COMMAND ----------

windowspec=Window.orderBy(col("totalsales").desc())

customer_sales=s_Customer_df.alias("c").join(
    salesjoined.alias("s"),
    col("c.customerid")==col("s.customerid"),
    "left"
).drop(col("s.customerid"))
high_value_customers=customer_sales.withColumn("discountprice",col("s.unitprice")*col("s.unitpricediscount"))\
    .groupBy("c.customerid").agg(round(sum(col("orderqty")*(col("unitprice")-col("discountprice"))),2).alias            
          ("totalSales"),
          countDistinct(col("salesorderid")).alias("no od orders"))\
    .orderBy(col("totalsales").desc())

high_value_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC first and last purchase of customer and days in between

# COMMAND ----------

windowspec1=Window.partitionBy("customerid").orderBy(col("orderdate"))
s_Customer_df.join(
    s_SalesOrderHeader_df,
    s_Customer_df["customerid"] == s_SalesOrderHeader_df["customerid"],
    "inner"
).drop(s_SalesOrderHeader_df["customerid"])\
  .select("customerid", "orderdate")\
  .groupBy("customerid").agg(min(col("orderdate").alias("first purchase")),max(col("orderdate").alias("last purchase")))\
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Order Fulfillment Analysis:
# MAGIC
# MAGIC What is the average order fulfillment time, and how does it vary across different products or regions?

# COMMAND ----------

salesjoined\
  .select("productid","TerritoryID","orderdate","shipdate")\
  .withColumn("Diff",datediff(col("shipdate"),col("orderdate")))\
  .groupBy("productid","TerritoryID").agg(ceil(avg("diff")).alias("avg"))\
  .orderBy(ceil(col("avg")).desc())\
  .show()
