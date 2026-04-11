# Databricks notebook source #

1. #To mount ext storage into databricks file system(DBFS)

dbutils.fs.mount(
  source = "wasbs://container@storageaccount.blob.core.windows.net",
  mount_point = "/mnt/container",
  extra_configs = {"fs.azure.account.key.storageaccount.blob.core.windows.net":"Pass_access_key"})
  
  or
  
dbutils.fs.mount(
   source="abfs://container@storageaccount.dfs.core.windows.net",
   mount_point="/mnt/container/",
   extra_configs={"fs.azure.account.key.storageaccount.dfs.core.windows.net": "PASS_ACCESS_KEY" }

2. #Expectional case when mounting is disabled, we use below code to directly read data

spark.conf.set(
    "fs.azure.account.key.storageaccount.dfs.core.windows.net",
    "Pass_access_key" )

3. #To read or list out details from storage account

dbutils.fs.ls("abfss://container@storageaccount.dfs.core.windows.net/Bronze")

4. #Creating dataframe for each table for reading and accessing.

df_product = spark.read.format( abfss://container@storageaccount.df.core.windows.net/Bronze/Product'
df_store = spark.read.format( abfss://container@storageaccount.df.core.windows.net/Bronze/Store'
df_transaction = spark.read.format( abfss://container@storageaccount.df.core.windows.net/Bronze/Transaction'
df_customers= spark.read.parquet("abfss://retail@01retailproject.dfs.core.windows.net/Bronze/Customer/sujit0809/azure_data_engg_projects/refs/heads/main")

5. #To see or check the table

display(df_product) 
display(df_store) 
display(df_transaction)
display(df_customers)

6. #To create silver layer- Data Cleaning

from pyspark.sql.functions import col

df_transaction = df_transaction.select(
    col("Transaction_id").cast("int"),
    col("Customer_id").cast("int"),
    col("Product_id").cast("int"),
    col("Store_id").cast("int"),
    col("Quantity_Kg").cast("int"),
    col("Transaction_date").cast("date")
)

df_product = df_product.select(
    col("Product_id").cast("int"),
    col("Product_name"),
    col("Category"),
    col("Price_Kg").cast("double")
)

df_store = df_store.select(
    col("Store_id").cast("int"),
    col("Store_name"),
    col("Location")
)

df_customers = df_customers.select(
    "Customer_id", "First_name", "Last_name", "Email", "City", "Registration_date"
).dropDuplicates(["Customer_id"])

7. #Frame that layer into Silver Dataframe

df_silver = df_transaction \
    .join(df_customers, "Customer_id") \
    .join(df_product, "Product_id") \
    .join(df_store, "Store_id") \
    .withColumn("Total_amount", col("Quantity_Kg") * col("Price_Kg"))

display(df_silver)

8. #Dump/Save that data into Silver storage (save the file)

Silver_path = "abfss://retail@01retailproject.dfs.core.windows.net/Silver"

df_silver.write.mode("overwrite").format("delta").save(Silver_path)

9. #To create table for silver dataset

spark.sql(f"""
CREATE TABLE retail_silver_cleaned
USING DELTA
LOCATION 'abfss://retail@01retailproject.dfs.core.windows.net/Silver'
""")

10. #Read data and load into dataframe (open the file)

silver_df = spark.read.format("delta").load("abfss://retail@01retailproject.dfs.core.windows.net/Silver")

11. #To create gold dataframe for business purpose

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "Transaction_date",
    "Product_id", "Product_name", "Category",
    "Store_id", "Store_name", "Location"
).agg(
    sum("Quantity_Kg").alias("Total_quantity_sold"),
    sum("Total_amount").alias("Total_sales_amount"),
    countDistinct("Transaction_id").alias("Number_of_transactions"),
    avg("Total_amount").alias("Average_transaction_value")
)

display(gold_df)

12. #Dump data into adls gold storage

gold_path = "abfss://retail@01retailproject.dfs.core.windows.net/Gold"

gold_df.write.mode("overwrite").format("delta").save(gold_path)

13. #Create Table for Gold dataset for business purpose and sharing reports

spark.sql("""
CREATE TABLE retail_gold_sales_summary
USING DELTA
LOCATION 'abfss://retail@01retailproject.dfs.core.windows.net/Gold' """)



