# Databricks notebook source
# MAGIC %md
# MAGIC Create Folder

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/Project1')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/Project1/Source')

# COMMAND ----------

# MAGIC %md
# MAGIC Import libraries

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Load the CSV files

# COMMAND ----------

orders_data = spark.read.csv("/FileStore/tables/Project/Source/Orders.csv", header=True, inferSchema=True)
return_items_data = spark.read.csv("/FileStore/tables/Project/Source/Returns.csv", header=True, inferSchema=True)
manager_data = spark.read.csv("/FileStore/tables/Project/Source/Users.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean the data

# COMMAND ----------

# MAGIC %md
# MAGIC Check and remove null values

# COMMAND ----------

orders_data.toPandas()

# COMMAND ----------

columns_to_cast = {
  "Order ID": IntegerType(),
  "Sales": DoubleType(),
  "Profit": DoubleType(),
  "Postal Code": IntegerType()
}
for col_name, col_type in columns_to_cast.items():
  orders_data = orders_data.withColumn(col_name, orders_data[col_name].cast(col_type))

# COMMAND ----------

orders_data = orders_data.withColumn("Order Date", to_date("Order Date", "MM/dd/yyyy"))
orders_data = orders_data.withColumn("Ship Date", to_date("Ship Date", "MM/dd/yyyy"))

# COMMAND ----------

orders_data.printSchema()

# COMMAND ----------

return_items_data.toPandas()

# COMMAND ----------

return_items_data.printSchema()

# COMMAND ----------

manager_data.toPandas()

# COMMAND ----------

manager_data.printSchema()

# COMMAND ----------

for i in orders_data.columns:
    print(f"{i}: {orders_data.filter(col(i).isNull()).count()}")

# COMMAND ----------

for i in return_items_data.columns:
    print(f"{i}: {return_items_data.filter(col(i).isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check and remove duplicates

# COMMAND ----------

duplicate_count = orders_data.count() - orders_data.dropDuplicates().count()
print(f"Total Duplicates: {duplicate_count}")

# COMMAND ----------

duplicate_count = return_items_data.count() - return_items_data.dropDuplicates().count()
print(f"Total Duplicates: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Data Analysis

# COMMAND ----------

orders_data1 = orders_data.withColumn("Order Month", date_format("Order Date", "MMMM"))
orders_data1 = orders_data1.dropna()
display(orders_data1)

# COMMAND ----------

# MAGIC %md
# MAGIC What is the total revenue generated?

# COMMAND ----------

total_revenue = orders_data1.agg(round(sum("Sales"),2).alias("Total Sales")).collect()[0]["Total Sales"]
print(f"Total Revenue Generated = ${total_revenue}")

# COMMAND ----------

#Total revenue by product category
revenue_by_pro_category = orders_data1.groupBy("Product Category").agg(round(sum("Sales"),2).alias("Total Revenue"))
display(revenue_by_pro_category)


# COMMAND ----------

#Total revenue by product sub category
revenue_by_pro_sub_category = orders_data1.groupBy("Product Sub-Category").agg(round(sum("Sales"),2).alias("Total Sales")).orderBy("Total Sales", ascending=False)
display(revenue_by_pro_sub_category)

# COMMAND ----------

#Customer Segment with most sales
sales_by_customer_seg = orders_data1.groupBy("Customer Segment").agg(round(sum("Sales"),2).alias("Total Sales")).orderBy("Total Sales", ascending=False)
display(sales_by_customer_seg)

# COMMAND ----------

# MAGIC %md
# MAGIC Sales by Manager and Region

# COMMAND ----------

#Join the Orders table and Users table
joined_df = orders_data1.join(manager_data, on="Region", how="inner")
sales_by_manager = joined_df.groupBy("Manager","Region").agg(round(sum("Sales"),2).alias("Total Sales")).orderBy("Total Sales", ascending=False)
display(sales_by_manager)

# COMMAND ----------

#How many orders were returned?
joined_df_return= orders_data1.join(return_items_data, on="Order ID", how="outer")
orders_returned= joined_df_return.filter(joined_df_return["Status"] == "Returned").count()
total_orders= orders_data1.count()
print(f"Total number of orders: {total_orders} and Total number of orders returned: {orders_returned}")

# COMMAND ----------

#Average Profit Margin
total_profit= orders_data1.agg(round(sum("Profit"),2).alias("Total Profit")).collect()[0]["Total Profit"]
print(f"Total profit: {total_profit}") 
avg_profit_margin = (total_profit/total_revenue)*100
print(f"Average Profit Margin: {avg_profit_margin:.2f}%")


# COMMAND ----------

# MAGIC %md
# MAGIC Sales Trend Over The Year

# COMMAND ----------

sales_over_year= orders_data1.groupBy("Order Month","Month Order").agg(round(sum("Sales"),2).alias("Total Sales")).select("Order Month", "Total Sales").orderBy("Month Order")
display(sales_over_year)

