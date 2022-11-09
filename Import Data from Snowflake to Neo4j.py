# Databricks notebook source
# MAGIC %sh pwd

# COMMAND ----------

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext

# Set options for how to connect to Snowflake below
sfOptions = {
  "sfURL" : "ut59235.europe-west4.gcp.snowflakecomputing.com",
  "sfUser" : dbutils.secrets.get("auth", "sfUser"),
  "sfPassword" : dbutils.secrets.get("auth", "sfPassword"),
  "sfDatabase" : "snowflake_sample_data",
  "sfSchema" : "tpcds_sf100tcl",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  """
    select C_CUSTOMER_ID, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_BIRTH_COUNTRY 
    from snowflake_sample_data.tpcds_sf100tcl.customer 
    limit 2000;
  """) \
  .load()

df.cache()
df.show()

# COMMAND ----------

customer = df.select('c_customer_id', 'c_salutation', 'c_first_name', 'c_last_name')
countries = df.select('c_birth_country').filter("c_birth_country IS NOT NULL").dropDuplicates()
customer_born_in_country = df.select('c_customer_id', 'c_birth_country').filter("c_birth_country IS NOT NULL")

customer.show();
countries.show();

# Write the customer DF as a set of nodes
result = (customer.write
    .format("org.neo4j.spark.DataSource") 
    .option("url", "neo4j+s://3c5de8ae.databases.neo4j.io")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", dbutils.secrets.get("auth", "neo4jPassword"))
    .mode('overwrite')
    .option("labels", "Customer")
    .option("node.keys", "c_customer_id")
    .option("batch.size", 20000)
    .save())
    
# Write countries as a set of nodes
result = (countries.write
    .format("org.neo4j.spark.DataSource") 
    .option("url", "neo4j+s://3c5de8ae.databases.neo4j.io")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", dbutils.secrets.get("auth", "neo4jPassword"))
    .mode('overwrite')
    .option("labels", "Country")
    .option("node.keys", "c_birth_country:name")
    .option("batch.size", 20000)
    .save())

# Write the relationships that go between countries and customers
result = (customer_born_in_country.repartition(1).write
    .format("org.neo4j.spark.DataSource") 
    .mode('overwrite')
    .option("relationship", "BORN_IN")
    .option("url", "neo4j+s://3c5de8ae.databases.neo4j.io")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", dbutils.secrets.get("auth", "neo4jPassword"))
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", "Customer")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.target.labels", "Country")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "c_customer_id")
    .option("relationship.target.node.keys", "c_birth_country:name")
    .save())

print("All done!")
