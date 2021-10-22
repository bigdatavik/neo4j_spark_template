// Databricks notebook source
// MAGIC %md
// MAGIC # Neo4j Spark DataFrames Write and Read

// COMMAND ----------

// MAGIC %md
// MAGIC Let's get some quick practice with Spark DataFrame skills with Neo4j

// COMMAND ----------

// MAGIC %md
// MAGIC #### Start a simple Spark Session
// MAGIC ####### https://defkey.com/databricks-notebook-shortcuts

// COMMAND ----------

// MAGIC %md
// MAGIC #####  Spark Session

// COMMAND ----------

// MAGIC %run /Users/vik.malhotra@neotechnology.com/credentials/password

// COMMAND ----------

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


//val url = "bolt://35.175.142.233:7687"
val url = "neo4j+s://f69451e9.databases.neo4j.io"
val path = "dbfs:/FileStore/northwind/"

val username = "neo4j"
//val password = "engineering-airport-delimiter"
// val password = "WQHYL-T20I_oI24DUi2os-Jcr3Chbn1PFUq_k8Ll2nU"
val password = PASSWORD

val spark = SparkSession.builder()
    .config("neo4j.url", url )
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", username)
    .config("neo4j.authentication.basic.password", password)
    .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating Nodes and Relationships

// COMMAND ----------

// MAGIC %md
// MAGIC ####  Orders

// COMMAND ----------

// File location and type

val file_location = path + "orders.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val orders_df = df.select(col("OrderID0").alias("orderID"),col("ShipName").alias("shipName")).distinct()
//orders_df.count()

// COMMAND ----------

orders_df.show()

// COMMAND ----------

// https://neo4j.com/developer/spark/architecture/#_write_parallelism_in_neo4j

orders_df
       //.repartition(1) //did distinct, use repartition for relationships
      .write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      //.option("url", "bolt://3.86.186.88:7687")
      .option("labels", ":Order")
      .option("node.keys", "orderID")
      //.option("schema.optimization.type", "INDEX")
      .option("schema.optimization.type", "NODE_CONSTRAINTS")
      .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Products

// COMMAND ----------

// File location and type
val file_location = path + "products.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val product_df = df.select(col("ProductID").alias("productID"),col("ProductName").alias("productName"),col("UnitPrice").alias("unitPrice")).distinct()

//product_df.count()

// COMMAND ----------

product_df.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      //.option("url", "bolt://3.86.186.88:7687")
      .option("labels", ":Product")
      .option("node.keys", "productID")
      .option("schema.optimization.type", "INDEX")
      .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Suppliers

// COMMAND ----------

// File location and type
val file_location = path + "suppliers.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val supplier_df = df.select(col("SupplierID").alias("supplierID"),col("CompanyName").alias("companyName")).distinct()
supplier_df.show()

// COMMAND ----------

supplier_df.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
     // .option("url", "bolt://3.86.186.88:7687")
      .option("labels", ":Supplier")
      .option("node.keys", "supplierID")
      .option("schema.optimization.type", "INDEX")
      .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Category

// COMMAND ----------

// File location and type
val file_location = path + "categories.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val category_df = df.select(col("CategoryID").alias("categoryID"),col("CategoryName").alias("categoryName"), col("Description").alias("description")).distinct()
category_df.show()

// COMMAND ----------

category_df.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      //.option("url", "bolt://3.86.186.88:7687")
      .option("labels", ":Category")
      .option("node.keys", "categoryID")
      .option("schema.optimization.type", "INDEX")
      .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Employees

// COMMAND ----------

// File location and type
val file_location = path + "employees_rev.csv"
//val file_location = path + "employees.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  //.option("quote", "\"")
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val employee_df = df.select(col("EmployeeID").alias("employeeID"),col("FirstName").alias("firstName"),col("LastName").alias("lastName"),col("Title").alias("title")).distinct()

// COMMAND ----------

employee_df.show()

// COMMAND ----------

employee_df.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      //.option("url", "bolt://3.86.186.88:7687")
      .option("labels", ":Employee")
      .option("node.keys", "employeeID")
      .option("schema.optimization.type", "INDEX")
      .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Employee REPORTS_TO Employee

// COMMAND ----------

import org.apache.spark.sql.functions.col
val employee_report_to_df = df.select(col("EmployeeID").cast("Integer").alias("employeeID"),col("FirstName").alias("firstName"),col("LastName").alias("lastName"),col("Title").alias("title"), col("ReportsTo").cast("Integer").alias("reportsTo")).filter($"reportsTo".isNotNull).distinct()

// COMMAND ----------

employee_report_to_df.show()

// COMMAND ----------

// With keys
// https://neo4j.com/developer/spark/writing/#strategy-keys

employee_report_to_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
    //.option("url", "bolt://3.86.186.88:7687")
    .mode(SaveMode.Overwrite)
    .option("relationship", "REPORTS_TO")
    //.option("relationship.properties", "unitPrice, quantity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Employee")
    .option("relationship.source.save.mode", "overwrite")
    // .option("relationship.source.node.keys", "employeeID:employeeID") // Can create the rmployee nodes here also, instead of doing it before
    .option("relationship.source.node.properties", "firstName, lastName, title")
    .option("relationship.target.labels", ":Employee")
    .option("relationship.target.node.keys", "reportsTo:employeeID")
   // .option("relationship.target.node.properties", "firstName, lastName, title")
    .option("relationship.target.save.mode", "overwrite")
    .save()

// COMMAND ----------

// Native, use only when the use case is reading from one neo4j db and writing to another instance of neo4j as it requires columns in this format
// https://neo4j.com/developer/spark/writing/#strategy-native
// Caused by: IllegalArgumentException: NATIVE write strategy requires a schema like: rel.[props], source.[props], target.[props]. All of this columns are empty in the current schema.
// employee_report_to_df.write
//     .format("org.neo4j.spark.DataSource")
//     .option("url", "bolt://3.86.186.88:7687")
//     .mode(SaveMode.Overwrite)
//     .option("relationship", "REPORTS_TO")
//     //.option("relationship.properties", "unitPrice, quantity")
//     //.option("relationship.save.strategy", "keys")
//     .option("relationship.source.labels", ":Employee")
//     .option("relationship.source.save.mode", "overwrite")
//     .option("relationship.source.node.keys", "employeeID:employeeID")
//     //.option("relationship.source.node.properties", "shipName")
//     .option("relationship.target.labels", ":Employee")
//     .option("relationship.target.save.mode", "overwrite")
//     .option("relationship.target.node.keys", "reportsTo:employeeID")
//    // .option("relationship.target.node.properties", "instrument_color:color")
//      .save()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Order CONTAINS Products
// MAGIC ##### Creating Order properties also, if not run Orders in the earlier step

// COMMAND ----------

// File location and type
val file_location = path + "orders.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val orders_df = df.select(col("OrderID0").alias("orderID"),col("ShipName").alias("shipName"), col("ProductID").alias("productID"), col("UnitPrice").alias("unitPrice"), col("Quantity").alias("quantity"),
col("EmployeeID").alias("employeeID"))

orders_df.show()

// COMMAND ----------

orders_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
   // .option("url", "bolt://3.86.186.88:7687")
    .mode(SaveMode.Overwrite)
    .option("relationship", "CONTAINS")
    .option("relationship.properties", "unitPrice, quantity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Order")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "orderID:orderID")
    .option("relationship.source.node.properties", "shipName")
    .option("relationship.target.labels", ":Product")
    .option("relationship.target.node.keys", "productID:productID")
   // .option("relationship.target.node.properties", "productID") // all product properties are not there in this file, so cannot create Product node properties here
    .option("relationship.target.save.mode", "overwrite")
    .save()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Employee SOLD Order

// COMMAND ----------

orders_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
    //.option("url", "bolt://3.86.186.88:7687")
.mode(SaveMode.Overwrite)
    .option("relationship", "SOLD")
    //.option("relationship.properties", "unitPrice, quantity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Employee")
    //.option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "employeeID:employeeID")
    //.option("relationship.source.node.properties", "shipName")
    .option("relationship.target.labels", ":Order")
    .option("relationship.target.node.keys", "orderID:orderID")
   // .option("relationship.target.node.properties", "productID") // all product properties are not there in this file, so cannot create Product node properties here
    .option("relationship.target.save.mode", "overwrite")
    .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Product PART_OF Category

// COMMAND ----------

// File location and type
val file_location = path + "products.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
val product_df = df.select(col("ProductID").alias("productID"),col("CategoryID").alias("categoryID"),col("SupplierID").alias("supplierID")).distinct()

// COMMAND ----------

product_df.show()

// COMMAND ----------

product_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
    //.option("url", "bolt://3.86.186.88:7687")
.mode(SaveMode.Overwrite)
    .option("relationship", "PART_OF")
    //.option("relationship.properties", "unitPrice, quantity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Product")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "productID:productID")
    //.option("relationship.source.node.properties", "shipName")
    .option("relationship.target.labels", ":Category")
    .option("relationship.target.node.keys", "categoryID:categoryID")
   // .option("relationship.target.node.properties", "productID") 
    .option("relationship.target.save.mode", "overwrite")
    .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Supplier SUPPLIES Product

// COMMAND ----------

product_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
    //.option("url", "bolt://3.86.186.88:7687")
.mode(SaveMode.Overwrite)
    .option("relationship", "SUPPLIES")
    //.option("relationship.properties", "unitPrice, quantity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Supplier")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "supplierID:supplierID")
    //.option("relationship.source.node.properties", "shipName")
    .option("relationship.target.labels", ":Product")
    .option("relationship.target.node.keys", "productID:productID")
   // .option("relationship.target.node.properties", "productID") 
    .option("relationship.target.save.mode", "overwrite")
    .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Querying the Graph

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Simple Read with Labels

// COMMAND ----------

val df = spark.read.format("org.neo4j.spark.DataSource")
  //.option("url", "bolt://localhost:7687")
  .option("labels", "Order")
  .load()

df.show()

// COMMAND ----------

val df =  spark.read.format("org.neo4j.spark.DataSource")
 // .option("url", "bolt://localhost:7687")
  .option("relationship", "CONTAINS")
  .option("relationship.source.labels", "Order")
  .option("relationship.target.labels", "Product")
  .option("relationship.nodes.map", "false")
  .load()

// display(df)
df.show()

// df.where("`source.OrderID` = 11070 AND `target.productID` = 1").show()

// COMMAND ----------

// https://neo4j.com/developer/spark/reading/#_node_mapping

val df =  spark.read.format("org.neo4j.spark.DataSource")
 // .option("url", "bolt://localhost:7687")
  .option("relationship", "CONTAINS")
  .option("relationship.source.labels", "Order")
  .option("relationship.target.labels", "Product")
  .option("relationship.nodes.map", "true") //  node mapping
  .load()

df.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Which Employee had the Highest Cross-Selling Count of 'Chocolade' and Another Product?

// COMMAND ----------

val query = """
MATCH (choc:Product {productName:'Chocolade'})<-[:CONTAINS]-(:Order)<-[:SOLD]-(employee),(employee)-[:SOLD]->(o2)-[:CONTAINS]->(other:Product)
WITH employee.employeeID as employee, other.productName as otherProduct, count(distinct o2) as count
ORDER BY count DESC LIMIT 5
RETURN employee, otherProduct, count
"""

val result_df = spark.read.format("org.neo4j.spark.DataSource")
  //.option("url", "bolt://localhost:7687")
  .option("query", query)
  .load()

result_df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Which Employees Report to Each Other Indirectly?

// COMMAND ----------

val query = """

MATCH path = (e:Employee)<-[:REPORTS_TO*]-(sub)
WITH e, sub, [person in NODES(path) | person.employeeID][1..-1] AS path
RETURN e.employeeID AS manager, path as middleManager, sub.employeeID AS employee
ORDER BY size(path);

"""

val result_df = spark.read.format("org.neo4j.spark.DataSource")
  //.option("url", "bolt://localhost:7687")
  .option("query", query)
  .load()

result_df.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ##### How Many Orders were Made by Each Part of the Hierarchy?

// COMMAND ----------

val query = """ MATCH (e:Employee)
OPTIONAL MATCH (e)<-[:REPORTS_TO*0..]-(sub)-[:SOLD]->(order)
RETURN e.employeeID as employee, [x IN COLLECT(DISTINCT sub.employeeID) WHERE x <> e.employeeID] AS reportsTo, COUNT(distinct order) AS totalOrders
ORDER BY totalOrders DESC """

val result_df = spark.read.format("org.neo4j.spark.DataSource")
  //.option("url", "bolt://localhost:7687")
  .option("query", query)
  .load()

result_df.show()

// COMMAND ----------

//result_df.printSchema()
//result_df.describe().show()
//result_df.columns


//result_df.withColumn("new_column", result_df("totalOrders")/result_df("employee")).show()
//result_df.withColumn("new_column", col("totalOrders")/col("employee")).show()
//result_df.withColumnRenamed("employee", "Employee").show()

//result_df.orderBy(col("totalOrders").desc).select("totalOrders", "employee").show()
//result_df.orderBy("totalOrders", "employee").show()

//import org.apache.spark.sql.functions._
//result_df.select($"employee", desc("totalOrders")).show(false) // does not work

//result_df.withColumn("employeeInt", col("employee").cast("Integer")).filter(col("employeeInt") > 1).show()

//result_df.select(max(result_df("employee")), min(result_df("employee"))).show()

//result_df.select(corr(result_df("employee"), result_df("totalOrders"))).show()

// ignore df.withColumn("Year", year(df["Date"])).groupBy("Year").max().select("Year", "max(High)").show()
//result_df.groupBy("employee").max().select("employee", "max(totalOrders)").show()
//result_df.groupBy("employee").max().show()



// COMMAND ----------

// MAGIC %md
// MAGIC #### Neo4j Python driver example

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # pip install neo4j

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # from neo4j import GraphDatabase
// MAGIC # import pandas as pd 
// MAGIC 
// MAGIC # bolt_uri = "bolt://3.86.186.88:7687"
// MAGIC # driver = GraphDatabase.driver(bolt_uri, auth=("neo4j", "digit-bottoms-sterilizer"))
// MAGIC 
// MAGIC # query =  """ 
// MAGIC # MATCH (choc:Product {productName:'Chocolade'})<-[:CONTAINS]-(:Order)<-[:SOLD]-(employee),
// MAGIC #       (employee)-[:SOLD]->(o2)-[:CONTAINS]->(other:Product)
// MAGIC # RETURN employee.employeeID as employee, other.productName as otherProduct, count(distinct o2) as count
// MAGIC # ORDER BY count DESC
// MAGIC # LIMIT 5
// MAGIC # """
// MAGIC 
// MAGIC # with driver.session(database="neo4j") as session:
// MAGIC #     result = session.run(query)
// MAGIC #     df = pd.DataFrame([dict(record) for record in result])
// MAGIC 
// MAGIC # df.head()

// COMMAND ----------

// MAGIC %md
// MAGIC ####  Neo4j GDS Example

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # query = """
// MAGIC #     CALL gds.graph.create('got-interactions2', 'Person', {
// MAGIC #       INTERACTS: {
// MAGIC #         orientation: 'UNDIRECTED'
// MAGIC #       }
// MAGIC #     })
// MAGIC #     YIELD graphName, nodeCount, relationshipCount, createMillis
// MAGIC #     RETURN graphName, nodeCount, relationshipCount, createMillis
// MAGIC # """
// MAGIC 
// MAGIC # df = (spark.read.format("org.neo4j.spark.DataSource")
// MAGIC #     .option("url", "bolt://50.16.54.229:7687")
// MAGIC #     .option("authentication.type", "basic")
// MAGIC #     .option("authentication.basic.username", "neo4j")
// MAGIC #     .option("authentication.basic.password", "crack-ventilations-details")
// MAGIC #     .option("query", query)
// MAGIC #     .option("partitions", "1")
// MAGIC #     .load())

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # query = """
// MAGIC #     CALL gds.pageRank.stream('got-interactions2')
// MAGIC #     YIELD nodeId, score
// MAGIC #     RETURN gds.util.asNode(nodeId).name AS name, score
// MAGIC # """
// MAGIC 
// MAGIC # df = spark.read.format("org.neo4j.spark.DataSource") \
// MAGIC #     .option("url", "bolt://50.16.54.229:7687") \
// MAGIC #     .option("authentication.type", "basic") \
// MAGIC #     .option("authentication.basic.username", "neo4j") \
// MAGIC #     .option("authentication.basic.password", "crack-ventilations-details") \
// MAGIC #     .option("query", query) \
// MAGIC #     .option("partitions", "1") \
// MAGIC #     .load()
// MAGIC 
// MAGIC # df.show()

// COMMAND ----------

// Create a view or table

// val temp_table_name = "orders_csv"

// df.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

// %sql

/* Query the created temp table in a SQL cell */

// select * from `orders_csv`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Stop Here

// COMMAND ----------

// MAGIC %md
// MAGIC # Great Job!
