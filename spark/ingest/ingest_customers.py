from pyspark.sql import SparkSession

spark = (
SparkSession.builder
.appName("Ingest Customers")
.getOrCreate()
)

customers_df = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("data/raw/olist_customers_dataset.csv")
)

customers_df.printSchema()
customers_df.show(5)

jdbc_url = "jdbc:postgresql://localhost:5432/olist_dw"

properties = {
"user": "postgres",
"password": "postgres",
"driver": "org.postgresql.Driver"
}

(
customers_df.write
.mode("overwrite")
.jdbc(
url=jdbc_url,
table="dim_customers",
properties=properties
)
)

spark.stop()
