from pyspark.sql import SparkSession

spark = (
SparkSession.builder
.appName("Ingest Products")
.getOrCreate()
)

products_df = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("data/raw/olist_products_dataset.csv")
)

products_df.printSchema()
products_df.show(5)

jdbc_url = "jdbc:postgresql://localhost:5432/olist_dw"

properties = {
"user": "postgres",
"password": "postgres",
"driver": "org.postgresql.Driver"
}

(
products_df.write
.mode("overwrite")
.jdbc(
url=jdbc_url,
table="dim_products",
properties=properties
)
)

spark.stop()