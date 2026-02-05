from pyspark.sql import SparkSession

spark = (
SparkSession.builder
.appName("Ingest Sellers")
.getOrCreate()
)

sellers_df = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("data/raw/olist_sellers_dataset.csv")
)

sellers_df.printSchema()
sellers_df.show(5)

jdbc_url = "jdbc:postgresql://localhost:5432/olist_dw"

properties = {
"user": "postgres",
"password": "postgres",
"driver": "org.postgresql.Driver"
}

(
sellers_df.write
.mode("overwrite")
.jdbc(
url=jdbc_url,
table="dim_sellers",
properties=properties
)
)

spark.stop()