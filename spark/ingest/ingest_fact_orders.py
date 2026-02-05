from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
SparkSession.builder
.appName("Ingest Fact Order Items")
.getOrCreate()
)

order_items_df = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("data/raw/olist_order_items_dataset.csv")
)

orders_df = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("data/raw/olist_orders_dataset.csv")
)

fact_order_items_df = (
order_items_df
.join(orders_df, "order_id", "inner")
.select(
col("order_id"),
col("customer_id"),
col("seller_id"),
col("product_id"),
to_date(col("order_purchase_timestamp")).alias("order_date"),
col("price"),
col("freight_value")
)
)

fact_order_items_df.printSchema()
fact_order_items_df.show(5)

jdbc_url = "jdbc:postgresql://localhost:5432/olist_dw"

properties = {
"user": "postgres",
"password": "postgres",
"driver": "org.postgresql.Driver"
}

(
fact_order_items_df.write
.mode("overwrite")
.jdbc(
url=jdbc_url,
table="fact_order_items",
properties=properties
)
)

spark.stop()