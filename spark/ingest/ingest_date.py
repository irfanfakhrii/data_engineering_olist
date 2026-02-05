from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, weekofyear, when, to_date, expr

spark = (
SparkSession.builder
.appName("Ingest Dim Date")
.getOrCreate()
)

start_date = "2016-01-01"
end_date = "2026-12-31"

date_df = (
spark.sql(
f"""
SELECT sequence(
to_date('{start_date}'),
to_date('{end_date}'),
interval 1 day
) AS date_seq
"""
)
.selectExpr("explode(date_seq) as date_id")
)

dim_date_df = (
date_df
.withColumn("year", year(col("date_id")))
.withColumn("month", month(col("date_id")))
.withColumn("day", dayofmonth(col("date_id")))
.withColumn("day_of_week", dayofweek(col("date_id")))
.withColumn("week_of_year", weekofyear(col("date_id")))
.withColumn(
"is_weekend",
when(dayofweek(col("date_id")).isin([1, 7]), 1).otherwise(0)
)
)

dim_date_df.printSchema()
dim_date_df.show(5)

jdbc_url = "jdbc:postgresql://localhost:5432/olist_dw"

properties = {
"user": "postgres",
"password": "postgres",
"driver": "org.postgresql.Driver"
}

(
dim_date_df.write
.mode("overwrite")
.jdbc(
url=jdbc_url,
table="dim_date",
properties=properties
)
)

spark.stop()