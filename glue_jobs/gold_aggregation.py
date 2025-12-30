import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "TARGET_PATH",
        "DOMAIN",
        "PROCESS_DATE"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spark optimizations (enterprise standard)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

source_path = args["SOURCE_PATH"]
target_path = args["TARGET_PATH"]
domain = args["DOMAIN"]
process_date = args["PROCESS_DATE"]

# -----------------------------
# Read Silver (Trusted Data)
# -----------------------------
silver_df = spark.read.parquet(f"{source_path}/silver/{domain}")

# -----------------------------
# Filter for incremental load
# -----------------------------
silver_df = silver_df.filter(
    to_date(col("processed_ts")) == lit(process_date)
)

# -----------------------------
# FACT TABLE (Business Metrics)
# -----------------------------
fact_df = (
    silver_df
    .groupBy("domain", "record_status")
    .agg(
        count("*").alias("total_records"),
        countDistinct("id").alias("unique_entities"),
        max("processed_ts").alias("last_updated_ts")
    )
    .withColumn("process_date", lit(process_date))
)

# -----------------------------
# DIMENSION TABLE (Reference)
# -----------------------------
dim_df = (
    silver_df
    .select("id", "domain", "record_status")
    .dropDuplicates(["id"])
    .withColumn("effective_date", lit(process_date))
)

# -----------------------------
# Write GOLD – FACT
# -----------------------------
fact_df.write \
    .mode("overwrite") \
    .partitionBy("process_date") \
    .format("parquet") \
    .save(f"{target_path}/gold/{domain}/fact_metrics")

# -----------------------------
# Write GOLD – DIMENSION
# -----------------------------
dim_df.write \
    .mode("overwrite") \
    .partitionBy("effective_date") \
    .format("parquet") \
    .save(f"{target_path}/gold/{domain}/dim_entities")
