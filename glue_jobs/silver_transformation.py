import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DOMAIN"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.adaptive.enabled", "true")

source_path = args["SOURCE_PATH"]
target_path = args["TARGET_PATH"]
domain = args["DOMAIN"]

# Read Bronze data
df = spark.read.parquet(f"{source_path}/bronze/{domain}")

# Window to keep latest record
w = Window.partitionBy("id").orderBy(col("ingestion_ts").desc())

df_silver = (
    df
    # Validation
    .filter(col("id").isNotNull())

    # Standardization
    .withColumn("id", trim(col("id")))

    # Deduplication
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")

    # Business rule
    .withColumn(
        "record_status",
        when(col("is_active") == True, "ACTIVE").otherwise("INACTIVE")
    )

    # Enrichment
    .withColumn("processed_ts", current_timestamp())
)

# Write Silver data
df_silver.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{target_path}/silver/{domain}")
