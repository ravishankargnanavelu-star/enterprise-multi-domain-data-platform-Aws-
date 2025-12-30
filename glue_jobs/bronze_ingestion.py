import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DOMAIN"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

source_path = args["SOURCE_PATH"]
target_path = args["TARGET_PATH"]
domain = args["DOMAIN"]

df = spark.read.option("mergeSchema", "true").json(source_path)

df = df.withColumn("domain", lit(domain)) \
       .withColumn("ingestion_ts", current_timestamp())

df.write \
  .mode("append") \
  .format("parquet") \
  .save(f"{target_path}/bronze/{domain}")
