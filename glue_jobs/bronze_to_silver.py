import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

#INIT
args = getResolvedOptions(sys.argv, ['JOB_NAME'])  #Glue standard way to get job name from command line arguments
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#CONFIG
BUCKET = 'price-scope'
BRONZE_PATH = f"s3://{BUCKET}/bronze/source=smartprix/"
SILVER_PATH = f"s3://{BUCKET}/silver/source=smartprix/"

print(f"Reading bronze data from: {BRONZE_PATH}")

#READ BRONZE DATA
df_raw = spark.read.option('multiline', 'true').json(BRONZE_PATH)

print(f"Raw record count: {df_raw.count()}")
print("Raw schema:")
df_raw.printSchema()

#Cast Types
df_typed = df_raw.withColumn("current_price", F.col("current_price").cast(DoubleType())) \
    .withColumn("mrp", F.col("mrp").cast(DoubleType())) \
    .withColumn("discount_pct", F.col("discount_pct").cast(IntegerType())) \
    .withColumn("rating", F.col("rating").cast(DoubleType())) \
    .withColumn("scraped_at", F.to_timestamp(F.col("scraped_at"))) \
    .withColumn("product_name", F.trim(F.col("product_name")))

#filter invalid records
df_valid = df_typed.filter((F.col("product_name").isNotNull()) &
    (F.col("current_price").isNotNull()) &
    (F.col("current_price") > 0)
)

invalid_count = df_typed.count() - df_valid.count()
print(f"Dropped {invalid_count} invalid records. Valid record count: {df_valid.count()}")

#Enrich -product id correctly identifies the same product across multiple scrape runs. We use product_name + category to generate a unique hash for each product. This is not perfect, but it is a good start. We can improve this later by using more fields or a better hashing algorithm. - NORMALIZATION.
df_enriched = df_valid.withColumn("product_id", F.md5(F.concat_ws("||", F.lower(F.trim(F.col("product_name"))), F.col("category")))) \
    .withColumn("has_discount", F.col("discount_pct").isNotNull() & (F.col("discount_pct") > 0)) \
    .withColumn("is_rating_available", F.col("rating").isNotNull() & (F.col("rating") > 0)) \
    .withColumn("run_date", F.to_date(F.col("scraped_at"))) \
    .withColumn("discount_pct", F.when(F.col("discount_pct").isNull(), F.lit(0)).otherwise(F.col("discount_pct"))) \
    .withColumn("mrp", F.when(F.col("mrp").isNull(), F.col("current_price")).otherwise(F.col("mrp"))) \
    .withColumn("brand", F.split(F.col("product_name"), " ")[0])             

#mrp = current_price if discount is null. Our scraper is currently making mrp null when discount is null, but we can assume mrp = current_price in that case.

#dedup - One row per product per scrape timestamp
df_deduped = df_enriched.dropDuplicates(["product_id", "scraped_at"])
print(f" After deduplication, record count: {df_deduped.count()}")

#SELECT COLUMNS
df_final = df_deduped.select(
    "product_id", "product_name", "brand", "product_url", "category", "current_price", "mrp",
    "discount_pct", "has_discount", "rating", "is_rating_available", "source", 
    "scraped_at", "run_date"
)

#WRITE TO SILVER
#Partioned by category and run_date for efficient querying
df_final.write.mode("overwrite").partitionBy("category", "run_date").parquet(SILVER_PATH)

print(f"Written to silver path: {SILVER_PATH}")
print(f"Final record count: {df_final.count()}")

job.commit()