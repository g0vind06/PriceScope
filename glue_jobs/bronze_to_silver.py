import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timezone
import time
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

#INIT
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_date'])  #Glue standard way to get job name and run date from command line arguments
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_date = args['run_date']  #run_date is passed as a parameter to the Glue job. It is used to filter the bronze data for the specific run date.
print(f"Processing run_date: {run_date}")

dt=datetime.strptime(run_date, "%Y-%m-%d")

#CONFIG
BUCKET = 'price-scope'
BRONZE_PATH = f"s3://{BUCKET}/bronze/source=smartprix/category=*/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/*"
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

#Delete existing data for the run_date in silver before writing new data. This is to avoid duplicates when the job is re-run for the same run_date.
s3_client = boto3.client('s3', region_name='eu-north-1')
paginator = s3_client.get_paginator('list_objects_v2')

for category in df_final.select("category").distinct().rdd.flatMap(lambda x: x).collect():
    prefix = f"silver/source=smartprix/category={category}/run_date={run_date}/"
    print(f"Deleting existing data in silver for category: {category}, run_date: {run_date}")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3_client.delete_object(Bucket=BUCKET, Key=obj['Key'])
                print(f"Deleted: {obj['Key']}")

#WRITE TO SILVER
#Partioned by category and run_date for efficient querying
df_final.write.mode("append").partitionBy("category", "run_date").parquet(SILVER_PATH)

print(f"Appended to silver path: {SILVER_PATH} for run_date: {run_date}")
print(f"Final record count: {df_final.count()}")

#Added after phase 3 to register partitions for the newly added data.

def register_partitions(spark_df):
    """
    After writing the silver parquet, register new partitions in the Redshift spectrum so queries pick up new data.
    """

    client = boto3.client('redshift-data', region_name='eu-north-1')
    partitions = spark_df.select("category", "run_date").distinct().collect()
    for row in partitions:
        category = row['category']
        run_date = str(row['run_date'])

        s3_location = f"s3://{BUCKET}/silver/source=smartprix/category={category}/run_date={run_date}/"

        sql = f"ALTER TABLE spectrum_silver.stg_price_snapshots ADD IF NOT EXISTS PARTITION (category='{category}', run_date='{run_date}') LOCATION '{s3_location}'"

        try:
            response = client.execute_statement(
                WorkgroupName='pricescope-workgroup',
                Database='dev',
                SecretArn='arn:aws:secretsmanager:eu-north-1:361966322300:secret:Redshift/pricescope/admin-RFateP',
                Sql=sql
            )
            statement_id = response['Id']

            while True:
                status_response = client.describe_statement(Id=statement_id)
                status = status_response['Status']
                if status in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(1)                        #using sleep since execute_statement is asynchronous and we need to wait for the statement to finish before checking the status.
            if status == 'FINISHED':
                print(f"Successfully registered partition for category={category}, run_date={run_date}")
            else:
                error_message = status_response.get('Error', 'Unknown error')
                print(f"Failed to register partition for category={category}, run_date={run_date}: {error_message}")
        except Exception as e:
            print(f"Partition may already exist, skipping: {e}")

#Call this after writing to silver
register_partitions(df_final) 

job.commit()