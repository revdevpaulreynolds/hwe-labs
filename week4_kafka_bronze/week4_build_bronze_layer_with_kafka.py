import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

#marketplace     customer_id     review_id       product_id      product_parent  product_titleproduct_category star_rating     helpful_votes   total_votes     vine    verified_purchase    review_headline  review_body     purchase_date
# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load().selectExpr("CAST(value AS STRING)")
split_col = split(df['value'], '\t')
# print(split_col.getItem(0))
df_split = df \
  .withColumn("marketplace", split_col.getItem(0)) \
  .withColumn("customer_id", split_col.getItem(1)) \
  .withColumn("review_id", split_col.getItem(2)) \
  .withColumn("product_id", split_col.getItem(3)) \
  .withColumn("product_parent", split_col.getItem(4)) \
  .withColumn("product_title", split_col.getItem(5)) \
  .withColumn("product_category", split_col.getItem(6)) \
  .withColumn("star_rating", split_col.getItem(7)) \
  .withColumn("helpful_votes", split_col.getItem(8)) \
  .withColumn("total_votes", split_col.getItem(9)) \
  .withColumn("vine", split_col.getItem(10)) \
  .withColumn("verified_purchase", split_col.getItem(11)) \
  .withColumn("review_headline", split_col.getItem(12)) \
  .withColumn("review_body", split_col.getItem(13)) \
  .withColumn("purchase_date", split_col.getItem(14)) \
  .withColumn("review_timestamp", current_timestamp()) \
  .drop("value")

# df_split.printSchema()

# df_split.show(5, truncate=False)
# Process the received data
query = df_split \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "C:\\Users\\PaulReynolds\\Documents\\repos\\1904\\hwe-labs\\tmp\\kafka-checkpoint") \
    .option("path", "s3a://hwe-fall-2024/preynolds/bronze/reviews") \
    .start()

# # Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()