from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

def main():
    # Initialize SparkSession with Kafka package
    spark = SparkSession.builder \
        .appName("VelibStreamingProcessor") \
        .getOrCreate()

    # Set log level to avoid too much noise, but show job outputs
    spark.sparkContext.setLogLevel("WARN")

    print("\n==================================")
    print("Starting Spark Streaming Job...")
    print("==================================\n")

    # Define the schema of the Velib API JSON data
    # We define the fields we are interested in.
    velib_schema = StructType([
        StructField("stationcode", StringType(), True),
        StructField("name", StringType(), True),
        StructField("is_installed", StringType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("numbikesavailable", IntegerType(), True),
        StructField("numdocksavailable", IntegerType(), True),
        StructField("is_renting", StringType(), True),
        StructField("is_returning", StringType(), True),
        # You can add more fields if needed
    ])

    # Read stream from Kafka
    # In docker-compose, kafka is accessible via the service name 'kafka' at port 9092
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "velib-stations") \
        .option("startingOffsets", "latest") \
        .load()

    # Kafka values are in binary, so we cast to string first
    json_df = df.selectExpr("CAST(value AS STRING) as json_string")

    # Parse the JSON string into separate columns based on the schema
    parsed_df = json_df.select(from_json(col("json_string"), velib_schema).alias("data")).select("data.*")

    # Data Processing / Preprocessing example:
    # 1. Filter out stations that are not installed
    # 2. Add a calculated column for fill percentage
    processed_df = parsed_df \
        .filter(col("is_installed") == "OUI") \
        .withColumn("fill_percentage", expr("ROUND((numbikesavailable / capacity) * 100, 2)"))

    # Select only the crucial columns to display
    final_df = processed_df.select(
        "stationcode", 
        "name", 
        "capacity", 
        "numbikesavailable", 
        "numdocksavailable", 
        "fill_percentage"
    )

    # Output the processed data to the console in append mode
    query = final_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
