from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col, min

# Create a SparkSession
spark = SparkSession.builder.appName("CustomerDensity").getOrCreate()

# Read the data from the two tables
transactions_df = spark.read.format("csv").load("transaction_records.csv", header=True, inferSchema=True)
stores_df = spark.read.format("csv").load("stores.csv", header=True, inferSchema=True)

# Join the tables on the store_id
joined_df = transactions_df.join(stores_df, "store_id")

# Calculate customer density for each area
result_df = joined_df.groupBy("area_name").agg(
    countDistinct("customer_id").alias("total_customers"),
    min("area_size").alias("area_size")  # Use min to get a single value
).withColumn("customer_density", col("total_customers") / col("area_size")) \
  .orderBy(col("customer_density").desc()) \
  .limit(3)

# Show the top 3 areas with highest customer density
result_df.show()
