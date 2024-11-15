from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Create a SparkSession
spark = SparkSession.builder.appName("UniqueSignups").getOrCreate()

# Read the data from the CSV file
df = spark.read.format("csv").load("../../data/April_May_signups/data.csv", header=True, inferSchema=True)

# Filter data for April and May of any year
filtered_df = df.filter((month(df.transaction_start_date).isin([4, 5]))
                          # Adjust the year range as needed
                          & (year(df.transaction_start_date).between(2023, 2023)))
                         )

# Get unique signup IDs
unique_signups_df = filtered_df.select("signup_id").distinct()

# Show the unique signup IDs
unique_signups_df.show()
