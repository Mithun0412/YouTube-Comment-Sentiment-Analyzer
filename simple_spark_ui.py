import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import time

# Create Spark session with UI enabled
spark = SparkSession.builder \
    .appName("YouTubeCommentSentimentAnalyzer") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .config("spark.ui.bindAddress", "0.0.0.0") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./spark-logs") \
    .getOrCreate()

print("Spark UI URL:", spark.sparkContext.uiWebUrl)
print("Spark Version:", spark.version)

# Keep the Spark session alive
try:
    # Create a simple DataFrame without Python operations
    schema = StructType([StructField("comment", StringType(), True)])
    data = [("This is a positive comment",), ("This is a negative comment",), ("This is a neutral comment",)]
    df = spark.createDataFrame(data, schema)
    
    # Cache the DataFrame to generate UI events
    df.cache()
    
    # Perform a simple count operation
    count = df.count()
    print(f"Processed {count} rows")
    
    print("Spark is running with UI enabled. Press Ctrl+C to stop.")
    print("You can now access the Spark UI at:", spark.sparkContext.uiWebUrl)
    
    # Keep the application running
    while True:
        time.sleep(10)
        
except KeyboardInterrupt:
    print("Stopping Spark...")
    spark.stop()