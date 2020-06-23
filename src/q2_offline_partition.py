from pyspark.sql import SparkSession
import pyspark.sql.Function as F
import sys

spark = SparkSession.builder.appName("delayed_flights").getOrCreate()

# s3://usyddata3404/ontimeperformance_flights_large.csv
flights_file_path = sys.argv[1]

# s3://air-traffic-dataset/data-partitioned/ontimeperformance_flights_large
output_file_path = sys.argv[2]

# 50000 for T2-large
# 100000 for T2-massive
maxRow = int(sys.arv[3])

ftd = (
    spark.read.format("csv")
    .options(header="true")
    .load(flights_file_path)
)

spark.conf.set("spark.sql.files.maxRecordsPerFile", maxRow)

ftd = ftd.withColumn("year", F.year("flight_date"))

ftd.repartition("year","carrier_code") \
    .write.partitionBy("year", "carrier_code") \
    .option("header", "true") \
    .csv(output_file_path)