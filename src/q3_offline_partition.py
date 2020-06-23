from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

spark = SparkSession.builder.appName("countries_partioned").getOrCreate()

flights_file_path = sys.argv[1]
country_file_path = sys.argv[2]

output_file_path = sys.argv[3]

al = spark.read.format("csv").options(header="true").load(country_file_path)
fd = spark.read.format("csv").options(header="true").load(flights_file_path)

airlines_all_counts = (
    al.join(fd, al.carrier_code == fd.carrier_code)
    .drop(fd.carrier_code)
    .filter(F.col("actual_departure_time").isNotNull())
    .groupBy("name", "carrier_code", "tail_number", "country")
    .count()
)

maxRow = 600
spark.conf.set("spark.sql.files.maxRecordsPerFile", maxRow)

airlines_all_counts.repartition("country").write.partitionBy("country").option(
    "header", "true"
).csv(output_file_path)
