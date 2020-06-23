from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from operator import add
import sys
import operator
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

def parseAvgFlights(avg_flights_df):
    results = avg_flights_df.collect()
    results.sort(key=operator.itemgetter(0))
    for i in range(len(results)):
        print(
            "{} \t {} \t {} \t {} \t {}".format(
                results[i][0],
                results[i][1],
                results[i][2],
                results[i][3],
                results[i][4],
            ),
            file=sys.stdout,
        )

def delayed_flights(spark, flights_file_path, other_files_path, year):
    """

    PARAMETERS
    ----------

    spark: SparkSession
    file:  The data file e.g."s3://air-traffic-dataset/ontimeperformance_flights_test.csv".
    year: 1994-2008 inclusive for the tiny dataset.

    """

    flights_tiny_df = (
        spark.read.format("csv")
        .options(header="true")
        .load("{}/year={}".format(flights_file_path, year))
    )

    airlines_df = (
        spark.read.format("csv")
        .options(header="true")
        .load("{}/ontimeperformance_airlines.csv".format(other_files_path))
    )

    flights_tiny_df = flights_tiny_df \
        .withColumn(
            "scheduled_departure_timestamp",
            F.to_timestamp(
                F.when(
                    F.col("scheduled_depature_time") == "24:00:00", "00:00:00"
                ).otherwise(F.col("scheduled_depature_time")),
                "HH:mm:ss",
            ),
        ) \
        .withColumn(
            "actual_departure_timestamp",
            F.to_timestamp(
                F.when(F.col("actual_departure_time") == "24:00:00", "00:00:00").otherwise(
                    F.col("actual_departure_time")
                ),
                "HH:mm:ss",
            ),
        ) \
        .withColumn(
            "delayed_time",
            F.when(
                F.col("actual_departure_timestamp").cast("long")
                - F.col("scheduled_departure_timestamp").cast("long")
                > (60 * 60 * 12),
                (
                    F.col("scheduled_departure_timestamp").cast("long")
                    + (60 * 60 * 24)
                    - F.col("actual_departure_timestamp").cast("long")
                )
                / 60,
            )
            .when(
                F.col("scheduled_departure_timestamp").cast("long")
                - F.col("actual_departure_timestamp").cast("long")
                > (60 * 60 * 12),
                (
                    F.col("actual_departure_timestamp").cast("long")
                    + (60 * 60 * 24)
                    - F.col("scheduled_departure_timestamp").cast("long")
                )
                / 60,
            )
            .otherwise(
                (
                    F.col("actual_departure_timestamp").cast("long")
                    - F.col("scheduled_departure_timestamp").cast("long")
                )
                / 60
            ),
        ) \
        .filter(F.col("actual_departure_timestamp").isNotNull()) \
        .filter(F.col("delayed_time") > 0) \
        .groupBy("carrier_code") \
        .agg(
            F.count("delayed_time").alias("numOfDelays"),
            F.mean("delayed_time").alias("avgDelays"),
            F.min("delayed_time").alias("minDelay"),
            F.max("delayed_time").alias("maxDelay"),
        ) \
        .withColumn("avgDelays", F.round(F.col("avgDelays"), 2)) \
        .select(
            "carrier_code",
            "numOfDelays",
            "avgDelays",
            "minDelay",
            "maxDelay"
        ) \

    avg_flights = F.broadcast(airlines_df) \
        .join(
            flights_tiny_df,
            flights_tiny_df.carrier_code == airlines_df.carrier_code
        ) \
        .select(
            "name",
            "numOfDelays",
            "avgDelays",
            "minDelay",
            "maxDelay"
        )

    parseAvgFlights(avg_flights)

if __name__ == "__main__":

    spark = SparkSession.builder.appName("delayed_flights").getOrCreate()

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]
    year = sys.argv[3]

    delayed_flights(spark, flights_file_path, other_files_path, year)
    spark.stop()

    # local testing use: ../assignment_data_files/ontimeperformance_flights_test.csv



    ### PREVIOUS ###

    """

    BATCH 1
    - tried partitioning dataset at the beginning to 12
    - broadcasting airlines

    BATCH 2
    - repartitiong on carrier node after filtering

    """
