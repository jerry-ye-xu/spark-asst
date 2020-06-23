from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from operator import add
import sys
import operator
from pyspark.sql.functions import broadcast


def printCessnaModels(cessna_results):
    results = cessna_results.collect()
    results.sort(key=operator.itemgetter(1), reverse=True)
    for i in range(3):
        print("Cessna {} \t {}".format(results[i][0], results[i][1]), file=sys.stdout)


# "s3://aerodata/ontimeperformance_aircrafts.csv"
# "s3://aerodata/ontimeperformance_flights_tiny.csv"
def top_three(sc, flights_file_path, other_files_path):
    aircraft_df = (
        spark.read.format("csv")
        .options(header="true")
        .load("{}/ontimeperformance_aircrafts.csv".format(other_files_path))
    )
    flights_t_df = (
        spark.read.format("csv")
        .options(header="true")
        .load(flights_file_path)
    )

    cessna_models = aircraft_df.filter(
        aircraft_df["manufacturer"] == "CESSNA"
    ).withColumn("modelName", F.regexp_extract(F.col("model"), "\d{3}", 0))

    flights_count = flights_t_df.groupby("tail_number").count()

    cessna_results = (
        F.broadcast(cessna_models).join(
            flights_count,
            cessna_models.tailnum == flights_count.tail_number,
            how="left",
        )
        .groupby("modelName")
        .sum("count")
        .sort("sum(count)", ascending=False)
    )

    printCessnaModels(cessna_results)


if __name__ == "__main__":

    spark = SparkSession.builder.appName("TopThreeModels").getOrCreate()

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]

    top_three(spark, flights_file_path, other_files_path)
    spark.stop()
