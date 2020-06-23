from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from operator import add
import sys
import operator
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


# "s3://aerodata/ontimeperformance_aircrafts.csv"
# "s3://aerodata/ontimeperformance_flights_tiny.csv"

def parsePopularModels(airlines_model_ranking):
    results = airlines_model_ranking.collect()

    if len(results) == 0:
        return

    results.sort(key=operator.itemgetter(0, 4))
    currAirline = results[0][0]

    i = 0
    hasPrintOnce = False
    aircraftTypeString = ""
    while i < len(results):
        if currAirline == results[i][0]:
            aircraftTypeString += "{} {}, ".format(results[i][1], results[i][2])
            i += 1
        else:
            hasPrintOnce = True
            print(
                "{} \t [{}]".format(currAirline, aircraftTypeString[:-2]),
                file=sys.stdout,
            )
            aircraftTypeString = ""
            currAirline = results[i][0]

    if not hasPrintOnce:
        print(
            "{} \t [{}]".format(currAirline, aircraftTypeString[:-2]), file=sys.stdout
        )

def airline_top_five(spark, flights_file_path, other_files_path, country):

    flights_t_df = (
        spark.read.format("csv")
        .options(header="true")
        .load(flights_file_path)
    )

    aircraft_df = (
        spark.read.format("csv")
        .options(header="true")
        .load("{}/ontimeperformance_aircrafts.csv".format(other_files_path))
    )

    airline_df = (
        spark.read.format("csv")
        .options(header="true")
        .load("{}/ontimeperformance_airlines.csv".format(other_files_path))
    )

    aircraft_col = [
        "manufacturer",
        "model",
        "modelName",
        "tailnum",
    ]

    aircraft_modelName = (
        aircraft_df.filter(F.col("manufacturer").isNotNull())
        .filter(F.col("model").isNotNull())
        .withColumn("modelName", F.regexp_extract(F.col("model"), "\d{3}", 0))
        .select(aircraft_col)
    )

    airlines_all_counts = (
        airline_df.filter(airline_df["country"] == country)
        .join(flights_t_df, airline_df.carrier_code == flights_t_df.carrier_code)
        .drop(flights_t_df.carrier_code)
        .filter(F.col("actual_departure_time").isNotNull())
        .groupBy("name", "carrier_code", "tail_number")
        .count()
        .join(
            aircraft_modelName,
            flights_t_df.tail_number == aircraft_modelName.tailnum,
            how="left",
        )
        .filter(F.col("manufacturer").isNotNull())
        .filter(F.col("model").isNotNull())
        .groupBy("name", "manufacturer", "model")
        .sum("count")
        .withColumnRenamed("sum(count)", "numFlights")
        .cache()
    )

    window = Window.partitionBy(airlines_all_counts["name"]).orderBy(
        airlines_all_counts["numFlights"].desc()
    )

    airlines_model_ranking = (
        airlines_all_counts.select("*", F.rank().over(window).alias("rank"))
        .filter(F.col("rank") <= 5)
        .sort(["name", "rank"], ascending=True)
    )

    parsePopularModels(airlines_model_ranking)


if __name__ == "__main__":

    spark = SparkSession.builder.appName("airline_top_five").getOrCreate()

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]
    country = sys.argv[3]

    airline_top_five(spark, flights_file_path, other_files_path, country)
    spark.stop()
