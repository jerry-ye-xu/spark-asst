from pyspark import SparkConf, SparkContext
import csv
import operator
import sys
from datetime import datetime
from datetime import timedelta
from functools import reduce

def avg(ls):
    return reduce(lambda a, b: float(a) + float(b), ls) / len(ls)

def printls(ls):
    for _ in ls:
        print(_)

def change24(flight):
    if flight[9] == "24:00:00":
        flight[9] = "00:00:00"

    if flight[7] == "24:00:00":
        flight[7] = "00:00:00"

    return flight

def getDelay(time_actual, time_est):
    actual = datetime.strptime(time_actual, "%H:%M:%S")
    est = datetime.strptime(time_est, "%H:%M:%S")

    # this is a tiemdelta object now
    delay = actual - est

    if delay.total_seconds() / 3600 > 12:
        delay = delay - timedelta(hours=24)
        return delay.total_seconds()

    elif delay.total_seconds() / 3600 < -12:
        delay = delay + timedelta(hours=24)
        return delay.total_seconds()

    else:
        return delay.total_seconds()

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

def delayed_flights(sc, flights_file_path, other_files_path, year):
    airlines_rdd = sc.textFile("{}/ontimeperformance_airlines.csv".format(other_files_path))

    airlines_rdd = airlines_rdd.mapPartitions(lambda x: csv.reader(x))

    # only keep airlines from USA
    airlines_rdd = airlines_rdd.filter(lambda x : x[2] == "United States")

    # only keep carrier code and name columns
    airlines_rdd = airlines_rdd.map(
        lambda x: (x[0], x[1])
    )


    flights_rdd = sc.textFile(flights_file_path)
    flights_rdd = flights_rdd.mapPartitions(lambda x: csv.reader(x))
    flights_rdd = flights_rdd.filter(lambda x: x[3].split("-")[0] == str(year))
    flights_rdd = flights_rdd.filter(lambda x: (x[9] != ""))

    flights_rdd = flights_rdd.map(change24)

    flights_rdd_small = flights_rdd.map(lambda x: (x[1], x[7], x[9]))

    # rdd of carrier_code, delay
    flights_rdd_smaller = flights_rdd_small.map(lambda x: (x[0], getDelay(x[2], x[1])))

    # filter out delays greater than 0
    flights_rdd_smaller = flights_rdd_smaller.filter(lambda x: (x[1] > 0))

    flights_rdd_groups = flights_rdd_smaller.groupByKey()

    flights_rdd_result = flights_rdd_groups.map(
        lambda x: (x[0], (len(x[1]), avg(x[1]) / 60, min(x[1]) / 60, max(x[1]) / 60))
    )
    joined_rdd = flights_rdd_result.join(airlines_rdd)

    final_joined_rdd = joined_rdd.map(
        lambda x: (x[1][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])
    ).sortBy(lambda a: a[0])

    parseAvgFlights(final_joined_rdd)

if __name__ == "__main__":
    conf = SparkConf().setAppName("DelayedFlightsRDD")

    sc = SparkContext(conf=conf)

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]
    year = sys.argv[3]

    delayed_flights(sc, flights_file_path, other_files_path, year)
    sc.stop()
