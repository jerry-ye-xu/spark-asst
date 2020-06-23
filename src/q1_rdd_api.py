from pyspark import SparkConf, SparkContext
from operator import add
import csv
import sys
import re

def printCessnaModels(count):
    results = count
    for i in range(len(count)):
        # changed this line to have regex
        #print("Cessna {} \t {}".format(results[i][0], results[i][1]), file=sys.stdout)
        print("Cessna {} \t {}".format((re.findall(r'\d{3}', results[i][0])[0]), results[i][1]), file=sys.stdout)


def top_three_rdd(sc, flights_file_path, other_files_path):
    aircraft_rdd = sc.textFile(
        "{}/ontimeperformance_aircrafts.csv".format(other_files_path)
    )
    flight_rdd = sc.textFile(flights_file_path)

    # this is now a 2d list of aircrafts
    aircraft_rdd = aircraft_rdd.mapPartitions(lambda x: csv.reader(x))
    header = aircraft_rdd.first()

    # remove header and ANY ROW W NULL
    aircraft_rdd = aircraft_rdd.filter(lambda x: len(x) == len(header))
    aircraft_rdd = aircraft_rdd.filter(lambda x: x != header)

    # remove entries which are not cessna
    aircraft_rdd = aircraft_rdd.filter(lambda x: x[2] == "CESSNA")
    # print(aircraft_rdd.take(10))

    flight_rdd = flight_rdd.mapPartitions(lambda x: csv.reader(x))
    # print(flight_rdd.take(10))

    # filter out cancelled flights
    flight_rdd = flight_rdd.filter(lambda x : x[9] != "")


    # get list of (tail_num, model) from aircraft
    aircraft_rdd_small = aircraft_rdd.map(lambda x: (x[0], x[4]))
    #print(aircraft_rdd_small.take(10))

    # turn aircraft_rdd_small's model into just 3 digits
    aircraft_rdd_small = aircraft_rdd_small.map(lambda x: (x[0], re.findall(r'\d{3}', x[1])[0]))
    #print(aircraft_rdd_small.take(10))

    # list of (tail_num, flight_id) from flights
    flight_rdd_small = flight_rdd.map(lambda x: (x[6], x[0]))

    joined_rdd = aircraft_rdd_small.join(flight_rdd_small)
    # print(joined_rdd.take(10))

    final = joined_rdd.map(lambda x: (x[1][0], 1))

    counts = final.reduceByKey(lambda a, b: a + b).takeOrdered(3, key=lambda x: -x[1])
    printCessnaModels(counts)


if __name__ == "__main__":
    conf = SparkConf().setAppName("TopThreeModlesRDD")

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]

    sc = SparkContext(conf=conf)
    top_three_rdd(sc, flights_file_path, other_files_path)
    sc.stop()
