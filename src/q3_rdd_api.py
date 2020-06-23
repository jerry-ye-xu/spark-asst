from pyspark import SparkConf, SparkContext
import csv
import sys
from functools import reduce
import operator


def parsePopularModels(airlines_model_ranking):
    results = airlines_model_ranking
    # results.sort(key=operator.itemgetter(0, 4))

    if len(results) == 0:
        return

    currAirline = results[0][0]

    i = 0
    hasPrintOnce = False
    aircraftTypeString = ""
    while i < len(results):
        if currAirline == results[i][0]:
            aircraftTypeString += "{} {}, ".format(results[i][1][0], results[i][1][1])
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


def top_model_airlines(sc, flights_file_path, other_files_path, country):
    airlines_rdd = sc.textFile("{}/ontimeperformance_airlines.csv".format(other_files_path))

    #  carrier code , name , country
    airlines_rdd = airlines_rdd.mapPartitions(lambda x: csv.reader(x)).map(
        lambda x: (x[0], x[1], x[2])
    )

    # remove null
    airlines_rdd = airlines_rdd.filter(lambda x: x[0] != "")
    airlines_rdd = airlines_rdd.filter(lambda x: x[1] != "")
    airlines_rdd = airlines_rdd.filter(lambda x: x[2] != "")

    aircraft_rdd = sc.textFile("{}/ontimeperformance_aircrafts.csv".format(other_files_path))
    aircraft_rdd = aircraft_rdd.mapPartitions(lambda x: csv.reader(x))

    # remove header and ANY ROW W NULL
    header = aircraft_rdd.first()
    aircraft_rdd = aircraft_rdd.filter(lambda x: len(x) == len(header))
    aircraft_rdd = aircraft_rdd.filter(lambda x: x != header)

    # tailnum, manufacture, model
    aircraft_rdd_small = aircraft_rdd.map(lambda x: (x[0], x[2], x[4]))
    # print(aircraft_rdd_small.take(10))

    # carrier_code, tail_number, actual_departure_time
    flights_rdd = sc.textFile(flights_file_path)
    flights_rdd = flights_rdd.mapPartitions(lambda x: csv.reader(x)).map(
        lambda x: (x[1], x[6], x[9])
    )

    # remove null
    flights_rdd = flights_rdd.filter(lambda x: x[0] != "")
    flights_rdd = flights_rdd.filter(lambda x: x[1] != "")
    flights_rdd = flights_rdd.filter(lambda x: x[2] != "")
    # print(flights_rdd.take(10))

    # re mapping (carrie_code : (name,country))
    airlines_rdd = airlines_rdd.filter(lambda x: x[2] == country)
    airlines_rdd = airlines_rdd.map(lambda x: (x[0], (x[1], x[2])))

    # re mapping (carrier_code , tailnum)
    flights_rdd = flights_rdd.map(lambda x: (x[0], x[1]))

    # carrier_code :(name, country) , tailnum
    airlines_all_count = airlines_rdd.join(flights_rdd)
    # re map (tailnum: (carrier_code,name))
    airlines_all_count = airlines_all_count.map(lambda x: (x[1][1], (x[0], x[1][0][0])))
    # re shape (t,c,n:1)
    airlines_all_count = airlines_all_count.map(lambda x: ((x[0], x[1][0], x[1][1]), 1))
    airlines_all_count = airlines_all_count.reduceByKey(lambda a, b: a + b)
    # re shape (tailnum:(name,count))
    airlines_all_count = airlines_all_count.map(lambda x: (x[0][0], (x[0][2], x[1])))
    # re shape tailnum:(manufacture,model)
    aircraft_rdd_small = aircraft_rdd_small.map(lambda x: (x[0], (x[1], x[2])))

    airlines_all_count = airlines_all_count.join(aircraft_rdd_small)
    # 'N413AA', (('American Airlines Inc.', 286), ('MCDONNELL DOUGLAS', 'DC-9-82(MD-82)'))
    # name,manufacture,model:count
    airlines_all_count = airlines_all_count.map(
        lambda x: ((x[1][0][0], x[1][1][0], x[1][1][1]), x[1][0][1])
    )
    # name,manfufacturer,model:sum(count)
    airlines_all_count = airlines_all_count.reduceByKey(lambda a, b: a + b)

    airlines_all_count = airlines_all_count.map(
        lambda x: (x[0][0], (x[0][1], x[0][2], x[1]))
    )
    # sorting by count then by airline name
    airlines_all_count = airlines_all_count.sortBy(lambda x: x[1][2], False).sortByKey()
    return airlines_all_count

def take_top_5_for_key(l):
    l = l.collect()
    curr_airline = l[0][0]
    # print(curr_airline)
    taken = []
    ac = 0
    i = 0
    new_l = []
    while i < len(l):
        if curr_airline == l[i][0]:
            if ac < 5:
                new_l.append(l[i])
                ac += 1
                i += 1
            else:
                i += 1
        else:
            curr_airline = l[i][0]
            ac = 0

    return new_l

if __name__ == "__main__":
    conf = SparkConf().setAppName("TopModelsAirlines")
    spark = SparkContext(conf=conf)

    flights_file_path = sys.argv[1]
    other_files_path = sys.argv[2]
    country = sys.argv[3]

    all_model = top_model_airlines(spark,
        flights_file_path,
        other_files_path,
        country
    )
    x = take_top_5_for_key(all_model)
    parsePopularModels(x)
    spark.stop()
