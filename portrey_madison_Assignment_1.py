from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollarsdef correctRows(p):
def correctRows(p): 
    if (len(p) == 17):
        if (isfloat(p[5]) and isfloat(p[11])):
            if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
                return p

#Main

if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        print("Usage: main_task1 <file> <output_task1_dir> <output_task2_dir> [1|2]", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")
    rdd = sc.textFile(sys.argv[1])
    rows = rdd.map(lambda line: line.strip().split(","))
    clean = rows.map(correctRows).filter(lambda x: x is not None)

    which = sys.argv[4] if len(sys.argv) == 5 else None

    #Task 1
    if which in (None, "1"):
        med_driver = clean.map(lambda p: (p[0], p[1])).distinct()
        counts = med_driver.map(lambda t: (t[0], 1)).reduceByKey(add)
        top10_t1 = counts.takeOrdered(10, key=lambda kv: (-kv[1], kv[0]))
        sc.parallelize(top10_t1).map(lambda kv: f"{kv[0]},{kv[1]}") \
          .coalesce(1).saveAsTextFile(sys.argv[2])

    #Task 2
    if which in (None, "2"):
        driver_pairs = clean.map(lambda p: (p[1], (float(p[16]), int(float(p[4])))))
        summed = driver_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        money_per_min = summed.filter(lambda kv: kv[1][1] > 0) \
                              .map(lambda kv: (kv[0], kv[1][0] / (kv[1][1] / 60.0)))
        top10_t2 = money_per_min.takeOrdered(10, key=lambda kv: (-kv[1], kv[0]))
        sc.parallelize(top10_t2).map(lambda kv: f"{kv[0]},{kv[1]:.6f}") \
          .coalesce(1).saveAsTextFile(sys.argv[3])

    sc.stop()
