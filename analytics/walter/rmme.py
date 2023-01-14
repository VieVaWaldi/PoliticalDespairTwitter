import os
import csv
import sys
import time
from random import random
import pandas as pd
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
# from textblob import TextBlob
import numpy as np


def f():

    if not os.listdir(DATA_DIRECTORY):
        print('No input video files are present. Please put your files in the '
              '"data" directory, rebuild the image and run the container.')

    tweets_all = []
    tweets = []

    columns = []
    for i, file in enumerate(os.listdir(DATA_DIRECTORY)):

        columns.append(file)

        with open(DATA_DIRECTORY+"/"+file, "r") as user:

            # print(file)

            q2 = csv.reader(user, delimiter=';')
            for i, line in enumerate(q2):

                # print(line)

                if i != 0:
                    tweets.append(line[2])

            tweets_all.append(tweets)
            tweets = []

    confCluster = SparkConf().setAppName("Sentiment")
    sc = SparkContext(conf=confCluster)

    pdf = pd.DataFrame(tweets_all, columns)

    spark = SparkSession.builder.appName('Sentiment').getOrCreate()
    df = spark.createDataFrame(pdf)

    return df


def main():
    sc = SparkContext(appName="PythonPi")
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    time_dict = {}
    tic1 = int(round(time.time() * 1000))
    count = sc.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi = %f" % (4.0 * count / n))

    tac1 = int(round(time.time() * 1000))
    time_dict['TIME: '] = tac1 - tic1
    print(time_dict)
    sc.stop()


DATA_DIRECTORY = 'data_sanitized'
print(f())
