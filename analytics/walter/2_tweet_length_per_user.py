# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf

confCluster = SparkConf().setAppName("TweetsPerUser")
sc = SparkContext(conf=confCluster)


def get_columns(tweet):
    return tweet.split(';')


def sort_Tuple(tup):

    # getting length of list of tuples
    lst = len(tup)
    for i in range(0, lst):

        for j in range(0, lst-i-1):
            if (tup[j][1] > tup[j + 1][1]):
                temp = tup[j]
                tup[j] = tup[j + 1]
                tup[j + 1] = temp
    return tup


text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

rdd = text_file.map(lambda x: get_columns(x)[0]).map(
    lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()

sorted = sort_Tuple(rdd)
cnt = 0
for user, tweets in sorted:
    cnt += 1 
    print(cnt, user, tweets)
