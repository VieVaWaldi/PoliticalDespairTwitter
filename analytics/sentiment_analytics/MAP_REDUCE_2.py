#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 15 16:18:14 2023

@author: palmo
# export SPARK_LOCAL_IP="127.0.0.1"
"""


from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
from textblob import TextBlob as tb
# import findspark
import operator
# findspark.init()


def get_columns(tweet):
    return tweet.split(';')


def name_and_tweet(name, tweet_list):

    name_and_tweets = []
    global n

    for t in tweet_list:

        # print(t)
        # print(name)
        name_and_tweets.append([name, t])

    return name_and_tweets


def sentiment(tweets: list):

    sentiment_per_tweet = []
    polarity_per_tweet = []
    # print(tweets)

    testimonial = tb(tweets[1])

    sub = testimonial.sentiment.subjectivity
    pol = testimonial.sentiment.polarity

    sentiment_per_tweet = (tweets[0], sub, pol)
    #sentiment_per_tweet = (tweets[0],sub)
    #polarity_per_tweet  = (tweets[0],pol)

    return sentiment_per_tweet


def mittelwert(sentiment: list):

    name = sentiment[0][0]
    sentiment_per_user = {}
    sum_s = 0
    sum_p = 0
    n = 0

    for sent in sentiment:

        if name == sent[0]:

            sum_s += sent[1]
            sum_p += sent[2]
            n += 1

        elif name != sent[0]:

            sentiment_per_user[name] = [sum_s/n, sum_p/n]

            # Reset der Variablen
            sum_s = 0
            sum_p = 0
            n = 0
            name = sent[0]

            # Da wir hier schon einen anderen Namen haben müssen wir
            # die obere Berechnung hier auch ausführen
            # sonst lassen wir immer den ersten Wert weg
            sum_s += sent[1]
            sum_p += sent[2]
            n += 1

    return sentiment_per_user


def avg_map(row):
    return (row[1], (row[2], 1))


def avg_reduce_func(value1, value2):
    return (value1[0], (value1[1][0]+value1[1][1]), (value2[2][0] + value2[2][1]))


def main(dir='data_sanitized_2'):

    confCluster = SparkConf().setAppName("Sentiment").setMaster("local[6]")
    sc = SparkContext.getOrCreate(conf=confCluster)
    text_files = sc.textFile(dir)

    data_rdd = text_files.map(lambda line: [get_columns(line)[0], get_columns(line)[4]]
                              ).map(lambda user_text: sentiment(user_text)).collect()

    print(data_rdd[1])

    rdd1 = sc.parallelize(data_rdd)

    # SAMPLE OUTPUT of countsByKey.value: {u'2013-09-09': 215, u'2013-09-08': 69, ... snip ...}
    countsByKey = sc.broadcast(rdd1.countByKey())
    # Calculate the numerators (i.e. the SUMs).
    rdd1 = rdd1.reduceByKey(operator.add)
    rdd1 = rdd1.map(lambda x: (x[0], x[1]/countsByKey.value[x[0]])).collect()

    #data = rdd.groupByKey().mapValues(lambda x,y: sum(x) / len(x)).collect()

    # print(data[1])
    #data = reduce(lambda a,b: a+b)

    # Funktioniert aber ohne ein reduce von Sparks
    # Ist ehr ein handgeschriebenes reduceByKey
    """
    rdd = text_files.map(lambda line: [get_columns(line)[0],get_columns(line)[4]]
          ).map(lambda user_text: sentiment(user_text)).collect()
    rdd_n = mittelwert(rdd)    
    
    print(rdd_n['A._McEachin'])
    """

    # print(rdd[1])

    print()
    sc.stop()


main(dir='data_sanitized_2')
