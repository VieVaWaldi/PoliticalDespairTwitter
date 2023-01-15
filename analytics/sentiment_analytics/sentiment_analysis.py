# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
from textblob import TextBlob as tb

confCluster = SparkConf().setAppName("TweetsPerUser")
sc = SparkContext(conf=confCluster).getOrCreate()


def get_col(tweet):
    return tweet.split(';')


def polarity(tweet_text):
    testimonial = tb(tweet_text)
    return round(testimonial.sentiment.polarity, 2)


def subjectivity(tweet_text):
    testimonial = tb(tweet_text)
    return round(testimonial.sentiment.subjectivity, 2)


def sentiment_analysis_simple():
    """
        Output: (user, party, date, polarity, subjectivity): for each tweet
    """

    text_file = sc.textFile("data_sanitized_2")  # /A._McEachin.csv

    rdd = text_file.map(
        lambda x: [get_col(x)[0], get_col(x)[1], get_col(x)[2], get_col(x)[4]])
    # > [user, party, date, text]

    rdd = rdd.map(
        lambda x: (x[0], x[1], x[2], sentiment(x[3]))).collect()
    # > (user, party, date, text (sub, pol))

    # .filter(
    # lambda party_date: date_greater_than(
    #     party_date[1], date_after)).map(
    # lambda word: ((word[0], word[1]), 1)).reduceByKey(
    #     lambda a, b: a + b).sortBy(
    #         lambda x: x[1]).sortBy(
    #             lambda x: x[0][1]).collect()

    for i in rdd:
        print(i)
        # ((a, b), c) = i
        # print(a, b, c)
        # @Kenny here you can export ur data


sentiment_analysis_simple()
