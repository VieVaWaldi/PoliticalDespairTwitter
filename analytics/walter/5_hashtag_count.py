# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
import datetime


confCluster = SparkConf().setAppName("hashtagCount")
sc = SparkContext(conf=confCluster)


def get_columns(tweet):
    return tweet.split(';')


def name_and_hashtag(name, hashtag_list):
    name_and_hashtag = []
    for w in hashtag_list:
        name_and_hashtag.append((name, w))
    return name_and_hashtag


def hashtag_count_per_user():
    """
        Analytic 5_1
    """
    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    # 5_1 hashtagcount per user
    rdd = text_file.map(lambda line: [get_columns(line)[0], get_columns(line)[5]]).flatMap(
        lambda user_text: name_and_hashtag(
            user_text[0], user_text[1].split(','))
    ).map(lambda hashtag: (hashtag, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=name_lastname, b=hashtag) c=hashtagcount)
    # This is sorted by user and by hashtagcount
    for i in rdd:
        ((a, b), c) = i
        print(a, b, c)


def hashtag_count_per_party():
    """
        Analytic 5_2 
    """

    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    # 5_2 hashtagcount per party
    rdd = text_file.map(lambda line: [get_columns(line)[1], get_columns(line)[5]]).flatMap(
        lambda user_text: name_and_hashtag(
            user_text[0], user_text[1].split(','))
    ).map(lambda hashtag: (hashtag, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=party, b=hashtag) c=hashtagcount)
    # This is sorted by party and by hashtagcount
    for i in rdd:
        ((a, b), c) = i
        # if int(c) > 10000:
        print(a, b, c)


def date_greater_than(d1, d2='2022-03-01'):
    try:
        d1 = datetime.datetime.strptime(d1, '%Y-%m-%d')
        d2 = datetime.datetime.strptime(d2, '%Y-%m-%d')
    except:
        return False
    return d1 > d2


def hashtag_count_per_party_after_date(date_after):
    """
        Analytic 5_3
    """

    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    # 5_3 hashtagcount per party, after date
    rdd = text_file.map(lambda line: [get_columns(line)[1], get_columns(line)[2], get_columns(line)[5]]).filter(
        lambda party_date_text: date_greater_than(
            party_date_text[1], date_after)
    ).flatMap(
        lambda user_text: name_and_hashtag(
            user_text[0], user_text[2].split(','))
    ).map(lambda hashtag: (hashtag, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=party, b=hashtag) c=hashtagcount)
    # This is sorted by party and by hashtagcount
    # And filtered for tweets after the given date.
    for i in rdd:
        ((a, b), c) = i
        print(a, b, c)


# hashtags are other users mentioned in a tweet
hashtag_count_per_user()
# hashtag_count_per_party()
# hashtag_count_per_party_after_date('2022-03-01')
