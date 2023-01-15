# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
import csv
import datetime

confCluster = SparkConf().setAppName("TweetsPerUser")
sc = SparkContext(conf=confCluster).getOrCreate()


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


def analysis_tweets_per_user():

    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    rdd = text_file.map(lambda x: get_columns(x)[0]).map(
        lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()

    sorted = sort_Tuple(rdd)
    cnt = 0
    # Small hack to make the users and tweets into iterables
    tweets_numbers = []
    users = []
    for user, tweets in sorted:
        cnt += 1
        print(cnt, user, tweets)
        users.append(user)
        tweets_numbers.append(tweets)
    return users, tweets_numbers


def analysis_tweets_per_party():

    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    rdd = text_file.map(lambda x: get_columns(x)[1]).map(
        lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()

    sorted = sort_Tuple(rdd)
    cnt = 0
    # Small hack to make the users and tweets into iterables
    tweets_numbers = []
    party = []
    for user, tweets in sorted:
        cnt += 1
        print(cnt, user, tweets)
        party.append(user)
        tweets_numbers.append(tweets)
    return party, tweets_numbers


def date_greater_than(d1, d2='2022-03-01'):
    try:
        d1 = datetime.datetime.strptime(d1, '%Y-%m-%d')
        d2 = datetime.datetime.strptime(d2, '%Y-%m-%d')
    except:
        return False
    return d1 > d2


def analysis_tweets_per_party_with_date(date_after='2022-03-01'):

    text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

    rdd = text_file.map(
        lambda x: [get_columns(x)[1], get_columns(x)[2]]).filter(
        lambda party_date: date_greater_than(
            party_date[1], date_after)).map(
        lambda word: ((word[0], word[1]), 1)).reduceByKey(
            lambda a, b: a + b).sortBy(
                lambda x: x[1]).sortBy(
                    lambda x: x[0][1]).collect()

    for i in rdd:
        ((a, b), c) = i
        print(a, b, c)
        # here you can export ur data


########## Analysis without date

# users, tweets_numbers = analysis_tweets_per_user()
# party, tweets_numbers = analysis_tweets_per_party()

# Write into CSV the result
# with open('data_preprocessed/tweet_per_user.csv', 'w', newline='') as f:
#     writer = csv.writer(f)
#     writer.writerow(["username", "tweet count"])
#     for i in range(len(users)):
#         writer.writerow([users[i], tweets_numbers[i]])


########## Analysis with date
analysis_tweets_per_party_with_date(date_after='2022-03-01')
