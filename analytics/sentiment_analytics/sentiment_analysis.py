# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
from textblob import TextBlob as tb

import datetime
import csv
import time

confCluster = SparkConf().setAppName("TweetsPerUser")
sc = SparkContext(conf=confCluster).getOrCreate()


def get_col(tweet):
    return tweet.split(';')


def get_list_of_countries():
    countries = []
    file = open('analytics/sentiment_analytics/list_of_countries.txt')
    for line in file:
        countries.append(line.strip('\n').lower())
    return countries


def is_date_between(date, date_start='1999-01-01', date_end='2100-01-01'):
    try:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        date_start = datetime.datetime.strptime(date_start, '%Y-%m-%d')
        date_end = datetime.datetime.strptime(date_end, '%Y-%m-%d')
    except:
        return False
    return date_start < date and date < date_end


def sentiment(tweet_text, sent='pol'):
    if sent == 'pol':
        return polarity(tweet_text)
    elif sent == 'sub':
        return subjectivity(tweet_text)
    else:
        raise Exception('Fuck u')


def polarity(tweet_text):
    testimonial = tb(tweet_text)
    return round(testimonial.sentiment.polarity, 2)


def subjectivity(tweet_text):
    testimonial = tb(tweet_text)
    return round(testimonial.sentiment.subjectivity, 2)


def contains_hashtag(hashtags, category):
    for word in hashtags.split(','):
        w = word.strip()
        c = category.strip()
        if w == c:
            return True
    return False


def contains_category(tweet, category):
    for word in tweet.split(' '):
        for cat in category:
            if word == cat:
                return True
    return False


def sentiment_block(category, category_hashtag, date_start, date_end, sent):
    """
        This is the MapReduce part.
        Output: (user, party, polarity): for each tweet
        Is zipped in sentiment_analysis.
    """

    text_file = sc.textFile("data_sanitized")

    rdd = text_file.map(
        lambda x: [get_col(x)[0], get_col(x)[1], get_col(x)[2], get_col(x)[4], get_col(x)[5]])
    rdd_count = text_file.map(
        lambda x: [get_col(x)[0], get_col(x)[1], get_col(x)[2], get_col(x)[4], get_col(x)[5]])
    # > [user, party, date, text, hashtags]

    rdd = rdd.filter(
        lambda x: is_date_between(x[2], date_start, date_end))
    rdd_count = rdd_count.filter(
        lambda x: is_date_between(x[2], date_start, date_end))
    # > same data filtered after date

    if category_hashtag is not None:
        rdd = rdd.filter(lambda x: contains_hashtag(x[4], category_hashtag))
        rdd_count = rdd_count.filter(
            lambda x: contains_hashtag(x[4], category_hashtag))

    if category is not None:
        rdd = rdd.filter(lambda x: contains_category(x[3], category))
        rdd_count = rdd_count.filter(
            lambda x: contains_category(x[3], category))

    rdd = rdd.map(
        lambda x: ((x[0], x[1]), sentiment(x[3], sent=sent)))
    rdd_count = rdd_count.map(
        lambda x: ((x[0], x[1]), 1))
    # > (user, party, text, sentiment)

    rdd = rdd.reduceByKey(lambda a, b: a+b)
    rdd_count = rdd_count.reduceByKey(lambda a, b: a+b)
    # > same data reduced for sentiment

    rdd = rdd.sortBy(lambda x: x[0][0]).sortBy(lambda x: x[0][1]).collect()
    rdd_count = rdd_count.sortBy(lambda x: x[0][0]).sortBy(
        lambda x: x[0][1]).collect()
    # > same data sorted by user, then party

    # Output
    output = []
    for i in range(len(rdd)):
        ((name, party), c1) = rdd[i]
        ((a2, b2), c2) = rdd_count[i]
        polartiy = round(c1/c2, 2)
        if name == "User":
            continue
        output.append([name, party, polartiy])
        # print(name, party, polartiy)
    return output


def sentiment_analysis(category, category_hashtag, date_start='1999-01-01', date_end='2100-01-01'):
    start = time.time()
    pol_list_2d = sentiment_block(
        category, category_hashtag, date_start, date_end, sent='pol')
    sub_list_2d = sentiment_block(
        category, category_hashtag, date_start, date_end, sent='sub')

    data_set = []
    for i in range(len(pol_list_2d)):
        if pol_list_2d[i][0] == sub_list_2d[i][0]:
            data_set.append([pol_list_2d[i][0], pol_list_2d[i]
                            [1], pol_list_2d[i][2], sub_list_2d[i][2]])
            print(pol_list_2d[i][0], pol_list_2d[i][1],
                  pol_list_2d[i][2], sub_list_2d[i][2])
    print(f">>> Job Done! Took {time.time()-start} seconds.")
    return data_set

##############################
# >>> Sentiment Analysis <<< #

# >>> 1st Argument. Keep only tweets with words from this list
# >>> IsIgnored when None


# category = None
# category = ['covid', 'covid19']
category = ['google', 'data', 'facebook', 'apple', 'meta']
# category = ['war', 'ukraine']
# category = get_list_of_countries() # I saved a list of all countries

# >>> 2nd Argument. Keep only tweets with this hashtag
# >>> IsIgnored when None

category_hashtag = None
# category_hashtag = 'womensequalityday'

# >>> 3d argument, date_start and date_end. Only tweets from dates inbetween will be kept.
# >>> Set to date_start='1999-01-01', date_end='2100-01-01' if all tweets should be kept.
data_set = sentiment_analysis(category, category_hashtag,
                              date_start='1999-01-01', date_end='2100-01-01')


# >>> Saves aoutamically, just give it a good name lol, have fun :)
file_name = "Hi_KENNY"
# Write initial header of the CSV file
with open(f'data_preprocessed/sentiment_{file_name}.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Name", "Party", "Polarity", "Subjectivity"])
    f.close()

# Write the content of the CSV file
with open(f'data_preprocessed/sentiment_{file_name}.csv', 'a', newline='') as f:
    writer = csv.writer(f)
    for line in data_set:
        user, party, pol, sub = line
        writer.writerow([user, party, pol, sub])
    f.close()
