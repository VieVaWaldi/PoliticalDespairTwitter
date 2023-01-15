# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
import datetime


confCluster = SparkConf().setAppName("AnnotationCount")
sc = SparkContext(conf=confCluster)


def get_columns(tweet):
    return tweet.split(';')


def name_and_annotation(name, annotation_list):
    name_and_annotation = []
    for w in annotation_list:
        name_and_annotation.append((name, w))
    return name_and_annotation


def annotation_count_per_user():
    """
        Analytic 4_1
    """
    text_file = sc.textFile("data_sanitized_2")  # /A._McEachin.csv

    # 4_1 annotationcount per user
    rdd = text_file.map(lambda line: [get_columns(line)[0], get_columns(line)[6]]).flatMap(
        lambda user_text: name_and_annotation(
            user_text[0], user_text[1].split(','))
    ).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=name_lastname, b=word) c=annotationcount)
    # This is sorted by user and by annotationcount
    for i in rdd:
        ((a, b), c) = i
        print(a, b, c)


def annotation_count_per_party():
    """
        Analytic 4_2 
    """

    text_file = sc.textFile("data_sanitized_2")  # /A._McEachin.csv

    # 4_2 annotationcount per party
    rdd = text_file.map(lambda line: [get_columns(line)[1], get_columns(line)[6]]).flatMap(
        lambda user_text: name_and_annotation(
            user_text[0], user_text[1].split(','))
    ).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=party, b=word) c=annotationcount)
    # This is sorted by party and by annotationcount
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


def annotation_count_per_party_after_date(date_after):
    """
        Analytic 4_3
    """

    text_file = sc.textFile("data_sanitized_2")  # /A._McEachin.csv

    # 4_3 annotationcount per party, after date
    rdd = text_file.map(lambda line: [get_columns(line)[1], get_columns(line)[2], get_columns(line)[6]]).filter(
        lambda party_date_text: date_greater_than(
            party_date_text[1], date_after)
    ).flatMap(
        lambda user_text: name_and_annotation(
            user_text[0], user_text[2].split(','))
    ).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    # Output ((a=party, b=word) c=annotationcount)
    # This is sorted by party and by annotationcount
    # And filtered for tweets after the given date.
    for i in rdd:
        ((a, b), c) = i
        print(a, b, c)


# Annotations are other users mentioned in a tweet
annotation_count_per_user()
# annotation_count_per_party()
# annotation_count_per_party_after_date('2022-03-01')
