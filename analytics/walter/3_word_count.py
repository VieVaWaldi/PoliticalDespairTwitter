# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf

confCluster = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=confCluster)


def get_columns(tweet):
    return tweet.split(';')


def name_and_word(name, word_list):
    name_and_word = []
    for w in word_list:
        name_and_word.append((name, w))
    return name_and_word


text_file = sc.textFile("data_sanitized")  # /A._McEachin.csv

# 3_1 Wordcount per user
rdd = text_file.map(lambda line: [get_columns(line)[0], get_columns(line)[4]]).flatMap(
    lambda user_text: name_and_word(user_text[0], user_text[1].split(' '))
).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()


for i in rdd:
    ((a, b), c) = i
    if int(c) > 500:
        print(a, b, c)
