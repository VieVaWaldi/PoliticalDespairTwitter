# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 13:31:24 2023

@author: Chris
"""
# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf
from textblob import TextBlob as tb


"""
def helper():
    
    if not os.listdir(DATA_DIRECTORY):
        print('No input video files are present. Please put your files in the '
              '"data" directory, rebuild the image and run the container.')
    
    tweets_all = []
    tweets = []
    
    columns = []
    
    for i, file in enumerate(os.listdir(DATA_DIRECTORY)):
        
        columns.append(file)
        
        with open(DATA_DIRECTORY+"/"+file,"r") as user:
            
            #print(file)
            
            q2 = csv.reader(user, delimiter=';')
            for i,line in enumerate(q2):
                
                print(line)
                
                if i != 0:
                    tweets.append(line[4])
                    

            tweets_all.append(tweets)
            tweets = []
ext(conf=confCluster)

    
    confCluster = SparkConf().setAppName("Sentiment")
    sc = SparkCont
    pdf = pd.DataFrame(tweets_all, columns)

    spark = SparkSession.builder.appName('Sentiment').getOrCreate()
    df = spark.createDataFrame(tweets_all, columns)
    #df = spark.createDataFrame(pdf)
    
    return df
"""


def get_columns(tweet):
    return tweet.split(';')


def name_and_tweet(name, tweet_list):

    name_and_tweets = []

    for t in tweet_list:
        name_and_tweets.append((name, t))

    return name_and_tweets


def sentiment(tweets: list):

    sentiment_per_tweet = []

    for tweet in tweets:

        testimonial = tb(tweet[1])

        sub = testimonial.sentiment.subjectivity
        pol = testimonial.sentiment.polarity

        sentiment_per_tweet.append([tweet[0], sub, pol])

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


def main():

    confCluster = SparkConf().setAppName("Sentiment")
    sc = SparkContext(conf=confCluster)
    text_files = sc.textFile("data_sanitized")  # /A._McEachin.csv

    #rdd = text_files.map(lambda line: [get_columns(line)[1], get_columns(line)[4]]).collect()

    rdd = text_files.map(lambda line: [get_columns(line)[1], get_columns(line)[4]]
                         ).map(lambda user_text: sentiment(user_text)).reduce(lambda sentiment: mittelwert(sentiment)).collect()

    # .map(lambda word: (word, 1)
    #      ).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).sortBy(lambda x: x[0][0]).collect()

    print(rdd)
    print()
    sc.stop()


DATA_DIRECTORY = 'data_sanitized'
main()
"""
t = helper()

select_df = t.select("BradSchneider_D")
print(select_df)
print()
print(select_df[0])
#print(t['AdrianoEspaillat_D'])

print()
"""
