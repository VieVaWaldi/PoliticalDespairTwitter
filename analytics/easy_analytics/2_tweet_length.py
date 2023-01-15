# export SPARK_LOCAL_IP="127.0.0.1"

from pyspark import SparkContext, SparkConf

confCluster = SparkConf().setAppName("TweetLengthPerUser")
sc = SparkContext(conf=confCluster).getOrCreate()


def get_columns(tweet):
    return tweet.split(';')


def analysis_tweet_length_per_user():
    """
        2 Average tweet length per user
        Output: (user, party, avg_tweet_length) for all tweet

        Sorry this is really ugly. I combine 2 reduces to calculate the average.
    """

    text_file = sc.textFile("data_sanitized")

    rdd_tweet_length = text_file.map(
        lambda x: [get_columns(x)[0], get_columns(x)[1], get_columns(x)[4]]).map(
        # user_part_tweet_list
        lambda u_p_t_list: ((u_p_t_list[0], u_p_t_list[1]), len(
            u_p_t_list[2].split(' '))-1)).reduceByKey(lambda a, b: a+b).sortBy(
        lambda x: x[0][0]).sortBy(
        lambda x: x[0][1]).collect()
    # Output of this rdd is ((user, party), sum_anzahl_tweet_länge)

    rdd_tweet_count = text_file.map(
        lambda x: [get_columns(x)[0], get_columns(x)[1], get_columns(x)[4]]).map(
        # user_part_tweet_list
        lambda u_p_t_list: ((u_p_t_list[0], u_p_t_list[1]), 1)).reduceByKey(lambda a, b: a+b).sortBy(
        lambda x: x[0][0]).sortBy(
        lambda x: x[0][1]).collect()
    # Output of this rdd is ((user, party), sum_anzahl_tweets)

    # Output
    for i in range(len(rdd_tweet_length)):

        ((a1, b1), c1) = rdd_tweet_length[i]
        ((a2, b2), c2) = rdd_tweet_count[i]

        print(a1, b1, c1/c2)

        name = a1
        party = b1
        avg_tweet_length_per_user = c1/c2

        # @Kenny here is you data


# RUN ANALYSIS #

analysis_tweet_length_per_user()
