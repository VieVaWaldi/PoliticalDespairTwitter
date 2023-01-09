import nltk
from nltk.corpus import stopwords
from cleantext import clean
import csv
import re
import os

nltk.download("stopwords")
stop_words = set(stopwords.words("english"))

# regexWebURLPattern = re.compile(
#     """https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)""")


def sorted_alphanumeric(data):
    """
    Sorts alphanumerically.
    :param data: data to be sorted e.g. a list
    :return: sorted data alphanumerically e.g. "User_2" before "User_10"
    """
    def convert(text): return int(text) if text.isdigit() else text.lower()
    def alphanum_key(key): return [convert(c)
                                   for c in re.split('([0-9]+)', key)]
    return sorted(data, key=alphanum_key)


def ASCIIFilter(tweet):
    ASCIITweet = ""
    for c in tweet:
        if ord(c) > 127:
            #not ASCII
            continue
        else:
            ASCIITweet += c
    return ASCIITweet


def is_url(word):
    if word.find("http://") == 0 or word.find("https://") == 0:
        return True


def is_sonderfall(word):
    if word == "&amp;":
        return True
    return False


def tweetDecomposer(tweet):

    # Remove bas last line
    if tweet.find("No more data. finished scraping!!") == 0:
        return

    # Remove emojis
    clean(tweet, no_emoji=True)

    # Seperate by word and stop on too few lines
    tweet = tweet.lower()
    tweetWords = tweet.split()
    if len(tweetWords) < 4:
        return

    # remove tweet ID
    tweetWords = tweetWords[1:]

    date = tweetWords[0]
    time = tweetWords[1]
    timezone = tweetWords[2]

    # remove time/timezone and tweet username
    tweetWords = tweetWords[4:]

    mentions = []
    hashtags = []
    text = ""

    # removing stopwords
    tweetWords = " ".join([word for word in tweetWords
                           if word not in stop_words]).split()

    for tweetWord in tweetWords:

        # Remove All Emojis
        # clean(tweetWord, no_emoji=True)
        if len(tweetWord) == 0:
            continue

        # Remove URL
        if is_url(tweetWord):
            continue

        if is_sonderfall(word):
            continue

        # Annotations
        elif tweetWord[0] == "@":
            mentions.append(tweetWord[1:])
        # Hashtags
        elif tweetWord[0] == "#":
            hashtags.append(tweetWord[1:])
        else:
            text += tweetWord + " "
    return date, time, text, mentions, hashtags


def get_new_user_name(user_file):
    new_name = ""
    with open(f"user_list.csv", 'r', newline='') as file:
        for line in file:
            entries = line.split(',')
            if entries[2] == user_file:
                new_name = f"{entries[0]}{entries[1]}_{entries[4]}"
                new_name = new_name.strip("\n")
                return new_name


def save_sanitized_file(user_file, path, sanitized_tweets):

    new_name = get_new_user_name(user_file)

    with open(f".{path}{new_name}", 'w', newline='') as file:

        writer = csv.writer(file)
        writer.writerow(["Date", "Time", "Text", "Mentions", "Hashtags"])

        for san_tweet in sanitized_tweets:
            if san_tweet == None:
                continue
            writer.writerow([san_tweet[0], san_tweet[1],
                            san_tweet[2], san_tweet[3], san_tweet[4]])


directory = '/data/'
directory_sanitized = '/data_sanitized/'

path = os.getcwd()+directory
user_file_names = sorted_alphanumeric(os.listdir(path))

current_user_sanitized = []

for user_file in user_file_names:
    with open(path + user_file, 'r') as file:
        for line in file:
            for word in line.split("\n\n"):
                current_user_sanitized.append(tweetDecomposer(word))
        save_sanitized_file(user_file, directory_sanitized,
                            current_user_sanitized)
        current_user_sanitized = []
