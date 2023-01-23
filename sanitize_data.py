from nltk.corpus import stopwords
from cleantext import clean

import time
import nltk
import csv
import re
import os

nltk.download("stopwords")
STOP_WORDS = set(stopwords.words("english"))


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


def remove_url(word):
    if word == "&amp;":
        return True
    if word.find("http://") == 0 or word.find("https://") == 0:
        return True
    return False


def tweetDecomposer(tweet):

    # Stop on last line
    if tweet.find("No more data. finished scraping!!") == 0:
        return

    # Save, then remove emojis
    # emojis = adv.extract_emoji(tweet)
    tweet = clean(tweet, no_emoji=True)

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
                           if word not in STOP_WORDS]).split()

    for tweetWord in tweetWords:

        if remove_url(tweetWord):
            continue

        # Annotations
        if tweetWord[0] == "@":
            if tweetWord[1:] == " ":
                continue
            annotation = tweetWord[1:].strip()
            annotation = annotation.strip('.')
            mentions.append(annotation)
            continue
        # Hashtags
        if tweetWord[0] == "#":
            if tweetWord[1:] == " ":
                continue
            hashtag = tweetWord[1:].strip()
            hashtag = hashtag.strip('.')
            hashtags.append(hashtag)
            continue

        # remove special characters
        tweetWord = re.sub('[^A-Za-z0-9 ]+', '', tweetWord)

        if len(tweetWord) == 0:
            continue

        text += tweetWord + " "
    if len(text) == 0:
        return None
    return date, time, text, hashtags, mentions


def get_new_user_name(user_file):
    new_name = ""
    with open(f"user_list.csv", 'r', newline='') as file:
        for line in file:
            entries = line.split(',')
            if entries[2] == user_file:
                new_name = f"{entries[0]}_{entries[1]}"
                new_name = new_name.strip("\n")
                party = f"{entries[4]}".strip('\n')
                party = "Democrat" if party == "D" else "Republican"
                return new_name, party


def save_sanitized_file(user_file, path, sanitized_tweets):

    if len(sanitized_tweets) == 0:
        return
    try:
        new_name, party = get_new_user_name(user_file)
    except:
        print('Opssie doopsie f u')
        return

    with open(f".{path}{new_name}.csv", 'w', newline='') as file:

        writer = csv.writer(file, delimiter=";")
        writer.writerow(["User", "Party", "Date", "Time",
                        "Text", "Hashtags", "Mentions"])

        for san_tweet in sanitized_tweets:
            if san_tweet == None:
                continue
            hashtags = ','.join(san_tweet[3])
            annotations = ','.join(san_tweet[4])
            writer.writerow([new_name, party, san_tweet[0], san_tweet[1],
                            san_tweet[2], hashtags, annotations])

start = time.time()
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

print(">>>> JOB DONE, it took " + str(round(time.time() - start, 2)) + " seconds")