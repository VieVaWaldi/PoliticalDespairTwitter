# Political Despair Twitter

## Get data
wallteeeeer

Library to get more from the API, more tweets and with a larger time line -> Optimized-Modified-GetOldTweets3-OMGOT

TweetFormat:
ID YYY-MM-DD HH:MM:SS +TZTZ <USERNAME> TWEET

### Sanitize

What can be in a tweet:
- text (punctuations, white space)
- url http:// or https://
- emoticon
- #hashtag
- @user

Techniques:
- all lower case
- Stemming to retain only the stem of a word (eating -> eat)

### New Format

File with UserName and Party
-> DATE, TIME, TWEET, #HASHTAGS (seperate), @mentions 

## Spark

### Easy Questions (with easy visualization):

- How many tweets per user
- Mentions per user
- Hashtags per User
- Tweet Length comparison
- Most used words by user and by party -> Easy visualization using word clouds

### More complicated analysis:

- sentiment analysis using TextBlob with 2 measure for each tweet: polarity (-1 negative sentiment, 1 positive sentiment) and subjectivity (0 is factual statment, 1 is personal opinion). Compare these by user and or parties.
- The same thing over time
- Sentiment filtered out for specific keywords (eg if a tweet uses a country like germany, or a company or other users)

## Command Line Arguments Optimized-Modified-GetOldTweets3-OMGOT

This package was optimized to work efficiently and seamlessly on both Windows Command prompt (CMD), and on UNIX Terminal. Below are some command line arguments which is by no means exhaustive. run `python cli.py --help` in terminal to get the full argument options.

  - username (**str**): An optional specific username from a twitter account. Without "@".
  - since (**str. "yyyy-mm-dd"**): A lower bound date to restrict search.
  - until **(str. "yyyy-mm-dd"**): An upper bound date to restrist search.
  - search (**str**): A query text to be matched.
  - near(**str**): A reference location area from where tweets were generated
  - csv: Write as a .csv file
  - json: Write as a .json file
  - count: Display the number of tweets scraped at the end of the session
  - year: Filter a tweet before a specified year
