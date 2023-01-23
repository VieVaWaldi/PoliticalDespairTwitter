# Political Despair Twitter

## Presentation

- [ ] WIP

- Monday
- 5 min for each plus questions
- More is worse than less

## Get data 

- [x] Done

Library to get more from the API, more tweets and with a larger time line -> Optimized-Modified-GetOldTweets3-OMGOT

TweetFormat:

ID YYY-MM-DD HH:MM:SS +TZTZ <USERNAME> TWEET

### Sanitize

- [x] Done

What can be in a tweet:
- text (punctuations, white space)
- url http:// or https://
- emoticon
- #hashtag
- @user
- all lower case
- remove all stop words with nltk
- user and party in line
- remove hashtags and annotations from tweettext

**New Format**

File with UserName and Party

user; party; date; time; tweet; #h1, #h2; @1, @1

## Spark

#### Easy Questions (with easy visualization):

- [x] How many tweets per user

- [x] Tweet Length comparison per user

- [x] Word count per user -> Word cloud visualization
- [x] Word count per party
- [x] This in a time frame (since ukraine war, ...)

- [x] Most used annotations per user
- [x] Most used annotations sorted by party
- [x] This in a time frame (...)

- [x] Most used hashtags per user
- [x] Most used hashtags sorted by party
- [x] This in a time frame (...)


### More complicated analysis:

Sentiment analysis using TextBlob with 2 measures for each tweet: polarity (-1 negative sentiment, 1 positive sentiment) and subjectivity (0 is factual statment, 1 is personal opinion).

- [x] Sentiment Analysis (user, party, polarity, subjectivity): for each tweet

- [x] Sentiment Analysis (user, party, polarity, subjectivity): on avg for user

- [x] with filter for time

- [ ] with filter for category 
- [ ] sentiment only for one specific hashtag
- [ ] sentiment only for all countries
- [ ] sentiment only for specific country like germany
- [ ] sentiment only for companies or specific company

- ? specific timeframe for tweet analysis -> eg bumps in sentiment towards special dates like election etc
