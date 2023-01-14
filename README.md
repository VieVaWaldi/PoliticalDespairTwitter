# Political Despair Twitter

## Presentation

- [ ] WIP

- Monday
- 5 min for each plus questions
- More is worse than less

## Get data 

- [ ] Still missing about 20%

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
-> DATE, TIME, TWEET, #HASHTAGS (seperate), @mentions 

## Spark

#### Easy Questions (with easy visualization):

- [ ] Word Count for all Reps vs all Dems

- How many tweets per user
- Mentions per user
- Hashtags per User
- Tweet Length comparison
- Most used words by user and by party -> Easy visualization using word clouds !


### More complicated analysis:

- sentiment analysis using TextBlob with 2 measure for each tweet: polarity (-1 negative sentiment, 1 positive sentiment) and subjectivity (0 is factual statment, 1 is personal opinion). Compare these by user and or parties.
- The same thing over time
- Sentiment filtered out for specific keywords (eg if a tweet uses a country like germany, or a company or other users)

- tweet france -> parties sentiment towards specific countries of companies

- specific timeframe for tweet analysis -> eg bumps in sentiment towards special dates like election etc

