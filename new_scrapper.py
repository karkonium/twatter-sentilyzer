from __future__ import print_function

import io
import sys
import pandas as pd
from datetime import datetime, timezone

import snscrape.modules.twitter as sntwitter

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

CLIENT_ID = '519332323596-0p76lcmanu86696pbeuthcet86o63ssn.apps.googleusercontent.com'
CLIENT_SECRET = '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'
REDIRECT_URI = 'https://developers.google.com/oauthplayground'
REFRESH_TOKEN = '1//04iCubh8WnEpRCgYIARAAGAQSNwF-L9IrzDby3VK-pRw7qiLUddfWWkpkWinzasJ0XIeRWj2eLBcYzUEmrNCApbTw_rxJOtDGMZQ'

TWEETS_DIR_ID = '1AqW2M06ipWuqEH_lJgnoUuWRoLEvZEYs'
UPLOAD_DIR_ID = '1mdtBYdep8gL89z7EiCFVvkLvbemJExFz'


creds = Credentials.from_authorized_user_info({
    "refresh_token": REFRESH_TOKEN, 
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
    })

service = build('drive', 'v3', credentials=creds)


def upload_with_conversion(file_name, upload_file_path):
    try:
        file_metadata = {
            'name': file_name,
            # 'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [UPLOAD_DIR_ID]
        }
        media = MediaFileUpload(upload_file_path, resumable=True)
        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='id').execute()
        print(f'File with ID: "{file.get("id")}" has been uploaded.')

    except HttpError as error:
        print(f'An error occurred: {error}')
        file = None

    return file.get('id')



coins = [("Bitcoin", "BTC"),
    ("Bitcoin Cash", "BCH"),
    ("Binance Coin", "BNB"),
    ("EOS.IO", "EOS"),
    ("Ethereum Classic", "ETC"),
    ("Ethereum", "ETH"),
    ("Litecoin", "LTC"), 
    ("Monero", "XMR"),
    ("TRON", "TRX"),
    ("Stellar", "XLM"),
    ("Cardano", "ADA"),
    ("IOTA", "MIOTA"),
    ("Maker", "MKR"),
    ("Dogecoin", "DOGE")
]

twitter_following_limit = 5000 # based on https://help.twitter.com/en/using-twitter/twitter-follow-limit

def is_spam(tweet):
    if tweet.user.verified: 
        return False
    
    return (tweet.user.friendsCount == twitter_following_limit and tweet.user.followersCount < tweet.user.friendsCount) \
        or (tweet.user.friendsCount < tweet.user.followersCount * 0.1) \
        or (tweet.user.statusesCount / (datetime.now(timezone.utc) - tweet.user.created).days > 30) # avgs more than 30 tweets per day 
        # 55 tweets per day too many: https://coschedule.com/blog/how-often-to-post-on-social-media, https://blog.hubspot.com/blog/tabid/6307/bid/4594/is-22-tweets-per-day-the-optimum.aspx


def get_tweets(query, max_num_tweets):
    """ Gets at most max_num_tweets non-spam tweets matching query

    Args:
        query: twitter search query 
                (info on query: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)
        max_num_tweets: maximum number of tweets

    Returns: list of tweets and its data (structure of data: https://miro.medium.com/max/1400/1*b7499m8QPju3AH7WUreP2A.png
                specifically: {url, date, content, renderedContent, id, user, outlinks, tcooutlinks, replyCount, retweetCount, 
                likeCount, quoteCount, converstationId, lang, source, media, retweetedTweet, quotedTweet, mentionedUsers})
             number of spam tweets that were ignored
    """
    tweets_generator = sntwitter.TwitterSearchScraper(query).get_items()
    num_spam_tweets = 0
    tweets_list = []
    for _, tweet in enumerate(tweets_generator):
        if len(tweets_list) >= max_num_tweets: 
            break
        
        if not is_spam(tweet):
            tweets_list.append([tweet.url, tweet.date, tweet.rawContent, tweet.id, tweet.user, tweet.replyCount, 
                            tweet.retweetCount, tweet.likeCount, tweet.quoteCount, tweet.source])
        else:
            num_spam_tweets += 1
    
    return (tweets_list, num_spam_tweets)


def create_tweets_df(start_date, end_date, max_tweets_per_coin_per_hour = 100):
    daterange = pd.date_range(start_date, end_date, freq="1H").map(pd.Timestamp.timestamp).map(int)

    rows = []

    num_intervals = len(range(len(daterange) - 1, 0, -1))
    print("Number of hourly intervals: ", num_intervals)
        
    for i in range(len(daterange) - 1, 0, -1):
        for coin_name, coin_ticker in coins:
            keyword = f'{coin_name} OR {coin_ticker}' # case insenstive
            since_time, until_time = daterange[i - 1], daterange[i]
            tweets, _ = get_tweets(f'{keyword} lang:en since_time:{since_time} until_time:{until_time}', max_tweets_per_coin_per_hour)
            tweet_df = pd.DataFrame(tweets, columns=["url", "date", "content", "id", "user", "replyCount", "retweetCount",
                                        "likeCount", "quoteCount", "source"])
            tweet_df.rename(columns={"id": "tweetId", "url": "tweetUrl", "source": "machineType"}, inplace=True)
            tweet_df.drop_duplicates(subset=["tweetId"], inplace=True)
            
            tweet_df["coinName"] = coin_name
            tweet_df["coinTicker"] = coin_ticker
            rows.append(tweet_df)
        if ((i + 1) % 10) == 0:
            print(f"{i + 1}/{num_intervals} hourly intervals left")
        
        
    df = pd.concat(rows)
    df["followersCount"] = df.apply(lambda e: e["user"].followersCount, axis=1)
    df["friendsCount"] = df.apply(lambda e: e["user"].friendsCount, axis=1)
    df["user"] =  df.apply(lambda e: e["user"].username, axis=1)

    print(f"{len(df)} number of tweets collected")

    return df


import nltk

from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')

# calculate the negative, positive, neutral and compound scores, plus verbal evaluation
def sentiment(sentence):

    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    sentiment_dict = sid_obj.polarity_scores(sentence)
    compound = sentiment_dict['compound']

    if sentiment_dict['compound'] >= 0.05 :
        overall_sentiment = "Positive"

    elif sentiment_dict['compound'] <= - 0.05 :
        overall_sentiment = "Negative"
    else :
        overall_sentiment = "Neutral"
  
    return compound, overall_sentiment


import text2emotion as te

def get_emotion(text):
  return te.get_emotion(text)

import pandas as pd
from time import time

def calculate_sentiment(data):
    loop_range = 100
    average = 0

    all_compounds = []
    all_overall_sentiments = []
    all_happy = []
    all_angry = []
    all_sad = []
    all_fear = []
    all_surprise = []

    loop = 0

    loop_start = time()

    for i, c in enumerate(data["content"]):
        compound, overall_sentiment = sentiment(c)

        all_compounds.append(compound)
        all_overall_sentiments.append(overall_sentiment)

        emotions = get_emotion(c)
        happy = emotions["Happy"]
        angry = emotions["Angry"]
        sad = emotions["Sad"]
        fear = emotions["Fear"]
        surprise = emotions["Surprise"]

        all_happy.append(happy)
        all_angry.append(angry)
        all_sad.append(sad)
        all_fear.append(fear)
        all_surprise.append(surprise)

        loop += 1

        if loop >= loop_range:
            loop_end = time()

            elapsed_time = loop_end - loop_start

            average = elapsed_time / loop_range
            remaining = len(data) - (i + 1)
            estimated_time = remaining * average

            print("{} of {}, est. time remaining: {}s".format(i+1, len(data), round(estimated_time, 0)), flush=True)
            loop = 0

            loop_start = loop_end

    data["sentiment_score"] = all_compounds
    data["sentiment_label"] = all_overall_sentiments
    data["happy"] = all_happy
    data["fear"] = all_fear
    data["angry"] = all_angry
    data["sad"] = all_sad
    data["surprise"] = all_surprise

    return data


if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: `python3 twitter_scrapper_script.py <start-date>`<end-date>", 
            "dates must be in YYYY-MM-DD format", 
            "start-date must be earlier than end-date", 
            "start-date is inclusive, end-date is exclusive", 
            sep="\n\t")
        sys.exit()
        
    start_date, end_date = sys.argv[1], sys.argv[2]
    print(f"start date (inclusive): {start_date}, end date (exclusive): {end_date}")
    
    tweets_df = create_tweets_df(start_date, end_date)
    sentiment_df = calculate_sentiment(tweets_df)

    file_name = f"{start_date}:{end_date}--sentiment.plk"
    print(f"file created: {file_name}")
    sentiment_df.to_pickle(file_name)

    uploaded_file_id = upload_with_conversion(file_name, file_name)

    print(f"{file_name} cleaned and uploaded with file id: {uploaded_file_id}")

    # delete once uplaoded