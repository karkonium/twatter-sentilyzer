import io
import sys
import pandas as pd
from __future__ import print_function

from services import upload_file, get_tweets
from analyses import get_emotion, get_sentiment


CRYPTO_NAME_TICKER = [("Bitcoin", "BTC"),
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


def create_tweets_df(start_date, end_date, max_tweets_per_coin_per_hour = 100):
    daterange = pd.date_range(start_date, end_date, freq="1H").map(pd.Timestamp.timestamp).map(int)
    rows = []

    num_intervals = len(range(len(daterange) - 1, 0, -1))
    print("Number of hourly intervals: ", num_intervals)
        
    for i in range(len(daterange) - 1, 0, -1):
        for coin_name, coin_ticker in CRYPTO_NAME_TICKER:
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


def calculate_sentiment(data):
    start_time = time()

    results_df = pd.DataFrame(data['content'])
    results_df['sentiment_results'] = results_df['content'].apply(sentiment)
    results_df['emotion_results'] = results_df['content'].apply(get_emotion)

    results_df = results_df.join(pd.json_normalize(results_df['sentiment_results']))
    results_df = results_df.join(pd.json_normalize(results_df['emotion_results']))

    results_df.drop(['sentiment_results', 'emotion_results'], axis=1, inplace=True)

    enhanced_data = pd.concat([data, results_df.drop('content', axis=1)], axis=1)

    elapsed_time = time() - start_time
    print(f"Processed {len(data)} records in {elapsed_time:.2f} seconds.")

    return enhanced_data


if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: `python3 main.py <start-date>`<end-date>", 
            "dates must be in YYYY-MM-DD format", 
            "start-date must be earlier than end-date", 
            "start-date is inclusive, end-date is exclusive", 
            sep="\n\t")
        sys.exit()
        
    start_date, end_date = sys.argv[1], sys.argv[2]
    print(f"start date (inclusive): {start_date}, end date (exclusive): {end_date}")
    
    # get tweets
    tweets_df = create_tweets_df(start_date, end_date)

    # get sentiment
    sentiment_df = calculate_sentiment(tweets_df)

    file_name = f"{start_date}:{end_date}--sentiment.plk"
    print(f"file created: {file_name}")
    sentiment_df.to_pickle(file_name)

    # upload sentiment to google drive
    creds = Credentials.from_authorized_user_info({
        "refresh_token": REFRESH_TOKEN, 
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })
    service = build('drive', 'v3', credentials=creds)
    uploaded_file_id = upload_file(service, file_name, file_name)

    print(f"{file_name} cleaned and uploaded with file id: {uploaded_file_id}")

    # delete once uplaoded -- maybe idk
