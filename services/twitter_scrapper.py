import pandas as pd
from datetime import datetime, timezone

import snscrape.modules.twitter as sntwitter


twitter_following_limit = 5000 # based on https://help.twitter.com/en/using-twitter/twitter-follow-limit

def _is_spam(tweet):
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
        
        if not _is_spam(tweet):
            tweets_list.append([tweet.url, tweet.date, tweet.rawContent, tweet.id, tweet.user, tweet.replyCount, 
                            tweet.retweetCount, tweet.likeCount, tweet.quoteCount, tweet.source])
        else:
            num_spam_tweets += 1
    
    return (tweets_list, num_spam_tweets)