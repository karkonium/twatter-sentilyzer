import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

# calculate the negative, positive, neutral and compound scores, plus verbal evaluation
def get_sentiment(sentence):

    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    sentiment_dict = sid_obj.polarity_scores(sentence)
    compound = sentiment_dict['compound']

    if sentiment_dict['compound'] >= 0.05 :
        overall_sentiment = "Positive"
    elif sentiment_dict['compound'] <= - 0.05 :
        overall_sentiment = "Negative"
    else:
        overall_sentiment = "Neutral"
  
    return compound, overall_sentiment