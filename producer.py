from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from dotenv import load_dotenv
from textblob import TextBlob
import os
import json
import re

load_dotenv()

access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']

topic_name = os.environ['TOPIC_NAME']
kafka_server = os.environ['KAFKA_SERVER']

producer = KafkaProducer(bootstrap_servers=kafka_server,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


class TwitterAuth:

    def authenticate(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth


class TwitterStreamer:

    def __init__(self):
        self.twitterAuth = TwitterAuth()

    def stream_tweets(self):
        while True:
            listener = MyListener()
            auth = self.twitterAuth.authenticate()
            stream = Stream(auth, listener)
            stream.filter(languages=['en'], track=['Trump'])


class MyListener(StreamListener):

    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_status(self, status):
        if status.retweeted:
            return True
        tweet = self.parse_tweet(status)
        producer.send(topic_name, tweet)
        return True

    def parse_tweet(self, status):
        text = self.de_emojify(status.text)
        sentiment = TextBlob(text).sentiment

        tweet = {
            "created_at": str(status.created_at),
            "text": text,
            "hashtags": self.extract_hashtags(text),
            "polarity": sentiment.polarity,
            "subjectivity": sentiment.subjectivity,
            "user_id": status.user.id,
            "user_name": status.user.name,
            "user_location": self.de_emojify(status.user.location),
            "user_description": self.de_emojify(status.user.location),
            "user_verified": status.user.verified,
            "user_followers_count": status.user.followers_count,
            "user_statuses_count": status.user.statuses_count,
            "user_created_at": str(status.user.created_at),
            "user_default_profile_image": status.user.default_profile_image,
            "latitude": status.coordinates['coordinates'][0] if status.coordinates else None,
            "longitude": status.coordinates['coordinates'][1] if status.coordinates else None,
        }
        return tweet

    @staticmethod
    def clean_tweet(tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    @staticmethod
    def de_emojify(text):
        return text.encode('ascii', 'ignore').decode('ascii') if text else None

    @staticmethod
    def extract_hashtags(text):
        return re.findall(r"#(\w+)", text)


if __name__ == '__main__':
    streamer = TwitterStreamer()
    streamer.stream_tweets()
