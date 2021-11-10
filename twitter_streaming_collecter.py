import json
from datetime import datetime 
import pandas as pd
import argparse
import boto3
import os
import itertools

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from urllib3.exceptions import ProtocolError

class StdOutListener(StreamListener):

    def __init__(self, duration):
        self.start_date = datetime.utcnow()
        self.duration = duration
        self.stop = False
        self.data = []

    def on_data(self, data):
        tweet = json.loads(data)
        self.data.append(tweet)
        if (datetime.utcnow() - self.start_date).total_seconds() > self.duration:
            self.stop = True
            return False
        return True

    def on_error(self, status):
        if (datetime.utcnow() - self.start_date).total_seconds() > self.duration:
            self.stop = True
            return False
        return True

def set_job():
    parser = argparse.ArgumentParser(description='Collect tweets')
    parser.add_argument('--duration', type=int,  help='Number of seconds before to stop the collecter', default=300)
    parser.add_argument('--configuration', type=str,  help='Configuration file for the job', default="./configuration.json")
    parser.add_argument('--candidates', type=str,  help='Configuration file for the job', default="./candidates.json")

    args = parser.parse_args()
    duration = args.duration

    with open(args.configuration) as f:
        configuration = json.load(f)

    with open(args.candidates) as f:
        candidates = json.load(f)

    return duration, configuration, candidates

file_extension = ".csv.gz"

if __name__ == '__main__':
    duration, configuration, candidates = set_job()
    print(datetime.utcnow())
    print(f'Will save the tweets for the next {duration} sec')
    print(candidates)

    filters = [[item["name"]] + item["twitter_account"] for key, item in candidates.items()]
    filters = list(itertools.chain.from_iterable(filters))
    filters = list(dict.fromkeys(filters))
    print("Filters:", filters)

    collecter = StdOutListener(duration)
    auth = OAuthHandler(configuration["twitter"]["consumer_key"], configuration["twitter"]["consumer_secret"])
    auth.set_access_token(configuration["twitter"]["access_token"], configuration["twitter"]["access_token_secret"])
    stream = Stream(auth, collecter)

    while not collecter.stop:
        try:
            stream.filter(track=filters, languages=["en","fr"])
        except ProtocolError:
            continue

    dfp_tweets = pd.DataFrame(collecter.data)
    file_name = collecter.start_date.strftime('%Y%m%d_%H%M%S') + file_extension
    dfp_tweets.to_csv("tmp" + file_extension, index=None)

    s3_client = boto3.client('s3', aws_access_key_id=configuration["aws"]["key"], aws_secret_access_key=configuration["aws"]["secret"])
    partition = collecter.start_date.strftime('%Y%m%d')
    response = s3_client.upload_file("tmp" + file_extension, configuration["aws"]["bucket"], f'data/raw/twitter/{partition}/{file_name}')
    print(datetime.utcnow())
    print('DONE')