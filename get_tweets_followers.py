import socket
import sys
import requests_oauthlib
import json

import tweepy


def show_tweet(tweet):
    try:
        return ' '.join([tweet.created_at.strftime("%m/%d/%Y, %H:%M:%S"),
        tweet.user.screen_name,
        tweet.text])
    except:
        return '************************ bad or no tweet!************************'

class Listener(tweepy.streaming.StreamListener):
    
    def __init__(self, output_file=sys.stdout):
        super(Listener,self).__init__()
        self.output_file = output_file
        
    def on_status(self, tweet):
        try:
            print(show_tweet(tweet))
            conn.send(bytes(tweet.text+'\n','utf-8'))
        except:
            pass

    def on_error(self, status_code):
        print(status_code)
        return False

def get_followers(user_name, num_of_followers=5000):

  user_followers = []
  for page in tweepy.Cursor(API.followers_ids, screen_name=user_name).pages():
      user_followers.extend(page)
      if len(user_followers) >= num_of_followers:
          break

  tweeters = [str(x) for x in user_followers]

  return tweeters


# Replace this file with yours
twitter_creds_json_file = 'twitter_creds.json'

with open(twitter_creds_json_file) as data_file:
    TWITTER_CREDENTIAL = json.load(data_file)

ACCESS_TOKEN = TWITTER_CREDENTIAL['access_token']
ACCESS_SECRET = TWITTER_CREDENTIAL['access_token_secret']
CONSUMER_KEY = TWITTER_CREDENTIAL['consumer_key']
CONSUMER_SECRET = TWITTER_CREDENTIAL['consumer_secret']

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
API = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection on port: %s" % str(TCP_PORT))
#nc -lk 9999
conn, addr = s.accept()
print("Connected... Starting getting tweets from: " + str(addr))

stream = tweepy.Stream(auth=API.auth, listener=Listener())
tweeters = get_followers('nytimes') # Enter any user

try:
    print('Start streaming.')
    stream.filter(follow=tweeters)
except KeyboardInterrupt:
    print("Stopped.")
finally:
    stream.disconnect()
    print('Done.')
