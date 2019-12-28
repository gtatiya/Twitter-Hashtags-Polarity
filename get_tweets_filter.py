import socket
import sys
import requests_oauthlib
import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads(data)
          print(msg['text'])
          self.client_socket.send(bytes(msg['text']+'\n','utf-8'))
          
          return True
      except:
           e = sys.exc_info()
           print("Error 1: %s" % e[0])
           print("Error 2: %s" % e[1])
           print("Error 3: %s" % e[2])
           print("Error 4: %s" % e[2].tb_lineno)
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['trump']) # Enter any word

# Replace this file with yours
twitter_creds_json_file = 'twitter_creds.json'

with open(twitter_creds_json_file) as data_file:
    TWITTER_CREDENTIAL = json.load(data_file)

ACCESS_TOKEN = TWITTER_CREDENTIAL['access_token']
ACCESS_SECRET = TWITTER_CREDENTIAL['access_token_secret']
CONSUMER_KEY = TWITTER_CREDENTIAL['consumer_key']
CONSUMER_SECRET = TWITTER_CREDENTIAL['consumer_secret']

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection on port: %s" % str(TCP_PORT))
#nc -lk 9999
conn, addr = s.accept()
print("Connected... Starting getting tweets from: " + str(addr))

sendData(conn)
