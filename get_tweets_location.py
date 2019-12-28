import socket
import sys
import requests
import requests_oauthlib
import json


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130, -20, 100, 50'),('track', '#')]
    #query_data = [('locations', '-122.75, 36.8, -121.75, 37.8, -74, 40, -73, 41'), ('track', '#')] # this location value is San Francisco & NYC
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(str(line, 'utf-8'))
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(bytes(tweet_text+'\n','utf-8'))
        except:
           e = sys.exc_info()
           print("Error 1: %s" % e[0])
           print("Error 2: %s" % e[1])
           print("Error 3: %s" % e[2])
           print("Error 4: %s" % e[2].tb_lineno)

# Replace this file with yours
twitter_creds_json_file = 'twitter_creds.json'

with open(twitter_creds_json_file) as data_file:
    TWITTER_CREDENTIAL = json.load(data_file)

ACCESS_TOKEN = TWITTER_CREDENTIAL['access_token']
ACCESS_SECRET = TWITTER_CREDENTIAL['access_token_secret']
CONSUMER_KEY = TWITTER_CREDENTIAL['consumer_key']
CONSUMER_SECRET = TWITTER_CREDENTIAL['consumer_secret']

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection...")
#nc -lk 9999
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
