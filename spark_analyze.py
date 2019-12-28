#!/usr/bin/env python
# coding: utf-8

import json
import re
import sys
import socket
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming import StreamingContext


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(rdd_count, rdd_senti):
    print("------------------ %s ------------------" % datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd_count.context)
        # convert the RDD to Row RDD
        row_rdd_count = rdd_count.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        row_rdd_senti = rdd_senti.map(lambda w: Row(hashtag=w[0], hashtag_sentiment_score=w[1]))
        # create a DF from the Row RDD
        hashtags_df_count = sql_context.createDataFrame(row_rdd_count, ['hashtag','hashtag_count'])
        hashtags_df_senti = sql_context.createDataFrame(row_rdd_senti, ['hashtag','hashtag_sentiment_score'])
        # join two data frames and merge keys into one column
        hashtags_df = hashtags_df_senti.join(hashtags_df_count, 'hashtag', 'outer')
        # Register the dataframe as table
        hashtags_df.createOrReplaceTempView("hashtags_table")
        # get the top 10 hashtags from the table using SQL and print them        
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count, hashtag_sentiment_score from hashtags_table order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
    except:
        e = sys.exc_info()
        print("Error 1: %s" % e[0])
        print("Error 2: %s" % e[1])
        print("Error 3: %s" % e[2])

def give_tweet_polarity(a_tweet):
    a_tweet = PATTERN.findall(a_tweet)
    
    score = 0
    total_words_with_polarity = 0
    for a_word in a_tweet:
        a_word = a_word.lower()
        if a_word in SENTIMENT_DICT:
            score += SENTIMENT_DICT[a_word]
            total_words_with_polarity += 1

    if total_words_with_polarity > 0:
        score = float(score)/total_words_with_polarity
    
    return score

def hashtags_senti(rdd):
    list_of_hashtags = []
    list_of_tweets = rdd.collect()

    for i, a_tweet in enumerate(list_of_tweets):
        words = PATTERN.findall(a_tweet) # Only English
        words = [x.lower() for x in words]

        # Find hashtags and its tweet's polarity
        for a_word in words:
            if a_word.startswith('#'):
                list_of_hashtags.append([a_word, give_tweet_polarity(list_of_tweets[i])])
    
    list_of_hashtags = sc.parallelize(list_of_hashtags) # form an RDD

    # Map each hashtag to be a pair of (hashtag, 1)
    # Reduce to count total hashtags
    hashtags_count = list_of_hashtags.map(lambda word: (word[0], 1)).reduceByKey(lambda x, y: x + y)
    hashtags_senti = list_of_hashtags.map(lambda word: (word[0], round(word[1]))).reduceByKey(lambda x, y: x + y)

    # Process each RDD generated in each interval
    process_rdd(hashtags_count, hashtags_senti)

json_file = 'FINN-165-data.json'
with open(json_file) as data_file:
    SENTIMENT_DICT = json.load(data_file)

PATTERN = re.compile("[#a-zA-Z][a-zA-Z0-9]*")

# Create a local StreamingContext with 2 working thread and batch interval of 5 second
sc = SparkContext("local[2]", "TwitterPolarity")
ssc = StreamingContext(sc, 5)
#ssc.checkpoint("checkpoint_TwitterPolarity")

# Create a DStream that will connect to hostname:port, like localhost:9999
dataStream = ssc.socketTextStream(socket.gethostbyname(socket.gethostname()), 9999)
dataStream = dataStream.window(windowDuration=60, slideDuration=5)
# dataStream.pprint()

tweets = dataStream.flatMap(lambda text: [text])
tweets.foreachRDD(lambda a_tweet: hashtags_senti(a_tweet))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
