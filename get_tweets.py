# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 18:15:34 2022

@author: alessio
"""

import os
import time
import tweepy as tw
import pandas as pd
from tqdm import tqdm, notebook
import numpy as np
import pymongo
from pymongo import MongoClient


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

bearer_token = "AAAAAAAAAAAAAAAAAAAAAB1qVwEAAAAAvQI3qljNpYq4T20ruuI%2FZBOLzyc%3DqvqlQCBgb9N8C7Ed4yoM24s1S5fEt3fK06g5cUxviIa2DBULFH"
consumer_key = 'nDlTImTMQyUpdBnQcMDNBNdP7'
consumer_secret = 'O1HQqenUywF8Iw1nnSJA7WCTSp1wugUMubYTBez9SJkQ5LT7EW'
access_token = '1459140097153056771-48M2pvJR4M1bU2cGD0QJoFhclTNTbB'
access_token_secret = 'crGb0FJd24BNOXaV5Mf6QTrFOCTk3itHqfW88dXjrCYe0'

'''
consumer_api_key = os.environ[TWITTER_CONSUMER_API_KEY]
consumer_api_secret = os.environ["2iNYNhqb0ku1fiW3MtEZXoqCZ7wSNGLdXHCaz6UPpXoTtd8zqy"]
auth = tw.OAuthHandler(consumer_api_key, consumer_api_secret)
api = tw.API(auth, wait_on_rate_limit=True)
'''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


client = tw.Client(bearer_token=bearer_token, consumer_key=consumer_key, consumer_secret=consumer_secret,
                   access_token=access_token, access_token_secret=access_token_secret)

#nftsDataframe = pd.read_json("nft.json", orient="index")
#nomeNFT = nftsDataframe['Collection'].to_numpy()

def fromTweetsToDF(tweets):
    testo = list()
    id = list()
    for tweet in tweets.data:
        testo.append(tweet.text)
        id.append(tweet.id)
        
    data_vuoto = []
    df = pd.DataFrame(data_vuoto)

    df['id'] = id
    df['text'] = testo

    return df
    

def getTweets(query):
    tweets = client.search_recent_tweets(query=query,max_results=100)
    currentNFTdataset = fromTweetsToDF(tweets)

    for _ in range(9):
        if 'next_token' in tweets.meta:
            tweets = client.search_recent_tweets(query=query,max_results=100, next_token=tweets.meta['next_token'])
            df = fromTweetsToDF(tweets)
            currentNFTdataset = currentNFTdataset.append(df, ignore_index=True)
        else:
            break

        print("Presi i tweet di "+query)
    return  currentNFTdataset

#DIR = 'data'
#files = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

#nomeNFT = np.delete(nomeNFT, range(files), axis=None)

i = 0
client = MongoClient('localhost', 27017)
db = client.DataMan
collection = db['nft']
cursor = collection.find({})
#for proj in nomeNFT:
for document in cursor:
    try:
        print(document['Collection'])
        dataframe = getTweets(document['Collection'])
        #dataframe = getTweets(proj)
        dataframe.to_json("data/"+str(i)+".json", orient="index")

    except Exception as e:
        
        if str(e) ==  "429 Too Many Requests":
           
            print("TIMEOUT 15 minuti")

            time.sleep(15*60)
        print("ERRORE")
        
        data_vuoto = []
        dataframe = pd.DataFrame(data_vuoto)
        dataframe.to_json("data/"+str(i)+".json", orient="index")
        

        

    print("Creato il file numero: "+str(i))

    # if i % 300 == 0:
    #     # webhook = DiscordWebhook(url='https://discord.com/api/webhooks/929800123859468318/PEZ8WI4bZU3ywHzzlmRUend84XkeBHI2HSPX1IG76akFnAVwvpdyxShjrFHsPGVHzoJB', content='Lo schiavo ha eseguito ben '+str(i)+' task')
    #     # response = webhook.execute()
    #     # print(response)

    i += 1
          
