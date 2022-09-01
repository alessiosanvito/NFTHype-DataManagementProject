# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 18:15:34 2022

@author: gemelli
"""
import re
import os
import time
import tweepy as tw
import pandas as pd
from tqdm import tqdm, notebook
import numpy as np
from discord_webhook import DiscordWebhook


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

bearer_token = "AAAAAAAAAAAAAAAAAAAAAB1qVwEAAAAAWHZ4UsRT8ZOwzZGFs7x42Ax3XDA%3DjhWbzyKVasOqz6MGtW6YRp2TicLMRNXwBxmj12Le60Fwjugl3v"
consumer_key = 'duiJuWaSClcpb6pBE8MFwP3Ot'
consumer_secret = '2iNYNhqb0ku1fiW3MtEZXoqCZ7wSNGLdXHCaz6UPpXoTtd8zqy'
access_token = '1459140097153056771-pMv7agFChMoGKsdFr5XmoR0cfcOqYW'
access_token_secret = 'Z81ZJexfxiyk01hsDxpDXy6rnzuvVdiSBJf93KYcLbdbu'

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

nftsDataframe = pd.read_json("nft.json", orient="index")
nomeNFT = nftsDataframe['Collection'].to_numpy()


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
    q = ''.join(e for e in query if e.isalnum())
    q = q.replace("  ", ' ')
    print(q)
    tweets = client.search_recent_tweets(query=q, max_results=100)
    currentNFTdataset = fromTweetsToDF(tweets)

    for _ in range(9):
        if 'next_token' in tweets.meta:
            tweets = client.search_recent_tweets(
                query=q, max_results=100, next_token=tweets.meta['next_token'])
            df = fromTweetsToDF(tweets)
            currentNFTdataset = currentNFTdataset.append(df, ignore_index=True)
        else:
            break

        print("Presi i tweet di "+query)
    return currentNFTdataset


# i = 0

# for proj in nomeNFT:
#     try:

#         if os.path.isfile('./data/'+str(i)+'.json'):
#             print(str(i) + "File esistente")
#         else:
#             print(str(i) + "File non esistente")
#             dataframe = getTweets(proj)
#             dataframe.to_json("data/"+str(i)+".json", orient="index")
#             print("Creato il file numero: "+str(i))

#     except Exception as e:

#         if str(e) == "429 Too Many Requests":

#             print("TIMEOUT 15 minuti")

#             time.sleep(15*60)
#         print("ERRORE")

#         data_vuoto = [str(e)]
#         dataframe = pd.DataFrame(data_vuoto, index=["error"])
#         dataframe.to_json("data/"+str(i)+".json", orient="index")

#     i += 1




for i, nft in nftsDataframe.iterrows():
    if nft[0] in error:
        os.remove("data/"+str(i)+".json")
        try:

            if os.path.isfile('./data/'+str(i)+'.json'):
                print(str(i) + "File esistente")
            else:
                print(str(i) + "File non esistente")
                print(nft[0])
                dataframe = getTweets(nft[0])
                dataframe.to_json("data/"+str(i)+".json", orient="index")
                print("Creato il file numero: "+str(i))

        except Exception as e:

            if str(e) == "429 Too Many Requests":

                print("TIMEOUT 15 minuti")

                time.sleep(15*60)
            print("ERRORE")

            data_vuoto = [str(e)]
            dataframe = pd.DataFrame(data_vuoto, index=["error"])
            dataframe.to_json("data/"+str(i)+".json", orient="index")  
        

