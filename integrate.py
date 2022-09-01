import pandas as pd

nftsDataframe = pd.read_json("nft.json", orient="index")


nftsDataframe = nftsDataframe.astype({"Collection": str})
nftsDataframe["Volume"] = nftsDataframe["Volume"].map(
    lambda x: x.replace('.', ''))
nftsDataframe["Volume"] = nftsDataframe["Volume"].map(
    lambda x: x.replace(',', '.'))
nftsDataframe["Volume"] = pd.to_numeric(nftsDataframe["Volume"])
nftsDataframe["Floor Price"] = nftsDataframe["Floor Price"].map(
    lambda x: '' if x == '---' else x)
nftsDataframe["Floor Price"] = nftsDataframe["Floor Price"].map(
    lambda x: '0.01' if x == '< 0.01' else x)
nftsDataframe["Floor Price"] = pd.to_numeric(nftsDataframe["Floor Price"])
nftsDataframe["Items"] = nftsDataframe["Items"].map(
    lambda x: x.replace('.', ''))
nftsDataframe["Items"] = pd.to_numeric(nftsDataframe["Items"])
nftsDataframe["Owners"] = nftsDataframe["Owners"].map(lambda x: int(x * 1000))
nftsDataframe

tot = 0
tot_err = 0
error =[]
documents = []
for i, nft in nftsDataframe.iterrows():
    print("Docuemnto: "+str(i))
    document = {
        'Collection': nft[0],
        'Volume': nft[1],
        'Floor Price': nft[2],
        'Owners': nft[3],
        'Items': nft[4],
    }
    tweets = []
    tweetsDataframe = pd.read_json("data/"+str(i)+".json", orient="index")
    tweets_num = None

    for i, tweetNft in tweetsDataframe.iterrows():

        if i != "error":
            tweet = {
            'id': tweetNft[0],
            'text': tweetNft[1],
            }
            tweets.append(tweet)
        else:
            print("Documento con errore")
            
            if "'NoneType'" in tweetNft[0]:
                tweets = [{}]
                tweets_num = 0
                
            else:
                tot_err += 1
                tweets = [{
                'error': tweetNft[0],
                }]
                error.append(nft[0])
                tweets_num = 0
                
        
    if tweets_num is None:
        tweets_num = len(tweets)



    tot += tweets_num

    document['Tweets'] = tweets
    document['Tweets Number'] = tweets_num
    documents.append(document)
    
