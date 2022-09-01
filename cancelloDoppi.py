# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 13:36:43 2022

@author: gemelli
"""

from pymongo import MongoClient
from hashlib import md5
from bson.json_util import dumps

db = MongoClient()['DatMan']

'''
record_hashes = set()
record_duplicate = set()

#rimossi tweet duplicati - con lo stesso id_str
for record in db.provaDoppi.find():
    record_id = record.pop('_id')
   
    record_hash = record['id_str']

    if record_hash in record_hashes:
        db.provaDoppi.delete_one({'_id': record_id})
        record_duplicate.add(record_hash)
    else:
        record_hashes.add(record_hash)

print(len(list(db.provaDoppi.find())))








cursor = db.provaDoppi.aggregate(
    [
        {"$group": {"_id": "$ID", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": { "$gte": 2 }}}
    ]
)

response = []
for doc in cursor:
    del doc["unique_ids"][0]
    for id in doc["unique_ids"]:
        response.append(id)

db.provaDoppi.delete_many({"_id": {"$in": response}})
'''


from pymongo import MongoClient

db = MongoClient()['DatMan']

duplicates = []

cursor = db.provaDoppi.aggregate([
  { "$group": {
    "_id": { "id_str": "$id_str"},
    "dups": { "$addToSet": "$_id" },
    "count": { "$sum": 1 }
  }},
  { "$match": {
    "count": { "$gt": 1 }
  }}
],
allowDiskUse = True
)


for doc in cursor:
    del doc['dups'][0]
    for dupId in doc['dups']:
        duplicates.append(dupId)




print(duplicates)

  
db.provaDoppi.delete_many({"_id":{"$in":duplicates}})




