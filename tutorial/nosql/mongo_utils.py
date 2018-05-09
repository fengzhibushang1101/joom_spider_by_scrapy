#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/9 13:50
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import datetime
import pymongo

conn_list = [
    pymongo.MongoClient(host="localhost", port=27017, connect=False)
]
mongo_db = conn_list[0]['joom_data']


class Coll(object):
    def __init__(self, name):
        self.coll = mongo_db[name]

    def save(self, doc):
        doc["create_time"] = doc.get("create_time") or datetime.datetime.now()
        doc["update_time"] = doc.get("update_time") or datetime.datetime.now()
        return self.coll.insert(doc)

    def insert_many(self, docs):
        now = datetime.datetime.now()
        for doc in docs:
            doc["create_time"] = now
            doc["update_time"] = now
        return self.coll.insert_many(docs)


if __name__ == "__main__":
    coll = Coll("joom_pro")
    coll.save({"a": 1})
