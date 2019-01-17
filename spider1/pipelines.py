# -*- coding: utf-8 -*-
import pymongo
from spider1.settings import mongo_host,mogodb_port,mongo_db_name,mongo_db_collection
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class Spider1Pipeline(object):
    def __init__(self):
        host = mongo_host
        port = mogodb_port
        dbname = mongo_db_name
        sheetname = mongo_db_collection
        clint = pymongo.MongoClient(host=host,port=port)
        mydb = clint[dbname]
        self.post = mydb[sheetname]

    def process_item(self, item, spider):
        data = dict(item)
        self.post.insert(data)
        return item
