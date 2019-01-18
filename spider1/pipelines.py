# -*- coding: utf-8 -*-
import pymongo
import pymysql
import syslog
# from spider1.settings import mongo_host,mogodb_port,mongo_db_name,mongo_db_collection
from spider1.settings import mysql_host,mysql_port,mysql_name,mysql_password,mysql_dbname

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class Spider1Pipeline(object):
    def __init__(self):
        # host = mongo_host
        # port = mogodb_port
        # dbname = mongo_db_name
        # sheetname = mongo_db_collection
        # clint = pymongo.MongoClient(host=host,port=port)
        # mydb = clint[dbname]
        # self.post = mydb[sheetname]

        # 连接数据库
        self.connect = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            db=mysql_dbname,
            user=mysql_name,
            passwd=mysql_password,
            charset='utf8',
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        # data = dict(item)
        # self.post.insert(data)
        # return item

        try:
            # 查重处理
            self.cursor.execute(
                """select id from doubanmovie where serial_number = %s""",
                item['serial_number'])
            # 是否有重复数据
            repetition = self.cursor.fetchone()

            # 重复
            if repetition:
                pass

            else:
                # 插入数据
                self.cursor.execute(
                    """insert into doubanmovie(serial_number, movie_name, introduce, star, evaluate, describe1)
                    value (%s, %s, %s, %s, %s, %s)""",
                    (item['serial_number'],
                     item['movie_name'],
                     item['introduce'],
                     item['star'],
                     item['evaluate'],
                     item['describe']))

            # 提交sql语句
            self.connect.commit()

        except Exception as error:
            # 出现错误时打印错误日志
            syslog(error)
        return item
