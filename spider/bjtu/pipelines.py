# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql

class BjtuPipeline(object):

    def __init__(self):
        try:
            self.db = pymysql.connect(host='localhost',
                                      user='root',
                                      passwd='jiazequn',
                                      db='spider',
                                      port=3306,
                                      charset='utf8')
            self.cursor = self.db.cursor()
            print("connect db successful")
        except:
            print("Fail to connect mysql db")

    def process_item(self, item, spider):
        param = (item['url'], item['title'], item['content'])
        sql = "insert into pages (url, title, content) values (%s, %s, %s)"
        self.cursor.execute(sql, param)
        print("**************************" + item['title'])
        self.db.commit()
        return item

    def close_spider(self):
        self.db.commit()
        self.db.close()
        print("Done")

