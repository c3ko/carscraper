# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import sqlite3


class CarScraperPipeline(object):
    def process_item(self, item, spider):
        return item



class KijijiPipeline(object):

    def __init__(self):
        self.conn = None
        self.cur = None
        self.setup_db()
        self.create_table()

    def setup_db(self):
        self.conn = sqlite3.connect('./car_scraper.db')
        self.cur = self.conn.cursor()

    def close_db(self):
        self.conn.close()

    def __del__(self):
        self.close_db()

    def create_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Kijiji(
            make TEXT,
            model TEXT,
            year INTEGER,
            full_name TEXT,
            price INTEGER,
            location TEXT,
            mileage INTEGER,
            date_posted DateTime,
            link TEXT,
            PRIMARY KEY (link, date_posted)
            )
            """
        )

    def insert_into_db(self, item):

        self.cur.execute(
            """
            INSERT INTO Kijiji VALUES ("{}","{}","{}","{}","{}","{}","{}",datetime("{}"),"{}")
            """.format(item['make'], item['model'], item['year'], str(item['full_name']), item['price'],
                       str(item['location']), str(item['mileage']), item['date_posted'], str(item['link']))
        )
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            self.insert_into_db(item)

        except sqlite3.IntegrityError:
            pass