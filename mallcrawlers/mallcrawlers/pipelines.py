# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from mallcrawlers import items;

from itemadapter import ItemAdapter
import psycopg2

import datetime
from pathlib import Path
import io
from functools import partial
import json
import csv


class MallcrawlersPipeline:
    def __init__(self, settings):
        db_type = settings['DB_TYPE']
        db_hostname = settings['DB_HOSTNAME']
        db_username = settings['DB_USERNAME']
        db_password = settings['DB_PASSWORD']
        db_name = settings['DB_NAME']

        self.musinsa__skip_seen_items = settings['MUSINSA__SKIP_SEEN_ITEMS']

        self.use_db = bool(settings['USE_DATABASE'])
        self.conn = None
        self.cur = None
        if self.use_db:
            if db_type == 'PostgreSQL':
                self.conn = psycopg2.connect(host=db_hostname, user=db_username, password=db_password, dbname=db_name)
                self.cur = self.conn.cursor()
            else:
                raise ValueError("Unsupported database type: '{}'".format(db_type))

        if self.use_db:
            # Musinsa - item details table
            self.cur.execute("""
            CREATE TABLE IF NOT EXISTS musinsa__item_details (
                id serial PRIMARY KEY,
                goods_no VARCHAR(16) UNIQUE NOT NULL,
                stateall TEXT,
                stateallv2 TEXT,
                essential TEXT,
                actualsize TEXT,
                saved_date DATE
            )
            """)
            # Musinsa - reviews table
            self.cur.execute("""
            CREATE TABLE IF NOT EXISTS musinsa__item_reviews (
                id serial PRIMARY KEY,
                goods_no VARCHAR(16) NOT NULL,
                review_type VARCHAR(20) NOT NULL,
                reviewer_name VARCHAR(20),
                review_date VARCHAR(20),
                reviewer_profile VARCHAR(30),
                item_url VARCHAR(60),
                item_name VARCHAR(50),
                item_size VARCHAR(16),
                rating_active TEXT,
                review_list TEXT,
                review_photos TEXT,
                review_text TEXT,
                comments TEXT,
                review_page INTEGER,
                review_page_last INTEGER
            )
            """)

    @classmethod
    def from_crawler(clazz, crawler):
        return clazz(crawler.settings)

    def process_item(self, item, spider):
        if self.use_db:
            if isinstance(item, items.MusinsaItemDetailsItem):
                do_insert = True
                if self.musinsa__skip_seen_items:
                    self.cur.execute("SELECT id, saved_date FROM musinsa__item_details WHERE goods_no = %s",
                            (item['goods_no'],))
                    result = self.cur.fetchone()
                    if result:
                        spider.logger.warn("Item (goods_no='%s') already in database: %s", item['goods_no'],
                                result)
                        do_insert = False

                if do_insert:
                    dumpf = partial(json.dumps, ensure_ascii=False)
                    self.cur.execute("""
                    INSERT INTO musinsa__item_details
                    (goods_no, stateall, stateallv2, essential, actualsize, saved_date) VALUES
                    (%s,       %s,       %s,         %s,        %s,         %s)
                    """, 
                    (item['goods_no'], dumpf(item['stateall']), dumpf(item['stateallv2']), dumpf(item['essential']), dumpf(item['actualsize']),
                        datetime.datetime.now().strftime("%Y-%m-%d")));
                    self.conn.commit()
                    spider.logger.info("Inserted item details (goods_no='%s') into database", item['goods_no'])
            elif isinstance(item, items.MusinsaItemReviewItem):
                csvwriter = partial(csv.writer, quoting=csv.QUOTE_ALL)
                with io.StringIO() as sio:
                    csvwriter(sio).writerow(item['review_photos'])
                    review_photos = sio.getvalue()
                with io.StringIO() as sio:
                    csvwriter(sio).writerow(item['review_list'])
                    review_list = sio.getvalue()
                with io.StringIO() as sio:
                    csvwriter(sio).writerow(item['comments'])
                    comments = sio.getvalue()

                t = lambda s, l: (s or "")[:l]
                self.cur.execute("""
                INSERT INTO musinsa__item_reviews
                (goods_no, review_type, reviewer_name, review_date, reviewer_profile, item_url,
                item_name, item_size, rating_active, review_list, review_photos, 
                review_text, comments, review_page, review_page_last)
                VALUES
                (%s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
                """,
                (item['goods_no'], t(item['review_type'], 20), t(item['reviewer_name'], 20), t(item['review_date'], 20), t(item['reviewer_profile'], 30), t(item['item_url'], 60),
                t(item['item_name'], 50), t(item['item_size'], 16), item['rating_active'], review_list, review_photos,
                item['review_text'], comments, item['review_page'] or -1, item['review_page_last'] or -1))
                self.conn.commit()
                spider.logger.info("Inserted item review (goods_no='%s') into database", item['goods_no'])
        return item

    def close_spider(self, spider):
        if self.use_db:
            self.cur.close()
            self.conn.close()

#class JSONLinesWriterPipeline:
#    def __init__(self, filename_prefix="item"):
#        self.filepath = Path("out/musinsa/item_categories/{}_{}.jsonl".format(filename_prefix, datetime.datetime.now().strftime("%Y%m%dT%H%M%S")))
#        self.filepath.parent.mkdir(exist_ok=True, parents=True)
#
#    def open_spider(self, spider):
#        self.file = open(self.filepath, 'w', encoding='utf-8')
#
#    def close_spider(self, spider):
#        self.file.close()
#
#    def process_item(self, item, spider):
#        adapter = scrapy.ItemAdapter(item)
#        line = json.dumps(adapter.asdict()) + "\n"
#        self.file.write(line)
#        return item
#
#
#class MusinsaItemCategoryItemJsonWriterPipeline(JSONLinesWriterPipeline):
#    def __init__(self):
#        super().__init__("musinsa__item_categories")

