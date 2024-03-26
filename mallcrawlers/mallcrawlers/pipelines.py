# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from mallcrawlers.items import MusinsaItemCategoryItem

from itemadapter import ItemAdapter

import datetime
from pathlib import Path


#class MallcrawlersPipeline:
#    def process_item(self, item, spider):
#        return item

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

