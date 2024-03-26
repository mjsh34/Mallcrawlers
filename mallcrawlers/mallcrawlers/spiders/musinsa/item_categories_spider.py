from mallcrawlers.items import MusinsaItemCategoryItem

import scrapy

import json


class MusinsaItemCategoriesSpider(scrapy.Spider):
    name = "musinsa__item_categories"
    #custom_settings = {
    #    'ITEM_PIPELINES': {
    #        'mallcrawlers.pipelines.MusinsaItemCategoryItemJsonWriterPipeline': 300,
    #    }
    #}

    start_urls = ["https://display.musinsa.com/display/api/v1/categories/ITEM?sex=A"]

    def parse(self, response):
        data = json.loads(response.text)
        j = 1
        for cate_d in data['data']:
            for ccate_d in cate_d['childCategoryList']:
                yield MusinsaItemCategoryItem({
                    "own_id": j,
                    **{k: v for k, v in cate_d.items() if k != 'childCategoryList'},
                    **{("child_"+k): v for k, v in ccate_d.items()}
                    })
                j += 1

