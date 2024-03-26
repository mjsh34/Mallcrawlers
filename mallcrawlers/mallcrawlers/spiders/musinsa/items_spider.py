from mallcrawlers.items import MusinsaItemItem

import scrapy
from scrapy.utils.reactor import install_reactor

import csv
from urllib.parse import urlparse
from urllib.parse import parse_qs


class MusinsaItemsSpider(scrapy.Spider):
    name = "musinsa__items"

    # This did not lead to fewer 403's?
    #custom_settings = {
    #    "DOWNLOAD_DELAY": 1.5,
    #}

    def __init__(self, item_categories_csv="./musinsa__item_categories.csv", own_ids=None, all_only=False, sort_by='price_high', sub_sort=None, **kw):
        own_ids_str = own_ids
        self.sort_by = sort_by
        if sort_by == 'sale_high':
            sub_sort = sub_sort or '1y'
            if sub_sort not in ['1y', '3m', '1m', '1w', '1d']:
                raise ValueError("Unknown sub_sort: '{}'".format(sub_sort))
        elif sub_sort:
            self.logger.warning("Given sort argument is '%s' but sub_sort is set to '%s'. Possible mistake?",
                    sort_by, sub_sort)
        self.sub_sort = sub_sort

        if own_ids_str is not None:
            if "," in own_ids_str and "-" in own_ids_str:
                raise ValueError("Invalid own_ids argument: '{}'".format(own_ids_str))
            if "," in own_ids_str:
                own_ids = [int(o.strip()) for o in own_ids_str.split(",")]
            elif "-" in own_ids_str:
                own_id_start = int(own_ids_str.split("-")[0])
                own_id_end = int(own_ids_str.split("-")[1])
                own_ids = list(range(own_id_start, own_id_end+1))
            else:
                own_ids = [int(own_ids_str)]
            self.logger.info("Own ids (#=%d): %s", len(own_ids or []), own_ids)
        self.own_ids = own_ids
        to_scrape = []
        with open(item_categories_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                isall = row['child_categoryName'] == '전체'
                all_cond = (all_only and isall) or (not all_only and not isall)
                own_id_cond = (own_ids is None) or (int(row['own_id']) in own_ids)
                if all_cond and own_id_cond:
                    to_scrape.append({'cate': row['categoryCode'], 'ccate': row['child_categoryCode'], 'isall': isall})
        self.logger.info("Will scrape %d child categories (sorted by '%s')", len(to_scrape), sort_by)
        self.logger.debug("Child categories: %s", to_scrape)

        start_urls = []
        for i, d in enumerate(to_scrape):
            url = self.__get_url(cate=(d['cate'] if d['isall'] else d['ccate']), page=1)
            self.logger.debug("Start url %d: %s", i+1, url)
            start_urls.append(url)
        self.start_urls = start_urls
        super().__init__(**kw)

    def start_requests(self):
        start_req_objs = []
        for start_url in self.start_urls:
            start_req_objs.append(scrapy.Request(start_url, callback=self.parse,
                meta={'delay_http_codes': {403: 300}, 'max_retries_http_codes': {403: 4}}))
        return start_req_objs

    def __get_url(self, cate, page):
        DISPLAY_CNT = 90
        return ("https://www.musinsa.com/categories/item/{}?d_cat_cd={}".format(cate, cate)
                + "&brand=&list_kind=small&sort={}&sub_sort={}&page={}".format(self.sort_by, self.sub_sort or "", page)
                + "&display_cnt={}&exclusive_yn=&sale_goods=&timesale_yn=".format(DISPLAY_CNT)
                + "&ex_soldout=&plusDeliveryYn=&kids=&color=&price1=&price2=&shoeSizeOption=&tags=&campaign_id=&includeKeywords=&measure=")

    def parse(self, response):
        qs = parse_qs(urlparse(response.url).query)
        page = qs.get('page', [None])[0]
        cate = qs['d_cat_cd'][0]
        if page is None:
            self.logger.warning("Page info not found in url: %s", page)
        else:
            page = int(page)

        items = response.css("ul#searchList > li")
        self.logger.debug("%d items found on page %d", len(items), page or -1)

        for iitm, itm in enumerate(items):
            d = {}
            d['data_no'] = itm.attrib['data-no']
            ai = itm.css(".article_info")
            if ai is None:
                self.logger.error(".article_info not found for data_no=%s", d['data_no'])
            else:
                d['brand'] = "\t".join([s.strip() for s in ai.css(".item_title *::text").getall() if s.strip()]).strip()
                d['brand_url'] = ai.css(".item_title > a::attr(href)").get()
                d['item_name'] = ai.css(".list_info > [name=goods_link]::attr(title)").get()
                d['item_url'] = ai.css(".list_info > [name=goods_link]::attr(href)").get()
                d['price'] = "\t".join([s.strip() for s in ai.css(".price *::text").getall() if s.strip()]).strip()
            d['meta_page'] = page
            d['meta_item_index_in_page'] = iitm+1
            d['meta_total_items_in_page'] = len(items)
            yield MusinsaItemItem(d)

        # To next page
        # Fails after page 400
        cur_pg = int(response.css(".page_items_lists .section_container_list .currentPagingNum *::text").get())
        total_pg = int(response.css(".page_items_lists .section_container_list .totalPagingNum *::text").get())
        self.logger.info("Parsed page %d/%d (%s)", cur_pg, total_pg, self.own_ids)
        assert(cur_pg == page)
        if cur_pg < total_pg:
            next_pg = cur_pg + 1
            yield scrapy.Request(self.__get_url(cate, page=next_pg),
                    callback=self.parse,
                    meta={'delay_http_codes': {403: 250}, 'max_retries_http_codes': {403: 4}})
        elif cur_pg == total_pg:
            self.logger.info("Parsed all %d pages (%s)", total_pg, self.own_ids)

