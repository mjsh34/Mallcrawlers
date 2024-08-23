import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy.utils.reactor import install_reactor

# https://docs.scrapy.org/en/latest/topics/asyncio.html#handling-a-pre-installed-reactor
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
from twisted.internet import reactor, defer

import requests

import argparse
import sys
import time
import csv
import os
import os.path
from pathlib import Path
import random
import logging


ops_list = [
    'crawl_musinsa_items',
    'crawl_actualsizes',
]

def operation__crawl_musinsa_items():
    print = logging.info
    # https://docs.scrapy.org/en/latest/topics/practices.html#run-scrapy-from-a-script
    ap = argparse.ArgumentParser()
    ap.add_argument('--out_dir', '-o', help="Directory where all output files will be stored")
    ap.add_argument('--item_categories_csv', '-i', default="./musinsa__item_categories.csv")
    ap.add_argument('--sort_by', default='pop_category')
    ap.add_argument('--sub_sort_arg', default='')
    ap.add_argument('--from_all_category', action='store_true')
    ap.add_argument('--delay_between_categories', type=float, default=800, help="In seconds")
    ap.add_argument('--delay_on_403', type=float, default=200, help="In seconds")
    args = ap.parse_args()

    print("Item categories CSV: '{}'".format(args.item_categories_csv))

    out_dir_path = Path(args.out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)
    print("Output dir: '{}'".format(out_dir_path))

    cates_to_crawl = []
    with open(args.item_categories_csv, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if not row.get('own_id', None):
                print("own_id not found at row {}. Invalid CSV.".format(i+1), file=sys.stderr)
                exit(1)
            cates_to_crawl.append(row)
    print("Found {} categories to crawl.".format(len(cates_to_crawl)))

    settings = {**get_project_settings(), 'LOG_LEVEL': 'DEBUG'}
    configure_logging(settings)

    @defer.inlineCallbacks
    def crawl():
        from mallcrawlers.spiders import MusinsaItemsSpider
        for i, cate_d in enumerate(cates_to_crawl):
            own_id = cate_d['own_id']
            out_csv = str(out_dir_path / "musinsa__items_{}.csv".format(own_id))
            print("[{}/{}] Crawl {} ({}).".format(i+1, len(cates_to_crawl), own_id, cate_d))
            print("Output will be saved to: '{}'".format(out_csv))
            process = CrawlerRunner({**settings, 
                    'FEEDS': {
                        out_csv: {'format': 'csv'},
                    },
                })
            yield process.crawl(MusinsaItemsSpider, own_ids=str(own_id),
                    item_categories_csv=args.item_categories_csv, all_only=args.from_all_category, 
                    sort_by=args.sort_by, sub_sort=args.sub_sort_arg, delay_on_403=args.delay_on_403)
            if i < len(cates_to_crawl) - 1:
                sleepsecs = random.uniform(0.8, 1.2) * args.delay_between_categories
                print("Done crawling {} [{}/{}]. Wait {}s.".format(own_id, i+1, len(cates_to_crawl), sleepsecs))
                time.sleep(sleepsecs)
            else:
                print("Done crawling {} [{}/{}].".format(own_id, i+1, len(cates_to_crawl)))
        reactor.stop()

    crawl()
    reactor.run()


def operation__crawl_actualsizes():
    #print = logging.info
    # https://docs.scrapy.org/en/latest/topics/practices.html#run-scrapy-from-a-script
    ap = argparse.ArgumentParser()
    ap.add_argument('--out_dir', '-o', required=True, help="Directory where all output files will be stored")
    ap.add_argument('--goods_no_txt', '-i', required=True, help="File containing goods_no's (one per line)")
    ap.add_argument('--delay_on_403', type=float, default=200, help="In seconds")
    ap.add_argument('--max_retries', type=int, default=5)
    args = ap.parse_args()

    with open(args.goods_no_txt, encoding='utf-8') as f:
        goods_nos = [line.strip() for line in f]
    print(f"Found {len(goods_nos)} goods_no's to crawl.")

    out_dir_path = Path(args.out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        # 'cookie': 'PHPSESSID=88a015ab279887ef15c210e226963a0b; _gf=A; mss_service=M; gcc=US; gcuc=USD; gfv=.en; gcn=qgc3lU40vdWu1%2ByJj6N%2BvHZ%2FELEf2n%2BnIwXteTahUs7BISHUSKogLqnJLLItcnMk; AMP_MKTG_31f921b8ca=JTdCJTdE; mss.gprd.recently_viewed_goods=%5B3467190%5D; glc=en; AMP_31f921b8ca=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjI4YzhjNzMxYS0xZTA2LTRhYWQtODlmOS0xNDhiMDA2MWQ2NWMlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzI0MzcyMjgyMDI1JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTcyNDM3MjMxNjMxMyUyQyUyMmxhc3RFdmVudElkJTIyJTNBNyU3RA==; cart_no=xI%2BmbCSgj4cDeZ6Sz%2B6GIA%3D%3D; topBanner=%7B%22WEBY%22%3A0%2C%22WEBN%22%3A0%2C%22APPY%22%3A0%2C%22APPN%22%3A0%2C%22APPALL%22%3A0%7D',
        'origin': 'https://www.musinsa.com',
        'priority': 'u=1, i',
        'referer': 'https://www.musinsa.com/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Brave";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    }

    for ig, goods_no in enumerate(goods_nos):
        print(f"[{ig+1}/{len(goods_nos)}] Crawl goods_no={goods_no}")
        out_path = out_dir_path / f"{goods_no}.json"
        if out_path.is_file():
            print(f"Already crawled goods_no={goods_no}. Skipping.")
            continue
        url = 'https://goods-detail.musinsa.com/api2/goods/{}/actual-size'.format(goods_no)
        success = False
        n_retries = 0
        while n_retries < args.max_retries:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                print(f"Failed to crawl goods_no={goods_no}. Status code: {resp.status_code}.")
                n_retries += 1
                time.sleep(20 * n_retries)
            else:
                success = True
                break
        
        if not success:
            print(f"Failed to crawl goods_no={goods_no} after {args.max_retries} retries. Skipping.")
            continue

        out_str = resp.content.decode('utf-8')
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(out_str)
        
        r = random.random()
        if r < 0.9:
            #sleep_sec = random.uniform(0.8, 1.5) * 1.0
            sleep_sec = 0
        elif r < 0.97:
            #sleep_sec = random.uniform(0.8, 1.5) * 60
            sleep_sec = random.uniform(0.8, 1.5) * 2.0
        else:
            #sleep_sec = random.uniform(0.8, 1.5) * 600
            sleep_sec = random.uniform(0.8, 1.5) * 60
        print(f"Sleeping {sleep_sec:.2f}s.")
        time.sleep(sleep_sec)


if __name__ == '__main__':
    sysargs_all = list(sys.argv)
    n_global_args = 2
    sys.argv = sysargs_all[:n_global_args]

    ap_global = argparse.ArgumentParser()
    ap_global.add_argument('operation', choices=ops_list)
    args_global = ap_global.parse_args()
    op = args_global.operation

    sys.argv = [sysargs_all[0]] + sysargs_all[n_global_args:]
    globals()['operation__' + op]()


