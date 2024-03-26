import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

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

    from mallcrawlers.spiders import MusinsaItemsSpider
    for i, cate_d in enumerate(cates_to_crawl):
        own_id = cate_d['own_id']
        out_csv = str(out_dir_path / "musinsa__items_{}.csv".format(own_id))
        print("[{}/{}] Crawl {} ({}).".format(i+1, len(cates_to_crawl), own_id, cate_d))
        print("Output will be saved to: '{}'".format(out_csv))
        process = CrawlerProcess({**settings, 
                'FEEDS': {
                    out_csv: {'format': 'csv'},
                },
            })
        process.crawl(MusinsaItemsSpider, own_ids=str(own_id),
                item_categories_csv=args.item_categories_csv, all_only=args.from_all_category, 
                sort_by=args.sort_by, sub_sort=args.sub_sort_arg)
        process.start(stop_after_crawl=True)
        process.join()
        if i < len(cates_to_crawl) - 1:
            sleepsecs = random.uniform(0.8, 1.2) * 600
            print("Done crawling {} [{}/{}]. Sleep {}s.".format(own_id, i+1, len(cates_to_crawl), sleepsecs))
            time.sleep(sleepsecs)
        else:
            print("Done crawling {} [{}/{}].".format(own_id, i+1, len(cates_to_crawl)))


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


