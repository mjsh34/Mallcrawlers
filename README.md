# Installation
## Packages
- Python 3 (3.10.12)
- `scrapy==2.11.1`
- `pyyaml` (6.0.1)

### For prototyping
- `requests` (2.31.0)
- `jupyterlab` (4.1.5)

# Crawl
## Musinsa
### Step 1. Get categories
```sh
cd mallcrawlers
scrapy crawl musinsa__item_categories -O ./musinsa__item_categories.csv
```

### Step 2. Download lists of items
For each row of `categoryCode` inside `./musinsa__item_categories.csv` produced in Step 1, download items listed for the corresponding category, whereby a list is crawled for each subcategory defined inside `childCategoryList` (row), except when `categoryTitle` is `'전체'`, unless `all_only` is set to `yes` using the `-a` option when directly running `scrapy crawl`, or if `--from_all_category` is set when running `operations.py`, in both cases only the '전체' subcategory is considered.
`sort_by` corresponds to the `sort` http url argument for the catalogues with following relations to the buttons visible on the website:
- `pop_category`: "무신사 추천순"
- `new`: "신상품(재입고) 순"
- `price_low`: "낮은 가격순"
- `price_high`: "높은 가격순"
- `discount_rate`: "할인율순"
- `emt_high`: "후기순"
- `sale_high`: "판매순" (Additionally need to set `sub_sort`; default is `1y`.)

Crawl all categories (`python mallcrawlers/operations.py crawl_musinsa_items -h` for help):
```sh
cd mallcrawlers/  # IF THIS STEP IS OMITTED SETTINGS WILL NOT LOAD PROPERLY
python operations.py crawl_musinsa_items -o ./out/musinsa/musinsa_items/ -i ./musinsa__item_categories.csv --sort_by pop_category 2>&1 | tee -a logs/musinsa_items.txt
```
Note: As of March 2024 it does not seem possible to crawl past page 400.
 
Alternatively, using `scrapy crawl` command:
```su
# Crawl category 19 from `./mallcrawlers/musinsa__item_categories.csv`, from '전체' child category (via `-a all_only=yes`) sorted by "무신사 추천순" (via `-a sort_by=pop_category`).
cd mallcrawlers
scrapy crawl musinsa__items -O ./musinsa__items_19.csv -a "item_categories_csv=musinsa__item_categories.csv" -a own_ids=19 -a all_only=yes -a sort_by=pop_category -L DEBUG 2>&1 | tee -a logfile.txt
```
