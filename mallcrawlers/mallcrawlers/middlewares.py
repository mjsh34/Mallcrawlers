# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

import time
import random


class MallcrawlersSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class MallcrawlersDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self, **a):
        super().__init__(**a)
        self.__retry_counts_d = dict()

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Pause scraper and retry on error codes such as 403
        # ref: https://github.com/scrapy/scrapy/blob/02b97f98e74a994ad3e4d74e7ed55207e508a576/scrapy/downloadermiddlewares/retry.py#L145
        delay_d = request.meta.get('delay_http_codes', None)
        if delay_d is not None and response.status in delay_d:
            delay_base = delay_d[response.status]
            retry_count = self.__retry_counts_d.get(request.url, 0)
            max_retry_counts = request.meta.get('max_retries_http_codes', {}).get(response.status, 5)
            delay_s = random.uniform(1.05, 1.3) * (retry_count + 1) * delay_base
            if retry_count < max_retry_counts:
                spider.logger.info("Encountered HTTP status code %s with url '%s' [%d/%d]. Pausing crawler for %.01fs",
                        response.status, request.url, retry_count + 1, max_retry_counts, delay_s)
                spider.crawler.engine.pause()
                time.sleep(delay_s)
                spider.crawler.engine.unpause()
                self.__retry_counts_d[request.url] = retry_count + 1

                request_retry = request.copy()
                request_retry.meta['retry_http_code_times'] = self.__retry_counts_d[request.url]
                request_retry.dont_filter = True
                return request_retry
            else:
                spider.logger.error("Gave up retrying for '%s' after %d tries", request.url, retry_count)
                return response
        else:
            return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
