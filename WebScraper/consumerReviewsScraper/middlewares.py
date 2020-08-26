# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

import time
import logging
from datetime import datetime
from random import choice
from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint
from scrapy.utils.response import response_status_message
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse
from WebScraper.consumerReviewsScraper.models.main import db_connect
from WebScraper.consumerReviewsScraper.models.url_status import URLStatus
from WebScraper.core_customized import SUCCESS, FAILED, PENDING


class RandomUserAgentMiddleware(UserAgentMiddleware):
    """
    Try to use cached server (on heroku) for user agent list. If the server is not available, use predefined
    user agent list instead. Set User-Agent attribute to a random one for every request.
    """

    def __init__(self, user_agent='Scrapy'):
        super().__init__(user_agent=user_agent)

        self.user_agent_list = [
            'Mozilla/5.0 (X11; Linux i686; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Mozilla/5.0 (Windows NT 6.1; rv:68.7) Gecko/20100101 Firefox/68.7',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:69.2.1) Gecko/20100101 Firefox/69.2',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:77.0) Gecko/20190101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10547',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36 Edge/14.14359',
            'Mozilla/5.0 (X11) AppleWebKit/62.41 (KHTML, like Gecko) Edge/17.10859 Safari/452.6',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19577',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3739.0 Safari/537.36 Edg/75.0.109.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13) AppleWebKit/603.1.13 (KHTML, like Gecko) Version/10.1 Safari/603.1.13',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15',
        ]

    def process_request(self, request, spider):
        request.headers['User-Agent'] = choice(self.user_agent_list)


class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super().__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            self.crawler.engine.pause()
            time.sleep(self.crawler.settings.getint('CRAWLER_PAUSE_SECONDS', 60))
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response


class UpdateURLStatusMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    def __init__(self, settings):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Using customized de-duple class %s' % settings.get('DUPEFILTER_CLASS'))
        self.engine = None
        self.session = None

    @classmethod
    def from_crawler(cls, crawler):
        if crawler.settings.get('DUPEFILTER_CLASS', None) is None:
            raise NotConfigured
        mw = cls(crawler.settings)
        crawler.signals.connect(mw.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(mw.spider_closed, signal=signals.spider_closed)
        return mw

    def process_spider_input(self, response, spider):
        fp = response.meta.get('fp', None)
        if fp is None:
            self.logger.warning('Cannot get fingerprint from response. URLStatus will not be updated.')
        else:
            if response.status >= 400:
                self._upsert_fingerprint_and_status(fp, None, FAILED)
            elif 300 <= response.status < 400:
                self._upsert_fingerprint_and_status(fp, None, PENDING)
        return

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for r in result:
            if isinstance(r, Request):
                fp = r.meta.get('fp', None)
                self._upsert_fingerprint_and_status(fp, r, PENDING)
            else:
                fp = response.meta.get('fp', None)
                if fp is None:
                    self.logger.warning('Cannot get fingerprint from response. URLStatus will not be updated.')
                # update URLStatus to SUCCESS only when we have the item
                self._upsert_fingerprint_and_status(fp, None, SUCCESS)
            yield r

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            fp = r.meta.get('fp', None)
            # always update URLStatus of start_requests to PENDING, no matter they have succeeded or not.
            self._upsert_fingerprint_and_status(fp, r, PENDING, force=True)
            yield r

    def spider_opened(self, spider):
        self.logger.debug('Spider {} opened.'.format(spider.name))
        self.engine = db_connect()
        self.session = sessionmaker(bind=self.engine)()
        self.logger.info('Connected to database %s' % self.engine.engine.url.database)

    def spider_closed(self, spider):
        self.logger.debug('Spider {} closed.'.format(spider.name))
        self.session.close_all()
        self.logger.info('Disconnected to database %s ' % self.engine.engine.url.database)

    def _upsert_fingerprint_and_status(self, fingerprint, request, status, force=False) -> str:
        if fingerprint is not None:
            fp = fingerprint
        else:
            fp = request_fingerprint(request)

        url_status = self.session.query(URLStatus).filter_by(fingerprint=fp).one_or_none()
        if url_status is None:
            url_status = URLStatus(domain=self.get_domain(request), fingerprint=fp, status=PENDING,
                                   last_update_datetime=datetime.utcnow())
            self.session.add(url_status)
        elif url_status.status != SUCCESS or force is True:
            url_status.status = status
            url_status.last_update_datetime = datetime.utcnow()
            url_status.fingerprint = fp
        self.session.commit()
        return fp

    def get_domain(self, request):
        parsed_uri = urlparse(request.url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return domain


class WebscraperDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

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
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
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
        spider.logger.info('Spider opened: %s' % spider.name)
