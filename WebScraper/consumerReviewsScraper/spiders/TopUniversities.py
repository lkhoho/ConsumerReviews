import json
from datetime import datetime
from typing import Optional, Union
from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy.utils.request import request_fingerprint
from WebScraper.consumerReviewsScraper.items.university import UniversityRankingItem


class TopUniversitiesSpider(Spider):
    name = 'topuniversities_spider'

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),
        # 'LOG_LEVEL': 'DEBUG',

        # Auto-throttling
        # 'CONCURRENT_REQUESTS': 10,
        # 'DOWNLOAD_DELAY': 1,

        # Retry
        'CRAWLER_PAUSE_SECONDS': 60,
        'RETRY_HTTP_CODES': [429, 503, 504],

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,

            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'consumerReviewsScraper.middlewares.TooManyRequestsRetryMiddleware': 543,
        },

        'SPIDER_MIDDLEWARES': {
            'consumerReviewsScraper.middlewares.UpdateURLStatusMiddleware': 10,
        },
    }

    default_headers = {
        'Content-Type': 'text/plain',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
    }

    default_charset = 'utf-8'

    start_urls = [
        {
            'year': 2021,
            'url': 'https://www.topuniversities.com/sites/default/files/qs-rankings-data/946820.txt?_=1612665955195',
            'referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2021/business-management-studies',
            'subject': 'Business & Management Studies'
        },
        {
            'year': 2020,
            'url': 'https://www.topuniversities.com/sites/default/files/qs-rankings-data/926879.txt?_=1611537656648',
            'referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2020/business-management-studies',
            'subject': 'Business & Management Studies'
        }, {
            'year': 2019,
            'url': 'https://www.topuniversities.com/sites/default/files/qs-rankings-data/894191.txt?_=1611537714631',
            'referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2019/business-management-studies',
            'subject': 'Business & Management Studies'
        }, {
            'year': 2018,
            'url': 'https://www.topuniversities.com/sites/default/files/qs-rankings-data/379637.txt?_=1611537744274',
            'referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2018/business-management-studies',
            'subject': 'Business & Management Studies'
        }, {
            'year': 2017,
            'url': 'https://www.topuniversities.com/sites/default/files/qs-rankings-data/335228.txt?_=1611537787312',
            'referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2017/business-management-studies',
            'subject': 'Business & Management Studies'
        }]

    def start_requests(self):
        for obj in self.start_urls:
            self.default_headers['Referer'] = obj['referer']
            req = Request(url=obj['url'], callback=self.parse, headers=self.default_headers, dont_filter=True,
                          meta={
                              'year': obj['year'],
                              'subject': obj['subject']
                          })
            req.meta['fp'] = request_fingerprint(req)
            yield req

    def parse(self, response):
        self.logger.info('Parsing ranking page={}, year={}'.format(response.url, response.meta['year']))
        text = response.body.decode(self.default_charset)
        data = json.loads(text)
        for uni in data['data']:
            yield UniversityRankingItem(
                year=response.meta['year'],
                subject=response.meta['subject'],
                name=self._get_str(uni['title']),
                country=self._get_str(uni['country']),
                region=self._get_str(uni['region']),
                ranking=self._get_str(uni['rank_display']),
                score=self._get_float(uni['score']),
                created_datetime=datetime.utcnow()
            )

    def _get_float(self, value: Optional[str]) -> Optional[float]:
        if value is not None and len(value.strip()) > 0:
            return float(value.strip())
        else:
            return None

    def _get_int(self, value: Optional[str]) -> Optional[int]:
        if value is not None and len(value.strip()) > 0:
            return int(value.strip())
        else:
            return None

    def _get_str(self, value: Optional[str]) -> Optional[str]:
        if value is not None:
            return value.strip()
        else:
            return None
