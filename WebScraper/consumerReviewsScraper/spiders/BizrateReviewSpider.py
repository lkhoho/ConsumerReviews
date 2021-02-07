import re
import os
import json
from datetime import datetime
from typing import Optional
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint
from urllib.parse import unquote
from WebScraper.consumerReviewsScraper.items.bizrate import BizrateStoreItem, BizrateReviewItem


class BizrateReviewSpider(Spider):
    name = 'bizrate_spider'
    page_size = 20
    total_reviews = -1  # determined at the first review parsing

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),
        # 'LOG_LEVEL': 'DEBUG',

        # Auto-throttling
        # 'CONCURRENT_REQUESTS': 10,
        # 'DOWNLOAD_DELAY': 1,

        # Retry
        'CRAWLER_PAUSE_SECONDS': 60,
        'RETRY_HTTP_CODES': [429, 504],

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

    def start_requests(self):
        with open(os.sep.join(['data_sources', 'bizrate.com', 'stores.json'])) as fp:
            data_source = json.load(fp)

        for store in data_source['todo_stores']:
            req = Request(url=store['url'], callback=self.parse_store)
            req.meta['fp'] = request_fingerprint(req)
            yield req

        for store in data_source['todo_stores']:
            url = store['url']
            match = re.match(r'http.*/index--(\d+)', url)
            review_start = int(match[1])
            req = Request(url=url, callback=self.parse_reviews, meta={'review_start': review_start})
            req.meta['fp'] = request_fingerprint(req)
            yield req

    def parse_store(self, response):
        self.logger.info('Parsing store ' + response.url)
        page_data = self._parse_page_data(response)

        merchant = page_data['merchant']
        award = page_data['award']
        rating = page_data['rating']

        store = BizrateStoreItem(
            store_id=merchant['mid'],
            name=merchant['name'].strip(),
            description=merchant['description'].strip(),
            website=merchant['siteUrl'].strip(),
            is_certified=merchant.get('isCertified', False),
            award_year=award.get('awardYear', None),
            award_tier=award.get('awardTier', None),
            award_won_years=award.get('yearsWon', None),
            overall_satisfaction=self._str2float(rating.get('overall', None)),
            shop_again=self._str2float(rating.get('shopAgain', None)),
            to_recommend=self._str2float(rating.get('ffLikelihoodRecommend', None)),
            pos_overall_satisfaction=self._str2float(rating.get('posOverAll', None)),
            pos_shop_again=self._str2float(rating.get('posShopAgain', None)),
            pos_to_recommend=self._str2float(rating.get('posLikelihoodRecommend', None)),
            created_datetime=datetime.utcnow(),
            exp_ease_to_find=rating.get('easyToFindProducts', None),
            exp_site_design=rating.get('overallLookOfSite', None),
            exp_satisfaction_checkout=rating.get('posCheckout', None),
            exp_product_selection=rating.get('selectionOfProducts', None),
            exp_clarity_product_info=rating.get('clarityOfProductInformation', None),
            exp_charges_clearly=rating.get('chargesClearlyStatedBeforeOrder', None),
            exp_price_relative_other=rating.get('priceRelativeToOtherMerchants', None),
            exp_shipping_charges=rating.get('shippingCharges', None),
            exp_variety_shipping=rating.get('varietyOfShippingOptions', None),
            after_deliver_ontime=rating.get('onTimeDelivery', None),
            after_order_tracking=rating.get('orderTracking', None),
            after_product_met_expectations=rating.get('itemMetExpectation', None),
            after_customer_support=rating.get('customerSupport', None),
            after_product_availability=rating.get('availabilityOfWantedProducts', None),
            after_returns_process=rating.get('ffReturnProcess', None)
        )

        yield store

    def parse_reviews(self, response):
        page_data = self._parse_page_data(response)
        reviews = page_data.get('reviews', [])
        pager = page_data['pager']
        self.logger.info('Parsing reviews ==={}==='.format(pager['pagerCount']))

        for r in reviews:
            review_id = r.get('reviewId', None)
            if review_id is not None:
                match = re.match(r'(http.*/\d+)', response.url)
                store_url = match[1]
                url = store_url + '/' + str(review_id)
                req = Request(url=url, callback=self.parse_review_details)
                req.meta['fp'] = request_fingerprint(req)
                yield req

        next_page_url = pager.get('nextPage', None)
        if next_page_url is not None and len(next_page_url) > 0:
            match = re.match(r'(http.*/\d+)', response.url)
            store_url = match[1]
            match = re.match(r'.*/(index--\d+).*', next_page_url)
            next_page_str = match[1]
            url = store_url + '/' + next_page_str
            req = Request(url=url, callback=self.parse_reviews)
            req.meta['fp'] = request_fingerprint(req)
            yield req

    def parse_review_details(self, response):
        self.logger.info('Parsing review details: ' + response.url)
        match = re.match(r'http.*/(\d+)/(\d+)', response.url)
        store_id = match[1]
        review_id = match[2]
        page_data = self._parse_page_data(response)
        rating = page_data['rating']
        review = page_data['review']

        before_purchase_publish_date = review.get('posReviewDate', None)
        if before_purchase_publish_date is not None and len(before_purchase_publish_date) > 0:
            before_purchase_publish_date = datetime.strptime(before_purchase_publish_date, '%b %d, %Y').date()
        else:
            before_purchase_publish_date = None
        
        before_purchase_text = review.get('posReviewText', None)
        if before_purchase_text is not None:
            before_purchase_text = unquote(before_purchase_text).replace('+', ' ').strip()
        
        after_purchase_publish_date = review.get('reviewDate', None)
        if after_purchase_publish_date is not None and len(after_purchase_publish_date) > 0:
            after_purchase_publish_date = datetime.strptime(after_purchase_publish_date, '%b %d, %Y').date()
        else:
            after_purchase_publish_date = None
        
        after_purchase_text = review.get('reviewText', None)
        if after_purchase_text is not None:
            after_purchase_text = unquote(after_purchase_text).replace('+', ' ').strip()
        
        r = BizrateReviewItem(
            review_id=review_id,
            store_id=store_id,
            author=review.get('author', None),
            before_purchase_publish_date=before_purchase_publish_date,
            before_purchase_content=before_purchase_text,
            after_purchase_publish_date=after_purchase_publish_date,
            after_purchase_content=after_purchase_text,
            overall_satisfaction=self._str2int(rating.get('overall', None)),
            shop_again=self._str2int(rating.get('shopAgain', None)),
            to_recommend=self._str2int(rating.get('ffLikelihoodRecommend', None)),
            pos_overall_satisfaction=self._str2int(rating.get('posOverAll', None)),
            pos_shop_again=self._str2int(rating.get('posShopAgain', None)),
            pos_to_recommend=self._str2int(rating.get('posLikelihoodRecommend', None)),
            created_datetime=datetime.utcnow(),
            exp_ease_to_find=self._str2int(rating.get('easyToFindProducts', None)),
            exp_site_design=self._str2int(rating.get('overallLookOfSite', None)),
            exp_satisfaction_checkout=self._str2int(rating.get('posCheckout', None)),
            exp_product_selection=self._str2int(rating.get('selectionOfProducts', None)),
            exp_clarity_product_info=self._str2int(rating.get('clarityOfProductInformation', None)),
            exp_charges_clearly=self._str2int(rating.get('chargesClearlyStatedBeforeOrder', None)),
            exp_price_relative_other=self._str2int(rating.get('priceRelativeToOtherMerchants', None)),
            exp_shipping_charges=self._str2int(rating.get('shippingCharges', None)),
            exp_variety_shipping=self._str2int(rating.get('varietyOfShippingOptions', None)),
            after_deliver_ontime=self._str2int(rating.get('onTimeDelivery', None)),
            after_order_tracking=self._str2int(rating.get('orderTracking', None)),
            after_product_met_expectations=self._str2int(rating.get('itemMetExpectation', None)),
            after_customer_support=self._str2int(rating.get('customerSupport', None)),
            after_product_availability=self._str2int(rating.get('availabilityOfWantedProducts', None)),
            after_returns_process=self._str2int(rating.get('ffReturnProcess', None))
        )

        yield r

    def _parse_page_data(self, response) -> dict:
        """
        A shortcut way to parse page data. Assuming page has a struct in response having all fields we need.
        """

        page_data = response.xpath("body/script").get()
        start = page_data.find('{')
        end = page_data.rfind('}')
        page_data = page_data[start:end + 1]
        page_data = page_data.replace('\n', '') \
                             .replace('\'', '"') \
                             .replace(' || ""', '') \
                             .replace('encodeURIComponent(', '') \
                             .replace('),', ',') \
                             .replace(')', '') \
                             .strip()
        page_data = re.sub(r'awardYear:\s+(0000),', r'awardYear: 0,', page_data)  # 0000 is not a valid year in JSON
        page_data = re.sub(r'(\w+):\s+"', r'"\1": "', page_data)  # replace [name : "Online Shopper"] to ["name" : "Online Shopper"]
        page_data = re.sub(r'(\w+):\s+(\d+|true|false|{|\[)', r'"\1": \2', page_data)
        page_data = re.sub(r'([\w\d]+)\\([\w\d]+)', r'\1 \2', page_data)
        page_data = re.sub(r',\s+}', r'}', page_data)
        page_data = re.sub(r',\s+]', r']', page_data)
        page_data = re.sub(r': ""(\w+)"', r': "\1', page_data)  # replace [: ""Beginner"] to [: "Beginner]
        page_data = re.sub(r'n\\+a', r'n/a', page_data, flags=re.IGNORECASE)  # replace [n\\a] to [n/a]
        try:
            page_data = json.loads(page_data)
        except:
            with open('C:/Users/lkhoho/OneDrive/Desktop/pageerror.txt', 'w+') as fp:
                fp.write(page_data)
                fp.write('--------\n')

        return page_data

    def _str2int(self, val: str) -> Optional[int]:
        if val is None or len(val) == 0 or val.lower() == 'na' or val.lower() == 'n/a':
            return None
        else:
            return int(val)

    def _str2float(self, val: str) -> Optional[float]:
        if val is None or len(val) == 0 or val.lower() == 'na' or val.lower() == 'n/a':
            return None
        else:
            return float(val)
