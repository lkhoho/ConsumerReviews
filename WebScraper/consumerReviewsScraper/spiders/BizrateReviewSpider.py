import re
from datetime import datetime
import scrapy
from ..items import BizRateReview


class BizrateReviewSpider(scrapy.Spider):
    """
    Spider to scrape reviews from bizrate.com.
    """

    reviewSource = 'bizrate.com'
    name = 'bizrateReviewSpider'

    custom_settings = {
        'LOG_FILE': '/Users/keliu/Developer/python/ConsumerReviews/WebScraper/{}.log'.format(name)
    }

    def start_requests(self):
        urls = [
            'http://www.bizrate.com/reviews/overstock.com/23819/',
            'http://www.bizrate.com/reviews/brookstone/797/',
            'http://www.bizrate.com/reviews/midwayusa/77064/',
            'http://www.bizrate.com/reviews/dover-saddlery/139768/',
            'http://www.bizrate.com/reviews/drs.-foster---smith/55089/',
            'http://www.bizrate.com/reviews/zzounds/18022/',
            'http://www.bizrate.com/reviews/american-musical-supply/19940/',
            'http://www.bizrate.com/reviews/adam---eve/68435/',
            'http://www.bizrate.com/reviews/replacements%2C-ltd./83489/',
            'http://www.bizrate.com/reviews/fragrancenet.com/17911/',
            'http://www.bizrate.com/reviews/crutchfield/58/',
            'http://www.bizrate.com/reviews/boscov%27s/31490/',
            'http://www.bizrate.com/reviews/beallsflorida.com/26309/',
            'http://www.bizrate.com/reviews/musician%27s-friend/25210/',
            'http://www.bizrate.com/reviews/thredup/269632/',
            'http://www.bizrate.com/reviews/shoemall.com/57668/',
            'http://www.bizrate.com/reviews/lowe%27s/29931/'
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.logger.info('Parsing page ' + response.url)

        store_name = ' '.join(response.xpath('body/div[4]/div/h1/text()').extract_first().split()[:-1])
        review_id_pattern = re.compile('review_id--(\d+)\.html')

        selectors = response.xpath("//li[@class='review-item']")

        for selector in selectors:
            link = selector.xpath("ul/div[2]/div/div/li[@class='links']/a/@href").extract_first()
            review_id = re.search(review_id_pattern, link).group(1)
            yield scrapy.Request(url='http://www.bizrate.com' + link.strip(), callback=self.process_details,
                                 meta={'store_name': store_name, 'review_id': review_id})

        next_urls = response.xpath(
            "//ul[@id='pagination']/li[@class='page current']/following-sibling::li[@class='page ']/a/@href").extract()
        for url in next_urls:
            yield scrapy.Request(url='http://www.bizrate.com' + url.strip(), callback=self.parse)

    def process_details(self, response):
        self.logger.info('Parsing review details: ' + response.url)
        store_name = response.meta['store_name']
        review = BizRateReview()
        review['id'] = response.meta['review_id']
        review['scrapedDate'] = datetime.today().strftime(review.DATE_FORMAT)
        review['source'] = self.reviewSource
        review['reviewStore'] = store_name
        review['tags'].append(store_name.lower())
        store_ratings = response.xpath("//*[@id='store_ratings']")

        review['author'] = store_ratings.xpath("div[@class='authorship']/p[@class='author']/text()").extract_first()
        review['reviewAfterPurchase']['author'] = review['author']

        rating_scores = store_ratings.xpath("div[3]/div[1]/div/div")
        review['overallSatisfaction'] = self._extractScore(
            rating_scores.xpath("p[@class='rating'][1]/span[1]/text()").extract_first())
        review['wouldShopHereAgain'] = self._extractScore(
            rating_scores.xpath("p[@class='rating'][2]/span[1]/text()").extract_first())
        review['likelihoodToRecommend'] = self._extractScore(
            rating_scores.xpath("p[@class='rating'][3]/span[1]/text()").extract_first())

        rating_site_items = store_ratings.xpath("div[3]/div[2]/div[1]/div/div[@class='ratings']/span/@title").extract()
        rating_site_scores = [self._extractRating(s) for s in rating_site_items]
        review['ratingsSiteExperience']['easeOfFinding'] = rating_site_scores[0]
        review['ratingsSiteExperience']['designSite'] = rating_site_scores[1]
        review['ratingsSiteExperience']['satisfactionCheckout'] = rating_site_scores[2]
        review['ratingsSiteExperience']['productSelection'] = rating_site_scores[3]
        review['ratingsSiteExperience']['clarityProductInfo'] = rating_site_scores[4]
        review['ratingsSiteExperience']['chargesStatedClearly'] = rating_site_scores[5]
        review['ratingsSiteExperience']['priceRelativeOtherRetailers'] = rating_site_scores[6]
        review['ratingsSiteExperience']['shippingCharges'] = rating_site_scores[7]
        review['ratingsSiteExperience']['varietyShippingOptions'] = rating_site_scores[8]

        rating_after_items = store_ratings.xpath("div[3]/div[2]/div[2]/div/div[@class='ratings']/span/@title").extract()
        rating_after_scores = [self._extractRating(s) for s in rating_after_items]
        review['ratingsAfterPurchase']['onTimeDelivery'] = rating_after_scores[0]
        review['ratingsAfterPurchase']['orderTracking'] = rating_after_scores[1]
        review['ratingsAfterPurchase']['productMetExpectations'] = rating_after_scores[2]
        review['ratingsAfterPurchase']['customerSupport'] = rating_after_scores[3]
        review['ratingsAfterPurchase']['productAvailability'] = rating_after_scores[4]
        review['ratingsAfterPurchase']['returnsProcess'] = rating_after_scores[5]

        review_site = store_ratings.xpath("div[@*[name()='tal:condition']='posReviewText']")
        if len(review_site) > 0:
            review['date'] = self._reformatDate(review_site.xpath("div/p[1]/text()").extract_first(), review.DATE_FORMAT)
            content = review_site.xpath("div/p[2]/text()").extract_first()
            review['content'] = content.strip() if content else ''

        review_after = store_ratings.xpath("div[@*[name()='tal:condition']='reviewText']")
        if len(review_after) > 0:
            review['reviewAfterPurchase']['date'] = \
                self._reformatDate(review_after.xpath("div/p[1]/text()").extract_first(), review.DATE_FORMAT)
            content = review_after.xpath("div/p[2]/text()").extract_first()
            review['reviewAfterPurchase']['content'] = content.strip() if content else ''

        review['fingerPrint'] = review.computeFingerprint()

        return review

    def _extractScore(self, score_str: str) -> int:
        return -1 if score_str.lower() == 'unavailable' else int(score_str)

    def _extractRating(self, rating_str: str) -> int:
        if rating_str.lower() == 'not rated':
            return -1
        else:
            return int(rating_str.split()[0])

    def _reformatDate(self, date_str: str, dateFormat: str) -> str:
        # Oct 29, 2017
        return datetime.strptime(date_str, '%b %d, %Y').date().strftime(dateFormat)
