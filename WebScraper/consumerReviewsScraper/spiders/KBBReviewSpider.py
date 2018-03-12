import re
from datetime import datetime
import uuid
import scrapy
from ..items import KBBReviewItem


class KBBReviewSpider(scrapy.Spider):
    """
    Spider to scrape reviews from kbb.com.
    """

    name = 'kbbReviewSpider'
    reviewSource = 'kbb.com'

    custom_settings = {
        'LOG_FILE': '/Users/keliu/Developer/python/ConsumerReviews/WebScraper/{}.log'.format(name)
    }

    def start_requests(self):
        urls = [
            'https://www.kbb.com/bmw/3-series/2017/328d-consumer_reviews/?vehicleid=421753&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/buick/regal/2017/premium-ii-consumer_reviews/?vehicleid=421058&intent=buy-new&perPage=25',
            'https://www.kbb.com/chevrolet/impala/2017/lt-consumer_reviews/?vehicleid=419867&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/chevrolet/malibu-limited/2016/ls-consumer_reviews/?vehicleid=411692&intent=buy-new&perPage=25',
            'https://www.kbb.com/chevrolet/sonic/2017/lt-consumer_reviews/?vehicleid=421080&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/chrysler/300/2017/300-limited-consumer_reviews/?vehicleid=423764&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/dodge/charger/2017/r-t-consumer_reviews/?vehicleid=422569&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/ford/fiesta/2017/titanium-consumer_reviews/?vehicleid=422069&intent=buy-new&perPage=25',
            'https://www.kbb.com/ford/focus/2017/se-consumer_reviews/?vehicleid=422017&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/ford/fusion/2017/titanium-consumer_reviews/?vehicleid=416134&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/ford/taurus/2017/sel-consumer_reviews/?vehicleid=422015&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/honda/accord/2017/ex-l-consumer_reviews/?vehicleid=420016&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/hyundai/sonata/2017/base-style-consumer_reviews/?vehicleid=420225&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/lexus/ls/2017/ls-460-consumer_reviews/?vehicleid=422092&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/lincoln/mkz/2017/premiere-consumer_reviews/?vehicleid=419071&intent=buy-new&perPage=25',
            'https://www.kbb.com/mazda/mazda6/2017/touring-consumer_reviews/?vehicleid=420664&intent=buy-new&perPage=25',
            'https://www.kbb.com/mitsubishi/lancer/2017/es-consumer_reviews/?vehicleid=420000&intent=buy-new&perPage=25',
            'https://www.kbb.com/nissan/altima/2017/25-sr-consumer_reviews/?vehicleid=421281&intent=buy-new&perPage=25',
            'https://www.kbb.com/nissan/versa/2017/sv-consumer_reviews/?vehicleid=421099&intent=buy-new&perPage=25',
            'https://www.kbb.com/nissan/sentra/2017/sr-consumer_reviews/?vehicleid=421627&intent=buy-new&perPage=25',
            'https://www.kbb.com/tesla/model-s/2016/70-consumer_reviews/?vehicleid=413296&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/toyota/avalon/2017/xle-premium-consumer_reviews/?vehicleid=421870&intent=buy-new&perPage=25',
            'https://www.kbb.com/toyota/camry/2017/se-consumer_reviews/?vehicleid=419855&intent=buy-new&perPage=25',
            'https://www.kbb.com/toyota/corolla/2017/le-consumer_reviews/?vehicleid=421515&intent=buy-new&perPage=25',
            'https://www.kbb.com/volkswagen/cc/2017/20t-r-line-executive-consumer_reviews/?vehicleid=421468&intent=buy-new&perPage=25',
            'https://www.kbb.com/volkswagen/jetta/2017/14t-se-consumer_reviews/?vehicleid=420256&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/volkswagen/passat/2017/v6-sel-premium-consumer_reviews/?vehicleid=420790&intent=buy-new&category=sedan&perPage=25',
            'https://www.kbb.com/volvo/s60/2017/t5-inscription-consumer_reviews/?vehicleid=423783&intent=buy-new&category=sedan&perPage=25',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.logger.info('Parsing page ' + response.url)

        ymm = response.xpath("//h1/span[@class='vehicle-ymm']/text()").extract_first().split()
        year = ymm[0]
        make = ymm[1]
        model = ' '.join(ymm[2:])

        reviewSelectors = response.xpath("//div[@class='review-section']")
        for selector in reviewSelectors:
            nDivs = len(selector.xpath('div'))
            if nDivs == 4:
                headerDiv = selector.xpath('div[1]')
                recommendDiv = selector.xpath('div[2]')
                contentDiv = selector.xpath('div[3]')
            elif nDivs == 3:
                headerDiv = selector.xpath('div[1]')
                recommendDiv = None
                contentDiv = selector.xpath('div[2]')
            else:
                headerDiv, recommendDiv, contentDiv = None, None, None

            title = headerDiv.xpath('h2/text()').extract_first()
            author = headerDiv.xpath("p[@class='with-sub']/span/text()").extract_first()
            date = headerDiv.xpath("meta/@content").extract_first()

            self.logger.info('Processing title={}, author={}, date={}'.format(title, author, date))

            mileage = -1
            mileageStr = headerDiv.xpath("p[@class='duration']/strong/text()").extract_first().strip()
            mileageMatch = re.match('.*approximate mileage is ([\d,]+)', mileageStr)
            if mileageMatch:
                mileage = int(mileageMatch.groups()[0].replace(',', ''))

            scoreCard = headerDiv.xpath("div[@class='score-card']/dl")
            NA = 'Not Rated'
            ratingList = scoreCard.xpath("dd/span/text()").extract()
            overall = 0 if ratingList[0].strip() == NA else int(ratingList[0].strip().split('/')[0])
            value = 0 if ratingList[1].strip() == NA else int(ratingList[1].strip().split('/')[0])
            reliability = 0 if ratingList[2].strip() == NA else int(ratingList[2].strip().split('/')[0])
            quality = 0 if ratingList[3].strip() == NA else int(ratingList[3].strip().split('/')[0])
            performance = 0 if ratingList[4].strip() == NA else int(ratingList[4].strip().split('/')[0])
            styling = 0 if ratingList[5].strip() == NA else int(ratingList[5].strip().split('/')[0])
            comfort = 0 if ratingList[6].strip() == NA else int(ratingList[6].strip().split('/')[0])

            pros, cons = '', ''
            nRecommend = -1
            nRecommendOutOf = -1
            if recommendDiv:
                pTags = recommendDiv.xpath('p')

                if len(pTags) == 3:
                    proStr = ' '.join([x.strip() for x in pTags[0].xpath('text()').extract() if len(x.strip()) > 0])
                    pros = proStr[1:-1]

                    conStr = ' '.join([x.strip() for x in pTags[1].xpath('text()').extract() if len(x.strip()) > 0])
                    cons = conStr[1:-1]

                    recommendStr = pTags[-1].xpath('text()').extract_first()
                elif len(pTags) == 2:
                    proConTag = pTags[0]
                    proConStr = ' '.join([x.strip() for x in proConTag.xpath('text()').extract() if len(x.strip()) > 0])
                    if 'Pros' in proConTag.xpath('span/strong/text()').extract_first():
                        pros = proConStr[1:-1]
                    elif 'Cons' in proConTag.xpath('span/strong/text()').extract_first():
                        cons = proConStr[1:-1]

                    recommendStr = pTags[-1].xpath('text()').extract_first()
                elif len(pTags) == 1:
                    recommendStr = recommendDiv.xpath("p/text()").extract_first()
                else:
                    recommendStr = None

                if recommendStr:
                    matchGroups = re.match('.*\(1-(\d+)\):\s*(\d+)', recommendStr).groups()
                    nRecommend = int(matchGroups[1])
                    nRecommendOutOf = int(matchGroups[0])

            content = ''
            nHelpful, nHelpfulOutOf = -1, -1
            if contentDiv:
                content = contentDiv.xpath("p[@class='review-text']/text()").extract_first().strip()[1:-1]
                if content == 'This customer did not provide a text review.'[1:-1]:
                    content = ''

                helpful = contentDiv.xpath("p[@class='helpful-count']/strong/text()").extract()
                if helpful:
                    nHelpful = int(helpful[0])
                    nHelpfulOutOf = int(helpful[1])

            review = KBBReviewItem()
            review['author'] = author
            review['date'] = date
            review['title'] = title if title else ''
            review['content'] = content
            review['make'] = make
            review['model'] = model
            review['year'] = year
            review['mileage'] = mileage
            review['ratings']['overall'] = overall
            review['ratings']['value'] = value
            review['ratings']['reliability'] = reliability
            review['ratings']['quality'] = quality
            review['ratings']['performance'] = performance
            review['ratings']['styling'] = styling
            review['ratings']['comfort'] = comfort
            review['pros'] = pros
            review['cons'] = cons
            review['nRecommend'] = nRecommend
            review['nRecommendOutOf'] = nRecommendOutOf
            review['nHelpful'] = nHelpful
            review['nHelpfulOutOf'] = nHelpfulOutOf
            review['id'] = uuid.uuid4().hex
            review['scrapedDate'] = datetime.today().strftime(review.DATE_FORMAT)
            review['fingerprint'] = review.computeFingerprint()
            review['source'] = self.reviewSource
            review['tags'].append(year)
            review['tags'].append(make)
            review['tags'].append(model)
            yield review

        nextPage = response.xpath("//div[@class='pages']/a[@class='pagerLink pager-next']/@href").extract_first()
        if nextPage:
            nextPage = response.urljoin(nextPage).strip()
            yield scrapy.Request(url=nextPage, callback=self.parse)
