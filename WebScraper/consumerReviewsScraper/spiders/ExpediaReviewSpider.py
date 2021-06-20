import re
import simplejson
from datetime import datetime
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint
from WebScraper.consumerReviewsScraper.items.expedia import ExpediaHotelItem, ExpediaReviewItem


class ExpediaReviewSpider(Spider):
    name = 'expedia_hotel_reviews_spider'

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),

        # Auto-throttling
        'CONCURRENT_REQUESTS': 10,
        'DOWNLOAD_DELAY': 1,

        # Retry
        'CRAWLER_PAUSE_SECONDS': 60,
        'RETRY_HTTP_CODES': [429],

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,

            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'consumerReviewsScraper.middlewares.TooManyRequestsRetryMiddleware': 543,

            # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },

        'SPIDER_MIDDLEWARES': {
            'consumerReviewsScraper.middlewares.UpdateURLStatusMiddleware': 10,
        },
    }

    num_reviews_per_request = 500

    # specific_category_filters = {
    #     'Couples': {
    #         'travelCompanion': ['Partner'],
    #         'tripReason': []
    #     },
    #     'Solo travelers': {
    #         'travelCompanion': ['Self'],
    #         'tripReason': []
    #     },
    #     'Business travelers': {
    #         'travelCompanion': [],
    #         'tripReason': ['Business', 'Biz_Leisure']
    #     },
    #     'Families': {
    #         'travelCompanion': ['Family', 'Family_with_children'],
    #         'tripReason': []
    #     },
    #     'Families with small children': {
    #         'travelCompanion': ['Family_with_children'],
    #         'tripReason': []
    #     },
    #     'Groups': {
    #         'travelCompanion': ['Friends'],
    #         'tripReason': []
    #     },
    #     'Travels with pets': {
    #         'travelCompanion': ['Pet'],
    #         'tripReason': []
    #     }
    # }

    def __init__(self, url_file, *args, **kwargs):
        super(ExpediaReviewSpider, self).__init__(*args, **kwargs)
        with open(url_file) as fp:
            self.hotel_data = simplejson.load(fp)
        self.logger.info('Scraping hotels from file {}. {} target URLs to scrape.'
                         .format(url_file, len(self.hotel_data['hotels'])))

    def start_requests(self):
        for hotel in self.hotel_data['hotels']:
            hotel_id = re.findall(r'.*\.h(\d+)\..*', hotel['url'])[0]
            self.logger.info('Prepare to scrapy hotel {}. URL={}'.format(hotel['name'], hotel['url']))
            req = Request(url=hotel['url'], callback=self.parse_hotel, meta={'hotel_id': hotel_id}, dont_filter=True,
                          headers={
                              'Host': 'www.expedia.com',
                              'Connection': 'keep-alive',
                              'Accept-Language': 'en-us',
                              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                          })
            req.meta['fp'] = request_fingerprint(req)
            yield req

    def parse_hotel(self, response):
        hotel_id = response.meta['hotel_id']
        try:
            hotel_name = response.xpath("//h1[@data-stid='content-hotel-title']/text()").get().strip()
        except:
            hotel_name = None
            self.logger.warn('Parsing hotel name failed. Response={}'.format(response))
        try:
            num_reviews = int(response.xpath("//meta[@itemprop='reviewCount']/@content").get())
        except:
            self.logger.warn('Parsing num_reviews failed. Response={}'.format(response))
            num_reviews = None
        self.logger.info('Parsing hotel={}, hotelID={}, reviewCount={}'.format(hotel_name, hotel_id, num_reviews))

        try:
            hotel_intro = response.xpath(
                "//p[@class='uitk-type-paragraph-300 all-b-padding-three']/text()").get().strip()
        except:
            self.logger.warn('Parsing hotel introduction failed. Response={}'.format(response))
            hotel_intro = None

        try:
            nearby = response.xpath("//li[@class='location-summary__list-item']/dl/dt/text()").getall()
        except:
            self.logger.warn('Parsing hotel nearby-info failed. Response={}'.format(response))
            nearby = None

        try:
            around = response.xpath("//li[@class='transportation__list-item']/dl/dt/text()").getall()
        except:
            self.logger.warn('Parsing hotel around-info failed. Response={}'.format(response))
            around = None

        item = ExpediaHotelItem(
            hotel_id=hotel_id,
            name=hotel_name,
            nearby_info=simplejson.dumps(nearby),
            around_info=simplejson.dumps(around),
            introduction=hotel_intro,
            created_datetime=datetime.utcnow()
        )
        yield item

        graphql_payload = {
            'operationName': 'Reviews',
            'query': '\n        query Reviews($context: ContextInput!, $propertyId: String!, $pagination: PaginationInput!, $sortBy: PropertyReviewSort!, $filters: PropertyReviewFiltersInput!) {\n    propertyInfo(propertyId: $propertyId,  context: $context) {\n        reviewInfo(sortBy: $sortBy, pagination: $pagination, filters: $filters) {\n            summary {\n                superlative\n                totalCount {\n                    raw\n                    formatted\n                }\n                reviewCountLocalized\n                averageOverallRating {\n                    raw\n                    formatted\n                }\n                cleanliness {\n                    raw\n                    formatted\n                }\n                serviceAndStaff {\n                    raw\n                    formatted\n                }\n                amenityScore {\n                    raw\n                    formatted\n                }\n                hotelCondition {\n                    raw\n                    formatted\n                }\n                cleanlinessPercent\n                cleanlinessOverMax\n                serviceAndStaffPercent\n                serviceAndStaffOverMax\n                amenityScorePercent\n                amenityScoreOverMax\n                hotelConditionPercent\n                hotelConditionOverMax\n                ratingCounts {\n                    count {\n                        formatted\n                        raw\n                    }\n                    percent\n                    rating\n                }\n                lastIndex\n                reviewDisclaimer\n            }\n            reviews {\n                id\n                ratingOverall\n                superlative\n                submissionTime {\n                    raw\n                }\n                title\n                text\n                locale\n                author\n                userLocation\n                stayDuration\n                helpfulReviewVotes\n                negativeRemarks\n                positiveRemarks\n                locationRemarks\n                photos {\n                    description\n                    url\n                }\n                managementResponses {\n                    id\n                    date\n                    displayLocale\n                    userNickname\n                    response\n                }\n            }\n        }\n    }\n}\n\n    ',
            'variables': {
                'context': {
                    'siteId': 1,
                    'locale': 'en_US',
                    'device': {
                        'type': 'DESKTOP'
                    },
                    'identity': {
                        'duaid': '7f144b66-04b9-4684-abbb-b4a57f3c8aa9',
                        'expUserId': '-1',
                        'tuid': '-1',
                        'authState': 'ANONYMOUS',
                    },
                    'debugContext': {
                        'abacusOverrides': [],
                        'alterMode': 'RELEASED'
                    }
                },
                'propertyId': hotel_id,
                'sortBy': 'NEWEST_TO_OLDEST',
                'filters': {
                    'includeRecentReviews': True,
                    'includeRatingsOnlyReviews': True,
                    'tripReason': None,
                    'travelCompanion': None
                },
                'pagination': {
                    'startingIndex': 0,
                    'size': self.num_reviews_per_request
                }
            }
        }

        # get default category
        for start_index in range(0, num_reviews, self.num_reviews_per_request):
            graphql_payload['variables']['pagination']['startingIndex'] = start_index
            graphql_payload['variables']['filters']['tripReason'] = []
            graphql_payload['variables']['filters']['travelCompanion'] = []
            req = Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST',
                          body=simplejson.dumps(graphql_payload),
                          headers={
                              'Content-Type': 'application/json',
                              'Origin': 'https://www.expedia.com',
                              'Referer': response.url,
                              'client-info': 'shopping-pwa,unknown,unknown',
                              'credentials': 'same-origin',
                              'device-user-agent-id': '7f144b66-04b9-4684-abbb-b4a57f3c8aa9',
                              'cookie': 'tpid=v.1,1; iEAPID=0; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; NavActions=acctWasOpened; pwa_csrf=fcc6de5c-e8cc-4112-8c97-d4a6ecb86af1|XOZgVxqdxwQmwXfyblQzZQXVfwf4y-qetGLtzYGgrjs1FkYtgqW9wb0o6rNRZW6Fuc2WxCtQsT3xfxABjNlSAg; HMS=c401a9f1-a298-4662-a447-7536c67e0952; MC1=GUID=7f144b6604b94684abbbb4a57f3c8aa9; DUAID=7f144b66-04b9-4684-abbb-b4a57f3c8aa9; ak_bmsc=B7D2B9E7B4D2EB80B74EA5D708C0FA0E17202E4E7551000093B2285F24D6C041~plrZJiGsXFYN32DuLq19r/aWmi2/j2u+8jOSj8stx2xzcPeTFHt3+NoqSAXcUGNH+oMG+dRYZNMJPOksYJyyBeqZBH8Q1nAjzqhXaR/UznVnippXgaRhW5fmEn1aL5ffLqDd5wpw58Ppdr9fnnzFdHE9st+0PpOD7CFFvKhCHkYYlumKhfKgRYkyJAAi/VGSePKRs7DDMAcoKDQCK6abps4hyPGbpew82GEKC2SIA8fMw=; s_ppn=page.Hotels.Infosite.Information; CRQS=t|1`s|1`l|en_US`c|USD; CRQSS=e|0; utag_main=v_id:0173b6f9a24000123c87d0ad374d03082016b07a00c98$_sn:1$_ss:1$_st:1596504480130$ses_id:1596502680130%3Bexp-session$_pn:1%3Bexp-session; qualtrics_sample=true; qualtrics_SI_sample=true; s_ecid=MCMID%7C44196179068162063302142817747933649501; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; s_cc=true; _fbp=fb.1.1596502681668.1610379039; _gcl_au=1.1.468526097.1596502682; _ga=GA1.2.513821263.1596502682; _gid=GA1.2.634208041.1596502682; _gat_gtag_UA_35711341_2=1; xdid=ebcc5d2e-10f7-4668-b64b-b74bb32bf69a|1596502684|expedia.com; lastConsentChange=1596502688725; s_ppvl=https%253A%2F%2Fwww.expedia.com%2FSan-Francisco-Hotels-Adante-Hotel.h789395.Hotel-Information%253Fchkin%253D8%25252F1%25252F2020%2526chkout%253D8%25252F2%25252F2020%2526rm1%253Da1%2526lodging%253Dhotel%25252Cmotel%25252Cinn%25252Chostel%2526star%253D30%2526sort%253DstarRating%2526hwrqCacheKey%253D7b6972e0-b873-4022-83e3-c69bae2ecce9HWRQ1596296706596%2526cancellable%253Dfalse%2526regionId%253D6151902%2526vip%253Dfalse%2526regionFilter%253D6151902%2526c%253D836eb991-6f88-4561-bf98-2203e3a1c820%2526%2C100%2C100%2C6582%2C1172%2C1050%2C2048%2C1152%2C1.25%2CP; s_ppv=page.Hotels.Infosite.Information%2C17%2C17%2C1050%2C1172%2C1050%2C2048%2C1152%2C1.25%2CP; JSESSIONID=469415FC09BAEC850E1487C2844F6F4D; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1596502696572%5D%2C%22hitNumber%22%3A%5B%226%22%2C1596502696572%5D%2C%22visitNumber%22%3A%5B%221%22%2C1596502675624%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1596502696572%5D%7D; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18479%7CMCMID%7C44196179068162063302142817747933649501%7CMCAID%7CNONE%7CMCOPTOUT-1596509880s%7CNONE%7CMCAAMLH-1597107498%7C9%7CMCAAMB-1597107498%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C-2034012538%7CvVersion%7C2.5.0; user=; minfo=; accttype=',
                          },
                          meta={
                              'hotel_name': hotel_name,
                              'hotel_id': hotel_id,
                              'num_reviews': num_reviews,
                              'start_index': start_index
                          })
            req.meta['fp'] = request_fingerprint(req)
            yield req

        # get specific category - this may override items scraped in default category
        # for start_index in range(0, num_reviews, self.num_reviews_per_request):
        #     for category, category_filter in self.specific_category_filters.items():
        #         graphql_payload['variables']['pagination']['startingIndex'] = start_index
        #         graphql_payload['variables']['filters']['tripReason'] = category_filter['tripReason']
        #         graphql_payload['variables']['filters']['travelCompanion'] = category_filter['travelCompanion']
        #         yield Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST',
        #                       body=simplejson.dumps(graphql_payload),
        #                       headers={
        #                           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15',
        #                           'Content-Type': 'application/json',
        #                           'Origin': 'https://www.expedia.com',
        #                           'Referer': response.url,
        #                           'Cookie': 'cookie: tpid=v.1,1; iEAPID=0; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; pwa_csrf=d0786c3e-6946-4203-90b8-5ccd63d7a725|D3ahwRzbqnKIa9zr4_LpV1aNOwvz7xCzv6wnYCvRbhP4QOOwjDpY4SAzXvwCp8Oiho_ZjhGg4GwOuWEpJkqRcQ; MC1=GUID=0744d4a3ce1c4dd0bdc17a105a1f69d4; DUAID=0744d4a3-ce1c-4dd0-bdc1-7a105a1f69d4; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; s_ecid=MCMID%7C06179023703179136360062832346793475516; s_cc=true; _gcl_au=1.1.563409396.1568519291; _ga=GA1.2.1504753458.1568519291; xdid=ece7fa84-2935-467b-af14-517d385dc195|1568519294|expedia.com; _gid=GA1.2.425040987.1568735474; HMS=1f6643fd-2b09-4014-854a-6bb5a1b7e879; ak_bmsc=88CCC145B5CA605E85FBCD4EF0121AAA1724025E7E3B00007AFF825D33C5F203~pl8tx0aas9asJvQwdlnlBIcCCdeE4DOc6zW6sL7TOMkCR0rgs3x37rEHjVGhzzyJHE6zMow4hOqbaAsq+iPVq2luaM5Vpn5ZC6yJk1ZNqyV6hCpckz0l+ARafQrkIuDpXvCafNy/4s4l+iLDZF+KlgyGpTL5wfQENJL1Y1uaW9GyIOTdn2AuO6Xm3P4mpX/+zBXDxS/Usva1+3WDqTso3/UImxmPgajy79iH1lCfbr2Ps=; s_ppn=page.Hotels.Infosite.Information; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18159%7CvVersion%7C2.5.0%7CMCMID%7C06179023703179136360062832346793475516%7CMCAAMLH-1569470973%7C9%7CMCAAMB-1569470973%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1568873373s%7CNONE%7CMCAID%7CNONE; x-CGP-exp-30353=3; x-CGP-exp-15795=2; x-CGP-exp-28702=0; JSESSIONID=5FDA37C2D7FABC5FED3401C0D6D9D434; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1568866203682%5D%2C%22hitNumber%22%3A%5B%227%22%2C1568866203682%5D%2C%22visitNumber%22%3A%5B%226%22%2C1568866172885%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568866203682%5D%7D; utag_main=v_id:016d3308fe4f0017a4cce4e7b2a403072012a06a00c98$_sn:6$_ss:0$_st:1568868003252$ses_id:1568866174009%3Bexp-session$_pn:4%3Bexp-session; s_ppvl=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information%253Fchkin%253D9%25252F22%25252F2019%2526chkout%253D9%25252F23%25252F2019%2526regionId%253D179900%2526destination%253DTokyo%2B%252528and%2Bvicinity%252529%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526sort%253Drecommended%2526top_dp%253D118%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1568094513466%2C13%2C13%2C1050%2C1960%2C1050%2C2048%2C1152%2C1.25%2CP; s_ppv=page.Hotels.Infosite.Information%2C13%2C13%2C1050%2C1039%2C1050%2C2048%2C1152%2C1.25%2CL',
        #                       },
        #                       meta={
        #                           'hotel_name': hotel_name,
        #                           'hotel_id': property_ID,
        #                           'category': category,
        #                           'num_reviews': num_reviews,
        #                           'start_index': start_index
        #                       })

    def parse_graphql(self, response):
        hotel_name = response.meta['hotel_name']
        hotel_id = response.meta['hotel_id']
        num_reviews = response.meta['num_reviews']
        start_index = response.meta['start_index']
        json = simplejson.loads(response.body_as_unicode())
        self.logger.info('Parsing GraphQL response. Hotel={}, Hotel_ID={}, ReviewCount={}, StartIndex={}'
                         .format(hotel_name, hotel_id, num_reviews, start_index))

        if json.get('data', None) is None:
            self.logger.warn('Cannot get review data from GraphQL response. Response is %s' % json)
            return

        for review in json['data']['propertyInfo']['reviewInfo']['reviews']:
            item = ExpediaReviewItem(
                review_id=review['id'],
                author=review['author'],
                publish_datetime=datetime.strptime(review['submissionTime']['raw'].strip(), '%Y-%m-%dT%H:%M:%SZ'),
                content=review['text'],
                created_datetime=datetime.utcnow(),
                overall_rating=review['ratingOverall'],
                num_helpful=review['helpfulReviewVotes'],
                stay_duration=review['stayDuration'],
                superlative=review['superlative'],
                title=review['title'],
                locale=review['locale'],
                location=review['userLocation'],
                remarks_positive=review['positiveRemarks'],
                remarks_negative=review['negativeRemarks'],
                remarks_location=review['locationRemarks'],
                hotel_id=hotel_id)

            if len(review['managementResponses']) > 0:
                mgmt_resp = review['managementResponses'][0]
                item['response_id'] = mgmt_resp['id']
                item['response_author'] = mgmt_resp['userNickname']
                try:
                    item['response_publish_datetime'] = datetime.strptime(mgmt_resp['date'].strip(),
                                                                          '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    self.logger.warn('Parsing publish datetime for response failed. Value={}. ItemURL={}'
                                     .format(mgmt_resp['date'].strip(), response.url))
                    item['response_publish_datetime'] = None
                item['response_content'] = mgmt_resp['response']
                item['response_display_locale'] = mgmt_resp['displayLocale']
            else:
                item['response_id'] = None
                item['response_author'] = None
                item['response_publish_datetime'] = None
                item['response_content'] = None
                item['response_display_locale'] = None
            yield item
