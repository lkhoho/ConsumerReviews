import re
import simplejson
from datetime import datetime
from fake_useragent import UserAgent
from scrapy.http import Request, Response
from scrapy.spiders import CrawlSpider
from ..items.expedia import ExpediaReviewItem


class ExpediaReviewSpider(CrawlSpider):
    name = 'expedia_hotel_reviews_spider'

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),

        # default user agent
        # 'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
        # 'RETRY_HTTP_CODES': [429],

        # Auto-throttling
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 1,

        # a list of proxies
        # 'ROTATING_PROXY_LIST': [
        #     '103.42.253.183:34954',
        #     '85.172.12.245:8118',
        #     '37.200.224.179:8080',
        #     '103.80.238.195:53281',
        #     '125.26.7.115:30012',
        #     '138.186.21.86:53281',
        #     '183.87.153.98:49602',
        #     '1.179.206.161:49817',
        #     '192.140.42.81:33954',
        #     '1.20.100.45:51685',
        #     '179.108.86.219:49439',
        #     '180.92.238.226:53451',
        #     '137.116.120.30:80',
        #     '167.71.161.102:8080',
        #     '198.50.152.64:23500',
        #     '12.8.246.133:41845',
        # ],

        # cap of backoff (1 day)
        # 'ROTATING_PROXY_BACKOFF_CAP': 3600 * 24,

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,
            # 'consumerReviewsScraper.middlewares.TooManyRequestsRetryMiddleware': 543,
            # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
    }

    # user_agent = UserAgent()  # fake user agent

    start_urls = [
        'https://www.expedia.com/Tokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information?chkin=9%2F22%2F2019&chkout=9%2F23%2F2019&destination=Tokyo%20%28and%20vicinity%29%2C%20Japan&pwaDialog=reviews&pwa_ts=1568094513466&regionId=179900&rfrr=HSR&rm1=a2&sort=recommended&swpToggleOn=true&top_cur=USD&top_dp=118&x_pwa=1',
    ]

    num_reviews_per_request = 10

    # filter definitions used in graphql query
    category_filters = {
        'Couples': {
            'travelCompanion': ['Partner'],
            'tripReason': []
        },
        'Solo travelers': {
            'travelCompanion': ['Self'],
            'tripReason': []
        },
        'Business travelers': {
            'travelCompanion': [],
            'tripReason': ['Business', 'Biz_Leisure']
        },
        'Families': {
            'travelCompanion': ['Family', 'Family_with_children'],
            'tripReason': []
        },
        'Families with small children': {
            'travelCompanion': ['Family_with_children'],
            'tripReason': []
        },
        'Groups': {
            'travelCompanion': ['Friends'],
            'tripReason': []
        },
        'Travels with pets': {
            'travelCompanion': ['Pet'],
            'tripReason': []
        },
        'N/A': {
            'travelCompanion': [],
            'tripReason': []
        }
    }

    def parse_start_url(self, response):
        for url in self.start_urls:
            property_ID = re.findall(r'.*\.h(\d+)\..*', url)[0]
            yield Request(url=url, callback=self.parse_first_request, meta={'property_ID': property_ID},
                          headers={
                              'Cookie': 'tpid=v.1,1; iEAPID=0; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; pwa_csrf=d0786c3e-6946-4203-90b8-5ccd63d7a725|D3ahwRzbqnKIa9zr4_LpV1aNOwvz7xCzv6wnYCvRbhP4QOOwjDpY4SAzXvwCp8Oiho_ZjhGg4GwOuWEpJkqRcQ; MC1=GUID=0744d4a3ce1c4dd0bdc17a105a1f69d4; DUAID=0744d4a3-ce1c-4dd0-bdc1-7a105a1f69d4; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; s_ecid=MCMID%7C06179023703179136360062832346793475516; s_cc=true; _gcl_au=1.1.563409396.1568519291; _ga=GA1.2.1504753458.1568519291; xdid=ece7fa84-2935-467b-af14-517d385dc195|1568519294|expedia.com; _gid=GA1.2.425040987.1568735474; HMS=1f6643fd-2b09-4014-854a-6bb5a1b7e879; ak_bmsc=88CCC145B5CA605E85FBCD4EF0121AAA1724025E7E3B00007AFF825D33C5F203~pl8tx0aas9asJvQwdlnlBIcCCdeE4DOc6zW6sL7TOMkCR0rgs3x37rEHjVGhzzyJHE6zMow4hOqbaAsq+iPVq2luaM5Vpn5ZC6yJk1ZNqyV6hCpckz0l+ARafQrkIuDpXvCafNy/4s4l+iLDZF+KlgyGpTL5wfQENJL1Y1uaW9GyIOTdn2AuO6Xm3P4mpX/+zBXDxS/Usva1+3WDqTso3/UImxmPgajy79iH1lCfbr2Ps=; s_ppn=page.Hotels.Infosite.Information; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18159%7CvVersion%7C2.5.0%7CMCMID%7C06179023703179136360062832346793475516%7CMCAAMLH-1569470973%7C9%7CMCAAMB-1569470973%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1568873373s%7CNONE%7CMCAID%7CNONE; _gat_gtag_UA_35711341_2=1; x-CGP-exp-30353=3; x-CGP-exp-15795=2; x-CGP-exp-28702=0; utag_main=v_id:016d3308fe4f0017a4cce4e7b2a403072012a06a00c98$_sn:6$_ss:0$_st:1568867990819$ses_id:1568866174009%3Bexp-session$_pn:3%3Bexp-session; JSESSIONID=0AC11F5098F8DC8F4236C014032BB700; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1568866191757%5D%2C%22hitNumber%22%3A%5B%225%22%2C1568866191757%5D%2C%22visitNumber%22%3A%5B%226%22%2C1568866172885%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568866191757%5D%7D; s_ppvl=page.Hotels.Infosite.Information%2C13%2C13%2C1050%2C1039%2C1050%2C2048%2C1152%2C1.25%2CL; s_ppv=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information%253Fchkin%253D9%25252F22%25252F2019%2526chkout%253D9%25252F23%25252F2019%2526regionId%253D179900%2526destination%253DTokyo%2B%252528and%2Bvicinity%252529%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526sort%253Drecommended%2526top_dp%253D118%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1568094513466%2C13%2C13%2C1050%2C1960%2C1050%2C2048%2C1152%2C1.25%2CP',
                          },
                        #   headers={
                        #       'sec-fetch-user': '?1',
                        #       'sec-fetch-site': 'sec-fetch-site',
                        #       'sec-fetch-mode': 'navigate',
                        #       'utag_main': 'v_id:016d3308fe4f0017a4cce4e7b2a403072012a06a00c98',
                        #       's_cc': True,
                        #       's_ecid': 'MCMID%7C06179023703179136360062832346793475516',
                        #       's_ppn': 'page.Hotels.Infosite.Information',
                        #       's_ppv': 'page.Hotels.Infosite.Information%2C13%2C13%2C1050%2C1039%2C1050%2C2048%2C1152%2C1.25%2CL',
                        #       's_ppvl': 'https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information%253Fchkin%253D9%25252F22%25252F2019%2526chkout%253D9%25252F23%25252F2019%2526regionId%253D179900%2526destination%253DTokyo%2B%252528and%2Bvicinity%252529%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526sort%253Drecommended%2526top_dp%253D118%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1568094513466%2C13%2C13%2C1050%2C1960%2C1050%2C2048%2C1152%2C1.25%2CP',
                        #       'pwa_csrf': 'd0786c3e-6946-4203-90b8-5ccd63d7a725|D3ahwRzbqnKIa9zr4_LpV1aNOwvz7xCzv6wnYCvRbhP4QOOwjDpY4SAzXvwCp8Oiho_ZjhGg4GwOuWEpJkqRcQ',
                        #       'cesc': '%7B%22marketingClick%22%3A%5B%22false%22%2C1568525713230%5D%2C%22hitNumber%22%3A%5B%224%22%2C1568525713230%5D%2C%22visitNumber%22%3A%5B%222%22%2C1568524993321%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568525713230%5D%7D',
                        #       'ak_bmsc': '6CC530EF769AD1CDEBBF4C35570224041724025E7E3B000078B47D5D2546000C',
                        #       'MC1': 'GUID=0744d4a3ce1c4dd0bdc17a105a1f69d4',
                        #       'JSESSIONID': 'B0AEE38C898B8FA1C951AF935140D259',
                        #       'HMS': 'ff56ac79-13c2-4077-b2a7-cbda7ccd6775',
                        #       'DUAID': '0744d4a3-ce1c-4dd0-bdc1-7a105a1f69d4',
                        #       'xdid': 'ece7fa84-2935-467b-af14-517d385dc195|1568519294|expedia.com',
                        #   }, 
                        )


    def parse_first_request(self, response):
        property_ID = response.meta['property_ID']
        hotel_name = response.xpath("//h1[@data-stid='content-hotel-title']/text()").get().strip()
        num_reviews = int(response.xpath("//meta[@itemprop='reviewCount']/@content").get())
        self.logger.info('Parsing hotel={}, hotelID={}, reviewCount={}'.format(hotel_name, property_ID, num_reviews))

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
                        'duaid': '1b7e66a7-c1f7-403e-9d34-8d5e694b9fc7', 
                        'expUserId': '-1',
                        'tuid': '-1',
                        'authState': 'ANONYMOUS',
                    },
                    'debugContext': {
                        'abacusOverrides': [],
                        'alterMode': 'RELEASED'
                    }
                },
                'propertyId': property_ID,
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

        for start_index in range(0, num_reviews, self.num_reviews_per_request):
            graphql_payload['variables']['pagination']['startingIndex'] = start_index
            for category, category_filter in self.category_filters.items():
                graphql_payload['variables']['filters']['tripReason'] = category_filter['tripReason']
                graphql_payload['variables']['filters']['travelCompanion'] = category_filter['travelCompanion']
                yield Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST', body=simplejson.dumps(graphql_payload),
                              headers={
                                  'Content-Type': 'application/json',
                                  'Origin': 'https://www.expedia.com',
                                  'Referer': response.url,
                                  'Cookie': 'cookie: tpid=v.1,1; iEAPID=0; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; pwa_csrf=d0786c3e-6946-4203-90b8-5ccd63d7a725|D3ahwRzbqnKIa9zr4_LpV1aNOwvz7xCzv6wnYCvRbhP4QOOwjDpY4SAzXvwCp8Oiho_ZjhGg4GwOuWEpJkqRcQ; MC1=GUID=0744d4a3ce1c4dd0bdc17a105a1f69d4; DUAID=0744d4a3-ce1c-4dd0-bdc1-7a105a1f69d4; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; s_ecid=MCMID%7C06179023703179136360062832346793475516; s_cc=true; _gcl_au=1.1.563409396.1568519291; _ga=GA1.2.1504753458.1568519291; xdid=ece7fa84-2935-467b-af14-517d385dc195|1568519294|expedia.com; _gid=GA1.2.425040987.1568735474; HMS=1f6643fd-2b09-4014-854a-6bb5a1b7e879; ak_bmsc=88CCC145B5CA605E85FBCD4EF0121AAA1724025E7E3B00007AFF825D33C5F203~pl8tx0aas9asJvQwdlnlBIcCCdeE4DOc6zW6sL7TOMkCR0rgs3x37rEHjVGhzzyJHE6zMow4hOqbaAsq+iPVq2luaM5Vpn5ZC6yJk1ZNqyV6hCpckz0l+ARafQrkIuDpXvCafNy/4s4l+iLDZF+KlgyGpTL5wfQENJL1Y1uaW9GyIOTdn2AuO6Xm3P4mpX/+zBXDxS/Usva1+3WDqTso3/UImxmPgajy79iH1lCfbr2Ps=; s_ppn=page.Hotels.Infosite.Information; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18159%7CvVersion%7C2.5.0%7CMCMID%7C06179023703179136360062832346793475516%7CMCAAMLH-1569470973%7C9%7CMCAAMB-1569470973%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1568873373s%7CNONE%7CMCAID%7CNONE; x-CGP-exp-30353=3; x-CGP-exp-15795=2; x-CGP-exp-28702=0; JSESSIONID=5FDA37C2D7FABC5FED3401C0D6D9D434; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1568866203682%5D%2C%22hitNumber%22%3A%5B%227%22%2C1568866203682%5D%2C%22visitNumber%22%3A%5B%226%22%2C1568866172885%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568866203682%5D%7D; utag_main=v_id:016d3308fe4f0017a4cce4e7b2a403072012a06a00c98$_sn:6$_ss:0$_st:1568868003252$ses_id:1568866174009%3Bexp-session$_pn:4%3Bexp-session; s_ppvl=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information%253Fchkin%253D9%25252F22%25252F2019%2526chkout%253D9%25252F23%25252F2019%2526regionId%253D179900%2526destination%253DTokyo%2B%252528and%2Bvicinity%252529%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526sort%253Drecommended%2526top_dp%253D118%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1568094513466%2C13%2C13%2C1050%2C1960%2C1050%2C2048%2C1152%2C1.25%2CP; s_ppv=page.Hotels.Infosite.Information%2C13%2C13%2C1050%2C1039%2C1050%2C2048%2C1152%2C1.25%2CL',
                              },
                              meta={'hotel_name': hotel_name, 'hotel_id': property_ID, 'category': category, 'num_reviews': num_reviews, 'start_index': start_index})
        
    def parse_graphql(self, response):
        hotel_name = response.meta['hotel_name']
        hotel_id = response.meta['hotel_id']
        category = response.meta['category']
        num_reviews = response.meta['num_reviews']
        start_index = response.meta['start_index']
        json = simplejson.loads(response.body_as_unicode())
        total_count = int(json['data']['propertyInfo']['reviewInfo']['summary']['totalCount']['raw'])
        last_index = int(json['data']['propertyInfo']['reviewInfo']['summary']['lastIndex'])
        self.logger.info('Parsing GraphQL response. Hotel={}, HotelID={}, Category={}, ReviewCountInTotal={}, ReviewCountInCategory={}, StartIndex={}, LastIndex={}'.format(
            hotel_name, hotel_id, category, num_reviews, total_count, start_index, last_index))
        
        for review in json['data']['propertyInfo']['reviewInfo']['reviews']:
            item = ExpediaReviewItem(
                review_id=review['id'].strip(),
                author=review['author'].strip(),
                publish_datetime=datetime.strptime(review['submissionTime']['raw'].strip(), '%Y-%m-%dT%H:%M:%SZ'),
                content=review['text'].strip(),
                created_datetime=datetime.utcnow(),
                overall_rating=int(review['ratingOverall'].strip()),
                num_helpful=int(review['helpfulReviewVotes'].strip()),
                stay_duration=review['stayDuration'].strip(),
                category=category,
                superlative=review['superlative'].strip(),
                title=review['title'].strip(),
                locale=review['locale'].strip(),
                location=review['userLocation'].strip(),
                remarks_positive=review['positiveRemarks'].strip(),
                remarks_negative=review['negativeRemarks'].strip(),
                remarks_location=review['locationRemarks'].strip(),
                hotel_id=hotel_id,
                hotel_name=hotel_name)
            if len(review['managementResponses']) > 0:
                mgmt_resp = review['managementResponses'][0]
                item['response_id'] = mgmt_resp['id'].strip()
                item['response_author'] = mgmt_resp['userNickname'].strip()
                item['response_publish_datetime'] = datetime.strptime(mgmt_resp['date'].strip(), '%Y-%m-%dT%H:%M:%SZ')
                item['response_content'] = mgmt_resp['response'].strip()
                item['response_display_locale'] = mgmt_resp['displayLocale'].strip()
            yield item
