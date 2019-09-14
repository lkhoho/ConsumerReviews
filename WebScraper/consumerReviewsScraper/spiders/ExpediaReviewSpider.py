import re
from fake_useragent import UserAgent
from scrapy.http import Request, Response
from scrapy.spiders import CrawlSpider
from ..items.expedia import ExpediaReviewItem


class ExpediaReviewSpider(CrawlSpider):
    name = 'expedia_hotel_reviews_spider'
    start_urls = [
        'https://www.expedia.com/Tokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information?chkin=9%2F22%2F2019&chkout=9%2F23%2F2019&regionId=179900&destination=Tokyo+%28and+vicinity%29%2C+Japan&swpToggleOn=true&rm1=a2&x_pwa=1&sort=recommended&top_dp=118&top_cur=USD&rfrr=HSR&pwa_ts=1568094513466',
    ]
    num_reviews_per_request = 10
    user_agent = UserAgent()  # fake user agent

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

    def parse_start_url(self):
        for url in self.start_urls:
            property_ID = re.findall(r'.*\.h(\d+)\..*', url)[0]
            yield Request(url=url, callback=self.parse, headers={'User-Agent': self.user_agent.random}, meta={'property_ID': property_ID})


    def parse(self, response):
        property_ID = response.meta['property_ID']
        hotel_name = response.xpath("//h1[@data-stid='content-hotel-title']/text()").get().strip()
        num_reviews = int(response.xpath("//meta[@itemprop='reviewCount']/@content").get())
        self.logger.info('Parsing hotel={}, reviewCount={}'.format(hotel_name, num_reviews))

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
                    'tripReason': [],
                    'travelCompanion': []
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
                yield Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST', body=graphql_payload,
                              headers={
                                  'Content-Type': 'application/json',
                                  'Origin': 'https://www.expedia.com',
                                  'Referer': response.url,
                                  'Credentials' : 'same-origin',
                                  'Accept': '*/*',
                                  'Accept-Encoding': 'gzip, deflate, br',
                                  ':authority': 'www.expedia.com',
                                  ':method': 'POST',
                                  ':path': '/graphql',
                                  ':scheme': 'https',
                                  'device-user-agent-id': '1b7e66a7-c1f7-403e-9d34-8d5e694b9fc7',
                              },
                              cookies={
                                  'tpid': 'v.1,1',
                                  'iEAPID': 0,
                                  'currency': 'USD',
                                  'linfo': 'v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1',
                                  'MC1': 'GUID=1b7e66a7c1f7403e9d348d5e694b9fc7',
                                  'DUAID': '1b7e66a7-c1f7-403e-9d34-8d5e694b9fc7',
                                  'aspp': 'v.1,0|||||||||||||',
                                  'stop_mobi': 'yes', 
                                  'ipsnf3': 'v.3|us|1|753|chandler',
                                  'AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg': 1,
                                  's_ecid': 'MCMID%7C64964746673589299820431222456632799247',
                                  'AB_Test_TripAdvisor': 'A',
                                  'qualtrics_sample': False,
                                  'qualtrics_SI_sample': True,
                                  's_cc': True,
                                  '_fbp': 'fb.1.1567922778333.528356505',
                                  '_gcl_au': '1.1.1858745471.1567922778',
                                  '_ga': 'GA1.2.192394631.1567922779',
                                  'xdid': '0268f63e-c355-4250-b5c4-8a68a4d85c69|1567922780|expedia.com',
                                  'pwa_csrf': '4b8ad7b2-2512-4a04-9094-858a54e1ec5e|bvLogfjfVKVsAVdEF4EDe22udRD1tt8kiKgBm9_ji6RFhaxJE2rehsExnpikyc4o78WppdZJxs29-r0_7E_4WA',
                                  'x-cgp-exp': '27461.79023.pwa',
                                  '_gid': 'GA1.2.828196263.1568094513',
                                  'JSESSIONID': 'A8B8F84B476BDECEA0E24D5E70B462C3',
                                  '_tq_id.TV-721872-1.7ec4': 'a910f9b0b934e604.1567922779.0.1568097383..',
                                  'CONSENTMGR': 'ts:1568097392866%7Cconsent:true',
                                  'ak_bmsc': '4A1A6CB44B6331068667D2C106B094D91724021E01050000A268775DB129CB66~plohMD5QWEhGHtnwwctPNN0VfEM88f8W28qt6me9fkjK9UDvw3d3PShxbny+QK8OYvQ2agkabgFXq6xGQLcrZowUhovGl/+/eHVds0jLQeN4zO9uz0RqgPzLLCMvpNr59kCnz6v4NqPIcT8m5J35rJ+CYMQQW8Zs2FouzZESujt3am+ZX/9Zx0o+9wDQ+AEjiykoFlazHSSsETRisT9LzC6s1xTW/cLghGMq/nmpleR4o=',
                                  'AMCV_C00802BE5330A8350A490D4C%40AdobeOrg': '1406116232%7CMCIDTS%7C18150%7CMCMID%7C64964746673589299820431222456632799247%7CMCAAMLH-1568711460%7C9%7CMCAAMB-1568711460%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1568113860s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C2.5.0',
                                  'JSESSION': '980f38ee-14b2-4973-b62e-d38de2f9eeeb',
                                  'utag_main': 'v_id:016d0f7aecb800103991ef5cc8e603073001806b00c98$_sn:5$_ss:0$_st:1568108496497$ses_id:1568106660468%3Bexp-session$_pn:3%3Bexp-session',
                                  'cesc': '%7B%22marketingClick%22%3A%5B%22false%22%2C1568106696760%5D%2C%22hitNumber%22%3A%5B%226%22%2C1568106696760%5D%2C%22visitNumber%22%3A%5B%225%22%2C1568106658971%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568106696760%5D%2C%22cid%22%3A%5B%22Brand.DTI%22%2C1567922777381%5D%7D',
                                  's_ppvl': 'page.Hotels.Infosite.Information%2C14%2C14%2C1050%2C1043%2C1050%2C2048%2C1152%2C1.25%2CL',
                                  'HMS': 'b8d00bbc-61b0-499e-ba63-856aad123dd8',
                                  's_ppv': 'page.Hotels.Infosite.Information%2C54%2C14%2C5250%2C1043%2C1050%2C2048%2C1152%2C1.25%2CL',
                              },
                              meta={'hotel_name': hotel_name, 'hotel_id': property_ID, 'category': category, 'num_reviews': num_reviews, 'start_index': start_index})
        
    def parse_graphql(self, response):
        hotel_name = response.meta['hotel_name']
        hotel_id = response.meta['hotel_id']
        category = response.meta['category']
        self.logger.info('Parsing GraphQL response. Hotel={}, Category={}, '.format(hotel_name, num_reviews))



        selectors = response.xpath("//section[@id='reviews']/article")

        for selector in selectors:
            review = ExpediaReviewItem()
            review["score"] = int(selector.xpath("div[@class='summary']/span/span/text()").extract_first())
            val = selector.xpath("div[@class='summary']/blockquote/text()").re(r"for .+")
            if len(val) > 0:
                review["recommend_for"] = val[0].replace("for ", "")
                review["will_recommend"] = 1
            val = selector.xpath("div[@class='summary']/blockquote/div/text()").extract_first().strip().replace("\xa0", " ")
            m = re.match(r"by\s*([\w\s]+) from\s*([\w\s,]+)", val)
            if m is not None:
                review["author"] = m.group(1)
                review["location"] = m.group(2)
            else:
                review["author"] = re.match(r"by\s*([\w\s]+)", val).group(1)
            val = selector.xpath("div[@class='details']/h3/text()").extract_first()
            if val is not None:
                review["title"] = val.replace("\r\n", "").replace("\"", "'")
            review["date"] = selector.xpath("div[@class='details']/span/text()").extract_first().strip().replace("Posted ", "")
            val = selector.xpath("div[@class='details']/div[@class='review-text']/text()").extract_first()
            if val is not None:
                review["content"] = val.strip().replace("\r\n", "")
            remark_selectors = selector.xpath("div[@class='details']/div[@class='remark']")
            for remark_selector in remark_selectors:
                k = remark_selector.xpath("strong/text()").extract_first().lower().replace(":", "")
                v = "".join(remark_selector.xpath("text()").extract()).strip().replace("\r\n", " ")
                review["remark"][k] = v
            response_selector = selector.xpath("div[@class='details']/div[@class='management-response']")
            if len(response_selector) > 0:
                response_selector = response_selector[0]
                review["response"]["title"] = response_selector.xpath("div[@class='title']/text()").extract_first().strip()
                review["response"]["content"] = response_selector.xpath("div[@class='text']/text()").extract_first().strip()
                val = response_selector.xpath("div[@class='date-posted']/text()").extract_first().strip()
                m = re.match(r"(.+)\sby\s*(.+)", val)
                if m is not None:
                    review["response"]["date"] = m.group(1)
                    review["response"]["author"] = m.group(2)
                else:
                    review["response"]["author"] = re.match(r"by\s*(.+)", val).group(1).strip()
            yield review
