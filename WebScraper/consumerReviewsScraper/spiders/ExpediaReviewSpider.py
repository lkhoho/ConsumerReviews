import re
import os
import simplejson
from datetime import datetime
from scrapy.spiders import Spider
from scrapy.http import Request
from ..items.expedia import ExpediaHotelItem, ExpediaReviewItem


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

    def start_requests(self):
        with open(os.sep.join(['data_sources', 'expedia.com', 'ny_manhattan_hotels.json'])) as fp:
            hotel_data = simplejson.load(fp)
        target_urls = [hotel['url'] for hotel in hotel_data['hotels']]
        self.logger.info('%d target URLs to scrape.' % len(target_urls))
        for url in target_urls:
            hotel_id = re.findall(r'.*\.h(\d+)\..*', url)[0]
            # cookies_str = 's_ppv=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-Daiichi-Hotel-Tokyo-Seafort.h54207.Hotel-Information%253Fchkin%253D9%25252F28%25252F2019%2526chkout%253D9%25252F29%25252F2019%2526regionId%253D179165%2526destination%253DShinagawa%25252C%2BTokyo%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526price%253D0%25252C300%2526sort%253Drecommended%2526lodging%253Dryoken%2526top_dp%253D154%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1569187236031%2C76%2C11%2C9670%2C1817%2C1329%2C2560%2C1440%2C1%2CP; im_snid=218b3ae6-9483-40a2-a299-15be9658d5fe; intent_media_prefs=; _ga=GA1.2.1987570637.1566711464; _gcl_au=1.1.1400176059.1566711466; _gid=GA1.2.1658107351.1569111229; s_ppvl=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-Daiichi-Hotel-Tokyo-Seafort.h54207.Hotel-Information%253Fchkin%253D9%25252F28%25252F2019%2526chkout%253D9%25252F29%25252F2019%2526regionId%253D179165%2526destination%253DShinagawa%25252C%2BTokyo%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526price%253D0%25252C300%2526sort%253Drecommended%2526lodging%253Dryoken%2526top_dp%253D154%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1569187236031%2C11%2C11%2C1329%2C1817%2C1329%2C2560%2C1440%2C1%2CP; HMS=5869e9df-9fde-4bf3-9fe1-514c1e2a57ab; utag_main=v_id:016cc747baad00202ab6ff91d52401077001806f00838$_sn:6$_ss:1$_st:1569209282810$ses_id:1569207482810%3Bexp-session$_pn:1%3Bexp-session; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18162%7CMCMID%7C31160316888969874832212038078424992583%7CMCAAMLH-1569812281%7C9%7CMCAAMB-1569812281%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1569214681s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C2.5.0; accttype=v.2,8,1,EX01313DA2A31$F8$0D$5C$83$A1n$82$9B$1D$E4C$D57$E7$2E$E4$13$90$CF$1B$9Dv$F2Jq$F2$8F$15$B6Q$82; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1569207481206%5D%2C%22hitNumber%22%3A%5B%222%22%2C1569207481206%5D%2C%22visitNumber%22%3A%5B%226%22%2C1569207476842%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1569207481206%5D%2C%22cid%22%3A%5B%22Brand.DTI%22%2C1566711458123%5D%7D; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; minfo=v.5,EX012CE7CFD51$F8$0D$5C$9E$A1n!2$9B$1A$E4I$D51$E7$2E$E0$10$90$CF$1E$9Dv$F2J$7C$F2$80$1F$B6m$82$EB$DA$DF$B4wu$CC$83$E6$F7$E4$A2$F9q$E8$8A$AD$C3$B4$C5$CA$F9H$A0y$B3$B8S$FBK; s_cc=true; JSESSIONID=E0745E6433B9B513DD3B171202D77C22; x-CGP-exp-15795=2; x-CGP-exp-28702=0; x-CGP-exp-30353=3; s_ppn=page.Hotels.Infosite.Information; ak_bmsc=7ACDDF94046748D616665E71B323E4B21724025E65310000B434885D2BCD8A45~plravG6WC0jY3RwvKTEB42qG7PjqR+E/5CAZfDY0h5f1t8blMx2ik/Su4AUO/zrk3TeTuqc/IHHkWRz8oWPwwm4nZ3YfnEYOKUT578rc2sBgTklaecGHzmjrQg47HW97AlP7BDUVui0pacISUDNj0LvyWEqwXgvT1PDbFvGaKQ1Lf734c5vuh+3Xzjsq3+h0WCefUoYmZ6DzR5s+b8isthFnlef0NC0Yz7E1G1P1E7k9M=; pwa_csrf=0465e163-ba20-4396-91ea-95235164a013|aWYqXfr0mLyGo88Z4vG1xwlKSu5a0gxXv-WuQnOdr_59FR3ekNs2cj3a_cScP4Vl6Xsd9wufR4TuwZH-8wIPqQ; __gads=ID=7353435ca3b8ecb5:T=1566711464:RT=1569198472:S=ALNI_MY-zgBsfpIoNyUKhpCpNIXu4nTprA; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; iEAPID=0; _fbp=fb.1.1566711467999.1560575220; _tq_id.TV-721872-1.7ec4=21e10b98037947c8.1566711467.0.1569111229..; CONSENTMGR=ts:1569111222574%7Cconsent:true; rlt_marketing_code_cookie=; tpid=v.1,1; _ctpuid=7401134c-532a-437a-a5d8-bb71695abf37; x-cgp-exp=27461.79023.pwa; 0e16e784-167b-49ee-b1ac-0ff7866eb4fbfaktorChecksum=1074992617; IM_xu_2_freq_cap=Y; 0e16e784-167b-49ee-b1ac-0ff7866eb4fbcconsent=BOl1TLCOl1TLCADABAENAiAAAAAKOAAA; 0e16e784-167b-49ee-b1ac-0ff7866eb4fbeuconsent=BOl1TLCOl1TLCADABAENCi-AAAAp6AGAAUAA0AEAANAAigBM; lastConsentChange=1566711469042; 0e16e784-167b-49ee-b1ac-0ff7866eb4fbfaktorId=518ed4ea-f59f-43fc-aa7f-7773f177d43f; xdid=22ca708b-ce53-47fa-8d71-d835f278f876|1566711462|orbitz.com; AB_Test_TripAdvisor=A; s_ecid=MCMID%7C31160316888969874832212038078424992583; DUAID=e2775f0f-e36e-40fc-9d33-0459fdb1de00; MC1=GUID=e2775f0fe36e40fc9d330459fdb1de00; eid=111714606; s_fid=7A97AE8CE804BC6A-2FEC44C0972FA802; aspp=v.1,0|||||||||||||'
            # cookies = self._parse_cookie_str(cookies_str)
            # self.logger.info('Cookies=%s' % cookies)
            yield Request(url=url, callback=self.parse_hotel, meta={'hotel_id': hotel_id},
                          headers={
                              'Host': 'www.expedia.com',
                              'Connection': 'keep-alive',
                              'Accept-Language': 'en-us',
                              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                          })


    def parse_hotel(self, response):
        hotel_id = response.meta['hotel_id']
        hotel_name = response.xpath("//h1[@data-stid='content-hotel-title']/text()").get().strip()
        num_reviews = int(response.xpath("//meta[@itemprop='reviewCount']/@content").get())
        self.logger.info('Parsing hotel={}, hotelID={}, reviewCount={}'.format(hotel_name, hotel_id, num_reviews))
        hotel_intro = response.xpath("//p[@class='uitk-type-paragraph-300 all-b-padding-three']/text()").get().strip()
        nearby = response.xpath("//li[@class='location-summary__list-item']/dl/dt/text()").getall()
        around = response.xpath("//li[@class='transportation__list-item']/dl/dt/text()").getall()
        yield ExpediaHotelItem(
            hotel_id=hotel_id,
            name=hotel_name,
            nearby_info=simplejson.dumps(nearby),
            around_info=simplejson.dumps(around),
            introduction=hotel_intro,
            created_datetime=datetime.utcnow()
        )

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
            yield Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST',
                          body=simplejson.dumps(graphql_payload),
                          headers={
                              'Content-Type': 'application/json',
                              'Origin': 'https://www.expedia.com',
                              'Referer': response.url,
                              'Cookie': 'cookie: tpid=v.1,1; iEAPID=0; currency=USD; linfo=v.4,|0|0|255|1|0||||||||1033|0|0||0|0|0|-1|-1; pwa_csrf=d0786c3e-6946-4203-90b8-5ccd63d7a725|D3ahwRzbqnKIa9zr4_LpV1aNOwvz7xCzv6wnYCvRbhP4QOOwjDpY4SAzXvwCp8Oiho_ZjhGg4GwOuWEpJkqRcQ; MC1=GUID=0744d4a3ce1c4dd0bdc17a105a1f69d4; DUAID=0744d4a3-ce1c-4dd0-bdc1-7a105a1f69d4; AMCVS_C00802BE5330A8350A490D4C%40AdobeOrg=1; s_ecid=MCMID%7C06179023703179136360062832346793475516; s_cc=true; _gcl_au=1.1.563409396.1568519291; _ga=GA1.2.1504753458.1568519291; xdid=ece7fa84-2935-467b-af14-517d385dc195|1568519294|expedia.com; _gid=GA1.2.425040987.1568735474; HMS=1f6643fd-2b09-4014-854a-6bb5a1b7e879; ak_bmsc=88CCC145B5CA605E85FBCD4EF0121AAA1724025E7E3B00007AFF825D33C5F203~pl8tx0aas9asJvQwdlnlBIcCCdeE4DOc6zW6sL7TOMkCR0rgs3x37rEHjVGhzzyJHE6zMow4hOqbaAsq+iPVq2luaM5Vpn5ZC6yJk1ZNqyV6hCpckz0l+ARafQrkIuDpXvCafNy/4s4l+iLDZF+KlgyGpTL5wfQENJL1Y1uaW9GyIOTdn2AuO6Xm3P4mpX/+zBXDxS/Usva1+3WDqTso3/UImxmPgajy79iH1lCfbr2Ps=; s_ppn=page.Hotels.Infosite.Information; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1406116232%7CMCIDTS%7C18159%7CvVersion%7C2.5.0%7CMCMID%7C06179023703179136360062832346793475516%7CMCAAMLH-1569470973%7C9%7CMCAAMB-1569470973%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1568873373s%7CNONE%7CMCAID%7CNONE; x-CGP-exp-30353=3; x-CGP-exp-15795=2; x-CGP-exp-28702=0; JSESSIONID=5FDA37C2D7FABC5FED3401C0D6D9D434; cesc=%7B%22marketingClick%22%3A%5B%22false%22%2C1568866203682%5D%2C%22hitNumber%22%3A%5B%227%22%2C1568866203682%5D%2C%22visitNumber%22%3A%5B%226%22%2C1568866172885%5D%2C%22entryPage%22%3A%5B%22page.Hotels.Infosite.Information%22%2C1568866203682%5D%7D; utag_main=v_id:016d3308fe4f0017a4cce4e7b2a403072012a06a00c98$_sn:6$_ss:0$_st:1568868003252$ses_id:1568866174009%3Bexp-session$_pn:4%3Bexp-session; s_ppvl=https%253A%2F%2Fwww.expedia.com%2FTokyo-Hotels-APA-Hotel-Keisei-Ueno-Ekimae.h13094145.Hotel-Information%253Fchkin%253D9%25252F22%25252F2019%2526chkout%253D9%25252F23%25252F2019%2526regionId%253D179900%2526destination%253DTokyo%2B%252528and%2Bvicinity%252529%25252C%2BJapan%2526swpToggleOn%253Dtrue%2526rm1%253Da2%2526x_pwa%253D1%2526sort%253Drecommended%2526top_dp%253D118%2526top_cur%253DUSD%2526rfrr%253DHSR%2526pwa_ts%253D1568094513466%2C13%2C13%2C1050%2C1960%2C1050%2C2048%2C1152%2C1.25%2CP; s_ppv=page.Hotels.Infosite.Information%2C13%2C13%2C1050%2C1039%2C1050%2C2048%2C1152%2C1.25%2CL',
                          },
                          meta={
                              'hotel_name': hotel_name,
                              'hotel_id': hotel_id,
                              'num_reviews': num_reviews,
                              'start_index': start_index
                          })

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
                item['response_publish_datetime'] = datetime.strptime(mgmt_resp['date'].strip(), '%Y-%m-%dT%H:%M:%SZ')
                item['response_content'] = mgmt_resp['response']
                item['response_display_locale'] = mgmt_resp['displayLocale']
            else:
                item['response_id'] = None
                item['response_author'] = None
                item['response_publish_datetime'] = None
                item['response_content'] = None
                item['response_display_locale'] = None
            yield item

    # def _parse_cookie_str(self, cookie_str: str) -> dict:
    #     cookies = {}
    #     for kv_pair in cookie_str.split(';'):
    #         kv = kv_pair.strip().split('=')
    #         cookies[kv[0]] = kv[1]
    #     return cookies
