import simplejson
import yaml
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
            # 'consumerReviewsScraper.middlewares.UpdateURLStatusMiddleware': 10,
        },

        'FEEDS': {
            '/Users/keliu/Developer/ConsumerReviews/reviews.csv': {
                'format': 'csv',
                'item_classes': ['WebScraper.consumerReviewsScraper.items.expedia.ExpediaReviewItem'],
                'fields': ['review_id', 'hotel_id', 'author', 'publish_datetime', 'content', 'overall_rating',
                           'num_helpful', 'stay_duration', 'superlative', 'themes', 'locale', 'travelers']
            },
            '/Users/keliu/Developer/ConsumerReviews/hotels.csv': {
                'format': 'csv',
                'item_classes': ['WebScraper.consumerReviewsScraper.items.expedia.ExpediaHotelItem'],
                'fields': ['hotel_id', 'name', 'nearby_info', 'around_info', 'introduction', 'created_datetime']
            }
        }
    }

    NUM_REVIEWS_PER_REQUEST = 500
    XPATH_REVIEW_COUNT = "//meta[@itemprop='reviewCount']/@content"
    XPATH_HOTEL_DESCRIPTION = "//div[@itemprop='description']/descendant::text()"
    XPATH_HOTEL_NEARBY = "//h4[contains(text(), \"What's nearby\")]/following-sibling::div//li/text()"
    XPATH_HOTEL_AROUND = "//h4[contains(text(), \"Getting around\")]/following-sibling::div//li/text()"

    REVIEW_QUERY_STRING = \
        '''
        query PropertyFilteredReviewsQuery($context: ContextInput!, $propertyId: String!, $searchCriteria: PropertySearchCriteriaInput!) {
          propertyReviewSummaries(
            context: $context
            propertyIds: [$propertyId]
            searchCriteria: $searchCriteria
          ) {
            ...__PropertyReviewSummaryFragment
            __typename
          }
          propertyInfo(context: $context, propertyId: $propertyId) {
            id
            reviewInfo(searchCriteria: $searchCriteria) {
              ...__PropertyReviewsListFragment
              ...__WriteReviewLinkFragment
              sortAndFilter {
                ...TravelerTypeFragment
                __typename
              }
              __typename
            }
            __typename
          }
        }
        
        fragment __PropertyReviewSummaryFragment on PropertyReviewSummary {
          accessibilityLabel
          overallScoreWithDescription
          propertyReviewCountDetails {
            fullDescription
            __typename
          }
          ...ReviewDisclaimerFragment
          reviewSummaryDetails {
            label
            ratingPercentage
            formattedRatingOutOfMax
            __typename
          }
          totalCount {
            raw
            __typename
          }
          __typename
        }
        
        fragment ReviewDisclaimerFragment on PropertyReviewSummary {
          reviewDisclaimer
          reviewDisclaimerLabel
          reviewDisclaimerAccessibilityLabel
          __typename
        }
        
        fragment __PropertyReviewsListFragment on PropertyReviews {
          summary {
            paginateAction {
              text
              analytics {
                referrerId
                linkName
                __typename
              }
              __typename
            }
            __typename
          }
          reviews {
            ...ReviewParentFragment
            managementResponses {
              ...ReviewChildFragment
              __typename
            }
            reviewInteractionSections {
              primaryDisplayString
              reviewInteractionType
              __typename
            }
            __typename
          }
          ...NoResultsMessageFragment
          reviewFlag {
            ...ReviewFlagFragment
            __typename
          }
          __typename
        }
        
        fragment ReviewParentFragment on PropertyReview {
          id
          superlative
          locale
          title
          brandType
          reviewScoreWithDescription {
            label
            value
            __typename
          }
          text
          stayDuration
          submissionTime {
            longDateFormat
            __typename
          }
          themes {
            ...ReviewThemeFragment
            __typename
          }
          ...FeedbackIndicatorFragment
          ...AuthorFragment
          ...PhotosFragment
          ...TravelersFragment
          ...ReviewTranslationInfoFragment
          ...PropertyReviewSourceFragment
          ...PropertyReviewRegionFragment
          __typename
        }
        
        fragment AuthorFragment on PropertyReview {
          reviewAuthorAttribution {
            text
            __typename
          }
          __typename
        }
        
        fragment PhotosFragment on PropertyReview {
          id
          photos {
            description
            url
            __typename
          }
          __typename
        }
        
        fragment TravelersFragment on PropertyReview {
          travelers
          __typename
        }
        
        fragment ReviewThemeFragment on ReviewThemes {
          icon {
            id
            __typename
          }
          label
          __typename
        }
        
        fragment FeedbackIndicatorFragment on PropertyReview {
          reviewInteractionSections {
            primaryDisplayString
            accessibilityLabel
            reviewInteractionType
            __typename
          }
          __typename
        }
        
        fragment ReviewTranslationInfoFragment on PropertyReview {
          translationInfo {
            loadingTranslationText
            targetLocale
            translatedBy {
              description
              __typename
            }
            translationCallToActionLabel
            seeOriginalText
            __typename
          }
          __typename
        }
        
        fragment PropertyReviewSourceFragment on PropertyReview {
          propertyReviewSource {
            accessibilityLabel
            graphic {
              description
              id
              size
              token
              url {
                value
                __typename
              }
              __typename
            }
            text {
              value
              __typename
            }
            __typename
          }
          __typename
        }
        
        fragment PropertyReviewRegionFragment on PropertyReview {
          reviewRegion {
            id
            __typename
          }
          __typename
        }
        
        fragment ReviewChildFragment on ManagementResponse {
          id
          header {
            text
            __typename
          }
          response
          __typename
        }
        
        fragment NoResultsMessageFragment on PropertyReviews {
          noResultsMessage {
            __typename
            ...MessagingCardFragment
            ...EmptyStateFragment
          }
          __typename
        }
        
        fragment MessagingCardFragment on UIMessagingCard {
          graphic {
            __typename
            ... on Icon {
              id
              description
              __typename
            }
          }
          primary
          secondaries
          __typename
        }
        
        fragment EmptyStateFragment on UIEmptyState {
          heading
          body
          __typename
        }
        
        fragment ReviewFlagFragment on ReviewFlag {
          reviewFlagDialog {
            ...ReviewReportFlagTriggerFragment
            __typename
          }
          reviewFlagToast {
            value
            __typename
          }
          __typename
        }
        
        fragment ReviewReportFlagTriggerFragment on PropertyReviewFlagDialog {
          ...ReviewFlagDialogFragment
          trigger {
            icon {
              description
              id
              __typename
            }
            value
            __typename
          }
          __typename
        }
        
        fragment ReviewFlagDialogFragment on PropertyReviewFlagDialog {
          content {
            communityInfo
            partnerInfo
            flagOptions {
              label
              options {
                label
                optionValue
                isSelected
                __typename
              }
              __typename
            }
            partnerLink {
              value
              link {
                target
                uri {
                  value
                  __typename
                }
                __typename
              }
              icon {
                description
                id
                __typename
              }
              __typename
            }
            primaryUIButton {
              primary
              icon {
                description
                id
                __typename
              }
              __typename
            }
            __typename
          }
          toolbar {
            icon {
              description
              id
              __typename
            }
            title
            __typename
          }
          __typename
        }
        
        fragment TravelerTypeFragment on SortAndFilterViewModel {
          sortAndFilter {
            name
            label
            options {
              label
              isSelected
              optionValue
              description
              __typename
            }
            __typename
          }
          __typename
        }
        
        fragment __WriteReviewLinkFragment on PropertyReviews {
          writeReviewLink {
            link {
              uri {
                value
                relativePath
                __typename
              }
              clientSideAnalytics {
                referrerId
                linkName
                __typename
              }
              __typename
            }
            value
            accessibilityLabel
            __typename
          }
          __typename
        }
        '''

    def __init__(self, task_file, *args, **kwargs):
        super(ExpediaReviewSpider, self).__init__(*args, **kwargs)
        with open(task_file) as stream:
            self.task_conf = yaml.safe_load(stream)
        self.hotels = self.task_conf['Hotels']
        self.logger.info(f'Scraping hotels from file {task_file}. {len(self.hotels)} hotels to scrape.')

    def start_requests(self):
        for hotel in self.hotels:
            hotel_id = hotel['Id']
            hotel_name = hotel['Name']
            hotel_url = hotel['URL']
            max_reviews = hotel.get('MaxReviews')
            self.logger.info(f'Prepare to scrapy hotel {hotel_name}. URL={hotel_url}')
            req = Request(url=hotel_url, callback=self.parse_hotel, dont_filter=True,
                          meta={
                              'hotel_id': hotel_id,
                              'hotel_name': hotel_name,
                              'max_reviews': max_reviews
                          },
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
        hotel_name = response.meta['hotel_name']
        review_count = response.meta['max_reviews']

        if review_count is None:
            # max review count is not defined in task file, use the review count on web page.
            review_count = int(response.xpath(self.XPATH_REVIEW_COUNT).get().strip().replace(',', ''))
        self.logger.info(f'Parsing hotel={hotel_name}, hotelID={hotel_id}, reviewCount={review_count}')

        try:
            hotel_description = ' '.join(response.xpath(self.XPATH_HOTEL_DESCRIPTION).getall()).strip()
        except:
            self.logger.warn(f'Parsing hotel introduction failed. Response={response}')
            hotel_description = None

        try:
            nearby = response.xpath(self.XPATH_HOTEL_NEARBY).getall()
        except:
            self.logger.warn(f'Parsing hotel nearby-info failed. Response={response}')
            nearby = None

        try:
            around = response.xpath(self.XPATH_HOTEL_AROUND).getall()
        except:
            self.logger.warn(f'Parsing hotel around-info failed. Response={response}')
            around = None

        yield ExpediaHotelItem(
            hotel_id=hotel_id,
            name=hotel_name,
            nearby_info=simplejson.dumps(nearby),
            around_info=simplejson.dumps(around),
            introduction=hotel_description,
            created_datetime=datetime.utcnow())

        graphql_payload = {
            'operationName': 'PropertyFilteredReviewsQuery',
            'query': self.REVIEW_QUERY_STRING,
            'variables': {
                'context': {
                    'siteId': 1,
                    'locale': 'en_US',
                    'eapid': 0,
                    'currency': 'USD',
                    'device': {
                        'type': 'DESKTOP'
                    },
                    'identity': {
                        'duaid': '307f232d-6e74-49b8-8ea0-a5a15fe27fd3',
                        'expUserId': '-1',
                        'tuid': '-1',
                        'authState': 'ANONYMOUS',
                    },
                    'debugContext': {
                        'abacusOverrides': [],
                        'alterMode': 'RELEASED'
                    }
                },
                'propertyId': str(hotel_id),
                'searchCriteria': {
                    "primary": {
                        "dateRange": None,
                        "rooms": [
                            { "adults": 2 }
                        ],
                        "destination": {
                            "regionId": "178276"
                        }
                    },
                    "secondary": {
                        "booleans": [
                            {
                                "id": "includeRecentReviews",
                                "value": True
                            },
                            {
                                "id": "includeRatingsOnlyReviews",
                                "value": True
                            }
                        ],
                        "counts": [
                            {
                                "id": "startIndex",
                                "value": 0
                            },
                            {
                                "id": "size",
                                "value": self.NUM_REVIEWS_PER_REQUEST
                            }
                        ],
                        "selections": [
                            {
                                "id": "sortBy",
                                "value": "NEWEST_TO_OLDEST"
                            }
                        ]
                    }
                }
            }
        }

        # get default category
        for start_index in range(0, review_count, self.NUM_REVIEWS_PER_REQUEST):
            self.__set_start_index_in_payload(graphql_payload, start_index)
            req = Request(url='https://www.expedia.com/graphql', callback=self.parse_graphql, method='POST',
                          body=simplejson.dumps(graphql_payload),
                          headers={
                              'Content-Type': 'application/json',
                              'Origin': 'https://www.expedia.com',
                              'Referer': response.url,
                              'client-info': 'blossom-flex-ui,24a01b4ad7639c53cf36cd06f88ce31f153e81c8,us-east-1',
                          },
                          meta={
                              'hotel_name': hotel_name,
                              'hotel_id': hotel_id,
                              'review_count': review_count,
                              'start_index': start_index
                          })
            req.meta['fp'] = request_fingerprint(req)
            # self.logger.info(f'===debug===\n{simplejson.dumps(graphql_payload)}')
            yield req

    def parse_graphql(self, response):
        hotel_name = response.meta['hotel_name']
        hotel_id = response.meta['hotel_id']
        review_count = response.meta['review_count']
        start_index = response.meta['start_index']
        json = simplejson.loads(response.text)
        self.logger.info(f'''Parsing GraphQL response. Hotel={hotel_name}, Hotel_ID={hotel_id}, 
                         ReviewCount={review_count}, StartIndex={start_index}''')

        if json.get('data', None) is None:
            self.logger.warn('Cannot get review data from GraphQL response. Response is %s' % json)
            return

        for review in json['data']['propertyInfo']['reviewInfo']['reviews']:
            yield ExpediaReviewItem(
                review_id=review['id'],
                author=review['reviewAuthorAttribution']['text'],
                publish_datetime=datetime.strptime(
                    review['submissionTime']['longDateFormat'].strip(),
                    '%b %d, %Y'),
                content=review['text'],
                created_datetime=datetime.utcnow(),
                overall_rating=review['reviewScoreWithDescription']['value'],
                num_helpful=review['reviewInteractionSections'][0]['primaryDisplayString'],
                stay_duration=review['stayDuration'],
                superlative=review['superlative'],
                themes=review['themes'][0]['label'] if review.get('themes') is not None else None,
                locale=review['locale'],
                travelers=review['travelers'],
                hotel_id=hotel_id)

    def __set_start_index_in_payload(self, payload: dict, start_index: int) -> None:
        payload['variables']['searchCriteria']['secondary']['counts'][0]['value'] = start_index
