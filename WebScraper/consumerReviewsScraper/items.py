# -*- coding: utf-8 -*-

import hashlib
import scrapy


class MongoDbDocument(scrapy.Item):
    """
    Represent a document in MongoDB.
    """

    _id = scrapy.Field()
    tags = scrapy.Field()
    source = scrapy.Field()  # where this item is scraped

    def __init__(self):
        super().__init__()
        self['tags'] = []
        self['source'] = ''


class ConsumerReview(scrapy.Item):
    """
    Base class of consumer review containing basic info.
    """

    id = scrapy.Field()
    author = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    scrapedDate = scrapy.Field()
    fingerprint = scrapy.Field()  # MD5 hash value serves as a unique identifier of each consumer review

    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self):
        super().__init__()
        self['id'] = 0
        self['author'] = ''
        self['date'] = ''
        self['title'] = ''
        self['content'] = ''
        self['scrapedDate'] = ''
        self['fingerprint'] = ''

    def computeFingerprint(self):
        pass


class KBBReviewItem(ConsumerReview, MongoDbDocument):
    """
    Represent consumer reviews on kbb.com that can be persisted in MongoDB.
    """

    mileage = scrapy.Field()
    pros = scrapy.Field()
    cons = scrapy.Field()
    nRecommend = scrapy.Field()
    nRecommendOutOf = scrapy.Field()
    nHelpful = scrapy.Field()
    nHelpfulOutOf = scrapy.Field()
    ratings = scrapy.Field()
    year = scrapy.Field()
    make = scrapy.Field()
    model = scrapy.Field()

    def __init__(self):
        super().__init__()
        self['mileage'] = 0
        self['pros'] = ''
        self['cons'] = ''
        self['nRecommend'] = 0
        self['nRecommendOutOf'] = 0
        self['nHelpful'] = 0
        self['nHelpfulOutOf'] = 0
        self['year'] = ''
        self['make'] = ''
        self['model'] = ''
        self['ratings'] = {
            'overall': 0,
            'value': 0,
            'reliability': 0,
            'quality': 0,
            'performance': 0,
            'styling': 0,
            'comfort': 0
        }
        self['tags'] = ['review']

    def computeFingerprint(self):
        h = hashlib.md5()
        h.update(self['year'].encode())
        h.update(self['make'].encode())
        h.update(self['model'].encode())
        h.update(self['author'].encode())
        h.update(self['date'].encode())
        h.update(self['title'].encode())
        h.update(self['content'].encode())
        h.update(bytes(abs(self['mileage'])))
        avg = (self['ratings']['overall'] + self['ratings']['value'] + self['ratings']['reliability']
               + self['ratings']['quality'] + self['ratings']['performance'] + self['ratings']['styling']
               + self['ratings']['comfort']) / 7.0
        h.update(str(avg).encode())
        return h.hexdigest()


class EdmundsReviewItem(ConsumerReview):
    """ Edmunds review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["vehicle_type"] = ""
        self["helpful_count"] = "0/0"
        self["recommend_count"] = {"up": 0, "down": 0}
        self["best_features"] = []
        self["worst_features"] = []
        self["ratings"] = {
            "safety": 0,
            "performance": 0,
            "comfort": 0,
            "technology": 0,
            "interior": 0,
            "reliability": 0,
            "value": 0
        }

    @staticmethod
    def get_column_headers() -> list:
        return super(EdmundsReviewItem, EdmundsReviewItem).get_column_headers() + [
            "score", "vehicle_type", "helpful_count", "recommend_up_count", "recommend_down_count", "best_features",
            "worst_features", "ratings_safety", "ratings_performance", "ratings_comfort", "ratings_technology",
            "ratings_interior", "ratings_reliability", "ratings_value"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["vehicle_type"], self["helpful_count"], self["recommend_count"]["up"],
            self["recommend_count"]["down"], self["best_features"], self["worst_features"], self["ratings"]["safety"],
            self["ratings"]["performance"], self["ratings"]["comfort"], self["ratings"]["technology"],
            self["ratings"]["interior"], self["ratings"]["reliability"], self["ratings"]["value"]
        ]

    score = scrapy.Field()
    vehicle_type = scrapy.Field()
    helpful_count = scrapy.Field()
    recommend_count = scrapy.Field()
    best_features = scrapy.Field()
    worst_features = scrapy.Field()
    ratings = scrapy.Field()


class OrbitzReviewItem(ConsumerReview):
    """ Orbitz hotel review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["will_recommend"] = 0
        self["recommend_for"] = ""
        self["location"] = ""
        self["remark"] = {
            "pros": "",
            "cons": "",
            "location": ""
        }
        self["response"] = ConsumerReview()

    @staticmethod
    def get_column_headers() -> list:
        return super(OrbitzReviewItem, OrbitzReviewItem).get_column_headers() + [
            "score", "location", "will_recommend", "recommend_for", "remark_pros", "remark_cons", "remark_location",
            "response_title", "response_author", "response_date", "response_content"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["location"], self["will_recommend"], self["recommend_for"], self["remark"]["pros"],
            self["remark"]["cons"], self["remark"]["location"], self["response"]["title"], self["response"]["author"],
            self["response"]["date"], self["response"]["content"]
        ]

    score = scrapy.Field()
    will_recommend = scrapy.Field()
    recommend_for = scrapy.Field()
    location = scrapy.Field()
    remark = scrapy.Field()
    response = scrapy.Field()


class TripAdvisorReviewItem(ConsumerReview):
    """ TripAdvisor restaurant review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["via_mobile"] = 0
        self["helpful_count"] = 0
        self["location"] = ""
        self["level"] = 0
        self["review_count"] = 0
        self["contribution_count"] = 0
        self["helpful_votes"] = 0

    @staticmethod
    def get_column_headers() -> list:
        return super(TripAdvisorReviewItem, TripAdvisorReviewItem).get_column_headers() + [
            "score", "helpful_count", "location", "level", "review_count", "contribution_count", "helpful_votes"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["helpful_count"], self["location"], self["level"], self["review_count"],
            self["contribution_count"], self["helpful_votes"]
        ]

    score = scrapy.Field()
    via_mobile = scrapy.Field()
    helpful_count = scrapy.Field()
    location = scrapy.Field()
    level = scrapy.Field()
    review_count = scrapy.Field()
    contribution_count = scrapy.Field()
    helpful_votes = scrapy.Field()


class BizRateReview(ConsumerReview, MongoDbDocument):
    """
    Represent consumer reviews on bizrate.com that can be persisted in MongoDB.
    """

    def __init__(self):
        super().__init__()
        self['overallSatisfaction'] = -1
        self['wouldShopHereAgain'] = -1
        self['likelihoodToRecommend'] = -1
        self['ratingsSiteExperience'] = {'easeOfFinding': -1, 'designSite': -1, 'satisfactionCheckout': -1,
                                         'productSelection': -1, 'clarityProductInfo': -1, 'chargesStatedClearly': -1,
                                         'priceRelativeOtherRetailers': -1, 'shippingCharges': -1,
                                         'varietyShippingOptions': -1}
        self['ratingsAfterPurchase'] = {'onTimeDelivery': -1, 'orderTracking': -1, 'productMetExpectations': -1,
                                        'customerSupport': -1, 'productAvailability': -1, 'returnsProcess': -1}
        self['reviewAfterPurchase'] = {'author': '', 'date': '', 'title': '', 'content': ''}
        self['reviewStore'] = ''
        self['tags'] = ['review']

    def computeFingerprint(self):
        h = hashlib.md5()
        h.update(self['author'].encode())
        h.update(self['date'].encode())
        h.update(self['title'].encode())
        h.update(self['content'].encode())
        avg = (self['overallSatisfaction'] + self['likelihoodToRecommend'] + self['wouldShopHereAgain']) / 3.0
        h.update(str(avg).encode())
        return h.hexdigest()

    overallSatisfaction = scrapy.Field()
    wouldShopHereAgain = scrapy.Field()
    likelihoodToRecommend = scrapy.Field()
    ratingsSiteExperience = scrapy.Field()
    ratingsAfterPurchase = scrapy.Field()
    reviewAfterPurchase = scrapy.Field()
    reviewStore = scrapy.Field()
