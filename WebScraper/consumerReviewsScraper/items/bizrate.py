import hashlib
import scrapy
from .base import ConsumerReview, MongoDbDocument


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
