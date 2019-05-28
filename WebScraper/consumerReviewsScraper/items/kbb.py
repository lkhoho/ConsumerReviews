import hashlib
import scrapy
from .base import ConsumerReview, MongoDbDocument


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