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
