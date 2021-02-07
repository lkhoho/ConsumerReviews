import scrapy


class UniversityRankingItem(scrapy.Item):
    """
    Universities and their rankings on topuniversities.com.
    """

    name = scrapy.Field()
    country = scrapy.Field()
    region = scrapy.Field()
    subject = scrapy.Field()
    year = scrapy.Field()
    ranking = scrapy.Field()
    score = scrapy.Field()
    created_datetime = scrapy.Field()
