import scrapy


class HyoubanReview(scrapy.Item):
    """ Review """

    review_id = scrapy.Field()
    company_id = scrapy.Field()
    company = scrapy.Field()
    category = scrapy.Field()
    content = scrapy.Field()
    status = scrapy.Field()
    publish_date = scrapy.Field()
    num_helpful = scrapy.Field()
    attitude = scrapy.Field()
    created_datetime = scrapy.Field()
