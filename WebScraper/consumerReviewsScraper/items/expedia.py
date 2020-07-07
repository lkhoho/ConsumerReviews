import scrapy


class ExpediaHotelItem(scrapy.Item):
    """ Hotel on expedia.com """

    hotel_id = scrapy.Field()
    name = scrapy.Field()
    nearby_info = scrapy.Field()
    around_info = scrapy.Field()
    introduction = scrapy.Field()
    created_datetime = scrapy.Field()


class ExpediaReviewItem(scrapy.Item):
    """ Review of hotels on expedia.com """

    review_id = scrapy.Field()
    author = scrapy.Field()
    publish_datetime = scrapy.Field()
    content = scrapy.Field()
    created_datetime = scrapy.Field()
    overall_rating = scrapy.Field()
    num_helpful = scrapy.Field()
    stay_duration = scrapy.Field()
    response_id = scrapy.Field()
    response_author = scrapy.Field()
    response_publish_datetime = scrapy.Field()
    response_content = scrapy.Field()
    response_display_locale = scrapy.Field()
    superlative = scrapy.Field()
    title = scrapy.Field()
    locale = scrapy.Field()
    location = scrapy.Field()
    remarks_positive = scrapy.Field()
    remarks_negative = scrapy.Field()
    remarks_location = scrapy.Field()
    hotel_id = scrapy.Field()
