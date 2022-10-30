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
    superlative = scrapy.Field()
    themes = scrapy.Field()  # things like "Liked: Cleanliness, staff & service, amenities & facilities"
    locale = scrapy.Field()
    travelers = scrapy.Field()
    hotel_id = scrapy.Field()
