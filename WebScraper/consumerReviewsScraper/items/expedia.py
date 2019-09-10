import scrapy
from sqlite3 import Connection


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
    category = scrapy.Field()
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

    def insert(self, conn: Connection):
        conn.execute('''
            INSERT INTO expedia_review(
                `review_id`, 
                `author`, 
                `publish_datetime`, 
                `content`, 
                `created_datetime`, 
                `overall_rating`, 
                `num_helpful`, 
                `stay_duration`, 
                `category`, 
                `response_id`, 
                `response_author`, 
                `response_publish_datetime`, 
                `response_content`, 
                `response_display_locale`, 
                `superlative`, 
                `title`, 
                `locale`, 
                `location`, 
                `remarks_positive`, 
                `remarks_negative`, 
                `remarks_location`) 
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (self['review_id'], 
                self['author'], 
                self['publish_datetime'], 
                self['content'], 
                self['created_datetime'], 
                self['overall_rating'], 
                self['num_helpful'], 
                self['stay_duration'], 
                self['category'], 
                self['response_id'], 
                self['response_author'], 
                self['response_publish_datetime'], 
                self['response_content'],
                self['response_display_locale'], 
                self['superlative'], 
                self['title'], 
                self['locale'],
                self['location'],
                self['remarks_positive'], 
                self['remarks_negative'], 
                self['remarks_location'])
        )

        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM expedia_review WHERE `review_id`=?', (self['review_id'],))
        conn.commit()
