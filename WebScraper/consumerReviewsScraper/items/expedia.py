import simplejson
import scrapy
import sqlite3
from sqlite3 import Connection


class ExpediaHotelItem(scrapy.Item):
    """ Hotel on expedia.com """

    hotel_id = scrapy.Field()
    name = scrapy.Field()
    nearby_info = scrapy.Field()
    around_info = scrapy.Field()
    introduction = scrapy.Field()
    created_datetime = scrapy.Field()

    def upsert(self, conn: Connection):
        conn.execute('''
            INSERT INTO `expedia_hotel`(
                `hotel_id`,
                `name`,
                `nearby_info`,
                `around_info`,
                `introduction`,
                `created_datetime`)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(`hotel_id`) DO UPDATE SET
                `name`=?,
                `nearby_info`=?,
                `around_info`=?,
                `introduction`=?,
                `created_datetime`=?''',
            (
                # for insert
                self['hotel_id'],
                self['name'],
                self['nearby_info'],
                self['around_info'],
                self['introduction'],
                self['created_datetime'],
                # for update
                self['name'],
                self['nearby_info'],
                self['around_info'],
                self['introduction'],
                self['created_datetime'],
            )
        )

        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM `expedia_hotel` WHERE `hotel_id`=?', (self['hotel_id'],))
        conn.commit()


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

    def upsert(self, conn: Connection):
        conn.execute('''
            INSERT INTO `expedia_review`(
                `review_id`, 
                `author`, 
                `publish_datetime`, 
                `content`, 
                `created_datetime`, 
                `overall_rating`, 
                `num_helpful`, 
                `stay_duration`, 
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
                `remarks_location`, 
                `hotel_id`
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(`review_id`) DO UPDATE SET 
                `author`=?, 
                `publish_datetime`=?, 
                `content`=?, 
                `created_datetime`=?, 
                `overall_rating`=?, 
                `num_helpful`=?, 
                `stay_duration`=?, 
                `response_id`=?, 
                `response_author`=?, 
                `response_publish_datetime`=?, 
                `response_content`=?, 
                `response_display_locale`=?, 
                `superlative`=?, 
                `title`=?, 
                `locale`=?, 
                `location`=?, 
                `remarks_positive`=?, 
                `remarks_negative`=?, 
                `remarks_location`=?, 
                `hotel_id`=?''',
            (
                # for insert
                self['review_id'],
                self['author'],
                self['publish_datetime'],
                self['content'],
                self['created_datetime'],
                self['overall_rating'],
                self['num_helpful'],
                self['stay_duration'],
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
                self['remarks_location'],
                self['hotel_id'],
                # for update
                self['author'],
                self['publish_datetime'],
                self['content'],
                self['created_datetime'],
                self['overall_rating'],
                self['num_helpful'],
                self['stay_duration'],
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
                self['remarks_location'],
                self['hotel_id'],
            )
        )
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM `expedia_review` WHERE `review_id`=?', (self['review_id'],))
        conn.commit()
