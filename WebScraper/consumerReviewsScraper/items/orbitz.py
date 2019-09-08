import scrapy
from sqlite3 import Connection


class OrbitzReviewItem(scrapy.Item):
    """ Orbitz hotel review info. """

    review_id = scrapy.Field()
    author = scrapy.Field()
    publish_date = scrapy.Field()
    content = scrapy.Field()
    created_datetime = scrapy.Field()
    score = scrapy.Field()
    heading = scrapy.Field()
    num_helpful = scrapy.Field()
    stayed = scrapy.Field()
    response_id = scrapy.Field()
    response_author = scrapy.Field()
    response_publish_date = scrapy.Field()
    response_content = scrapy.Field()

    @property
    def field_values(self):
        return self['review_id'], self['author'], self['publish_date'], self['content'], \
                self['created_datetime'], self['score'], self['heading'], self['num_helpful'], \
                self['stayed'], self['response_id'], self['response_author'], self['response_publish_date'], \
                self['response_content']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO orbitz_review(`review_id`, `author`, `publish_date`, `content`, '
                     '`created_datetime`, `score`, `heading`, `num_helpful`, `stayed`, '
                     '`response_id`, `response_author`, `response_publish_date`, `response_content`) '
                     'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(`review_id`) DO UPDATE SET '
                     '`author`=?, `publish_date`=?, `content`=?, `created_datetime`=?, `score`=?, '
                     '`heading`=?, `num_helpful`=?, `stayed`=?, `response_id=?`, `response_author`=?, '
                     '`response_publish_date`=?, `response_content`=?',
                     (*self.field_values, self['author'], self['publish_date'], self.['content'], 
                      self['created_datetime'], self['score'], self['heading'], self['num_helpful'], 
                      self['stayed'], self['response_id'], self['response_author'], self['response_publish_date'], 
                      self['response_content']))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM orbitz_review WHERE `review_id`=?', (self['review_id'],))
        conn.commit()
