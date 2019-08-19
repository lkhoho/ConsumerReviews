import scrapy
from sqlite3 import Connection


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
    created_datetime = scrapy.Field()

    @property
    def field_values(self):
        return self['review_id'], self['company_id'], self['company'], self['category'], self['content'], \
                self['status'], self['publish_date'], self['num_helpful'], self['created_datetime']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO hyouban_review(`review_id`, `company_id`, `company`, `category`, `content`, '
                     '`status`, `publish_date`, `num_helpful`, `created_datetime`) '
                     'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(`review_id`) DO UPDATE SET '
                     '`company_id`=?, `company`=?, `category`=?, `content`=?, `status`=?, `publish_date`=?, '
                     '`num_helpful`=?, `created_datetime`=?',
                     (*self.field_values, self.field_values[1], self.field_values[2], self.field_values[3], 
                      self.field_values[4], self.field_values[5], self.field_values[6], self.field_values[7], 
                      self.field_values[8]))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM hyouban_review WHERE `review_id`=?', (self['review_id'],))
        conn.commit()
