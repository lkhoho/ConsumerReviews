import scrapy
from sqlite3 import Connection


class XcarForum(scrapy.Item):
    """ 论坛 """

    fid = scrapy.Field()
    name = scrapy.Field()
    created_datetime = scrapy.Field()
    threads = scrapy.Field()
    managers = scrapy.Field()

    @property
    def field_values(self):
        return self['fid'], self['name'], self['created_datetime']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO xcar_forum(`fid`, `name`, `created_datetime`) VALUES (?, ?, ?) '
                     'ON CONFLICT(`fid`) DO UPDATE SET `name`=?, `created_datetime`=?',
                     (*self.field_values, self.field_values[1], self.field_values[2]))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM xcar_forum WHERE `fid`=?', (self['fid'],))
        conn.commit()


class XcarThread(scrapy.Item):
    """ 帖子 """

    tid = scrapy.Field()
    title = scrapy.Field()
    forum = scrapy.Field()
    num_views = scrapy.Field()
    num_replies = scrapy.Field()
    is_elite = scrapy.Field()
    created_datetime = scrapy.Field()

    @property
    def field_values(self):
        return self['tid'], self['title'], self['forum'], self['num_views'], self['num_replies'], self['is_elite'], \
               self['created_datetime']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO xcar_thread(`tid`, `title`, `forum`, `num_views`, `num_replies`, `is_elite`, '
                     '`created_datetime`) VALUES(?, ?, ?, ?, ? ,?, ?) ON CONFLICT(`tid`) DO UPDATE SET '
                     '`title`=?, `forum`=?, `num_views`=?, `num_replies`=?, `is_elite`=?, `created_datetime`=?',
                     (*self.field_values, self.field_values[1], self.field_values[2], self.field_values[3],
                      self.field_values[4], self.field_values[5], self.field_values[6]))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM xcar_thread WHERE `tid`=?', (self['tid'],))
        conn.commit()


class XcarPost(scrapy.Item):
    """ 帖子楼层（评论） """

    pid = scrapy.Field()
    author = scrapy.Field()
    content = scrapy.Field()
    publish_datetime = scrapy.Field()
    created_datetime = scrapy.Field()
    is_flag = scrapy.Field()
    thread = scrapy.Field()

    @property
    def field_values(self):
        return self['pid'], self['author'], self['content'], self['publish_datetime'], self['created_datetime'], \
               self['is_flag'], self['thread']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO xcar_post(`pid`, `author`, `content`, `publish_datetime`, `created_datetime`, '
                     '`is_flag`, `thread`) VALUES(?, ?, ?, ?, ?, ?, ?) ON CONFLICT(`pid`) DO UPDATE SET '
                     '`author`=?, `content`=?, `publish_datetime`=?, `created_datetime`=?, `is_flag`=?, `thread`=?',
                     (*self.field_values, self.field_values[1], self.field_values[2], self.field_values[3],
                      self.field_values[4], self.field_values[5], self.field_values[6]))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM xcar_post WHERE `pid`=?', (self['pid'],))
        conn.commit()


class XcarUser(scrapy.Item):
    """ 会员 """

    uid = scrapy.Field()
    name = scrapy.Field()
    gender = scrapy.Field()
    avatar_url = scrapy.Field()
    register_date = scrapy.Field()
    location = scrapy.Field()
    coin = scrapy.Field()
    rank = scrapy.Field()
    num_follows = scrapy.Field()  # 关注数
    num_fans = scrapy.Field()  # 粉丝数
    num_posts = scrapy.Field()  # 发帖数
    created_datetime = scrapy.Field()
    manage = scrapy.Field()

    @property
    def field_values(self):
        return self['uid'], self['name'], self['gender'], self['avatar_url'], self['register_date'],\
               self['location'], self['coin'], self['rank'], self['num_follows'], self['num_fans'],\
               self['num_posts'], self['created_datetime'], self['manage']

    def upsert(self, conn: Connection):
        conn.execute('INSERT INTO xcar_user(`uid`, `name`, `gender`, `avatar_url`, `register_date`, `location`, '
                     '`coin`, `rank`, `num_follows`, `num_fans`, `num_posts`, `created_datetime`, `manage`) '
                     'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(`uid`) DO UPDATE SET '
                     '`name`=?, `gender`=?, `avatar_url`=?, `register_date`=?, `location`=?, `coin`=?, `rank`=?, '
                     '`num_follows`=?, `num_fans`=?, `num_posts`=?, `created_datetime`=?, `manage`=?',
                     (*self.field_values, self.field_values[1], self.field_values[2], self.field_values[3],
                      self.field_values[4], self.field_values[5], self.field_values[6], self.field_values[7],
                      self.field_values[8], self.field_values[9], self.field_values[10], self.field_values[11],
                      self.field_values[12]))
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM xcar_user WHERE `uid`=?', (self['uid'],))
        conn.commit()
