import scrapy
from typing import Union, Dict
from sqlite3 import Connection
from ..models import *


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

    id = scrapy.Field()
    title = scrapy.Field()
    forum = scrapy.Field()
    num_views = scrapy.Field()
    num_replies = scrapy.Field()
    is_elite = scrapy.Field()
    created_datetime = scrapy.Field()

    def upsert(self, conn: Connection):
        field_values = [self['id'], self['title'], self['forum'], self['num_views'], self['num_replies'],
                        self['is_elite'], self['created_datetime']]
        conn.execute('INSERT INTO xcar_thread VALUES(?, ?, ?, ?, ? ,?, ?) ON CONFLICT(`tid`) DO UPDATE SET '
                     '`tid`=?, `title`=?, `forum`=?, `num_views`=?, `num_replies`=?, `is_elite`=?, '
                     '`created_datetime`=?', *field_values, *field_values)
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM ? WHERE `tid`=?', self._table_, self['id'])
        conn.commit()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']

    def get_ORM_entity(self):
        return XcarThreadEntity(
            id=self.get_id(),
            title=self['title'],
            forum=self['forum'],
            num_views=self['num_views'],
            num_replies=self['num_replies'],
            is_elite=self['is_elite'],
            created_datetime=self['created_datetime'])


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

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']

    def get_ORM_entity(self):
        return XcarPostEntity(
            id=self.get_id(),
            author=self['author'],
            content=self['content'],
            publish_datetime=self['publish_datetime'],
            created_datetime=self['created_datetime'],
            is_flag=self['is_flag'],
            thread=self['thread'])


class XcarUser(scrapy.Item):
    """ 会员 """

    id = scrapy.Field()
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

    _table_ = 'xcar_user'

    def upsert(self, conn: Connection):
        field_values = [self['id'], self['name'], self['gender'], self['avatar_url'], self['register_date'],
                        self['location'], self['coin'], self['rank'], self['num_follows'], self['num_fans'],
                        self['num_posts'], self['created_datetime'], self['manage']]
        conn.execute('INSERT INTO ? VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(`uid`) UPDATE SET '
                     '`uid`=?, `name`=?, `gender`=?, `avatar_url`=?, `register_date`=?, `location`=?, `coin`=?, '
                     '`rank`=?, `num_follows`=?, `num_fans`=?, `num_posts`=?, `created_datetime`=?, `manage`=?',
                     self._table_, *field_values, *field_values)
        conn.commit()

    def delete(self, conn: Connection):
        conn.execute('DELETE FROM ? WHERE `uid`=?', self._table_, self['id'])
        conn.commit()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']

    def get_ORM_entity(self):
        return XcarUserEntity(
            id=self.get_id(),
            name=self['name'],
            gender=self['gender'],
            avatar_url=self['avatar_url'],
            register_date=self['register_date'],
            location=self['location'],
            coin=self['coin'],
            rank=self['rank'],
            num_follows=self['num_follows'],
            num_fans=self['num_fans'],
            num_posts=self['num_posts'],
            created_datetime=self['created_datetime'],
            manage=self['manage'])
