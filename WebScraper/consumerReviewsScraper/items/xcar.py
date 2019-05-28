import scrapy
from typing import Union, Dict
from ..models import *
from .base import DatabaseItem


class XcarForum(DatabaseItem):
    """ 论坛 """

    id = scrapy.Field()
    name = scrapy.Field()
    created_datetime = scrapy.Field()
    threads = scrapy.Field()
    managers = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']

    def get_ORM_entity(self):
        return XcarForumEntity(
            id=self.get_id(),
            name=self['name'],
            created_datetime=self['created_datetime'])


class XcarThread(DatabaseItem):
    """ 帖子 """

    id = scrapy.Field()
    title = scrapy.Field()
    forum = scrapy.Field()
    num_views = scrapy.Field()
    num_replies = scrapy.Field()
    is_elite = scrapy.Field()
    created_datetime = scrapy.Field()

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


class XcarPost(DatabaseItem):
    """ 帖子楼层（评论） """

    id = scrapy.Field()
    author = scrapy.Field()
    content = scrapy.Field()
    publish_datetime = scrapy.Field()
    created_datetime = scrapy.Field()
    is_flag = scrapy.Field()
    thread = scrapy.Field()

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


class XcarUser(DatabaseItem):
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
