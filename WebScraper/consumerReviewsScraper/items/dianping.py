import scrapy
from typing import Union, Dict
from .base import DatabaseItem


class DPBadge(DatabaseItem):
    """ 徽章 """
    table_name = 'dp_badge'
    id = scrapy.Field()
    name = scrapy.Field()
    pic_url = scrapy.Field()
    notes = scrapy.Field()  # 说明
    acquired_condition = scrapy.Field()  # 获得条件
    member_id = scrapy.Field()
    acquired_date = scrapy.Field()  # 获得日期
    scraped_datetime = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']


class DPCommunity(DatabaseItem):
    """ 社区部落 """
    table_name = 'dp_community'
    city = scrapy.Field()
    name = scrapy.Field()
    num_topics = scrapy.Field()
    num_members = scrapy.Field()
    about = scrapy.Field()  # 简介
    created_datetime = scrapy.Field()  # 部落创建日期
    managers = scrapy.Field()  # 部落管理员
    scraped_datetime = scrapy.Field()  # 爬取日期

    def get_id(self) -> Union[int, Dict[str, str]]:
        return {'city': self['city'], 'name': self['name']}


class DPMember(DatabaseItem):
    """ 会员 """
    table_name = 'dp_member'
    id = scrapy.Field()
    name = scrapy.Field()
    is_vip = scrapy.Field()
    gender = scrapy.Field()
    location = scrapy.Field()
    experience = scrapy.Field()  # 贡献值
    rank = scrapy.Field()  # 社区等级
    register_date = scrapy.Field()  # 注册日期
    about = scrapy.Field()  # 简介
    num_follows = scrapy.Field()  # 关注数
    num_fans = scrapy.Field()  # 粉丝数
    num_interactive = scrapy.Field()  # 互动数
    badges = scrapy.Field()  # 获得的徽章
    avatar_url = scrapy.Field()  # 头像URL
    tags = scrapy.Field()  # 用户标签
    relationship = scrapy.Field()  # 恋爱状况
    birthday = scrapy.Field()  # 生日
    zodiac = scrapy.Field()  # 星座
    body_type = scrapy.Field()  # 体型
    scraped_datetime = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']


class DPReview(DatabaseItem):
    """ 评论（包含餐馆、帖子等） """
    table_name = 'dp_review'
    id = scrapy.Field()
    author_id = scrapy.Field()
    publish_datetime = scrapy.Field()
    content = scrapy.Field()
    num_likes = scrapy.Field()  # 点赞数
    reply_to = scrapy.Field()  # 对哪条评论的回复
    topic_id = scrapy.Field()  # 对哪条帖子的评论
    scraped_datetime = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']


class DPTopic(DatabaseItem):
    """ 社区帖子 """
    table_name = 'dp_topic'
    id = scrapy.Field()
    author_id = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    publish_datetime = scrapy.Field()
    num_views = scrapy.Field()  # 浏览数
    num_likes = scrapy.Field()  # 点赞数
    num_replies = scrapy.Field()  # 回帖数
    mark_elite_datetime = scrapy.Field()  # 设为精华帖的日期时间
    mark_elite_by = scrapy.Field()  # 被谁设为精华贴
    edit_datetime = scrapy.Field()  # 重新编辑的日期
    scraped_datetime = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return self['id']


class DPBonus(DatabaseItem):
    """ 社区帖子加分 """
    table_name = 'dp_bonus'
    topic_id = scrapy.Field()  # 帖子id
    member_id = scrapy.Field()  # 加分人
    points = scrapy.Field()  # 分数
    reason = scrapy.Field()  # 加分理由
    scraped_datetime = scrapy.Field()

    def get_id(self) -> Union[int, Dict[str, str]]:
        return {'topic_id': self['topic_id'], 'member_id': self['member_id']}
