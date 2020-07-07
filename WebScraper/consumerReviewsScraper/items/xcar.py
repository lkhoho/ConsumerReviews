import scrapy


class XcarForum(scrapy.Item):
    """ 论坛 """

    fid = scrapy.Field()
    name = scrapy.Field()
    created_datetime = scrapy.Field()
    threads = scrapy.Field()
    managers = scrapy.Field()


class XcarThread(scrapy.Item):
    """ 帖子 """

    tid = scrapy.Field()
    title = scrapy.Field()
    forum = scrapy.Field()
    num_views = scrapy.Field()
    num_replies = scrapy.Field()
    is_elite = scrapy.Field()
    created_datetime = scrapy.Field()


class XcarPost(scrapy.Item):
    """ 帖子楼层（评论） """

    pid = scrapy.Field()
    author = scrapy.Field()
    content = scrapy.Field()
    publish_datetime = scrapy.Field()
    created_datetime = scrapy.Field()
    is_flag = scrapy.Field()
    thread = scrapy.Field()


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
