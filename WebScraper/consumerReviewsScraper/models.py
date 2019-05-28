import datetime
from pony.orm import Database, PrimaryKey, Required, Optional, Set, ObjectNotFound
from .settings import DB_PROVIDER, DB_NAME


force_created_table = True

db = Database()


class XcarForumEntity(db.Entity):
    """ Pony ORM model of the xcar_forum table """

    _table_ = 'xcar_forum'
    id = PrimaryKey(int, auto=False, column='fid')  # use forum ID on website as primary key
    name = Required(str)
    created_datetime = Required(datetime.datetime)
    threads = Set('XcarThreadEntity')
    managers = Set('XcarUserEntity')

    @classmethod
    def upsert(cls, **kwargs):
        try:
            instance = cls[tuple(kwargs[pk_attr.name] for pk_attr in cls._pk_attrs_)]
        except ObjectNotFound:
            return cls(**kwargs)
        else:
            instance.set(**kwargs)
            return instance


class XcarThreadEntity(db.Entity):
    """ Pony ORM model of the xcar_thread table """

    _table_ = 'xcar_thread'
    id = PrimaryKey(int, auto=False, column='tid')  # use thread ID on website as primary key
    title = Required(str)
    forum = Required(XcarForumEntity)
    num_views = Required(int)
    num_replies = Required(int)
    is_elite = Required(int)
    created_datetime = Required(datetime.datetime)
    posts = Set('XcarPostEntity')

    @classmethod
    def upsert(cls, **kwargs):
        try:
            instance = cls[tuple(kwargs[pk_attr.name] for pk_attr in cls._pk_attrs_)]
        except ObjectNotFound:
            return cls(**kwargs)
        else:
            instance.set(**kwargs)
            return instance


class XcarPostEntity(db.Entity):
    """ Pony ORM model of the xcar_review table """

    _table_ = 'xcar_post'
    id = PrimaryKey(int, auto=False)  # use post ID on website as primary key
    author = Required('XcarUserEntity')
    content = Optional(str)
    publish_datetime = Required(datetime.datetime)
    created_datetime = Required(datetime.datetime)
    is_flag = Required(int)
    thread = Required(XcarThreadEntity)

    @classmethod
    def upsert(cls, **kwargs):
        try:
            instance = cls[tuple(kwargs[pk_attr.name] for pk_attr in cls._pk_attrs_)]
        except ObjectNotFound:
            return cls(**kwargs)
        else:
            instance.set(**kwargs)
            return instance


class XcarUserEntity(db.Entity):
    """ Pony ORM model of the xcar_user table """

    _table_ = 'xcar_user'
    id = PrimaryKey(int, auto=False, column='uid')  # use user ID on website as primary key
    name = Optional(str)
    gender = Optional(str)
    avatar_url = Optional(str)
    register_date = Optional(datetime.date)
    location = Optional(str)
    coin = Optional(int)
    rank = Optional(str)
    num_follows = Optional(int)
    num_fans = Optional(int)
    num_posts = Optional(int)
    created_datetime = Required(datetime.datetime)
    manage = Optional(XcarForumEntity)
    posts = Set(XcarPostEntity)

    @classmethod
    def upsert(cls, **kwargs):
        try:
            instance = cls[tuple(kwargs[pk_attr.name] for pk_attr in cls._pk_attrs_)]
        except ObjectNotFound:
            return cls(**kwargs)
        else:
            instance.set(**kwargs)
            return instance


if force_created_table:
    print('Force to create table.')
    db.bind(provider=DB_PROVIDER, filename=DB_NAME, create_db=False)
    db.generate_mapping(create_tables=True)
