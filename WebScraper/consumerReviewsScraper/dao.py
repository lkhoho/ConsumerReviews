import sqlite3
from typing import Union, Dict
from .items.base import DatabaseItem


class BaseDatabaseEntityDao(object):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(self, obj: DatabaseItem):
        pass

    def update(self, obj: DatabaseItem):
        pass

    def get(self, pk: Union[int, Dict[str, str]]):
        pass

    def delete(self, pk: Union[int, Dict[str, str]]):
        pass

    def upsert(self, obj: DatabaseItem):
        pk = obj.get_id()
        existing = self.get(pk)
        if existing is not None:
            self.update(obj)
        else:
            self.create(obj)


class DPBadgeDao(BaseDatabaseEntityDao):
    def __init__(self, conn):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_badge values (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                          (None, obj['scraped_datetime'], obj['id'], obj['name'], obj['pic_url'], obj['notes'],
                           obj['acquired_condition'], obj['acquired_date'], obj['member_id']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_badge set name=?, pic_url=?, notes=?, acquired_condition=?, member_id=?, '
                          'acquired_date=?, scraped_datetime=? where id=?',
                          (obj['name'], obj['pic_url'], obj['notes'], obj['acquired_condition'], obj['member_id'],
                           obj['acquired_date'], obj['scraped_datetime'], obj['id']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_badge where id=?', (pk,)).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_badge where id=?', (pk,))
        self.conn.commit()


class DPCommunityDao(BaseDatabaseEntityDao):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_community values (?, ?, ?, ?, ?, ?, ?)',
                          (obj['created_datetime'], obj['name'], obj['num_topics'], obj['num_members'],
                           obj['about'], obj['scraped_datetime'], obj['city']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_community set num_topics=?, num_members=?, about=?, created_datetime=?, '
                          'scraped_datetime=? where city=? and name=?',
                          (obj['num_topics'], obj['num_members'], obj['about'], obj['created_datetime'],
                           obj['scraped_datetime'], obj['city'], obj['name']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_community where city=? and name=?',
                                 (pk['city'], pk['name'])).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_community where city=? and name=?',
                          (pk['city'], pk['name']))
        self.conn.commit()


class DPMemberDao(BaseDatabaseEntityDao):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_member values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                          (None, obj['scraped_datetime'], obj['id'], obj['name'], obj['is_vip'],
                           obj['gender'], obj['location'], obj['experience'], obj['rank'], obj['register_date'],
                           obj['about'], obj['num_follows'], obj['num_fans'], obj['num_interactive'],
                           len(obj['badges']), obj['avatar_url'], obj['tags'], obj['relationship'], obj['birthday'],
                           obj['zodiac'], obj['body_type']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_member set name=?, is_vip=?, gender=?, location=?, experience=?, `rank`=?, '
                          'register_date=?, about=?, num_follows=?, num_fans=?, num_interactive=?, num_badges=?, '
                          'avatar_url=?, tags=?, relationship=?, birthday=?, zodiac=?, body_type=?, scraped_datetime=? '
                          'where id=?',
                          (obj['name'], obj['is_vip'], obj['gender'], obj['location'], obj['experience'], obj['rank'],
                           obj['register_date'], obj['about'], obj['num_follows'], obj['num_fans'],
                           obj['num_interactive'], len(obj['badges']), obj['avatar_url'], obj['tags'],
                           obj['relationship'], obj['birthday'], obj['zodiac'], obj['body_type'],
                           obj['scraped_datetime'], obj['id']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_member where id=?', (pk,)).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_member where id=?', (pk,))
        self.conn.commit()


class DPReviewDao(BaseDatabaseEntityDao):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_review values (?, ?, ?, ?, ?, ?, ?, ?)',
                          (None, obj['scraped_datetime'], obj['author_id'], obj['publish_datetime'],
                           obj['content'], obj['num_likes'], obj['reply_to'], obj['id']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_review set author_id=?, publish_datetime=?, content=?, num_likes=?, '
                          'reply_to_review_id=?, scraped_datetime=? where id=?',
                          (obj['author_id'], obj['publish_datetime'], obj['content'], obj['num_likes'], obj['reply_to'],
                           obj['scraped_datetime'], obj['id']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_review where id=?', (pk,)).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_review where id=?', (pk,))
        self.conn.commit()


class DPTopicDao(BaseDatabaseEntityDao):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_topic values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                          (None, obj['scraped_datetime'], obj['id'], obj['title'], obj['content'],
                           obj['publish_datetime'], obj['num_views'], obj['num_likes'], obj['mark_elite_datetime'],
                           obj['mark_elite_by'], obj['author_id'], obj['num_replies'], obj['edit_datetime']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_topic set author_id=?, title=?, content=?, publish_datetime=?, num_views=?, '
                          'num_likes=?, num_replies=?, mark_elite_datetime=?, mark_elite_by=?, edit_datetime=?, '
                          'scraped_datetime=? where id=?',
                          (obj['author_id'], obj['title'], obj['content'], obj['publish_datetime'], obj['num_views'],
                           obj['num_likes'], obj['num_replies'], obj['mark_elite_datetime'], obj['mark_elite_by'],
                           obj['edit_datetime'], obj['scraped_datetime'], obj['id']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_topic where id=?', (pk,)).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_topic where id=?', (pk,))
        self.conn.commit()


class DPBonusDao(BaseDatabaseEntityDao):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    def create(self, obj: DatabaseItem):
        self.conn.execute('insert into dp_bonus values (?, ?, ?, ?, ?, ?)',
                          (None, obj['topic_id'], obj['member_id'], obj['points'], obj['reason'],
                           obj['scraped_datetime']))
        self.conn.commit()

    def update(self, obj: DatabaseItem):
        self.conn.execute('update dp_bonus set points=?, reason=?, scraped_datetime=? where topic_id=? and member_id=?',
                          (obj['points'], obj['reason'], obj['scraped_datetime'], obj['topic_id'], obj['member_id']))
        self.conn.commit()

    def get(self, pk: Union[int, Dict[str, str]]):
        return self.conn.execute('select * from dp_bonus where topic_id=? and member_id=?',
                                 (pk['topic_id'], pk['member_id'],)).fetchone()

    def delete(self, pk: Union[int, Dict[str, str]]):
        self.conn.execute('delete from dp_bonus where topic_id=? and member_id=?',
                          (pk['topic_id'], pk['member_id'],))
        self.conn.commit()
