# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from WebScraper.consumerReviewsScraper.models.main \
    import db_connect, create_bizrate_tables, create_expedia_tables, create_steam_tables, create_url_status_table
from WebScraper.consumerReviewsScraper.models.bizrate import BizrateStore, BizrateReview
from WebScraper.consumerReviewsScraper.models.expedia import ExpediaHotel, ExpediaReview
from WebScraper.consumerReviewsScraper.models.steam import SteamUserProfile
from WebScraper.consumerReviewsScraper.models.university import University, UniversityRanking
from WebScraper.consumerReviewsScraper.items.bizrate import BizrateStoreItem, BizrateReviewItem
from WebScraper.consumerReviewsScraper.items.expedia import ExpediaHotelItem, ExpediaReviewItem
from WebScraper.consumerReviewsScraper.items.steam import SteamUserProfileItem
from WebScraper.consumerReviewsScraper.items.university import UniversityRankingItem
# from .items.kbb import KBBReviewItem
# from .items.edmunds import EdmundsReviewItem
# from .items.dianping import DPBadge, DPBonus, DPCommunity, DPMember, DPReview, DPTopic


class SqlItemPipeline(object):
    """
    Save items to relational database (through Sqlalchemy).
    """

    def __init__(self):
        self.engine = None
        self.session = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def open_spider(self, spider):
        self.engine = db_connect()
        create_bizrate_tables(self.engine)
        create_expedia_tables(self.engine)
        create_steam_tables(self.engine)
        create_url_status_table(self.engine)
        self.logger.info('Connected to database %s' % self.engine.engine.url.database)
        self.session = sessionmaker(bind=self.engine)()
    
    def close_spider(self, spider):
        self.session.close_all()
        self.logger.info('Disconnected to database %s ' % self.engine.engine.url.database)

    def process_item(self, item, spider):
        try:
            model = None
            if isinstance(item, BizrateStoreItem):
                model = self.session.query(BizrateStore) \
                                     .filter(BizrateStore.store_id == item['store_id']) \
                                     .one_or_none()
                if model is None:
                    model = BizrateStore(**item)
                    self.session.add(model)
                else:
                    model.created_datetime = datetime.utcnow()
            elif isinstance(item, BizrateReviewItem):
                model = self.session.query(BizrateReview) \
                                     .filter(BizrateReview.review_id == item['review_id']) \
                                     .one_or_none()
                if model is None:
                    model = BizrateReview(**item)
                    self.session.add(model)
                else:
                    model.created_datetime = datetime.utcnow()
            elif isinstance(item, ExpediaHotelItem):
                model = self.session.query(ExpediaHotel) \
                                     .filter(ExpediaHotel.hotel_id == item['hotel_id']) \
                                     .one_or_none()
                if model is None:
                    model = ExpediaHotel(**item)
                    self.session.add(model)
                else:
                    model.created_datetime = datetime.utcnow()
            elif isinstance(item, ExpediaReviewItem):
                model = self.session.query(ExpediaReview) \
                                     .filter(ExpediaReview.review_id == item['review_id']) \
                                     .one_or_none()
                if model is None:
                    model = ExpediaReview(**item)
                    self.session.add(model)
                else:
                    model.created_datetime = datetime.utcnow()
            elif isinstance(item, SteamUserProfileItem):
                if item['user_id'] is not None:
                    model = self.session.query(SteamUserProfile)\
                                .filter(SteamUserProfile.user_id == item['user_id'])\
                                .one_or_none()
                elif item['profile_id'] is not None:
                    model = self.session.query(SteamUserProfile)\
                                .filter(SteamUserProfile.profile_id == item['profile_id'])\
                                .one_or_none()

                if model is None:
                    model = SteamUserProfile(**item)
                    self.session.add(model)
                else:
                    model.created_datetime = datetime.utcnow()
            elif isinstance(item, UniversityRankingItem):
                model = self.session.query(University)\
                            .filter(University.name == item['name'])\
                            .one_or_none()
                if model is None:
                    uni = University(name=item['name'],
                                     country=item['country'],
                                     region=item['region'],
                                     created_datetime=item['created_datetime'])
                    self.session.add(uni)
                    self.session.commit()
                    university_pid = uni.pid
                else:
                    university_pid = model.pid
                    model.created_datetime = datetime.utcnow()
                model = self.session.query(UniversityRanking)\
                            .filter(and_(UniversityRanking.university_id == university_pid,
                                         UniversityRanking.subject == item['subject'],
                                         UniversityRanking.year == item['year']))\
                            .one_or_none()
                if model is None:
                    rank = UniversityRanking(subject=item['subject'],
                                             year=item['year'],
                                             ranking=item['ranking'],
                                             score=item['score'],
                                             university_id=university_pid,
                                             created_datetime=item['created_datetime'])
                    self.session.add(rank)
                else:
                    model.created_datetime = datetime.utcnow()
            else:
                model = None

            self.session.commit()
        except SQLAlchemyError as err:
            self.session.rollback()
            self.logger.warn('Failed to save item to database. Item={}'.format(item))
            self.logger.error(str(err))
        
        return item


# class SaveExpediaItemsPipeline(object):
#     """ Save hotel reviews of expedia.com to Sqlite 3. """

#     def __init__(self):
#         self.conn = None

#     def open_spider(self, spider):
#         self.conn = sqlite3.connect(DB_NAME)
#         spider.logger.info('Connected to Sqlite file %s' % DB_NAME)
    
#     def close_spider(self, spider):
#         self.conn.close()
#         spider.logger.info('Disconnected to Sqlite file %s ' % DB_NAME)

#     def process_item(self, item, spider):
#         item.upsert(self.conn)
#         return item


# class SaveHyoubanItemsPipeline(object):
#     """ Save items of hyouban.com to Sqlite 3. """

#     def __init__(self):
#         self.conn = None

#     def open_spider(self, spider):
#         self.conn = sqlite3.connect(DB_NAME)
#         spider.logger.info('Connected to Sqlite file %s' % DB_NAME)
    
#     def close_spider(self, spider):
#         self.conn.close()
#         spider.logger.info('Disconnected to Sqlite file %s ' % DB_NAME)

#     def process_item(self, item, spider):
#         item.upsert(self.conn)
#         return item


# class SaveXcarItemsPipeline(object):
#     """ Save items of xcar.com to Sqlite 3. """

#     def __init__(self):
#         self.conn = None

#     def open_spider(self, spider):
#         self.conn = sqlite3.connect(DB_NAME)
#         spider.logger.info('Connected to Sqlite file %s' % DB_NAME)

#     def close_spider(self, spider):
#         self.conn.close()
#         spider.logger.info('Disconnected to Sqlite file %s ' % DB_NAME)

#     def process_item(self, item, spider):
#         item.upsert(self.conn)
#         return item


# class SaveDianpingItemsPipeline(object):
#     def __init__(self):
#         self.db_name = '/Users/keliu/consumer_reviews_working/consumer_reviews.db'
#         self.conn = None
#         self.db_entity_dao = None

#     def open_spider(self, spider):
#         self.conn = sqlite3.connect(self.db_name)
#         spider.logger.info('Connected to Sqlite file %s' % self.db_name)

#     def close_spider(self, spider):
#         self.conn.close()
#         spider.logger.info('Disconnected to Sqlite file %s ' % self.db_name)

#     def process_item(self, item, spider):
#         item.upsert(self.conn)
#         return item


# class SaveToMongoDb(object):
#     """
#     Persist items into MongoDB as documents.
#     """
#
#     def __init__(self):
#         self.conn_str = 'mongodb://lkhoho:56b2AmDI@lkhoho-cluster-shard-00-00-d12bg.mongodb.net:27017,lkhoho-cluster-shard-00-01-d12bg.mongodb.net:27017,lkhoho-cluster-shard-00-02-d12bg.mongodb.net:27017/admin?replicaSet=lkhoho-cluster-shard-0&ssl=true'
#         self.client = None
#         self.dbName = 'ConsumerReviews'
#         self.collectionName = None
#         self.collection = None  # document collection
#         self.nCurrentDocuments = 0  # current document count
#
#     def open_spider(self, spider):
#         position = spider.reviewSource.rfind('.')
#         self.collectionName = spider.reviewSource[:position]
#         self.client = mongo.MongoClient()
#         spider.logger.info('Open connection to database [{}] in MongoDB.'.format(self.dbName))
#         self.collection = self.client[self.dbName][self.collectionName]
#         self.nCurrentDocuments = self.collection.count()
#         assert self.nCurrentDocuments == self.nCurrentDocuments, 'Error: queried documents count != documents count'
#         spider.logger.info('Database {}, collection {} has {} documents for now.'.format(
#             self.dbName, self.collectionName, self.nCurrentDocuments))
#
#     def close_spider(self, spider):
#         self.client.close()
#         nItems = self.collection.count()
#         spider.logger.info('Close connection to database [{}] in MongoDB. {} new items are persisted.'.format(
#             self.dbName, nItems - self.nCurrentDocuments))
#
#     def process_item(self, item, spider):
#         if not self.collection.find_one({'id': item['id']}):
#             result = self.collection.insert_one(item)
#             spider.logger.info('Item inserted as {}.'.format(result.inserted_id))
#             return item
#         else:
#             raise DropItem("Item is already in MongoDB. Will be dropped.")
